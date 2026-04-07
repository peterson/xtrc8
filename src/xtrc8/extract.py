#!/usr/bin/env python3
"""
extract — Resolve t.co links from tweets and auto-clip papers, gists, and repos.

Library usage:
    from xtrc8.extract import run_extract
    clipped = run_extract(db_path=Path(".tweets-cache.db"))

CLI usage:
    xtrc8 extract [--dry-run] [--skip-replies] [--db PATH]
"""

import argparse
import asyncio
import json
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import httpx

from .tweets import (
    get_db,
    _create_browser_context,
)
from .clip import clip_web, clip_arxiv, clip_pdf_url
from .util import slugify


def _get_extract_db(db: sqlite3.Connection) -> sqlite3.Connection:
    """Ensure extract-specific tables exist."""
    db.execute("""
        CREATE TABLE IF NOT EXISTS extracted_links (
            url TEXT PRIMARY KEY,
            tweet_id TEXT,
            resolved_url TEXT,
            link_type TEXT,
            clipped INTEGER DEFAULT 0,
            clipped_path TEXT
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS reply_scraped (
            tweet_id TEXT PRIMARY KEY,
            scraped_at TEXT,
            reply_text TEXT,
            reply_urls TEXT
        )
    """)
    db.commit()
    return db


def resolve_tco(url: str, client: httpx.Client) -> str | None:
    try:
        resp = client.head(url, follow_redirects=False)
        return resp.headers.get("location")
    except Exception:
        return None


def classify_url(url: str) -> str:
    if not url:
        return "unknown"
    if "arxiv.org" in url:
        return "arxiv"
    if "gist.github.com" in url:
        return "gist"
    if url.endswith(".pdf"):
        return "pdf"
    if "github.com" in url:
        parts = url.rstrip("/").split("/")
        github_idx = next((i for i, p in enumerate(parts) if "github.com" in p), -1)
        if github_idx >= 0 and len(parts) > github_idx + 2:
            return "github"
    return "other"


def clip_gist(url: str, dest_dir: Path) -> Path | None:
    dest_dir.mkdir(parents=True, exist_ok=True)

    parts = url.rstrip("/").split("/")
    gist_id = parts[-1]

    try:
        with httpx.Client(timeout=30) as client:
            resp = client.get(f"https://api.github.com/gists/{gist_id}")
            resp.raise_for_status()
            data = resp.json()
    except Exception:
        return None

    description = data.get("description", "")
    owner = data.get("owner", {}).get("login", "unknown")
    files = data.get("files", {})

    content_parts = []
    for fname, fdata in files.items():
        content_parts.append(f"## {fname}\n\n{fdata.get('content', '')}")

    date = datetime.now().strftime("%Y-%m-%d")
    slug = re.sub(r'[^a-z0-9]+', '-', description.lower()[:50]).strip('-') or gist_id[:12]
    filename = f"{date}-gist-{owner}-{slug}.md"
    path = dest_dir / filename

    lines = [
        "---",
        f"title: {description}",
        f"url: {url}",
        f"author: {owner}",
        f"date: {date}",
        "source: gist.github.com",
        "---",
        "",
        *content_parts,
        "",
    ]
    path.write_text("\n".join(lines))
    return path


def _fetch_author_replies(db, auth_db_path: Path | None = None) -> int:
    # Exclude purged tweets — they are deleted from the consumer's perspective
    # and must not have their reply chains re-scraped.
    rows = db.execute("""
        SELECT t.id, t.url, t.author_handle
        FROM tweets t
        WHERE t.ingested = 1
        AND t.purged IS NULL
        AND t.id NOT IN (SELECT tweet_id FROM reply_scraped)
    """).fetchall()

    if not rows:
        return 0

    # Grab db_path before crossing thread boundary — SQLite connections
    # cannot be shared across threads.
    db_path = Path(db.execute("PRAGMA database_list").fetchone()[2])

    print(f"\nScraping author replies for {len(rows)} tweets...")
    coro = _scrape_replies(db_path, rows, auth_db_path)
    # If already inside an event loop (e.g. Textual worker), await directly
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            new_urls = pool.submit(asyncio.run, coro).result()
    else:
        new_urls = asyncio.run(coro)
    return new_urls


async def _scrape_replies(db_path, rows, auth_db_path: Path | None = None) -> int:
    db = get_db(db_path)
    pw, browser, context = await _create_browser_context(auth_db_path)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_urls = 0

    for i, r in enumerate(rows):
        tweet_url = r["url"]
        author = r["author_handle"]
        tweet_id = r["id"]

        if (i + 1) % 10 == 0:
            print(f"  {i + 1}/{len(rows)}...")

        try:
            page = await context.new_page()
            replies_data = []

            async def intercept(response, _data=replies_data):
                url = response.url
                if "TweetDetail" in url and "graphql" in url.lower():
                    try:
                        d = await response.json()
                        _data.append(d)
                    except Exception:
                        pass

            page.on("response", intercept)
            await page.goto(tweet_url, wait_until="domcontentloaded", timeout=20000)
            try:
                await page.wait_for_selector('[data-testid="tweet"]', timeout=8000)
            except Exception:
                pass
            await asyncio.sleep(2)
            await page.close()

            reply_urls_found = []

            for data in replies_data:
                _extract_author_reply(data, author, tweet_id, reply_urls_found)

            for url_found in reply_urls_found:
                resolved = None
                link_type = "unknown"
                if "t.co" in url_found:
                    try:
                        with httpx.Client(timeout=10, follow_redirects=False) as client:
                            resp = client.head(url_found)
                            resolved = resp.headers.get("location", "")
                            link_type = classify_url(resolved)
                    except Exception:
                        pass
                else:
                    resolved = url_found
                    link_type = classify_url(url_found)

                if resolved:
                    db.execute("""
                        INSERT OR IGNORE INTO extracted_links
                        (url, tweet_id, resolved_url, link_type)
                        VALUES (?, ?, ?, ?)
                    """, (url_found, tweet_id, resolved, link_type))
                    if link_type in ("arxiv", "pdf", "gist", "github"):
                        new_urls += 1

            db.execute("""
                INSERT OR REPLACE INTO reply_scraped
                (tweet_id, scraped_at, reply_text, reply_urls)
                VALUES (?, ?, ?, ?)
            """, (tweet_id, now, " | ".join(reply_urls_found), json.dumps(reply_urls_found)))
            db.commit()

        except Exception:
            db.execute("""
                INSERT OR REPLACE INTO reply_scraped
                (tweet_id, scraped_at, reply_text, reply_urls)
                VALUES (?, ?, ?, ?)
            """, (tweet_id, now, "", "[]"))
            db.commit()

    await browser.close()
    await pw.stop()
    db.close()

    print(f"  Found {new_urls} new clippable URLs from replies.")
    return new_urls


def _extract_author_reply(data, author, parent_tweet_id, out_urls):
    if isinstance(data, dict):
        entries = []
        for key, val in data.items():
            if key == "entries" and isinstance(val, list):
                entries.extend(val)
            elif isinstance(val, (dict, list)):
                _extract_author_reply(val, author, parent_tweet_id, out_urls)

        for entry in entries:
            try:
                content = entry.get("content", {})
                items = content.get("items", [])
                for item in items:
                    ic = item.get("item", {}).get("itemContent", {})
                    result = ic.get("tweet_results", {}).get("result", {})
                    if result.get("__typename") == "TweetWithVisibilityResults":
                        result = result.get("tweet", result)
                    user = result.get("core", {}).get("user_results", {}).get("result", {})
                    screen_name = (
                        user.get("core", {}).get("screen_name") or
                        user.get("legacy", {}).get("screen_name")
                    )
                    legacy = result.get("legacy", {})
                    reply_to = legacy.get("in_reply_to_status_id_str", "")

                    if screen_name == author and reply_to == parent_tweet_id:
                        text = legacy.get("full_text", "")
                        urls = re.findall(r'https?://\S+', text)
                        out_urls.extend(u.rstrip(".,;:)") for u in urls)
                        return
            except Exception:
                continue

    elif isinstance(data, list):
        for item in data:
            _extract_author_reply(item, author, parent_tweet_id, out_urls)


def run_extract(db_path: Path, dry_run: bool = False, skip_replies: bool = False,
                output_dir: Path | None = None, auth_db_path: Path | None = None) -> int:
    """Run the full extraction pipeline. Returns number of items clipped."""
    db = get_db(db_path)
    _get_extract_db(db)

    if not dry_run and not skip_replies:
        _fetch_author_replies(db, auth_db_path)

    # Get all imported (and not-purged) tweets with t.co links. Purged tweets
    # are deleted from the consumer's perspective; their links must not be
    # re-resolved or re-clipped.
    rows = db.execute("""
        SELECT id, full_text, author_handle
        FROM tweets WHERE ingested = 1 AND purged IS NULL
    """).fetchall()

    to_resolve = []
    for r in rows:
        urls = re.findall(r'https://t\.co/\S+', r["full_text"] or "")
        for u in urls:
            u = u.rstrip(".,;:)")
            existing = db.execute(
                "SELECT url FROM extracted_links WHERE url = ?", (u,)
            ).fetchone()
            if not existing:
                to_resolve.append((u, r["id"], r["author_handle"]))

    if to_resolve:
        print(f"Resolving {len(to_resolve)} new t.co links...")
        with httpx.Client(timeout=10, follow_redirects=False) as client:
            for i, (url, tid, author) in enumerate(to_resolve):
                resolved = resolve_tco(url, client)
                link_type = classify_url(resolved or "")
                db.execute("""
                    INSERT OR IGNORE INTO extracted_links (url, tweet_id, resolved_url, link_type)
                    VALUES (?, ?, ?, ?)
                """, (url, tid, resolved, link_type))
                if (i + 1) % 50 == 0:
                    print(f"  {i + 1}/{len(to_resolve)}...")
                    db.commit()
        db.commit()
        print("  Done resolving.")

    # Summary
    counts = db.execute("""
        SELECT link_type, COUNT(*) as cnt, SUM(clipped) as clipped
        FROM extracted_links
        GROUP BY link_type ORDER BY cnt DESC
    """).fetchall()

    print("\nLink types found:")
    for r in counts:
        print(f"  {r['link_type']}: {r['cnt']} ({r['clipped']} already clipped)")

    to_clip = db.execute("""
        SELECT url, resolved_url, link_type, tweet_id
        FROM extracted_links
        WHERE clipped = 0 AND link_type IN ('arxiv', 'pdf', 'gist', 'github')
    """).fetchall()

    if not to_clip:
        print("\nNo new items to clip.")
        db.close()
        return 0

    print(f"\n{len(to_clip)} links to clip:")
    for r in to_clip:
        print(f"  [{r['link_type']}] {r['resolved_url'][:100]}")

    if dry_run:
        db.close()
        return 0

    # Determine output directories
    base = output_dir or Path.cwd()
    refs_dir = base / "refs"
    papers_dir = base / "papers"

    clipped = 0
    for r in to_clip:
        resolved = r["resolved_url"]
        link_type = r["link_type"]
        original_url = r["url"]

        if link_type == "gist":
            print(f"  Clipping gist: {resolved[:80]}...")
            path = clip_gist(resolved, refs_dir)
            if path:
                db.execute(
                    "UPDATE extracted_links SET clipped = 1, clipped_path = ? WHERE url = ?",
                    (str(path), original_url),
                )
                clipped += 1
                print(f"    → {path.name}")

        elif link_type in ("arxiv", "pdf"):
            print(f"  Clipping {link_type}: {resolved[:80]}...")
            try:
                from .clip import detect_arxiv
                arxiv_id = detect_arxiv(resolved) if link_type == "arxiv" else None
                if arxiv_id:
                    clip_arxiv(arxiv_id, papers_dir)
                else:
                    clip_pdf_url(resolved, papers_dir)
                db.execute(
                    "UPDATE extracted_links SET clipped = 1 WHERE url = ?",
                    (original_url,),
                )
                clipped += 1
                print("    OK")
            except Exception as e:
                print(f"    FAILED: {e}")

        elif link_type == "github":
            print(f"  Clipping repo README: {resolved[:80]}...")
            try:
                clip_web(resolved, refs_dir)
                db.execute(
                    "UPDATE extracted_links SET clipped = 1 WHERE url = ?",
                    (original_url,),
                )
                clipped += 1
                print("    OK")
            except Exception as e:
                print(f"    FAILED: {e}")

        db.commit()

    print(f"\nClipped {clipped} items.")

    _update_repos_index(db, base)

    db.close()
    return clipped


def _update_repos_index(db, base_dir: Path):
    """Maintain _repos.md with all GitHub repos found in tweets."""
    repos = db.execute("""
        SELECT el.resolved_url, el.clipped, el.clipped_path, t.author_handle, t.created_at
        FROM extracted_links el
        JOIN tweets t ON el.tweet_id = t.id
        WHERE el.link_type = 'github'
        ORDER BY t.created_at DESC
    """).fetchall()

    if not repos:
        return

    lines = [
        "# Repos",
        "",
        "GitHub repositories referenced in bookmarked tweets. Auto-maintained by xtrc8.",
        "",
        "| Repo | Via | Clipped |",
        "|------|-----|---------|",
    ]
    for r in repos:
        url = r["resolved_url"]
        parts = url.rstrip("/").split("/")
        try:
            gh_idx = next(i for i, p in enumerate(parts) if "github.com" in p)
            short = "/".join(parts[gh_idx + 1:gh_idx + 3])
        except StopIteration:
            short = url
        via = f"@{r['author_handle']}"
        clipped_mark = "yes" if r["clipped"] else ""
        lines.append(f"| [{short}]({url}) | {via} | {clipped_mark} |")

    lines.append("")
    repos_path = base_dir / "_repos.md"
    repos_path.write_text("\n".join(lines))
    print(f"Updated {repos_path} ({len(repos)} repos)")


def main():
    parser = argparse.ArgumentParser(description="Extract and clip links from tweets")
    parser.add_argument("--dry-run", action="store_true", help="Show links without clipping")
    parser.add_argument("--skip-replies", action="store_true", help="Skip reply scraping")
    parser.add_argument("--db", default=None, help="SQLite database path")
    parser.add_argument("--output-dir", "-o", default=None, help="Base output directory for clipped content")
    args = parser.parse_args()

    db_path = Path(args.db) if args.db else Path(".tweets-cache.db")
    output_dir = Path(args.output_dir) if args.output_dir else None

    run_extract(db_path, args.dry_run, args.skip_replies, output_dir, db_path)


if __name__ == "__main__":
    main()
