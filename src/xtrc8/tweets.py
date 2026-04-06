#!/usr/bin/env python3
"""
tweets — Twitter/X bookmark sync, export, and TUI selector.

Library usage:
    from xtrc8.tweets import get_db, export_tweet, get_playwright_cookies

CLI usage:
    xtrc8 tweets auth [--db PATH]
    xtrc8 tweets sync [--count N] [--folder NAME] [--all] [--auto]
    xtrc8 tweets select
    xtrc8 tweets status
    xtrc8 tweets folders
"""

import argparse
import asyncio
import json
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from .util import sanitize_handle, slugify

# ---------------------------------------------------------------------------
# Defaults — overridable via CLI flags or library params
# ---------------------------------------------------------------------------

_DEFAULT_DB = Path(".tweets-cache.db")
_DEFAULT_OUTPUT = Path("tweets")

X_DOMAIN = "https://x.com"
BOOKMARKS_URL = f"{X_DOMAIN}/i/bookmarks"


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_db(db_path: Path | None = None) -> sqlite3.Connection:
    db_path = db_path or _DEFAULT_DB
    db = sqlite3.connect(db_path, timeout=10)
    db.row_factory = sqlite3.Row
    # WAL mode: readers don't block writers and vice versa, better
    # concurrency for TUI + background jobs. busy_timeout lets conflicting
    # writers wait instead of failing immediately with "database is locked".
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=5000")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("""
        CREATE TABLE IF NOT EXISTS tweets (
            id TEXT PRIMARY KEY,
            author_handle TEXT,
            author_name TEXT,
            created_at TEXT,
            full_text TEXT,
            url TEXT,
            media_json TEXT,
            quote_url TEXT,
            in_reply_to TEXT,
            lang TEXT,
            favorite_count INTEGER,
            retweet_count INTEGER,
            bookmark_count INTEGER,
            folder_name TEXT,
            folder_id TEXT,
            synced_at TEXT,
            ingested INTEGER DEFAULT 0,
            ingested_at TEXT
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS folders (
            name TEXT PRIMARY KEY,
            folder_id TEXT,
            auto_ingest INTEGER DEFAULT 0
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS auth (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    # Migration: add folder columns to tweets if missing (existing DBs)
    cols = {r[1] for r in db.execute("PRAGMA table_info(tweets)").fetchall()}
    if "folder_name" not in cols:
        db.execute("ALTER TABLE tweets ADD COLUMN folder_name TEXT")
    if "folder_id" not in cols:
        db.execute("ALTER TABLE tweets ADD COLUMN folder_id TEXT")
    db.commit()
    return db


def get_auto_ingest_folders(db: sqlite3.Connection) -> set[str]:
    rows = db.execute("SELECT name FROM folders WHERE auto_ingest = 1").fetchall()
    return {r["name"] for r in rows}


def set_folder_auto_ingest(db: sqlite3.Connection, name: str, auto: bool):
    # (unfiled) is a synthetic/virtual folder — never persist it to the
    # folders table. Auto-ingest toggling for (unfiled) is a no-op.
    if name == "(unfiled)":
        return
    db.execute("""
        INSERT INTO folders (name, auto_ingest)
        VALUES (?, ?)
        ON CONFLICT(name) DO UPDATE SET auto_ingest = excluded.auto_ingest
    """, (name, 1 if auto else 0))
    db.commit()


def upsert_folder(db: sqlite3.Connection, name: str, folder_id: str):
    db.execute("""
        INSERT INTO folders (name, folder_id, auto_ingest)
        VALUES (?, ?, 0)
        ON CONFLICT(name) DO UPDATE SET folder_id = excluded.folder_id
    """, (name, folder_id))
    db.commit()


def upsert_tweet(db: sqlite3.Connection, t: dict) -> bool:
    existing = db.execute(
        "SELECT id FROM tweets WHERE id = ?", (t["id"],)
    ).fetchone()
    db.execute("""
        INSERT INTO tweets (
            id, author_handle, author_name, created_at, full_text, url,
            media_json, quote_url, in_reply_to, lang,
            favorite_count, retweet_count, bookmark_count,
            folder_name, folder_id, synced_at
        ) VALUES (
            :id, :author_handle, :author_name, :created_at, :full_text, :url,
            :media_json, :quote_url, :in_reply_to, :lang,
            :favorite_count, :retweet_count, :bookmark_count,
            :folder_name, :folder_id, :synced_at
        ) ON CONFLICT(id) DO UPDATE SET
            full_text = excluded.full_text,
            media_json = excluded.media_json,
            favorite_count = excluded.favorite_count,
            retweet_count = excluded.retweet_count,
            bookmark_count = excluded.bookmark_count,
            folder_name = COALESCE(excluded.folder_name, tweets.folder_name),
            folder_id = COALESCE(excluded.folder_id, tweets.folder_id),
            synced_at = excluded.synced_at
    """, t)
    return existing is None


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def cmd_auth(db_path: Path):
    print("Twitter/X cookie authentication setup")
    print("=" * 45)
    print()
    print("To get your cookies:")
    print("  1. Open x.com in your browser and log in")
    print("  2. Open DevTools (F12) -> Application -> Cookies -> https://x.com")
    print("  3. Copy the values for: auth_token, ct0")
    print()

    auth_token = input("auth_token: ").strip()
    ct0 = input("ct0: ").strip()

    if not auth_token or not ct0:
        print("ERROR: Both values are required.", file=sys.stderr)
        sys.exit(1)

    db = get_db(db_path)
    db.execute("INSERT OR REPLACE INTO auth (key, value) VALUES (?, ?)",
               ("auth_token", auth_token))
    db.execute("INSERT OR REPLACE INTO auth (key, value) VALUES (?, ?)",
               ("ct0", ct0))
    db.commit()
    db.close()
    print(f"\nSaved to {db_path}")


def get_playwright_cookies(db_path: Path | None = None) -> list[dict]:
    """Load cookies from the auth table in Playwright format."""
    db = get_db(db_path)
    rows = db.execute("SELECT key, value FROM auth").fetchall()
    db.close()

    if not rows:
        print("ERROR: No cookies found. Run: xtrc8 tweets auth", file=sys.stderr)
        sys.exit(1)

    raw = {r["key"]: r["value"] for r in rows}
    cookies = []
    for name, value in raw.items():
        cookies.append({
            "name": name,
            "value": value,
            "domain": ".x.com",
            "path": "/",
            "secure": True,
            "httpOnly": name == "auth_token",
            "sameSite": "None",
        })
    return cookies


# ---------------------------------------------------------------------------
# Playwright helpers
# ---------------------------------------------------------------------------

async def _create_browser_context(db_path: Path | None = None):
    from playwright.async_api import async_playwright
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless=True)
    context = await browser.new_context(
        viewport={"width": 1280, "height": 900},
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ),
    )
    await context.add_cookies(get_playwright_cookies(db_path))
    return pw, browser, context


def _parse_tweet_from_graphql(entry: dict, now: str,
                               folder_name: str | None = None,
                               folder_id: str | None = None) -> dict | None:
    """Extract a tweet row dict from a GraphQL timeline entry."""
    try:
        content = entry.get("content", {})
        item = (
            content.get("itemContent", {}) or
            content.get("content", {}).get("tweetResult", {})
        )

        result = item.get("tweet_results", item.get("tweetResult", {}))
        tweet_data = result.get("result", result)

        if tweet_data.get("__typename") == "TweetWithVisibilityResults":
            tweet_data = tweet_data.get("tweet", tweet_data)

        if not tweet_data or tweet_data.get("__typename") not in ("Tweet", None):
            return None

        user_result = tweet_data.get("core", {}).get("user_results", {}).get("result", {})
        legacy_user = user_result.get("legacy", {})
        core_user = user_result.get("core", {})
        legacy_tweet = tweet_data.get("legacy", {})

        tweet_id = legacy_tweet.get("id_str") or tweet_data.get("rest_id", "")
        if not tweet_id:
            return None

        handle = (
            core_user.get("screen_name")
            or legacy_user.get("screen_name")
            or "unknown"
        )
        author_name = (
            core_user.get("name")
            or legacy_user.get("name")
            or "unknown"
        )
        full_text = legacy_tweet.get("full_text", "")
        created_at = legacy_tweet.get("created_at", "")

        media_list = []
        extended = legacy_tweet.get("extended_entities", {})
        for m in extended.get("media", []):
            media_list.append({
                "type": m.get("type", "photo"),
                "url": m.get("media_url_https", m.get("media_url", "")),
            })

        quote_url = None
        quoted = tweet_data.get("quoted_status_result", {}).get("result", {})
        if quoted:
            q_legacy = quoted.get("legacy", {})
            q_user = quoted.get("core", {}).get("user_results", {}).get("result", {})
            q_handle = (
                q_user.get("core", {}).get("screen_name")
                or q_user.get("legacy", {}).get("screen_name")
            )
            q_id = q_legacy.get("id_str") or quoted.get("rest_id")
            if q_handle and q_id:
                quote_url = f"https://x.com/{q_handle}/status/{q_id}"

        return {
            "id": tweet_id,
            "author_handle": handle,
            "author_name": author_name,
            "created_at": created_at,
            "full_text": full_text,
            "url": f"https://x.com/{handle}/status/{tweet_id}",
            "media_json": json.dumps(media_list),
            "quote_url": quote_url,
            "in_reply_to": legacy_tweet.get("in_reply_to_status_id_str"),
            "lang": legacy_tweet.get("lang"),
            "favorite_count": legacy_tweet.get("favorite_count", 0),
            "retweet_count": legacy_tweet.get("retweet_count", 0),
            "bookmark_count": legacy_tweet.get("bookmark_count", 0),
            "folder_name": folder_name,
            "folder_id": folder_id,
            "synced_at": now,
        }
    except Exception:
        return None


def _extract_entries_from_response(data: dict) -> list[dict]:
    """Recursively find timeline entries in a GraphQL response."""
    entries = []

    def _walk(obj):
        if isinstance(obj, dict):
            if "entries" in obj and isinstance(obj["entries"], list):
                entries.extend(obj["entries"])
            for v in obj.values():
                _walk(v)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    _walk(data)
    return entries


# ---------------------------------------------------------------------------
# Folder discovery
# ---------------------------------------------------------------------------

def _walk_for_folders(obj, out: list[dict]):
    if isinstance(obj, dict):
        if "bookmark_collections_slice" in obj:
            items = obj["bookmark_collections_slice"].get("items", [])
            for item in items:
                name = item.get("name", "")
                fid = item.get("id", "")
                if name and fid:
                    out.append({"name": name, "id": fid})
            return
        if "bookmark_folder" in obj:
            f = obj["bookmark_folder"]
            out.append({"name": f.get("name", ""), "id": f.get("id", "")})
        for v in obj.values():
            _walk_for_folders(v, out)
    elif isinstance(obj, list):
        for item in obj:
            _walk_for_folders(item, out)


async def _fetch_folders(context) -> list[dict]:
    page = await context.new_page()
    folders_data: list[dict] = []

    async def intercept(response):
        if "BookmarkFolder" in response.url and "graphql" in response.url.lower():
            try:
                data = await response.json()
                _walk_for_folders(data, folders_data)
            except Exception:
                pass

    page.on("response", intercept)
    await page.goto(BOOKMARKS_URL, wait_until="domcontentloaded", timeout=30000)
    try:
        await page.wait_for_selector('[data-testid="tweet"]', timeout=15000)
    except Exception:
        pass
    await asyncio.sleep(3)
    await page.close()
    return folders_data


# ---------------------------------------------------------------------------
# Folders command
# ---------------------------------------------------------------------------

def cmd_folders_cli(db_path: Path):
    asyncio.run(_folders(db_path))


async def _folders(db_path: Path):
    from rich.console import Console
    from rich.table import Table

    console = Console()
    console.print("[bold]Fetching bookmark folders...[/bold]")

    pw, browser, context = await _create_browser_context(db_path)
    folders_data = await _fetch_folders(context)

    db = get_db(db_path)
    for f in folders_data:
        upsert_folder(db, f["name"], f["id"])
    auto_names = get_auto_ingest_folders(db)

    table = Table(title="Bookmark Folders")
    table.add_column("Name")
    table.add_column("ID")
    table.add_column("Auto-ingest")

    if folders_data:
        for f in folders_data:
            auto = "yes" if f["name"] in auto_names else ""
            table.add_row(f["name"], f["id"], auto, style="green" if auto else "")
    else:
        console.print("[yellow]No folders found (you may only have the default bookmarks).[/yellow]")

    console.print(table)
    db.close()
    await browser.close()
    await pw.stop()


# ---------------------------------------------------------------------------
# Sync — single page scrape helper
# ---------------------------------------------------------------------------

async def _sync_one_page(
    context, url: str, count: int, no_early_stop: bool,
    folder_name: str | None, folder_id: str | None,
    db: sqlite3.Connection, now: str, console,
) -> tuple[int, int]:
    collected_entries: list[dict] = []

    async def intercept_bookmarks(response):
        url_str = response.url
        if ("Bookmark" in url_str) and ("graphql" in url_str.lower()):
            try:
                data = await response.json()
                entries = _extract_entries_from_response(data)
                collected_entries.extend(entries)
            except Exception:
                pass

    page = await context.new_page()
    page.on("response", intercept_bookmarks)

    await page.goto(url, wait_until="networkidle", timeout=30000)
    await asyncio.sleep(3)

    fetched = 0
    new = 0
    dup_streak = 0
    EARLY_STOP_THRESHOLD = 20

    def process_collected():
        nonlocal fetched, new, dup_streak
        for entry in collected_entries:
            if fetched >= count:
                break
            row = _parse_tweet_from_graphql(entry, now, folder_name, folder_id)
            if row:
                is_new = upsert_tweet(db, row)
                if is_new:
                    new += 1
                    dup_streak = 0
                else:
                    dup_streak += 1
                fetched += 1
        collected_entries.clear()
        db.commit()

    process_collected()

    scroll_attempts = 0
    max_scroll_attempts = 50

    while fetched < count and scroll_attempts < max_scroll_attempts:
        if not no_early_stop and dup_streak >= EARLY_STOP_THRESHOLD:
            console.print(f"  [dim]Early stop — {dup_streak} consecutive known tweets[/dim]")
            break

        prev_fetched = fetched
        await page.evaluate("window.scrollBy(0, window.innerHeight * 2)")
        await asyncio.sleep(2)
        process_collected()

        if fetched == prev_fetched:
            scroll_attempts += 1
            if scroll_attempts >= 3:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await asyncio.sleep(3)
                process_collected()
                if fetched == prev_fetched:
                    console.print("  [dim]Reached end of list.[/dim]")
                    break
        else:
            scroll_attempts = 0
            if fetched % 50 == 0:
                console.print(f"  [dim]{fetched} tweets so far, {new} new[/dim]")

    await page.close()
    return fetched, new


# ---------------------------------------------------------------------------
# Sync command
# ---------------------------------------------------------------------------

def cmd_sync_cli(count: int, folder_name: str | None, sync_all: bool,
                 auto_ingest: bool, no_early_stop: bool,
                 db_path: Path, output_dir: Path):
    asyncio.run(_sync(count, folder_name, sync_all, auto_ingest, no_early_stop,
                       db_path, output_dir))


async def _sync(
    count: int,
    folder_name: str | None,
    sync_all: bool,
    auto_ingest: bool,
    no_early_stop: bool,
    db_path: Path,
    output_dir: Path,
):
    from rich.console import Console

    console = Console()
    db = get_db(db_path)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    pw, browser, context = await _create_browser_context(db_path)

    targets: list[tuple[str, str, str | None, str | None]] = []

    if sync_all or auto_ingest:
        console.print("[dim]Discovering bookmark folders...[/dim]")
        folders = await _fetch_folders(context)
        for f in folders:
            upsert_folder(db, f["name"], f["id"])

        targets.append(("main bookmarks", BOOKMARKS_URL, None, None))
        for f in folders:
            targets.append((
                f["name"],
                f"{BOOKMARKS_URL}/{f['id']}",
                f["name"],
                f["id"],
            ))
        console.print(f"  [dim]Found {len(folders)} folders + main bookmarks[/dim]")

    elif folder_name:
        console.print("[dim]Discovering bookmark folders...[/dim]")
        folders = await _fetch_folders(context)
        for f in folders:
            upsert_folder(db, f["name"], f["id"])
        folder_map = {f["name"]: f["id"] for f in folders}
        if folder_name not in folder_map:
            console.print(f"[red]Folder '{folder_name}' not found.[/red]")
            console.print(f"Available: {', '.join(folder_map.keys())}")
            await browser.close()
            await pw.stop()
            return
        fid = folder_map[folder_name]
        targets.append((folder_name, f"{BOOKMARKS_URL}/{fid}", folder_name, fid))

    else:
        targets.append(("main bookmarks", BOOKMARKS_URL, None, None))

    total_fetched = 0
    total_new = 0
    auto_ingested = 0
    auto_folder_names = get_auto_ingest_folders(db)

    for label, url, fname, fid in targets:
        console.print(f"\n[bold]Syncing: {label}[/bold] (up to {count})")
        fetched, new = await _sync_one_page(
            context, url, count, no_early_stop, fname, fid, db, now, console,
        )
        total_fetched += fetched
        total_new += new
        console.print(f"  {fetched} tweets, {new} new")

        if auto_ingest and fname and fname in auto_folder_names and new > 0:
            n = _auto_ingest_folder(db, fname, now, output_dir)
            auto_ingested += n
            console.print(f"  [green]Auto-ingested {n} tweets from '{fname}'[/green]")

    await browser.close()
    await pw.stop()
    db.close()

    console.print(f"\n[green]Done.[/green] Total: {total_fetched} tweets, {total_new} new.")
    if auto_ingested > 0:
        console.print(f"[green]Auto-ingested {auto_ingested} tweets into {output_dir}[/green]")


def _auto_ingest_folder(db: sqlite3.Connection, folder_name: str, now: str,
                        output_dir: Path) -> int:
    rows = db.execute(
        "SELECT * FROM tweets WHERE folder_name = ? AND ingested = 0",
        (folder_name,),
    ).fetchall()
    for row in rows:
        export_tweet(row, output_dir)
        db.execute(
            "UPDATE tweets SET ingested = 1, ingested_at = ? WHERE id = ?",
            (now, row["id"]),
        )
    db.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def cmd_status_cli(db_path: Path):
    from rich.console import Console
    from rich.table import Table

    db = get_db(db_path)
    console = Console()

    total = db.execute("SELECT COUNT(*) FROM tweets").fetchone()[0]
    ingested = db.execute("SELECT COUNT(*) FROM tweets WHERE ingested = 1").fetchone()[0]
    pending = total - ingested

    table = Table(title="Tweet Cache Status")
    table.add_column("Status", style="bold")
    table.add_column("Count", justify="right")
    table.add_row("cached (not ingested)", str(pending), style="yellow")
    table.add_row("ingested", str(ingested), style="green")
    table.add_row("total", str(total), style="bold")
    console.print(table)

    if total > 0:
        folder_rows = db.execute("""
            SELECT COALESCE(folder_name, '(unfiled)') as fname,
                   COUNT(*) as total,
                   SUM(CASE WHEN ingested = 0 THEN 1 ELSE 0 END) as pending
            FROM tweets GROUP BY folder_name ORDER BY total DESC
        """).fetchall()
        if folder_rows:
            console.print()
            tf = Table(title="By Folder")
            tf.add_column("Folder")
            tf.add_column("Total", justify="right")
            tf.add_column("Pending", justify="right")
            for r in folder_rows:
                tf.add_row(r["fname"], str(r["total"]), str(r["pending"]))
            console.print(tf)

        rows = db.execute("""
            SELECT author_handle, COUNT(*) as cnt
            FROM tweets WHERE ingested = 0
            GROUP BY author_handle ORDER BY cnt DESC LIMIT 10
        """).fetchall()
        if rows:
            console.print()
            t2 = Table(title="Top Authors (not yet ingested)")
            t2.add_column("Author")
            t2.add_column("Tweets", justify="right")
            for r in rows:
                t2.add_row(f"@{r['author_handle']}", str(r['cnt']))
            console.print(t2)

    db.close()


# ---------------------------------------------------------------------------
# Export helpers
# ---------------------------------------------------------------------------

# Languages to treat as non-English and translate
_SKIP_TRANSLATE = {"en", "zxx", "und", "qme", "qst"}


def _translate_text(text: str, source_lang: str) -> str | None:
    from deep_translator import GoogleTranslator

    if not text or not text.strip():
        return None

    clean = re.sub(r'https?://\S+', '', text).strip()
    if not clean:
        return None

    try:
        result = GoogleTranslator(source=source_lang, target="en").translate(clean)
        return result
    except Exception:
        try:
            result = GoogleTranslator(source="auto", target="en").translate(clean)
            return result
        except Exception:
            return None


def _download_media(url: str, tweet_id: str, index: int, media_dir: Path) -> Path | None:
    import httpx

    media_dir.mkdir(parents=True, exist_ok=True)

    ext = ".jpg"
    if "video" in url or url.endswith(".mp4"):
        ext = ".mp4"
    elif url.endswith(".png"):
        ext = ".png"
    elif url.endswith(".gif"):
        ext = ".gif"

    local_name = f"{tweet_id}-{index}{ext}"
    local_path = media_dir / local_name

    if local_path.exists():
        return local_path

    try:
        with httpx.Client(timeout=30, follow_redirects=True) as client:
            resp = client.get(url)
            resp.raise_for_status()
            local_path.write_bytes(resp.content)
        return local_path
    except Exception:
        return None


def export_tweet(row: sqlite3.Row, output_dir: Path) -> Path:
    """Export a tweet to a markdown file in output_dir."""
    output_dir.mkdir(parents=True, exist_ok=True)
    media_dir = output_dir / "media"

    try:
        dt = datetime.strptime(row["created_at"], "%a %b %d %H:%M:%S %z %Y")
        date_str = dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        date_str = datetime.now().strftime("%Y-%m-%d")

    slug = slugify(row["full_text"], max_len=40)
    if not slug:
        slug = row["id"]

    handle = sanitize_handle(row["author_handle"])
    filename = f"{date_str}-{handle}-{slug}.md"
    path = output_dir / filename

    if path.exists():
        path = output_dir / f"{date_str}-{handle}-{row['id']}.md"

    media = json.loads(row["media_json"]) if row["media_json"] else []
    lang = row["lang"] or "en"
    needs_translation = lang not in _SKIP_TRANSLATE
    full_text = row["full_text"] or ""

    translation = None
    if needs_translation and full_text.strip():
        translation = _translate_text(full_text, lang)

    lines = [
        "---",
        f"author: @{row['author_handle']}",
        f"date: {date_str}",
        f"url: {row['url']}",
        "type: tweet",
        f"lang: {lang}",
    ]
    if translation:
        lines.append("translated: true")
    lines += ["---", ""]

    if translation:
        lines += ["## Translation", "", translation, "", "## Original", "", full_text]
    else:
        lines.append(full_text)

    if media:
        lines += [""]
        for i, m in enumerate(media):
            murl = m.get("url", "")
            if not murl:
                continue
            local = _download_media(murl, row["id"], i, media_dir)
            if local:
                rel = f"media/{local.name}"
                lines.append(f"![{m.get('type', 'media')}]({rel})")
            else:
                lines.append(f"![{m.get('type', 'media')}]({murl})")
        lines.append("")

    if row["quote_url"]:
        lines += ["## Quoted", "", f"- {row['quote_url']}"]

    lines.append("")
    path.write_text("\n".join(lines))
    return path


# ---------------------------------------------------------------------------
# TUI selector
# ---------------------------------------------------------------------------

def cmd_select_cli(db_path: Path, output_dir: Path):
    db = get_db(db_path)
    count = db.execute("SELECT COUNT(*) FROM tweets").fetchone()[0]
    db.close()
    if count == 0:
        print("No tweets in cache. Run 'sync' first.")
        return
    app = _build_tui(db_path, output_dir)
    app.run()


def _build_tui(db_path: Path, output_dir: Path):
    from textual.app import App, ComposeResult
    from textual.widgets import Header, Footer, DataTable, Static
    from textual.binding import Binding
    from textual.containers import Horizontal, Vertical

    class TweetSelector(App):
        TITLE = "xtrc8 Tweet Selector"
        CSS = """
        #top-panes { height: 1fr; }
        #folders-pane {
            width: 28;
            border-right: solid $accent;
        }
        #folders-pane DataTable { height: 1fr; }
        #folders-title {
            height: 1;
            background: $accent;
            color: $text;
            padding: 0 1;
            text-style: bold;
        }
        #tweets-pane { width: 1fr; }
        #tweets-table { height: 1fr; }
        #preview {
            height: 10;
            border-top: solid $accent;
            padding: 0 1;
            overflow-y: auto;
        }
        #sync-status {
            height: 1;
            background: $surface-darken-1;
            color: $text-muted;
            padding: 0 1;
        }
        #status {
            height: 1;
            background: $accent;
            color: $text;
            padding: 0 1;
            text-style: bold;
        }
        """
        BINDINGS = [
            Binding("tab", "switch_pane", "Switch pane", show=True),
            Binding("space", "toggle_select", "Toggle", show=True, priority=True),
            Binding("a", "select_all", "All", show=True),
            Binding("n", "select_none", "None", show=True),
            Binding("i", "ingest", "Import", show=True),
            Binding("u", "unimport", "Un-import", show=True),
            Binding("f", "toggle_filter", "Filter", show=True),
            Binding("q", "try_quit", "Quit", show=True),
        ]

        _SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"

        def __init__(self):
            super().__init__()
            self._db_path = db_path
            self._output_dir = output_dir
            self.selected: set[str] = set()
            self.imported: set[str] = set()
            self.auto_folders: set[str] = set()
            self.tweet_rows: list[dict] = []
            self.visible_rows: list[dict] = []
            self.folder_names: list[str] = []
            self._sync_text = ""
            self._spinner_idx = 0
            self._spinner_timer = None
            self.active_pane = "tweets"
            self.show_filter = "all"
            self._load_data()

        def _load_data(self):
            db = get_db(self._db_path)
            rows = db.execute("SELECT * FROM tweets").fetchall()
            self.tweet_rows = [dict(r) for r in rows]

            def _parse_date(t):
                try:
                    return datetime.strptime(t["created_at"], "%a %b %d %H:%M:%S %z %Y")
                except (ValueError, TypeError):
                    return datetime.min.replace(tzinfo=timezone.utc)

            self.tweet_rows.sort(key=_parse_date, reverse=True)
            self.visible_rows = list(self.tweet_rows)
            self.imported = {t["id"] for t in self.tweet_rows if t["ingested"]}

            all_folders = db.execute("""
                SELECT f.name, f.folder_id, f.auto_ingest,
                       COALESCE(t.cnt, 0) as cnt
                FROM folders f
                LEFT JOIN (
                    SELECT COALESCE(folder_name, '(unfiled)') as fname,
                           COUNT(*) as cnt
                    FROM tweets
                    GROUP BY folder_name
                ) t ON f.name = t.fname
                WHERE f.name != '(unfiled)'
                ORDER BY f.name
            """).fetchall()
            unfiled_count = db.execute(
                "SELECT COUNT(*) FROM tweets WHERE folder_name IS NULL"
            ).fetchone()[0]

            self.folder_names = []
            if unfiled_count > 0:
                self.folder_names.append(("(unfiled)", unfiled_count))
            for r in all_folders:
                self.folder_names.append((r["name"], r["cnt"]))

            self.auto_folders = get_auto_ingest_folders(db)
            db.close()

        def compose(self) -> ComposeResult:
            yield Header()
            with Horizontal(id="top-panes"):
                with Vertical(id="folders-pane"):
                    yield Static("FOLDERS", id="folders-title")
                    yield DataTable(id="folders-table")
                with Vertical(id="tweets-pane"):
                    yield DataTable(id="tweets-table")
            yield Static("", id="preview")
            yield Static("", id="sync-status")
            yield Static("", id="status")
            yield Footer()

        def on_mount(self):
            ft = self.query_one("#folders-table", DataTable)
            ft.cursor_type = "row"
            ft.add_columns(" ", "Folder", "#")
            for fname, cnt in self.folder_names:
                mark = "✓" if fname in self.auto_folders else "·"
                ft.add_row(mark, fname, str(cnt), key=fname)

            tt = self.query_one("#tweets-table", DataTable)
            tt.cursor_type = "row"
            tt.add_columns(" ", "Date", "Folder", "Author", "Tweet")

            self._apply_folder_selections()
            self._rebuild_tweets_table()
            self._update_status()
            tt.focus()

            self._sync_interval = 600
            self.run_worker(self._bg_sync_loop, exclusive=True)

        async def _bg_sync_loop(self):
            while True:
                await self._bg_refresh_and_sync()
                await asyncio.sleep(self._sync_interval)

        async def _bg_refresh_and_sync(self):
            self.call_later(self._set_sync_status, "Connecting to X...")
            try:
                pw, browser, context = await _create_browser_context(self._db_path)
            except Exception:
                self.call_later(self._set_sync_status, "")
                return

            self.call_later(self._set_sync_status, "Refreshing folders...")
            try:
                fresh_folders = await _fetch_folders(context)
            except Exception:
                fresh_folders = []

            db = get_db(self._db_path)
            if fresh_folders:
                for f in fresh_folders:
                    upsert_folder(db, f["name"], f["id"])

                fresh_names = {f["name"] for f in fresh_folders}
                current_names = {fname for fname, _ in self.folder_names if fname != "(unfiled)"}

                if fresh_names != current_names:
                    self._reload_folder_list(db)
                    self.call_later(self._rebuild_folders_table)

                self.call_later(
                    self._set_sync_status,
                    f"Folders OK ({len(fresh_folders)}). Syncing tweets..."
                )

            now = datetime.now(timezone.utc).isoformat(timespec="seconds")
            EARLY_STOP = 20
            new_total = 0

            targets = [(None, None, BOOKMARKS_URL, "main")]
            folder_rows = db.execute("SELECT name, folder_id FROM folders").fetchall()
            for r in folder_rows:
                targets.append((
                    r["name"], r["folder_id"],
                    f"{BOOKMARKS_URL}/{r['folder_id']}", r["name"],
                ))

            for i, (fname, fid, url, label) in enumerate(targets, 1):
                self.call_later(
                    self._set_sync_status,
                    f"Syncing {label} ({i}/{len(targets)}) — {new_total} new so far"
                )

                try:
                    sync_ctx = await browser.new_context(
                        viewport={"width": 1280, "height": 900},
                        user_agent=(
                            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                        ),
                    )
                    await sync_ctx.add_cookies(get_playwright_cookies(self._db_path))
                except Exception:
                    continue

                collected_entries: list[dict] = []

                async def intercept(response, _entries=collected_entries):
                    if ("Bookmark" in response.url) and ("graphql" in response.url.lower()):
                        try:
                            data = await response.json()
                            entries = _extract_entries_from_response(data)
                            _entries.extend(entries)
                        except Exception:
                            pass

                page = await sync_ctx.new_page()
                page.on("response", intercept)

                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    try:
                        await page.wait_for_selector(
                            '[data-testid="tweet"]', timeout=10000
                        )
                    except Exception:
                        pass
                    await asyncio.sleep(2)
                except Exception:
                    await sync_ctx.close()
                    continue

                dup_streak = 0
                for entry in collected_entries:
                    row = _parse_tweet_from_graphql(entry, now, fname, fid)
                    if row:
                        is_new = upsert_tweet(db, row)
                        if is_new:
                            new_total += 1
                            dup_streak = 0
                        else:
                            dup_streak += 1
                        if dup_streak >= EARLY_STOP:
                            break

                await sync_ctx.close()
                db.commit()

            db.close()
            await browser.close()
            await pw.stop()

            if new_total > 0:
                self._reload_tweet_data()
                self.call_later(self._on_bg_sync_complete, new_total)
            else:
                self.call_later(lambda: self._set_sync_status("Up to date", spinning=False))

        def _reload_folder_list(self, db):
            self.auto_folders = get_auto_ingest_folders(db)
            all_folders = db.execute("""
                SELECT f.name, COALESCE(t.cnt, 0) as cnt
                FROM folders f
                LEFT JOIN (
                    SELECT COALESCE(folder_name, '(unfiled)') as fname,
                           COUNT(*) as cnt
                    FROM tweets GROUP BY folder_name
                ) t ON f.name = t.fname
                WHERE f.name != '(unfiled)'
                ORDER BY f.name
            """).fetchall()
            unfiled_count = db.execute(
                "SELECT COUNT(*) FROM tweets WHERE folder_name IS NULL"
            ).fetchone()[0]
            self.folder_names = []
            if unfiled_count > 0:
                self.folder_names.append(("(unfiled)", unfiled_count))
            for r in all_folders:
                self.folder_names.append((r["name"], r["cnt"]))

        def _reload_tweet_data(self):
            db = get_db(self._db_path)
            rows = db.execute("SELECT * FROM tweets").fetchall()
            self.tweet_rows = [dict(r) for r in rows]

            def _parse_date(t):
                try:
                    return datetime.strptime(t["created_at"], "%a %b %d %H:%M:%S %z %Y")
                except (ValueError, TypeError):
                    return datetime.min.replace(tzinfo=timezone.utc)

            self.tweet_rows.sort(key=_parse_date, reverse=True)
            self.imported = {t["id"] for t in self.tweet_rows if t["ingested"]}
            db.close()

        def _on_bg_sync_complete(self, new_count: int):
            self._apply_filter()
            self._rebuild_tweets_table()
            self._rebuild_folders_table()
            self._update_status()
            self._set_sync_status(f"Done — {new_count} new tweets synced", spinning=False)
            self.notify(f"Synced {new_count} new tweets")

        def _set_sync_status(self, text: str, spinning: bool = True):
            self._sync_text = text
            if text and spinning:
                self._spinner_idx = 0
                self._render_sync_status()
                if self._spinner_timer is None:
                    self._spinner_timer = self.set_interval(0.1, self._tick_spinner)
            else:
                if self._spinner_timer is not None:
                    self._spinner_timer.stop()
                    self._spinner_timer = None
                if text:
                    self.query_one("#sync-status", Static).update(f" ✓ {text}")
                else:
                    self.query_one("#sync-status", Static).update("")

        def _tick_spinner(self):
            self._spinner_idx = (self._spinner_idx + 1) % len(self._SPINNER)
            self._render_sync_status()

        def _render_sync_status(self):
            frame = self._SPINNER[self._spinner_idx]
            self.query_one("#sync-status", Static).update(f" {frame} {self._sync_text}")

        def _rebuild_folders_table(self):
            ft = self.query_one("#folders-table", DataTable)
            ft.clear()
            for fname, cnt in self.folder_names:
                mark = "✓" if fname in self.auto_folders else "·"
                ft.add_row(mark, fname, str(cnt), key=fname)

        def _tweet_mark(self, tid: str) -> str:
            if tid in self.imported:
                return "◆"
            elif tid in self.selected:
                return "✓"
            return "·"

        def _apply_filter(self):
            if self.show_filter == "staged":
                self.visible_rows = [
                    t for t in self.tweet_rows if t["id"] in self.selected
                ]
            elif self.show_filter == "folders":
                self.visible_rows = [
                    t for t in self.tweet_rows
                    if (t.get("folder_name") or "(unfiled)") in self.auto_folders
                ]
            elif self.show_filter == "imported":
                self.visible_rows = [
                    t for t in self.tweet_rows if t["id"] in self.imported
                ]
            else:
                self.visible_rows = list(self.tweet_rows)

        def action_toggle_filter(self):
            cycle = ["all", "folders", "staged", "imported"]
            idx = cycle.index(self.show_filter)
            self.show_filter = cycle[(idx + 1) % len(cycle)]
            self._apply_filter()
            self._rebuild_tweets_table()
            self._update_status()

        def _apply_folder_selections(self):
            self.selected.clear()
            for t in self.tweet_rows:
                if t["id"] in self.imported:
                    continue
                fname = t.get("folder_name") or "(unfiled)"
                if fname in self.auto_folders:
                    self.selected.add(t["id"])

        def on_descendant_focus(self, event):
            widget = event.widget
            if hasattr(widget, "id"):
                if widget.id == "folders-table":
                    self.active_pane = "folders"
                elif widget.id == "tweets-table":
                    self.active_pane = "tweets"
                self._update_status()

        def action_switch_pane(self):
            if self.active_pane == "tweets":
                self.query_one("#folders-table", DataTable).focus()
            else:
                self.query_one("#tweets-table", DataTable).focus()

        def action_toggle_select(self):
            if self.active_pane == "folders":
                self._toggle_folder()
            else:
                self._toggle_tweet()

        def _toggle_folder(self):
            ft = self.query_one("#folders-table", DataTable)
            row_idx = ft.cursor_row
            if row_idx is None or row_idx >= len(self.folder_names):
                return
            fname = self.folder_names[row_idx][0]
            if fname in self.auto_folders:
                self.auto_folders.discard(fname)
            else:
                self.auto_folders.add(fname)
            mark = "✓" if fname in self.auto_folders else "·"
            ft.update_cell_at((row_idx, 0), mark)
            db = get_db(self._db_path)
            set_folder_auto_ingest(db, fname, fname in self.auto_folders)
            db.close()
            self._apply_folder_selections()
            self._refresh_tweet_marks()
            self._update_status()

        def _toggle_tweet(self):
            tt = self.query_one("#tweets-table", DataTable)
            row_idx = tt.cursor_row
            if row_idx is None or row_idx >= len(self.visible_rows):
                return
            tid = self.visible_rows[row_idx]["id"]
            if tid in self.imported:
                return
            if tid in self.selected:
                self.selected.discard(tid)
            else:
                self.selected.add(tid)
            self._refresh_tweet_marks()
            self._update_status()

        def action_select_all(self):
            if self.active_pane == "folders":
                self.auto_folders = {f for f, _ in self.folder_names}
                ft = self.query_one("#folders-table", DataTable)
                for idx in range(len(self.folder_names)):
                    ft.update_cell_at((idx, 0), "✓")
                db = get_db(self._db_path)
                for fname, _ in self.folder_names:
                    set_folder_auto_ingest(db, fname, True)
                db.close()
                self._apply_folder_selections()
                self._refresh_tweet_marks()
            else:
                self.selected = {t["id"] for t in self.tweet_rows}
                self._refresh_tweet_marks()
            self._update_status()

        def action_select_none(self):
            if self.active_pane == "folders":
                self.auto_folders.clear()
                ft = self.query_one("#folders-table", DataTable)
                for idx in range(len(self.folder_names)):
                    ft.update_cell_at((idx, 0), "·")
                db = get_db(self._db_path)
                for fname, _ in self.folder_names:
                    set_folder_auto_ingest(db, fname, False)
                db.close()
                self._apply_folder_selections()
                self._refresh_tweet_marks()
            else:
                self.selected.clear()
                self._refresh_tweet_marks()
            self._update_status()

        def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted):
            table = event.data_table
            if table.id != "tweets-table" or event.row_key is None:
                return
            tid = str(event.row_key.value)
            tweet = next((t for t in self.visible_rows if t["id"] == tid), None)
            if tweet:
                preview = self.query_one("#preview", Static)
                text = tweet["full_text"] or ""
                folder = tweet.get("folder_name") or "(unfiled)"
                meta = (
                    f"@{tweet['author_handle']} | {tweet['created_at'] or '?'} | "
                    f"{folder} | {tweet.get('favorite_count', 0)} likes"
                )
                preview.update(f"[bold]{meta}[/bold]\n\n{text}")

        def action_ingest(self):
            to_import = self.selected - self.imported
            if not to_import:
                return
            db = get_db(self._db_path)
            now = datetime.now(timezone.utc).isoformat(timespec="seconds")
            count = 0
            for t in self.tweet_rows:
                if t["id"] in to_import:
                    row = db.execute(
                        "SELECT * FROM tweets WHERE id = ?", (t["id"],)
                    ).fetchone()
                    export_tweet(row, self._output_dir)
                    db.execute(
                        "UPDATE tweets SET ingested = 1, ingested_at = ? WHERE id = ?",
                        (now, t["id"]),
                    )
                    self.imported.add(t["id"])
                    self.selected.discard(t["id"])
                    count += 1
            db.commit()
            db.close()
            self._refresh_tweet_marks()
            self._update_status()
            self._set_sync_status(f"Imported {count} tweets — extracting links…")
            self.notify(f"Imported {count} tweets. Extracting links...")

            self.run_worker(self._bg_extract_links, thread=True, exclusive=False)

        def _bg_extract_links(self):
            from .extract import run_extract
            clipped = run_extract(self._db_path, auth_db_path=self._db_path)
            if clipped > 0:
                self.call_later(
                    self._set_sync_status,
                    f"Done — clipped {clipped} papers/gists from links",
                    False,
                )
                self.call_later(
                    self.notify,
                    f"Auto-clipped {clipped} papers/gists from tweet links",
                )
            else:
                self.call_later(
                    self._set_sync_status,
                    "Done — no new papers/gists found",
                    False,
                )
                self.call_later(
                    self.notify,
                    "Link extraction complete — no new papers/gists found",
                )

        def action_unimport(self):
            if self.active_pane != "tweets":
                return
            tt = self.query_one("#tweets-table", DataTable)
            row_idx = tt.cursor_row
            if row_idx is None or row_idx >= len(self.visible_rows):
                return
            tid = self.visible_rows[row_idx]["id"]
            if tid not in self.imported:
                return
            t = self.visible_rows[row_idx]
            db = get_db(self._db_path)
            row = db.execute("SELECT * FROM tweets WHERE id = ?", (tid,)).fetchone()
            for path in self._output_dir.iterdir():
                if path.suffix == ".md" and tid in path.stem:
                    path.unlink()
                    break
                slug = slugify(row["full_text"], max_len=40)
                if slug and slug in path.stem and row["author_handle"] in path.stem:
                    path.unlink()
                    break
            db.execute(
                "UPDATE tweets SET ingested = 0, ingested_at = NULL WHERE id = ?",
                (tid,),
            )
            db.commit()
            db.close()
            self.imported.discard(tid)
            self._refresh_tweet_marks()
            self._update_status()
            self.notify(f"Un-imported @{t['author_handle']}'s tweet")

        def action_try_quit(self):
            staged = self.selected - self.imported
            if staged:
                self._pending_quit = True
                self.notify(
                    f"{len(staged)} staged tweets not imported. Press q again to quit.",
                    severity="warning",
                )
                return
            self.exit()

        def check_action(self, action: str, parameters) -> bool:
            if action == "try_quit" and getattr(self, "_pending_quit", False):
                self.exit()
                return False
            self._pending_quit = False
            return True

        def _refresh_tweet_marks(self):
            tt = self.query_one("#tweets-table", DataTable)
            for idx, t in enumerate(self.visible_rows):
                tt.update_cell_at((idx, 0), self._tweet_mark(t["id"]))

        def _rebuild_tweets_table(self):
            tt = self.query_one("#tweets-table", DataTable)
            tt.clear()
            for t in self.visible_rows:
                try:
                    dt = datetime.strptime(t["created_at"], "%a %b %d %H:%M:%S %z %Y")
                    date_str = dt.strftime("%Y-%m-%d")
                except (ValueError, TypeError):
                    date_str = "?"
                fname = t.get("folder_name") or "(unfiled)"
                folder_short = fname[:10]
                text_preview = (t["full_text"] or "")[:70].replace("\n", " ")
                tt.add_row(
                    self._tweet_mark(t["id"]), date_str, folder_short,
                    f"@{t['author_handle']}", text_preview,
                    key=t["id"],
                )

        def _update_status(self):
            status = self.query_one("#status", Static)
            total = len(self.tweet_rows)
            sel = len(self.selected)
            imp = len(self.imported)
            vis = len(self.visible_rows)
            filt = self.show_filter.upper()
            filter_labels = {
                "ALL": f"ALL TWEETS ({total})",
                "FOLDERS": f"SELECTED FOLDERS ({vis})",
                "STAGED": f"STAGED ({vis})",
                "IMPORTED": f"IMPORTED ({vis})",
            }
            status.update(
                f" {filter_labels[filt]} | {sel} staged | {imp} imported | "
                f"F=filter  SPACE=toggle  I=import  Q=quit"
            )

    return TweetSelector()


# ---------------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Twitter/X bookmark ingest")
    parser.add_argument("--db", default=None, help=f"SQLite database path (default: {_DEFAULT_DB})")
    parser.add_argument("--output-dir", "-o", default=None,
                        help=f"Tweet export directory (default: {_DEFAULT_OUTPUT})")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("auth", help="Set up cookie authentication")
    sub.add_parser("folders", help="List bookmark folders")

    p_sync = sub.add_parser("sync", help="Sync bookmarks to local cache")
    p_sync.add_argument("--count", type=int, default=200, help="Max bookmarks to fetch per source")
    p_sync.add_argument("--folder", type=str, default=None, help="Sync a specific bookmark folder")
    p_sync.add_argument("--all", action="store_true", help="Sync main bookmarks + all folders")
    p_sync.add_argument("--auto", action="store_true", help="Sync all + auto-ingest configured folders")
    p_sync.add_argument("--no-early-stop", action="store_true", help="Disable early stop on duplicates")

    sub.add_parser("select", help="TUI to select tweets for ingest")
    sub.add_parser("status", help="Show cache status")

    args = parser.parse_args()

    db_path = Path(args.db) if args.db else _DEFAULT_DB
    output_dir = Path(args.output_dir) if args.output_dir else _DEFAULT_OUTPUT

    if args.command == "auth":
        cmd_auth(db_path)
    elif args.command == "folders":
        cmd_folders_cli(db_path)
    elif args.command == "sync":
        cmd_sync_cli(args.count, args.folder, args.all, args.auto, args.no_early_stop,
                     db_path, output_dir)
    elif args.command == "status":
        cmd_status_cli(db_path)
    elif args.command == "select":
        cmd_select_cli(db_path, output_dir)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
