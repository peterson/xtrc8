#!/usr/bin/env python3
"""
clip — Web article, PDF, and arxiv content extraction.

Library usage:
    from xtrc8.clip import clip_web, clip_pdf, clip_arxiv, clip_pdf_url

    path = clip_web("https://example.com/article", dest_dir=Path("output/refs"))
    path = clip_pdf(Path("paper.pdf"), dest_dir=Path("output/papers"))
    path = clip_arxiv("2301.00001", dest_dir=Path("output/papers"))

CLI usage:
    xtrc8 clip <url-or-file> [--output-dir DIR] [--to refs|papers|datasheets]
"""

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from .util import slugify


def clip_web(url: str, dest_dir: Path) -> Path:
    """Fetch a web article and convert to markdown."""
    import trafilatura

    dest_dir.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {url}...")
    downloaded = trafilatura.fetch_url(url)
    if not downloaded:
        print("ERROR: Could not fetch URL.", file=sys.stderr)
        sys.exit(1)

    result = trafilatura.extract(
        downloaded,
        include_links=True,
        include_images=True,
        include_tables=True,
        output_format="txt",
        with_metadata=True,
    )
    if not result:
        print("ERROR: Could not extract article content.", file=sys.stderr)
        sys.exit(1)

    metadata = trafilatura.extract_metadata(downloaded)

    title = metadata.title if metadata and metadata.title else ""
    author = metadata.author if metadata and metadata.author else ""
    date = metadata.date if metadata and metadata.date else datetime.now().strftime("%Y-%m-%d")
    source = urlparse(url).netloc.replace("www.", "")

    slug = slugify(title) if title else slugify(source)
    filename = f"{slug}.md"
    path = dest_dir / filename

    if path.exists():
        path = dest_dir / f"{slug}-{hash(url) % 10000}.md"

    lines = [
        "---",
        f"title: {title}",
        f"url: {url}",
        f"author: {author}",
        f"date: {date}",
        f"source: {source}",
        "---",
        "",
        result,
        "",
    ]

    path.write_text("\n".join(lines))
    return path


def _extract_pdf_metadata(pdf_path: Path) -> dict:
    """Extract metadata from a PDF file."""
    import pymupdf

    doc = pymupdf.open(str(pdf_path))
    meta = doc.metadata or {}

    title = meta.get("title", "").strip()
    author = meta.get("author", "").strip()
    subject = meta.get("subject", "").strip()
    keywords = meta.get("keywords", "").strip()

    pub_date = ""
    raw_date = meta.get("creationDate", "")
    if raw_date:
        m = re.search(r"D:(\d{4})(\d{2})(\d{2})", raw_date)
        if m:
            pub_date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    page_count = len(doc)
    doc.close()

    return {
        "title": title,
        "author": author,
        "subject": subject,
        "keywords": keywords,
        "pub_date": pub_date,
        "page_count": page_count,
    }


def _generate_pdf_summary(md_text: str, metadata: dict) -> str:
    """Generate a structured summary from extracted markdown text."""
    abstract = ""
    lines = md_text.split("\n")
    in_abstract = False
    abstract_lines = []
    for line in lines:
        lower = line.strip().lower()
        if lower.startswith("abstract") or lower.startswith("**abstract"):
            in_abstract = True
            rest = line.split(".", 1)[1].strip() if "." in line else ""
            if rest:
                abstract_lines.append(rest)
            continue
        if in_abstract:
            if line.strip().startswith("#") or line.strip().startswith("**") and len(abstract_lines) > 2:
                break
            abstract_lines.append(line)
            if len(abstract_lines) > 20:
                break
    abstract = "\n".join(abstract_lines).strip()

    headings = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("## ") and not stripped.startswith("###"):
            heading = stripped[3:].strip().rstrip("#").strip()
            if heading and heading.lower() not in ("abstract", "references", "bibliography"):
                headings.append(heading)

    parts = []
    if abstract:
        parts.append("## Abstract\n")
        parts.append(abstract)
        parts.append("")

    if headings:
        parts.append("## Structure\n")
        for h in headings[:30]:
            parts.append(f"- {h}")
        parts.append("")

    parts.append("## Full Text\n")
    parts.append("See source PDF for complete content including equations and figures.")
    parts.append(f"Pages: {metadata.get('page_count', '?')}")
    parts.append("")

    return "\n".join(parts)


def clip_pdf(pdf_path: Path, dest_dir: Path, keep_pdf: bool = True) -> Path:
    """Convert a PDF to a structured markdown summary.
    Keeps the original PDF alongside — PDF is source of truth for
    equations and images; markdown is a searchable summary + index."""
    import pymupdf4llm

    dest_dir.mkdir(parents=True, exist_ok=True)

    print(f"Processing {pdf_path.name}...")

    meta = _extract_pdf_metadata(pdf_path)
    title = meta["title"]

    md_full = pymupdf4llm.to_markdown(str(pdf_path))

    if not title:
        for line in md_full.split("\n"):
            line = line.strip()
            if line.startswith("# "):
                title = line[2:].strip()
                break
            elif line and not title:
                title = line[:80]

    summary = _generate_pdf_summary(md_full, meta)

    slug = slugify(title) if title else slugify(pdf_path.stem)
    filename = f"{slug}.md"
    path = dest_dir / filename

    if path.exists():
        path = dest_dir / f"{slug}-{hash(str(pdf_path)) % 10000}.md"

    if keep_pdf:
        pdf_dest = path.with_suffix(".pdf")
        if pdf_path.resolve() != pdf_dest.resolve():
            import shutil
            shutil.copy2(pdf_path, pdf_dest)
            print(f"  PDF kept: {pdf_dest.name}")

    fm_lines = ["---", f"title: {title}"]
    if meta["author"]:
        fm_lines.append(f"author: {meta['author']}")
    if meta["pub_date"]:
        fm_lines.append(f"date: {meta['pub_date']}")
    fm_lines.append(f"source_pdf: {path.with_suffix('.pdf').name}")
    if meta["keywords"]:
        fm_lines.append(f"keywords: {meta['keywords']}")
    fm_lines.append(f"pages: {meta['page_count']}")
    fm_lines += ["---", ""]

    pdf_link = f"[Source PDF]({path.with_suffix('.pdf').name})"
    lines = fm_lines + [f"# {title}", "", pdf_link, "", summary]

    path.write_text("\n".join(lines))
    return path


def clip_pdf_url(url: str, dest_dir: Path) -> Path:
    """Download a PDF from a URL and convert to markdown. Keeps the PDF."""
    import httpx

    dest_dir.mkdir(parents=True, exist_ok=True)

    print(f"Downloading PDF from {url}...")
    try:
        with httpx.Client(timeout=60, follow_redirects=True) as client:
            resp = client.get(url)
            resp.raise_for_status()
    except Exception as e:
        print(f"ERROR: Could not download {url}: {e}", file=sys.stderr)
        sys.exit(1)

    url_stem = Path(urlparse(url).path).stem
    tmp_pdf = dest_dir / f"_tmp_{slugify(url_stem)}.pdf"
    tmp_pdf.write_bytes(resp.content)

    try:
        result = clip_pdf(tmp_pdf, dest_dir, keep_pdf=True)
        text = result.read_text()
        text = text.replace(
            f"source_file: {tmp_pdf.name}",
            f"source_file: {url_stem}.pdf\nurl: {url}",
        )
        result.write_text(text)
    finally:
        tmp_pdf.unlink(missing_ok=True)

    return result


def clip_arxiv(arxiv_id: str, dest_dir: Path) -> Path:
    """Download an arxiv paper PDF and convert to markdown."""
    import httpx

    dest_dir.mkdir(parents=True, exist_ok=True)

    pdf_url = f"https://arxiv.org/pdf/{arxiv_id}.pdf"
    print(f"Downloading arxiv:{arxiv_id}...")

    try:
        with httpx.Client(timeout=60, follow_redirects=True) as client:
            resp = client.get(pdf_url)
            resp.raise_for_status()
    except Exception as e:
        print(f"ERROR: Could not download {pdf_url}: {e}", file=sys.stderr)
        sys.exit(1)

    tmp_pdf = dest_dir / f"_tmp_{arxiv_id.replace('/', '_')}.pdf"
    tmp_pdf.write_bytes(resp.content)

    try:
        result = clip_pdf(tmp_pdf, dest_dir, keep_pdf=True)
        text = result.read_text()
        text = text.replace(
            f"source_file: {tmp_pdf.name}",
            f"source_file: arxiv:{arxiv_id}\nurl: https://arxiv.org/abs/{arxiv_id}",
        )
        result.write_text(text)
    finally:
        tmp_pdf.unlink(missing_ok=True)

    return result


def detect_arxiv(url: str) -> str | None:
    """Extract arxiv ID from URL if it's an arxiv link."""
    patterns = [
        r'arxiv\.org/abs/(\d+\.\d+)',
        r'arxiv\.org/pdf/(\d+\.\d+)',
        r'arxiv\.org/abs/([\w-]+/\d+)',
    ]
    for p in patterns:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None


_VIDEO_HOSTS = (
    "youtube.com", "youtu.be", "m.youtube.com",
    "vimeo.com",
    "twitch.tv", "clips.twitch.tv",
    "dailymotion.com",
    "soundcloud.com",
    "rumble.com",
    "odysee.com",
    "peertube",
)


def detect_video_url(url: str) -> bool:
    """Return True if url looks like a video host supported by yt-dlp."""
    host = urlparse(url).netloc.lower().lstrip("www.")
    return any(v in host for v in _VIDEO_HOSTS)


def _format_captions_as_text(subtitle_path: Path) -> str:
    """Convert an srt or vtt caption file to plain paragraph text,
    stripping timestamps and sequence numbers. Preserves paragraph breaks
    on blank-line boundaries."""
    if not subtitle_path.exists():
        return ""

    raw = subtitle_path.read_text(encoding="utf-8", errors="replace")
    lines: list[str] = []
    for line in raw.splitlines():
        s = line.strip()
        if not s:
            if lines and lines[-1] != "":
                lines.append("")
            continue
        # Skip WEBVTT header
        if s.startswith("WEBVTT") or s.startswith("NOTE") or s.startswith("Kind:") or s.startswith("Language:"):
            continue
        # Skip numeric sequence markers (srt)
        if s.isdigit():
            continue
        # Skip timestamp lines: 00:00:00,000 --> 00:00:00,000
        if "-->" in s:
            continue
        # Strip inline <c> tags and positioning metadata
        s = re.sub(r"<[^>]+>", "", s)
        s = re.sub(r"^\s*align:\S+\s*position:\S+\s*$", "", s)
        if s.strip():
            lines.append(s.strip())

    # De-dupe consecutive identical lines (auto-captions often repeat)
    deduped: list[str] = []
    for line in lines:
        if not deduped or deduped[-1] != line:
            deduped.append(line)

    text = "\n".join(deduped).strip()
    # Collapse 3+ blank lines to single blank
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def clip_video(
    url: str,
    dest_dir: Path,
    *,
    download_video: bool = True,
    max_height: int = 720,
    keep_srt: bool = True,
) -> Path:
    """Clip a video from YouTube/Vimeo/Twitch/etc via yt-dlp.

    Writes:
      - {slug}.md         : markdown with metadata + full caption transcript
      - {slug}.mp4        : the video (if download_video=True)
      - {slug}.info.json  : full yt-dlp metadata dump
      - {slug}.en.srt     : original subtitle file (if keep_srt=True)
      - media/{slug}-thumb.{jpg,webp,png} : thumbnail image

    Returns path to the markdown file.
    """
    try:
        import yt_dlp
    except ImportError:
        print("ERROR: yt-dlp not installed. Run: uv add yt-dlp", file=sys.stderr)
        sys.exit(1)

    dest_dir.mkdir(parents=True, exist_ok=True)
    media_dir = dest_dir / "media"
    media_dir.mkdir(exist_ok=True)

    print(f"Fetching video metadata from {url}...")

    # First pass: metadata-only to get title/id for slug
    probe_opts = {"quiet": True, "no_warnings": True, "skip_download": True}
    with yt_dlp.YoutubeDL(probe_opts) as ydl:
        try:
            info = ydl.extract_info(url, download=False)
        except Exception as e:
            print(f"ERROR: Could not fetch video info: {e}", file=sys.stderr)
            sys.exit(1)

    title = info.get("title") or "untitled"
    uploader = info.get("uploader") or info.get("channel") or "unknown"
    video_id = info.get("id") or "unknown"
    upload_date = info.get("upload_date")  # YYYYMMDD
    if upload_date and len(upload_date) == 8:
        date_str = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:]}"
    else:
        date_str = datetime.now().strftime("%Y-%m-%d")

    slug_base = slugify(title, max_len=40) or video_id
    slug = f"{date_str}-{slugify(uploader, max_len=20)}-{slug_base}"
    out_stem = dest_dir / slug

    # Second pass: download video + subs + thumbnail + metadata
    download_opts = {
        "outtmpl": str(out_stem) + ".%(ext)s",
        "writeinfojson": True,
        "writethumbnail": True,
        "writesubtitles": True,
        "writeautomaticsub": True,
        "subtitleslangs": ["en", "en-US", "en-GB"],
        "subtitlesformat": "srt/vtt/best",
        "quiet": False,
        "no_warnings": True,
        "merge_output_format": "mp4",
    }
    if download_video:
        download_opts["format"] = f"bestvideo[height<={max_height}][ext=mp4]+bestaudio[ext=m4a]/best[height<={max_height}]/best"
    else:
        download_opts["skip_download"] = True

    with yt_dlp.YoutubeDL(download_opts) as ydl:
        try:
            ydl.download([url])
        except Exception as e:
            print(f"WARN: Download phase had issues: {e}", file=sys.stderr)
            # Continue — we may still have metadata and captions

    # Locate downloaded artefacts
    video_file = None
    srt_file = None
    thumb_file = None
    info_json = None
    for candidate in dest_dir.glob(f"{slug}.*"):
        suf = candidate.suffix.lower()
        name = candidate.name
        if suf == ".mp4" and video_file is None:
            video_file = candidate
        elif suf in (".srt", ".vtt") and "en" in name.lower():
            if srt_file is None or suf == ".srt":  # prefer srt
                srt_file = candidate
        elif suf in (".jpg", ".jpeg", ".png", ".webp") and thumb_file is None:
            thumb_file = candidate
        elif suf == ".json" and name.endswith(".info.json"):
            info_json = candidate

    # Move thumbnail to media/ subdir
    if thumb_file is not None:
        new_thumb = media_dir / f"{slug}-thumb{thumb_file.suffix}"
        thumb_file.rename(new_thumb)
        thumb_file = new_thumb

    # Extract caption text
    caption_text = ""
    if srt_file:
        caption_text = _format_captions_as_text(srt_file)
        if not keep_srt:
            srt_file.unlink(missing_ok=True)
            srt_file = None

    # Build markdown
    description = (info.get("description") or "").strip()
    duration = info.get("duration")
    duration_str = ""
    if duration:
        h, rem = divmod(int(duration), 3600)
        m, s = divmod(rem, 60)
        duration_str = f"{h:02d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"

    fm_lines = [
        "---",
        f"title: {title.replace(chr(10), ' ')}",
        f"url: {url}",
        f"channel: {uploader}",
        f"date: {date_str}",
        f"video_id: {video_id}",
    ]
    if duration_str:
        fm_lines.append(f"duration: {duration_str}")
    if video_file:
        fm_lines.append(f"source_video: {video_file.name}")
    if thumb_file:
        fm_lines.append(f"thumbnail: media/{thumb_file.name}")
    fm_lines.append("source: " + urlparse(url).netloc.replace("www.", ""))
    fm_lines.append("---")

    body_parts = [f"# {title}", ""]
    if thumb_file:
        body_parts.append(f"![{title}](media/{thumb_file.name})")
        body_parts.append("")
    body_parts.append(f"**Channel:** {uploader}  ")
    body_parts.append(f"**Date:** {date_str}  ")
    if duration_str:
        body_parts.append(f"**Duration:** {duration_str}  ")
    body_parts.append(f"**URL:** {url}  ")
    body_parts.append("")
    if description:
        body_parts.append("## Description")
        body_parts.append("")
        body_parts.append(description)
        body_parts.append("")
    if caption_text:
        body_parts.append("## Transcript")
        body_parts.append("")
        body_parts.append(caption_text)
        body_parts.append("")
    else:
        body_parts.append("_No captions available for this video._")
        body_parts.append("")

    md_path = dest_dir / f"{slug}.md"
    md_path.write_text("\n".join(fm_lines + [""] + body_parts))

    print(f"  Video markdown: {md_path.name}")
    if video_file:
        print(f"  Video file:     {video_file.name}")
    if thumb_file:
        print(f"  Thumbnail:      media/{thumb_file.name}")
    if info_json:
        print(f"  Metadata JSON:  {info_json.name}")

    return md_path


def main():
    parser = argparse.ArgumentParser(description="Web/PDF content clipper")
    parser.add_argument("source", help="URL or file path to clip")
    parser.add_argument(
        "--output-dir", "-o", default=None,
        help="Base output directory (default: current directory)",
    )
    parser.add_argument(
        "--to", default=None,
        choices=["refs", "papers", "datasheets", "videos", "misc"],
        help="Subdirectory within output-dir (auto-detected if omitted)",
    )
    parser.add_argument(
        "--no-video", action="store_true",
        help="For video URLs: don't download the mp4, only metadata/captions/thumbnail",
    )
    parser.add_argument(
        "--max-height", type=int, default=720,
        help="Max video height to download (default: 720)",
    )
    args = parser.parse_args()

    source = args.source
    base = Path(args.output_dir) if args.output_dir else Path.cwd()

    if Path(source).is_file():
        p = Path(source)
        if p.suffix.lower() == ".pdf":
            dest = base / (args.to or "papers")
            result = clip_pdf(p, dest)
        else:
            print(f"ERROR: Unsupported file type: {p.suffix}", file=sys.stderr)
            sys.exit(1)
    elif source.startswith("http"):
        arxiv_id = detect_arxiv(source)
        if arxiv_id:
            dest = base / (args.to or "papers")
            result = clip_arxiv(arxiv_id, dest)
        elif source.lower().endswith(".pdf"):
            dest = base / (args.to or "papers")
            result = clip_pdf_url(source, dest)
        elif detect_video_url(source):
            dest = base / (args.to or "videos")
            result = clip_video(
                source, dest,
                download_video=not args.no_video,
                max_height=args.max_height,
            )
        else:
            dest = base / (args.to or "refs")
            result = clip_web(source, dest)
    else:
        print(f"ERROR: Not a valid URL or file path: {source}", file=sys.stderr)
        sys.exit(1)

    print(f"Saved to {result}")


if __name__ == "__main__":
    main()
