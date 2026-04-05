# xtractr

Content extraction toolkit for building personal knowledge bases. Clips web articles, PDFs, arxiv papers, and Twitter/X bookmarks into structured markdown with frontmatter.

## Install

```bash
uv add git+https://github.com/dpeterso/xtractr
# or for local development
uv add --editable ../xtractr
```

Playwright (required for Twitter features) needs a one-time browser install:

```bash
uv run playwright install chromium
```

## Quick start

```bash
# Clip a web article
xtractr clip https://example.com/some-article

# Clip an arxiv paper (downloads PDF, extracts markdown summary)
xtractr clip https://arxiv.org/abs/2301.00001

# Clip a local PDF
xtractr clip paper.pdf

# Clip into a specific subdirectory
xtractr clip https://example.com/article --to refs --output-dir ./raw
```

## Tools

### clip — Web, PDF, and arxiv extraction

Fetches content and converts to markdown with YAML frontmatter (title, author, date, source URL). PDFs are converted via pymupdf4llm with the original PDF kept alongside.

```bash
xtractr clip <url-or-file> [--output-dir DIR] [--to refs|papers|datasheets|misc]
```

| Source type | Detection | Output |
|-------------|-----------|--------|
| Web article | Any HTTP URL | Markdown via trafilatura |
| arxiv | `arxiv.org/abs/` or `arxiv.org/pdf/` URLs | PDF download + markdown summary |
| PDF URL | URLs ending in `.pdf` | PDF download + markdown summary |
| Local PDF | File path ending in `.pdf` | Markdown summary + PDF copy |

As a library:

```python
from pathlib import Path
from xtractr.clip import clip_web, clip_pdf, clip_arxiv, clip_pdf_url

clip_web("https://example.com/article", dest_dir=Path("output/refs"))
clip_pdf(Path("paper.pdf"), dest_dir=Path("output/papers"))
clip_arxiv("2301.00001", dest_dir=Path("output/papers"))
clip_pdf_url("https://example.com/report.pdf", dest_dir=Path("output/papers"))
```

### tweets — Twitter/X bookmark sync and export

Syncs your X bookmarks into a local SQLite cache using Playwright to intercept GraphQL responses. Includes a TUI for browsing and selecting tweets to export as markdown.

#### Auth setup

Cookies are stored in the SQLite database (no separate credentials file).

```bash
xtractr tweets auth
```

You'll be prompted for `auth_token` and `ct0` from your browser's DevTools (Application > Cookies > x.com).

#### Sync bookmarks

```bash
# Sync main bookmarks (up to 200)
xtractr tweets sync

# Sync everything — main + all folders
xtractr tweets sync --all

# Sync a specific folder
xtractr tweets sync --folder "Research"

# Sync all + auto-ingest tweets from configured folders
xtractr tweets sync --auto

# Fetch more, disable early stop on duplicates
xtractr tweets sync --count 500 --no-early-stop
```

#### Browse and export

```bash
# Interactive TUI — browse, filter, select, and export tweets
xtractr tweets select
```

TUI keybindings:

| Key | Action |
|-----|--------|
| `Tab` | Switch between folders and tweets panes |
| `Space` | Toggle selection (folder auto-ingest or tweet) |
| `a` / `n` | Select all / none |
| `i` | Import selected tweets to output directory |
| `u` | Un-import highlighted tweet |
| `f` | Cycle filter: all → folders → staged → imported |
| `q` | Quit (warns if staged tweets not imported) |

The TUI syncs new bookmarks in the background every 10 minutes.

#### Other commands

```bash
xtractr tweets status    # Cache stats: total, ingested, by folder, top authors
xtractr tweets folders   # List bookmark folders and auto-ingest settings
```

#### Export format

Exported tweets are markdown files with frontmatter:

```yaml
---
author: @handle
date: 2025-03-15
url: https://x.com/handle/status/123
type: tweet
lang: en
---
```

Non-English tweets are auto-translated (via Google Translate) with both translation and original text included. Media (images) are downloaded locally.

#### Custom paths

All commands accept `--db` and `--output-dir`:

```bash
xtractr tweets --db ./my-cache.db --output-dir ./raw/tweets sync --all
```

As a library:

```python
from pathlib import Path
from xtractr.tweets import get_db, export_tweet, cmd_sync_cli

db = get_db(Path("tweets.db"))
# ... query tweets, export, etc.
```

### extract — Auto-clip links from tweets

Scans imported tweets for URLs, resolves t.co shortlinks, and auto-clips papers, gists, and GitHub repos found in bookmarks. Also scrapes author reply threads for additional links.

```bash
# Preview what would be clipped
xtractr extract --dry-run

# Run full extraction (resolve links + clip)
xtractr extract

# Skip reply thread scraping (faster)
xtractr extract --skip-replies

# Custom paths
xtractr extract --db ./tweets.db --output-dir ./raw
```

Link types detected and clipped:

| Type | Detection | Action |
|------|-----------|--------|
| arxiv | `arxiv.org` URLs | Download PDF + markdown summary |
| PDF | URLs ending in `.pdf` | Download + markdown summary |
| GitHub gist | `gist.github.com` URLs | Fetch via API, save as markdown |
| GitHub repo | `github.com/owner/repo` URLs | Clip README via trafilatura |

A `_repos.md` index is auto-maintained with all GitHub repos found.

As a library:

```python
from pathlib import Path
from xtractr.extract import run_extract

clipped = run_extract(
    db_path=Path("tweets.db"),
    output_dir=Path("raw"),
    dry_run=False,
    skip_replies=True,
)
print(f"Clipped {clipped} items")
```

## Short aliases

If installed as a package, short CLI aliases are available:

| Alias | Equivalent |
|-------|-----------|
| `xc` | `xtractr clip` |
| `xt` | `xtractr tweets` |
| `xe` | `xtractr extract` |

## Dependencies

- **trafilatura** — web article extraction
- **pymupdf** + **pymupdf4llm** — PDF text extraction and markdown conversion
- **httpx** — HTTP client for downloads and API calls
- **playwright** — browser automation for X bookmark scraping
- **textual** — TUI framework for tweet selector
- **rich** — terminal formatting
- **deep-translator** — auto-translation of non-English tweets
