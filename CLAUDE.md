# xtractr — Standing Instructions for Claude Code

## What this repo is

xtractr is a standalone Python toolkit for extracting and archiving web content:
- **clip** — Fetch web articles (via trafilatura), convert PDFs (via pymupdf4llm),
  download arxiv papers. Outputs structured markdown with frontmatter.
- **tweets** — Sync Twitter/X bookmarks via Playwright, browse with a Textual TUI,
  export to markdown with media download and translation.
- **extract** — Resolve t.co shortened URLs from tweets, auto-clip papers, gists,
  and GitHub repos found in bookmarks.

All paths are parametric — no hardcoded project structure. Designed to be used both
as a library (`from xtractr.clip import clip_web`) and as a CLI (`xtractr clip <url>`).

## Package layout

```
src/xtractr/
  __init__.py     — version
  util.py         — slugify and shared helpers
  cli.py          — unified CLI dispatcher
  clip.py         — web/PDF/arxiv clipping
  tweets.py       — X bookmark sync, TUI, export
  extract.py      — tweet link resolution and auto-clipping
```

## Design principles

- **No global state** — all functions take explicit path parameters
- **Library-first** — CLI is a thin wrapper around importable functions
- **Minimal coupling** — each module can be used independently
- Playwright is used for X scraping (not twikit — it's unreliable)

## Dependencies

Core: trafilatura, pymupdf, pymupdf4llm, httpx, rich
Twitter: playwright, textual, deep-translator

## Running

```bash
uv run xtractr clip <url>
uv run xtractr tweets sync --all
uv run xtractr tweets select
uv run xtractr extract --dry-run
```

Or via short aliases: `xc`, `xt`, `xe`.
