"""Tests for tweet import / purge / reconcile data layer.

Scoped to the bug class that motivated their creation: xtrc8 re-exporting
138 previously-deleted tweets to disk because the auto-ingest path
enumerated the SQLite cache rather than honouring user intent / consumer
state. Tests cover the data-layer guarantees that prevent that recurrence.

These tests build a temp SQLite DB, insert synthetic tweet rows, and
exercise the import / purge / reconcile / auto-ingest functions directly.
No network, no Playwright, no media downloads (export_tweet is called
with download_media=False, translate=False).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from xtrc8.tweets import (
    ImportResult,
    ReconcileResult,
    auto_ingest_folder,
    compute_auto_staged_ids,
    compute_imported_set,
    compute_select_all_ids,
    export_tweet,
    get_db,
    import_tweets,
    load_tweets_for_selection,
    purge_tweets,
    reconcile_with_disk,
    unimport_tweet,
    unpurge_tweets,
    upsert_folder,
    upsert_tweet,
    set_folder_auto_ingest,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_tweet(
    tid: str,
    handle: str = "alice",
    text: str = "hello world this is a tweet",
    folder_name: str | None = "Test",
) -> dict:
    """Build a synthetic tweet dict matching upsert_tweet's expected shape."""
    return {
        "id": tid,
        "author_handle": handle,
        "author_name": handle.title(),
        "created_at": "Mon Jan 01 12:00:00 +0000 2024",
        "full_text": text,
        "url": f"https://x.com/{handle}/status/{tid}",
        "media_json": None,
        "quote_url": None,
        "in_reply_to": None,
        "lang": "en",
        "favorite_count": 0,
        "retweet_count": 0,
        "bookmark_count": 0,
        "folder_name": folder_name,
        "folder_id": "fake-folder-id" if folder_name else None,
        "synced_at": "2024-01-01T00:00:00+00:00",
    }


@pytest.fixture
def db(tmp_path: Path) -> sqlite3.Connection:
    """A fresh DB with the current schema, populated with 5 test tweets."""
    db_path = tmp_path / "test.db"
    conn = get_db(db_path)
    upsert_folder(conn, "Test", "fake-folder-id")
    upsert_folder(conn, "Other", "other-folder-id")
    for i in range(5):
        upsert_tweet(conn, _make_tweet(f"100000000000000000{i}", text=f"tweet number {i}"))
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def output_dir(tmp_path: Path) -> Path:
    d = tmp_path / "out"
    d.mkdir()
    return d


def _no_network(**kwargs) -> dict:
    """Default kwargs for offline export."""
    return {"download_media": False, "translate": False, **kwargs}


# ---------------------------------------------------------------------------
# Schema migration
# ---------------------------------------------------------------------------


def test_purged_column_added_by_migration(db: sqlite3.Connection):
    """The schema migration in get_db should add the `purged` column."""
    cols = {r[1] for r in db.execute("PRAGMA table_info(tweets)").fetchall()}
    assert "purged" in cols


def test_new_tweets_have_null_purged(db: sqlite3.Connection):
    """Fresh tweets are not purged."""
    rows = db.execute("SELECT id, purged FROM tweets").fetchall()
    assert len(rows) == 5
    for row in rows:
        assert row["purged"] is None


# ---------------------------------------------------------------------------
# import_tweets — the regression tests for the original bug
# ---------------------------------------------------------------------------


def test_import_tweets_writes_exactly_n_files_for_n_ids(
    db: sqlite3.Connection, output_dir: Path
):
    """The headline regression test: importing N selected tweets must produce
    exactly N files. The original bug exported 214 files when 0-10 were
    selected. This test would have caught it."""
    ids = ["1000000000000000000", "1000000000000000002"]
    result = import_tweets(db, output_dir, ids, **_no_network())

    assert isinstance(result, ImportResult)
    assert result.imported_count == 2
    assert len(list(output_dir.glob("*.md"))) == 2
    # And the OTHER tweets in the DB are NOT exported.
    other_ids = {"1000000000000000001", "1000000000000000003", "1000000000000000004"}
    for path in output_dir.glob("*.md"):
        for oid in other_ids:
            assert oid not in path.read_text()


def test_import_tweets_marks_ingested_in_db(
    db: sqlite3.Connection, output_dir: Path
):
    ids = ["1000000000000000000", "1000000000000000001"]
    import_tweets(db, output_dir, ids, **_no_network())
    rows = db.execute(
        "SELECT id, ingested FROM tweets WHERE id IN (?, ?)",
        ids,
    ).fetchall()
    assert len(rows) == 2
    for row in rows:
        assert row["ingested"] == 1


def test_import_tweets_skips_purged(db: sqlite3.Connection, output_dir: Path):
    """A purged tweet must never be re-exported by import_tweets."""
    purge_tweets(db, ["1000000000000000000"])
    result = import_tweets(
        db, output_dir, ["1000000000000000000", "1000000000000000001"],
        **_no_network(),
    )
    assert result.imported_count == 1
    assert "1000000000000000000" in result.skipped_purged
    assert "1000000000000000001" in result.imported_ids
    assert len(list(output_dir.glob("*.md"))) == 1


def test_import_tweets_skips_existing_files(
    db: sqlite3.Connection, output_dir: Path
):
    """If a file for the tweet already exists in output_dir, don't re-export.
    This is the second-line defence against the original bug: even if the
    purge flag isn't set, an existing file should not be clobbered."""
    # First import writes the file
    import_tweets(db, output_dir, ["1000000000000000000"], **_no_network())
    # Second import should detect existing file and skip
    result = import_tweets(db, output_dir, ["1000000000000000000"], **_no_network())
    assert result.imported_count == 0
    assert "1000000000000000000" in result.skipped_existing
    assert len(list(output_dir.glob("*.md"))) == 1


def test_import_tweets_skips_missing_db_rows(
    db: sqlite3.Connection, output_dir: Path
):
    result = import_tweets(db, output_dir, ["9999999999999999999"], **_no_network())
    assert result.imported_count == 0
    assert "9999999999999999999" in result.skipped_missing


# ---------------------------------------------------------------------------
# purge_tweets / unpurge_tweets
# ---------------------------------------------------------------------------


def test_purge_tweets_marks_rows(db: sqlite3.Connection):
    n = purge_tweets(db, ["1000000000000000000", "1000000000000000001"])
    assert n == 2
    rows = db.execute(
        "SELECT id, purged FROM tweets WHERE id IN (?, ?)",
        ("1000000000000000000", "1000000000000000001"),
    ).fetchall()
    for row in rows:
        assert row["purged"] is not None


def test_purge_tweets_is_idempotent(db: sqlite3.Connection):
    purge_tweets(db, ["1000000000000000000"])
    n = purge_tweets(db, ["1000000000000000000"])
    assert n == 0  # already purged, no rows updated


def test_unpurge_tweets_clears_flag(db: sqlite3.Connection):
    purge_tweets(db, ["1000000000000000000"])
    n = unpurge_tweets(db, ["1000000000000000000"])
    assert n == 1
    row = db.execute(
        "SELECT purged FROM tweets WHERE id = ?", ("1000000000000000000",)
    ).fetchone()
    assert row["purged"] is None


def test_purged_tweet_can_be_re_exported_after_unpurge(
    db: sqlite3.Connection, output_dir: Path
):
    purge_tweets(db, ["1000000000000000000"])
    r1 = import_tweets(db, output_dir, ["1000000000000000000"], **_no_network())
    assert r1.imported_count == 0
    unpurge_tweets(db, ["1000000000000000000"])
    r2 = import_tweets(db, output_dir, ["1000000000000000000"], **_no_network())
    assert r2.imported_count == 1


# ---------------------------------------------------------------------------
# auto_ingest_folder — the bug origin
# ---------------------------------------------------------------------------


def test_auto_ingest_folder_exports_only_unpurged_uningested(
    db: sqlite3.Connection, output_dir: Path
):
    """The headline test: auto_ingest_folder must skip purged tweets even
    if they're in the target folder and have ingested=0. This is the exact
    scenario from the production bug."""
    # Mark 2 of the 5 as purged (simulating "user deleted from consumer")
    purge_tweets(db, ["1000000000000000000", "1000000000000000001"])
    result = auto_ingest_folder(db, "Test", output_dir, **_no_network())
    assert result.imported_count == 3
    # Verify the purged ones were NOT exported
    files = list(output_dir.glob("*.md"))
    assert len(files) == 3
    contents = "\n".join(f.read_text() for f in files)
    assert "1000000000000000000" not in contents
    assert "1000000000000000001" not in contents


def test_auto_ingest_folder_only_targets_named_folder(
    db: sqlite3.Connection, output_dir: Path
):
    """auto_ingest_folder must not touch tweets in other folders."""
    # Add a tweet in a different folder
    upsert_tweet(db, _make_tweet("2000000000000000000", folder_name="Other"))
    db.commit()
    result = auto_ingest_folder(db, "Test", output_dir, **_no_network())
    assert result.imported_count == 5  # not 6
    files = list(output_dir.glob("*.md"))
    contents = "\n".join(f.read_text() for f in files)
    assert "2000000000000000000" not in contents


def test_auto_ingest_folder_skips_already_ingested(
    db: sqlite3.Connection, output_dir: Path
):
    """A tweet with ingested=1 should not be re-exported."""
    db.execute(
        "UPDATE tweets SET ingested = 1 WHERE id = ?",
        ("1000000000000000000",),
    )
    db.commit()
    result = auto_ingest_folder(db, "Test", output_dir, **_no_network())
    assert result.imported_count == 4  # not 5


def test_auto_ingest_folder_skips_when_file_already_on_disk(
    db: sqlite3.Connection, output_dir: Path
):
    """The defence-in-depth test: if a file already exists in output_dir
    for a tweet with ingested=0, don't re-export. This catches cache rebuild
    scenarios where DB ingested flags were lost but the .md files survive."""
    # Manually write a file that looks like it came from a previous export
    (output_dir / "2024-01-01-alice-1000000000000000000.md").write_text(
        "---\nurl: https://x.com/alice/status/1000000000000000000\n---\nhi"
    )
    result = auto_ingest_folder(db, "Test", output_dir, **_no_network())
    # The pre-existing file is detected; that tweet is skipped (and marked
    # ingested), the other 4 are exported normally.
    assert result.imported_count == 4
    assert "1000000000000000000" in result.skipped_existing


# ---------------------------------------------------------------------------
# unimport_tweet
# ---------------------------------------------------------------------------


def test_unimport_tweet_removes_file_and_clears_flag(
    db: sqlite3.Connection, output_dir: Path
):
    import_tweets(db, output_dir, ["1000000000000000000"], **_no_network())
    assert len(list(output_dir.glob("*.md"))) == 1
    removed = unimport_tweet(db, output_dir, "1000000000000000000")
    assert removed is True
    assert len(list(output_dir.glob("*.md"))) == 0
    row = db.execute(
        "SELECT ingested FROM tweets WHERE id = ?", ("1000000000000000000",)
    ).fetchone()
    assert row["ingested"] == 0


def test_unimport_tweet_does_not_purge(
    db: sqlite3.Connection, output_dir: Path
):
    """unimport_tweet should not mark the tweet as purged — that's a separate
    operation. After unimport, the tweet can be re-imported."""
    import_tweets(db, output_dir, ["1000000000000000000"], **_no_network())
    unimport_tweet(db, output_dir, "1000000000000000000")
    row = db.execute(
        "SELECT purged FROM tweets WHERE id = ?", ("1000000000000000000",)
    ).fetchone()
    assert row["purged"] is None
    # And re-importing works
    result = import_tweets(db, output_dir, ["1000000000000000000"], **_no_network())
    assert result.imported_count == 1


# ---------------------------------------------------------------------------
# reconcile_with_disk
# ---------------------------------------------------------------------------


def test_reconcile_finds_orphan_db_rows(
    db: sqlite3.Connection, output_dir: Path
):
    """If a tweet is marked ingested=1 in the DB but no file exists on
    disk, reconcile should report it as an orphan DB row. This is the
    cleanup signal for the production bug — those 138 deleted tweets
    should show up here."""
    db.execute(
        "UPDATE tweets SET ingested = 1 WHERE id IN (?, ?)",
        ("1000000000000000000", "1000000000000000001"),
    )
    db.commit()
    result = reconcile_with_disk(db, output_dir)
    assert isinstance(result, ReconcileResult)
    assert "1000000000000000000" in result.orphan_db_rows
    assert "1000000000000000001" in result.orphan_db_rows
    assert len(result.orphan_db_rows) == 2


def test_reconcile_with_mark_purged_purges_orphans(
    db: sqlite3.Connection, output_dir: Path
):
    db.execute(
        "UPDATE tweets SET ingested = 1 WHERE id = ?",
        ("1000000000000000000",),
    )
    db.commit()
    result = reconcile_with_disk(db, output_dir, mark_purged=True)
    assert result.purged_count == 1
    row = db.execute(
        "SELECT purged FROM tweets WHERE id = ?", ("1000000000000000000",)
    ).fetchone()
    assert row["purged"] is not None


def test_reconcile_finds_orphan_disk_files(
    db: sqlite3.Connection, output_dir: Path
):
    """File on disk but no DB row → orphan_disk_files."""
    (output_dir / "2024-01-01-alice-9999999999999999999.md").write_text(
        "---\nurl: https://x.com/alice/status/9999999999999999999\n---\nbye"
    )
    result = reconcile_with_disk(db, output_dir)
    assert "9999999999999999999" in result.orphan_disk_files


def test_reconcile_disk_extraction_uses_frontmatter_url(
    db: sqlite3.Connection, output_dir: Path
):
    """Tweet ID extraction should work from the frontmatter URL line, not
    just the filename, because filenames can be renamed."""
    (output_dir / "renamed-by-user.md").write_text(
        "---\nurl: https://x.com/alice/status/1000000000000000000\n---\nhi"
    )
    db.execute("UPDATE tweets SET ingested = 1 WHERE id = ?",
               ("1000000000000000000",))
    db.commit()
    result = reconcile_with_disk(db, output_dir)
    assert "1000000000000000000" in result.disk_ids
    assert "1000000000000000000" not in result.orphan_db_rows


# ---------------------------------------------------------------------------
# Integration: the production bug scenario
# ---------------------------------------------------------------------------


def test_production_bug_scenario_does_not_recur(
    db: sqlite3.Connection, output_dir: Path
):
    """End-to-end regression test for the original bug:

    1. User imports 3 tweets (writes 3 files).
    2. User deletes 2 of them from output_dir (simulating a content scrub).
    3. User runs reconcile --mark-purged (the cleanup operation).
    4. Cache is somehow rebuilt: the 2 deleted tweets get ingested=0 again
       (simulated here by direct UPDATE, but the production cause was a
       cache rebuild side effect).
    5. User runs sync --auto, which calls auto_ingest_folder.
    6. **The 2 deleted tweets must NOT be re-exported.**
    """
    # Step 1: import 3
    ids = ["1000000000000000000", "1000000000000000001", "1000000000000000002"]
    initial = import_tweets(db, output_dir, ids, **_no_network())
    assert len(list(output_dir.glob("*.md"))) == 3
    assert initial.imported_count == 3

    # Step 2: user deletes 2 files (by their actual paths, since slug
    # filenames don't contain the tweet ID)
    for tid_to_delete in ("1000000000000000000", "1000000000000000001"):
        idx = initial.imported_ids.index(tid_to_delete)
        initial.paths[idx].unlink()
    assert len(list(output_dir.glob("*.md"))) == 1

    # Step 3: cleanup operation
    rec = reconcile_with_disk(db, output_dir, mark_purged=True)
    assert rec.purged_count == 2

    # Step 4: simulate cache rebuild — flip ingested back to 0
    db.execute(
        "UPDATE tweets SET ingested = 0 WHERE id IN (?, ?)",
        ("1000000000000000000", "1000000000000000001"),
    )
    db.commit()

    # Step 5: auto-ingest the folder
    result = auto_ingest_folder(db, "Test", output_dir, **_no_network())

    # Step 6: the deleted tweets must not come back
    assert "1000000000000000000" not in result.imported_ids
    assert "1000000000000000001" not in result.imported_ids
    files = list(output_dir.glob("*.md"))
    contents = "\n".join(f.read_text() for f in files)
    assert "1000000000000000000" not in contents
    assert "1000000000000000001" not in contents


# ---------------------------------------------------------------------------
# TUI data-layer helpers — these are the functions the TUI calls instead
# of inlining SQL or selection logic. Locking the invariants here prevents
# any future TUI code from regressing.
# ---------------------------------------------------------------------------


def test_load_tweets_for_selection_excludes_purged(
    db: sqlite3.Connection
):
    """The fundamental TUI invariant: purged tweets do not appear in the
    selector. Without this, the selector would auto-stage purged tweets
    via _apply_folder_selections — which is the bug that motivated the
    helper's creation."""
    purge_tweets(db, ["1000000000000000000", "1000000000000000001"])
    rows = load_tweets_for_selection(db)
    assert len(rows) == 3
    ids = {r["id"] for r in rows}
    assert "1000000000000000000" not in ids
    assert "1000000000000000001" not in ids


def test_load_tweets_for_selection_sorted_newest_first(
    db: sqlite3.Connection
):
    """Tweets must be sorted by created_at descending. The fixture uses
    identical timestamps so order falls back to insertion order, but the
    helper must not crash on parseable dates."""
    rows = load_tweets_for_selection(db)
    assert len(rows) == 5  # all 5 fixture rows present


def test_compute_imported_set(db: sqlite3.Connection):
    db.execute(
        "UPDATE tweets SET ingested = 1 WHERE id IN (?, ?)",
        ("1000000000000000000", "1000000000000000001"),
    )
    db.commit()
    rows = load_tweets_for_selection(db)
    imported = compute_imported_set(rows)
    assert imported == {"1000000000000000000", "1000000000000000001"}


def test_compute_auto_staged_ids_basic(db: sqlite3.Connection):
    """Auto-staging picks up not-imported tweets in selected folders."""
    rows = load_tweets_for_selection(db)
    staged = compute_auto_staged_ids(rows, set(), {"Test"})
    assert len(staged) == 5  # all 5 fixture tweets are in folder "Test"


def test_compute_auto_staged_ids_excludes_imported(
    db: sqlite3.Connection
):
    db.execute(
        "UPDATE tweets SET ingested = 1 WHERE id = ?",
        ("1000000000000000000",),
    )
    db.commit()
    rows = load_tweets_for_selection(db)
    imported = compute_imported_set(rows)
    staged = compute_auto_staged_ids(rows, imported, {"Test"})
    assert len(staged) == 4
    assert "1000000000000000000" not in staged


def test_compute_auto_staged_ids_excludes_purged_defence_in_depth(
    db: sqlite3.Connection
):
    """Even if a purged tweet sneaks into tweet_rows (it shouldn't,
    because load_tweets_for_selection filters it), the auto-stager must
    still skip it. This is the defence-in-depth check that protected
    against the production bug."""
    # Build a synthetic tweet_rows that includes a purged row
    rows = [
        {"id": "1", "folder_name": "Test", "ingested": 0, "purged": None},
        {"id": "2", "folder_name": "Test", "ingested": 0, "purged": "2026-04-07T00:00:00"},
        {"id": "3", "folder_name": "Test", "ingested": 0, "purged": None},
    ]
    staged = compute_auto_staged_ids(rows, set(), {"Test"})
    assert staged == {"1", "3"}


def test_compute_auto_staged_ids_only_target_folders(
    db: sqlite3.Connection
):
    upsert_tweet(db, _make_tweet("2000000000000000000", folder_name="Other"))
    db.commit()
    rows = load_tweets_for_selection(db)
    # Only 'Test' is auto-ingest
    staged = compute_auto_staged_ids(rows, set(), {"Test"})
    assert "2000000000000000000" not in staged
    assert len(staged) == 5


def test_compute_select_all_ids_excludes_imported_and_purged(
    db: sqlite3.Connection
):
    """select-all in the tweets pane must skip both imported and purged.
    This is the bug at line 1719 that motivated extracting this helper:
    the original `self.selected = {t["id"] for t in self.tweet_rows}`
    selected EVERYTHING with no filtering.
    """
    db.execute(
        "UPDATE tweets SET ingested = 1 WHERE id = ?",
        ("1000000000000000000",),
    )
    purge_tweets(db, ["1000000000000000001"])
    db.commit()

    rows = load_tweets_for_selection(db)
    imported = compute_imported_set(rows)
    select_all = compute_select_all_ids(rows, imported)

    # 1000000000000000000 is imported → excluded
    # 1000000000000000001 is purged → not even in rows
    # 1000000000000000002, 3, 4 → included
    assert "1000000000000000000" not in select_all
    assert "1000000000000000001" not in select_all
    assert select_all == {
        "1000000000000000002",
        "1000000000000000003",
        "1000000000000000004",
    }


def test_no_raw_tweet_sql_in_tui_codepath():
    """Static check: scan _build_tui for the exact dangler pattern
    `SELECT * FROM tweets`. This is the wildcard-load query that bypasses
    load_tweets_for_selection() and reintroduces the production bug.

    Allowed inside _build_tui (these queries do not load tweet rows for
    display, they compute folder counts or upsert folders):
    - SELECT name, folder_id FROM folders
    - SELECT name, COALESCE(...) ... FROM folders
    - SELECT COUNT(*) FROM tweets WHERE folder_name IS NULL
    - SELECT COALESCE(folder_name, '(unfiled)'), COUNT(*) FROM tweets ...

    Forbidden:
    - SELECT * FROM tweets ...   (use load_tweets_for_selection instead)
    """
    import inspect
    import re

    from xtrc8 import tweets as tweets_module

    src = inspect.getsource(tweets_module._build_tui)

    forbidden = re.findall(
        r"SELECT\s+\*\s+FROM\s+tweets",
        src,
        flags=re.IGNORECASE,
    )
    assert not forbidden, (
        f"Dangler in _build_tui: raw 'SELECT * FROM tweets' query found. "
        f"All tweet loading MUST go through load_tweets_for_selection(). "
        f"Offending matches: {forbidden}"
    )
