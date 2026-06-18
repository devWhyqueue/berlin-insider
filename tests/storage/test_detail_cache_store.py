from __future__ import annotations

from pathlib import Path

from berlin_insider.storage.detail_cache import SqliteDetailCacheStore


def _store(db_path: Path) -> SqliteDetailCacheStore:
    return SqliteDetailCacheStore(db_path)


def test_detail_cache_upsert_and_get_round_trip(tmp_path: Path) -> None:
    store = _store(tmp_path / "cache.db")
    store.upsert_detail(
        url="https://example.com/event?utm_source=test",
        source_id="mitvergnuegen",
        detail_text="Detail body",
        detail_hash="abc123",
        detail_metadata={"start_date": "2026-03-14", "end_date": "2026-03-15"},
        detail_status="ok",
    )
    entry = store.get("https://example.com/event")
    assert entry is not None
    assert entry.canonical_url == "https://example.com/event"
    assert entry.detail_text == "Detail body"
    assert entry.detail_hash == "abc123"
    assert entry.detail_metadata == {"start_date": "2026-03-14", "end_date": "2026-03-15"}
    assert entry.summary is None


def test_detail_cache_summary_upsert_guarded_by_hash(tmp_path: Path) -> None:
    store = _store(tmp_path / "cache.db")
    store.upsert_detail(
        url="https://example.com/event",
        source_id="mitvergnuegen",
        detail_text="Detail body",
        detail_hash="abc123",
        detail_metadata={},
        detail_status="ok",
    )
    store.upsert_summary(
        url="https://example.com/event",
        detail_hash="wrong-hash",
        summary="Should not persist",
    )
    unchanged = store.get("https://example.com/event")
    assert unchanged is not None
    assert unchanged.summary is None

    store.upsert_summary(
        url="https://example.com/event",
        detail_hash="abc123",
        summary="Persisted summary",
    )
    changed = store.get("https://example.com/event")
    assert changed is not None
    assert changed.summary == "Persisted summary"


def test_detail_cache_clears_summary_when_hash_changes(tmp_path: Path) -> None:
    store = _store(tmp_path / "cache.db")
    store.upsert_detail(
        url="https://example.com/event",
        source_id="mitvergnuegen",
        detail_text="Detail body",
        detail_hash="abc123",
        detail_metadata={"start_date": "2026-03-14"},
        detail_status="ok",
    )
    store.upsert_summary(
        url="https://example.com/event",
        detail_hash="abc123",
        summary="Persisted summary",
    )
    store.upsert_detail(
        url="https://example.com/event",
        source_id="mitvergnuegen",
        detail_text="Changed body",
        detail_hash="def456",
        detail_metadata={"start_date": "2026-03-20"},
        detail_status="ok",
    )
    entry = store.get("https://example.com/event")
    assert entry is not None
    assert entry.summary is None
    assert entry.detail_metadata == {"start_date": "2026-03-20"}
