from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from berlin_insider.fetcher.models import SourceId
from berlin_insider.fetcher.sources import SOURCES
from berlin_insider.parser.models import (
    ParseRunResult,
    ParseStatus,
    ParsedCategory,
    ParsedItem,
    SourceParseResult,
    WeekendRelevance,
)
from berlin_insider.storage.sqlite import sqlite_connection
from berlin_insider.storage.url_normalize import canonicalize_url
from berlin_insider.storage.item_store import persist_items, upsert_source_websites


def test_upsert_source_websites_inserts_configured_sources(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    upsert_source_websites(db_path)
    with sqlite_connection(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) FROM sources").fetchone()
    assert row is not None
    assert row[0] == len(SOURCES)


def test_persist_items_upserts_deduplicated_items(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    parse_result = ParseRunResult(
        started_at=datetime(2026, 2, 28, 8, 0, tzinfo=UTC),
        finished_at=datetime(2026, 2, 28, 8, 1, tzinfo=UTC),
        results=[
            SourceParseResult(
                source_id=SourceId.MITVERGNUEGEN,
                status=ParseStatus.SUCCESS,
                items=[
                    ParsedItem(
                        source_id=SourceId.MITVERGNUEGEN,
                        item_url="https://example.com/item?utm_source=test",
                        title="Item",
                        description="Desc",
                        summary="Summary",
                        event_start_at=datetime(2026, 2, 28, 18, 0, tzinfo=UTC),
                        event_end_at=None,
                        location="Berlin",
                        category=ParsedCategory.EVENT,
                        category_confidence=0.9,
                        weekend_relevance=WeekendRelevance.LIKELY_THIS_WEEKEND,
                        weekend_confidence=0.9,
                    )
                ],
                warnings=[],
                error_message=None,
                duration_ms=10,
            )
        ],
        total_items=1,
        failed_sources=[],
    )

    persist_items(db_path, parse_result)
    persist_items(db_path, parse_result)

    with sqlite_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT source_id, canonical_url, title, summary
            FROM items
            """
        ).fetchone()
        count_row = conn.execute("SELECT COUNT(*) FROM items").fetchone()
    assert row is not None
    assert count_row is not None
    assert count_row[0] == 1
    assert row[0] == SourceId.MITVERGNUEGEN.value
    assert row[1] == canonicalize_url("https://example.com/item?utm_source=test")
    assert row[2] == "Item"
    assert row[3] == "Summary"
