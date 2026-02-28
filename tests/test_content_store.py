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
from berlin_insider.storage.content_store import persist_parse_run, upsert_source_websites
from berlin_insider.storage.sqlite import sqlite_connection


def test_upsert_source_websites_inserts_configured_sources(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    upsert_source_websites(db_path)
    with sqlite_connection(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) FROM source_websites").fetchone()
    assert row is not None
    assert row[0] == len(SOURCES)


def test_persist_parse_run_stores_run_and_items(tmp_path: Path) -> None:
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
                        item_url="https://example.com/item",
                        title="Item",
                        description="Desc",
                        detail_text="Long detail text",
                        event_start_at=datetime(2026, 2, 28, 18, 0, tzinfo=UTC),
                        event_end_at=None,
                        location="Berlin",
                        category=ParsedCategory.EVENT,
                        category_confidence=0.9,
                        weekend_relevance=WeekendRelevance.LIKELY_THIS_WEEKEND,
                        weekend_confidence=0.9,
                        parse_notes=["ok"],
                        raw={"fetch_method": "rss"},
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
    run_id = persist_parse_run(db_path, parse_result)

    with sqlite_connection(db_path) as conn:
        run_row = conn.execute("SELECT run_id, total_items FROM parse_runs").fetchone()
        item_row = conn.execute(
            "SELECT source_id, item_url, detail_text FROM parsed_items WHERE run_id = ?",
            (run_id,),
        ).fetchone()
    assert run_row is not None
    assert run_row[0] == run_id
    assert run_row[1] == 1
    assert item_row is not None
    assert item_row[0] == SourceId.MITVERGNUEGEN.value
    assert item_row[1] == "https://example.com/item"
    assert item_row[2] == "Long detail text"
