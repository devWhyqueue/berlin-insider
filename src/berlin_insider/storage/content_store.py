from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

from berlin_insider.fetcher.sources import SOURCES
from berlin_insider.parser.models import ParseRunResult
from berlin_insider.storage.sqlite import ensure_schema, now_utc_iso, sqlite_connection


def upsert_source_websites(db_path: Path) -> None:
    """Persist the configured source website registry."""
    ensure_schema(db_path)
    rows = [
        (
            source_id.value,
            adapter.definition.source_url,
            adapter.__class__.__name__,
            now_utc_iso(),
        )
        for source_id, adapter in SOURCES.items()
    ]
    with sqlite_connection(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO source_websites (source_id, source_url, adapter_kind, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
                source_url = excluded.source_url,
                adapter_kind = excluded.adapter_kind,
                updated_at = excluded.updated_at
            """,
            rows,
        )
        conn.commit()


def persist_parse_run(db_path: Path, parse_result: ParseRunResult) -> str:
    """Persist one parse run snapshot and all parsed items."""
    ensure_schema(db_path)
    run_id = _build_run_id(parse_result.finished_at.isoformat())
    with sqlite_connection(db_path) as conn:
        _insert_parse_run(conn, run_id, parse_result)
        _insert_parsed_items(conn, run_id, parse_result)
        conn.commit()
    return run_id


def _insert_parse_run(conn, run_id: str, parse_result: ParseRunResult) -> None:
    conn.execute(
        """
        INSERT INTO parse_runs (run_id, started_at, finished_at, total_items, failed_sources_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            run_id,
            parse_result.started_at.isoformat(),
            parse_result.finished_at.isoformat(),
            parse_result.total_items,
            json.dumps([source_id.value for source_id in parse_result.failed_sources]),
        ),
    )


def _insert_parsed_items(conn, run_id: str, parse_result: ParseRunResult) -> None:
    conn.executemany(
        """
        INSERT INTO parsed_items (
            run_id,
            source_id,
            item_url,
            title,
            description,
            detail_text,
            summary,
            event_start_at,
            event_end_at,
            location,
            category,
            category_confidence,
            weekend_relevance,
            weekend_confidence,
            parse_notes_json,
            raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        _parsed_item_rows(run_id, parse_result),
    )


def _parsed_item_rows(run_id: str, parse_result: ParseRunResult) -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    for source_result in parse_result.results:
        for item in source_result.items:
            rows.append(
                (
                    run_id,
                    item.source_id.value,
                    item.item_url,
                    item.title,
                    item.description,
                    item.detail_text,
                    item.summary,
                    item.event_start_at.isoformat() if item.event_start_at else None,
                    item.event_end_at.isoformat() if item.event_end_at else None,
                    item.location,
                    item.category.value,
                    item.category_confidence,
                    item.weekend_relevance.value,
                    item.weekend_confidence,
                    json.dumps(item.parse_notes, ensure_ascii=False),
                    json.dumps(item.raw, ensure_ascii=False, default=str),
                )
            )
    return rows


def _build_run_id(finished_at_iso: str) -> str:
    return f"parse-{finished_at_iso}-{uuid4().hex[:10]}"
