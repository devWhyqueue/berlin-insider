from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from berlin_insider.fetcher.sources import SOURCES
from berlin_insider.parser.models import ParsedCategory, ParsedItem, ParseRunResult
from berlin_insider.storage.sqlite import ensure_schema, now_utc_iso, sqlite_connection
from berlin_insider.storage.url_normalize import canonicalize_url


@dataclass(slots=True)
class ItemRecord:
    item_id: int
    canonical_url: str
    source_id: str
    title: str | None
    description: str | None
    summary: str | None
    event_start_at: str | None
    event_end_at: str | None
    location: str | None
    category: ParsedCategory | None


class SqliteItemStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        ensure_schema(self._db_path)

    def upsert_parse_result(self, parse_result: ParseRunResult) -> None:
        """Upsert every parsed item from one parse result into durable storage."""
        now = now_utc_iso()
        with sqlite_connection(self._db_path) as conn:
            for source_result in parse_result.results:
                for item in source_result.items:
                    _upsert_item(conn, item=item, now=now)
            conn.commit()

    def upsert_item(self, item: ParsedItem) -> None:
        """Upsert one parsed item into durable storage."""
        now = now_utc_iso()
        with sqlite_connection(self._db_path) as conn:
            _upsert_item(conn, item=item, now=now)
            conn.commit()

    def get_by_url(self, url: str) -> ItemRecord | None:
        """Return one durable item record by canonical URL."""
        canonical_url = canonicalize_url(url)
        with sqlite_connection(self._db_path) as conn:
            row = conn.execute(
                """
                SELECT
                    item_id,
                    canonical_url,
                    source_id,
                    original_url,
                    title,
                    description,
                    summary,
                    event_start_at,
                    event_end_at,
                    location,
                    category
                FROM items
                WHERE canonical_url = ?
                LIMIT 1
                """,
                (canonical_url,),
            ).fetchone()
        return _row_to_item_record(row)


def upsert_source_websites(db_path: Path) -> None:
    """Persist the configured source registry into the sources table."""
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
            INSERT INTO sources (source_id, source_url, adapter_kind, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
                source_url = excluded.source_url,
                adapter_kind = excluded.adapter_kind,
                updated_at = excluded.updated_at
            """,
            rows,
        )
        conn.commit()


def persist_items(db_path: Path, parse_result: ParseRunResult) -> None:
    """Upsert parsed items into the durable items table."""
    SqliteItemStore(db_path).upsert_parse_result(parse_result)


def _item_values(*, item: ParsedItem, now: str) -> tuple[object, ...]:
    return (
        canonicalize_url(item.item_url),
        item.source_id.value,
        item.item_url,
        item.title,
        item.description,
        item.summary,
        item.event_start_at.isoformat() if item.event_start_at is not None else None,
        item.event_end_at.isoformat() if item.event_end_at is not None else None,
        item.location,
        item.category.value,
        item.category_confidence,
        item.weekend_relevance.value,
        item.weekend_confidence,
        now,
        now,
    )


def _upsert_item(conn, *, item: ParsedItem, now: str) -> None:  # noqa: ANN001
    _ensure_source_exists(conn, source_id=item.source_id.value, now=now)
    conn.execute(
        """
        INSERT INTO items (
            canonical_url,
            source_id,
            original_url,
            title,
            description,
            summary,
            event_start_at,
            event_end_at,
            location,
            category,
            category_confidence,
            weekend_relevance,
            weekend_confidence,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(canonical_url) DO UPDATE SET
            source_id = excluded.source_id,
            original_url = excluded.original_url,
            title = excluded.title,
            description = excluded.description,
            summary = excluded.summary,
            event_start_at = excluded.event_start_at,
            event_end_at = excluded.event_end_at,
            location = excluded.location,
            category = excluded.category,
            category_confidence = excluded.category_confidence,
            weekend_relevance = excluded.weekend_relevance,
            weekend_confidence = excluded.weekend_confidence,
            updated_at = excluded.updated_at
        """,
        _item_values(item=item, now=now),
    )


def _ensure_source_exists(conn, *, source_id: str, now: str) -> None:  # noqa: ANN001
    conn.execute(
        """
        INSERT OR IGNORE INTO sources (source_id, source_url, adapter_kind, updated_at)
        VALUES (?, ?, ?, ?)
        """,
        (source_id, f"https://{source_id}", "derived", now),
    )


def _row_to_item_record(row: tuple[object, ...] | None) -> ItemRecord | None:
    if row is None:
        return None
    item_id = row[0]
    if not isinstance(item_id, int):
        return None
    category_raw = str(row[10]).strip() if row[10] is not None else None
    category = ParsedCategory(category_raw) if category_raw else None
    return ItemRecord(
        item_id=item_id,
        canonical_url=str(row[1]),
        source_id=str(row[2]),
        title=str(row[4]) if row[4] is not None else None,
        description=str(row[5]) if row[5] is not None else None,
        summary=str(row[6]) if row[6] is not None else None,
        event_start_at=str(row[7]) if row[7] is not None else None,
        event_end_at=str(row[8]) if row[8] is not None else None,
        location=str(row[9]) if row[9] is not None else None,
        category=category,
    )
