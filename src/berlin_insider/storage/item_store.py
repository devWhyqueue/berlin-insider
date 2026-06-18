from __future__ import annotations

from pathlib import Path

from berlin_insider.fetcher.models import SourceId
from berlin_insider.fetcher.sources import SOURCES
from berlin_insider.parser.models import ParsedItem, ParseRunResult
from berlin_insider.storage.item_record import ItemRecord, row_to_item_record
from berlin_insider.storage.sqlite import ensure_schema, now_utc_iso, sqlite_connection
from berlin_insider.storage.url_normalize import canonicalize_url

_GET_ITEM_BY_URL_SQL = """
SELECT
    item_id, canonical_url, source_id, original_url, title, description, clean_text,
    summary, event_start_at, event_end_at, event_date_source, location, price_text,
    price_amount, price_currency, is_free, category
FROM items
WHERE canonical_url = ?
LIMIT 1
"""


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
            row = conn.execute(_GET_ITEM_BY_URL_SQL, (canonical_url,)).fetchone()
        return row_to_item_record(row)


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
        item.clean_text,
        item.summary,
        item.event_start_at.isoformat() if item.event_start_at is not None else None,
        item.event_end_at.isoformat() if item.event_end_at is not None else None,
        item.event_date_source,
        item.location,
        item.price_text,
        item.price_amount,
        item.price_currency,
        int(item.is_free) if item.is_free is not None else None,
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
            clean_text,
            summary,
            event_start_at,
            event_end_at,
            event_date_source,
            location,
            price_text,
            price_amount,
            price_currency,
            is_free,
            category,
            category_confidence,
            weekend_relevance,
            weekend_confidence,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(canonical_url) DO UPDATE SET
            source_id = excluded.source_id,
            original_url = excluded.original_url,
            title = excluded.title,
            description = excluded.description,
            clean_text = excluded.clean_text,
            summary = excluded.summary,
            event_start_at = excluded.event_start_at,
            event_end_at = excluded.event_end_at,
            event_date_source = excluded.event_date_source,
            location = excluded.location,
            price_text = excluded.price_text,
            price_amount = excluded.price_amount,
            price_currency = excluded.price_currency,
            is_free = excluded.is_free,
            category = excluded.category,
            category_confidence = excluded.category_confidence,
            weekend_relevance = excluded.weekend_relevance,
            weekend_confidence = excluded.weekend_confidence,
            updated_at = excluded.updated_at
        """,
        _item_values(item=item, now=now),
    )


def _ensure_source_exists(conn, *, source_id: str, now: str) -> None:  # noqa: ANN001
    configured = _configured_source(source_id)
    if configured is not None:
        conn.execute(
            """
            INSERT INTO sources (source_id, source_url, adapter_kind, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
                source_url = excluded.source_url,
                adapter_kind = excluded.adapter_kind,
                updated_at = excluded.updated_at
            """,
            (
                source_id,
                configured.definition.source_url,
                configured.__class__.__name__,
                now,
            ),
        )
        return
    conn.execute(
        """
        INSERT OR IGNORE INTO sources (source_id, source_url, adapter_kind, updated_at)
        VALUES (?, ?, ?, ?)
        """,
        (source_id, f"https://{source_id}", "derived", now),
    )


def _configured_source(source_id: str):  # noqa: ANN202
    try:
        source_key = SourceId(source_id)
    except ValueError:
        return None
    return SOURCES.get(source_key)
