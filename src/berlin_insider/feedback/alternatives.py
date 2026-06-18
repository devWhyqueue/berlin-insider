from __future__ import annotations

from datetime import UTC, datetime
from difflib import SequenceMatcher
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from berlin_insider.digest import DigestKind
from berlin_insider.feedback.models import DeliveredItem
from berlin_insider.parser.models import ParsedCategory
from berlin_insider.storage.sqlite import sqlite_connection

try:
    _BERLIN_TZ = ZoneInfo("Europe/Berlin")
except ZoneInfoNotFoundError:
    _BERLIN_TZ = UTC


def find_daily_alternative(
    db_path: Path,
    *,
    local_date: str,
    excluded_urls: set[str],
    excluded_title: str | None,
) -> DeliveredItem | None:
    """Return one same-day item not already delivered as a daily primary."""
    for item in _candidate_items(db_path):
        if item.canonical_url in excluded_urls:
            continue
        if _same_title(item.title, excluded_title):
            continue
        if _local_date(item.event_start_at) != local_date:
            continue
        if _has_primary_delivery(db_path, canonical_url=item.canonical_url):
            continue
        return item
    return None


def _candidate_items(db_path: Path) -> list[DeliveredItem]:
    with sqlite_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT item_id, canonical_url, title, COALESCE(summary, description),
                   location, category, event_start_at, event_end_at
            FROM items
            WHERE event_start_at IS NOT NULL
            ORDER BY updated_at DESC
            LIMIT 500
            """
        ).fetchall()
    return [_delivered_item_from_row(row) for row in rows]


def _has_primary_delivery(db_path: Path, *, canonical_url: str) -> bool:
    with sqlite_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM message_deliveries deliveries
            JOIN items primary_item ON primary_item.item_id = deliveries.primary_item_id
            WHERE deliveries.digest_kind = ? AND primary_item.canonical_url = ?
            LIMIT 1
            """,
            (DigestKind.DAILY.value, canonical_url),
        ).fetchone()
    return row is not None


def _delivered_item_from_row(row: tuple[object, ...]) -> DeliveredItem:
    category = _category(row[5])
    return DeliveredItem(
        item_id=_require_int(row[0]),
        canonical_url=str(row[1]),
        title=_str(row[2]),
        summary=_str(row[3]),
        location=_str(row[4]),
        category=category,
        event_start_at=_str(row[6]),
        event_end_at=_str(row[7]),
    )


def _local_date(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(_BERLIN_TZ).date().isoformat()


def _same_title(left: str | None, right: str | None) -> bool:
    left_key = " ".join((left or "").casefold().split())
    right_key = " ".join((right or "").casefold().split())
    if not left_key or not right_key:
        return False
    return SequenceMatcher(None, left_key, right_key).ratio() >= 0.88


def _require_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("expected integer item_id")
    return value


def _str(value: object) -> str | None:
    return str(value) if value is not None else None


def _category(value: object) -> ParsedCategory | None:
    raw = _str(value)
    return ParsedCategory(raw) if raw else None
