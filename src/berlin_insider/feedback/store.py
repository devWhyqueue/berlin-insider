from __future__ import annotations

from pathlib import Path

from berlin_insider.digest import DigestKind
from berlin_insider.feedback.models import (
    DeliveredItem,
    FeedbackEvent,
    MessageDeliveryRecord,
    TelegramUpdatesState,
)
from berlin_insider.parser.models import ParsedCategory
from berlin_insider.storage.sqlite import ensure_schema, sqlite_connection

_UPSERT_FEEDBACK_SQL = """
INSERT INTO feedback_events (
    message_key,
    telegram_user_id,
    vote,
    voted_at,
    updated_at
) VALUES (?, ?, ?, ?, ?)
ON CONFLICT(message_key, telegram_user_id) DO UPDATE SET
    vote = excluded.vote,
    voted_at = excluded.voted_at,
    updated_at = excluded.updated_at
"""

_UPSERT_MESSAGE_DELIVERY_SQL = """
INSERT INTO message_deliveries (
    message_key,
    digest_kind,
    local_date,
    sent_at,
    telegram_message_id,
    primary_item_id,
    alternative_item_id
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(message_key) DO UPDATE SET
    digest_kind = excluded.digest_kind,
    local_date = excluded.local_date,
    sent_at = excluded.sent_at,
    telegram_message_id = excluded.telegram_message_id,
    primary_item_id = excluded.primary_item_id,
    alternative_item_id = excluded.alternative_item_id
"""

_GET_MESSAGE_DELIVERY_SQL = """
SELECT
    deliveries.message_key,
    deliveries.digest_kind,
    deliveries.local_date,
    deliveries.sent_at,
    deliveries.telegram_message_id,
    primary_item.item_id,
    primary_item.canonical_url,
    primary_item.title,
    primary_item.summary,
    primary_item.location,
    primary_item.category,
    primary_item.event_start_at,
    primary_item.event_end_at,
    alternative_item.item_id,
    alternative_item.canonical_url,
    alternative_item.title,
    alternative_item.summary,
    alternative_item.location,
    alternative_item.category,
    alternative_item.event_start_at,
    alternative_item.event_end_at
FROM message_deliveries deliveries
JOIN items primary_item ON primary_item.item_id = deliveries.primary_item_id
LEFT JOIN items alternative_item ON alternative_item.item_id = deliveries.alternative_item_id
WHERE deliveries.message_key = ?
"""


class SqliteFeedbackStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        ensure_schema(self._db_path)

    def upsert(self, event: FeedbackEvent) -> None:
        """Insert or update one feedback vote."""
        with sqlite_connection(self._db_path) as conn:
            conn.execute(
                _UPSERT_FEEDBACK_SQL,
                (
                    event.message_key,
                    event.telegram_user_id,
                    event.vote,
                    event.voted_at,
                    event.updated_at,
                ),
            )
            conn.commit()

    def count(self) -> int:
        """Return the number of deduplicated feedback rows."""
        with sqlite_connection(self._db_path) as conn:
            row = conn.execute("SELECT COUNT(*) AS count FROM feedback_events").fetchone()
        return int(row[0]) if row is not None else 0


class SqliteTelegramUpdatesStateStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        ensure_schema(self._db_path)

    def load(self) -> TelegramUpdatesState:
        """Load the latest Telegram update offset."""
        with sqlite_connection(self._db_path) as conn:
            row = conn.execute(
                "SELECT last_update_id FROM telegram_updates_state WHERE id = 1"
            ).fetchone()
        if row is None:
            return TelegramUpdatesState()
        value = row[0]
        return TelegramUpdatesState(last_update_id=value if isinstance(value, int) else None)

    def save(self, state: TelegramUpdatesState) -> None:
        """Persist the latest Telegram update offset."""
        with sqlite_connection(self._db_path) as conn:
            conn.execute(
                """
                INSERT INTO telegram_updates_state (id, last_update_id)
                VALUES (1, ?)
                ON CONFLICT(id) DO UPDATE SET
                    last_update_id = excluded.last_update_id
                """,
                (state.last_update_id,),
            )
            conn.commit()


class SqliteMessageDeliveryStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        ensure_schema(self._db_path)

    def upsert(self, record: MessageDeliveryRecord) -> None:
        """Insert or update one delivered message record."""
        with sqlite_connection(self._db_path) as conn:
            conn.execute(
                _UPSERT_MESSAGE_DELIVERY_SQL,
                (
                    record.message_key,
                    record.digest_kind.value,
                    record.local_date,
                    record.sent_at,
                    record.telegram_message_id,
                    record.primary_item.item_id,
                    record.alternative_item.item_id
                    if record.alternative_item is not None
                    else None,
                ),
            )
            conn.commit()

    def get(self, message_key: str) -> MessageDeliveryRecord | None:
        """Return one delivered message record by message key."""
        with sqlite_connection(self._db_path) as conn:
            row = conn.execute(_GET_MESSAGE_DELIVERY_SQL, (message_key,)).fetchone()
        if row is None:
            return None
        return MessageDeliveryRecord(
            message_key=str(row[0]),
            digest_kind=_parse_digest_kind(row[1]),
            local_date=str(row[2]),
            sent_at=str(row[3]),
            telegram_message_id=str(row[4]),
            primary_item=_delivered_item_from_row(row, 5),
            alternative_item=_optional_delivered_item_from_row(row, 13),
        )


def _delivered_item_from_row(row: tuple[object, ...], start_index: int) -> DeliveredItem:
    item_id_raw = row[start_index]
    if not isinstance(item_id_raw, int):
        raise ValueError("expected integer item_id in delivery row")
    category_raw = str(row[start_index + 5]).strip() if row[start_index + 5] is not None else None
    category = ParsedCategory(category_raw) if category_raw else None
    return DeliveredItem(
        item_id=item_id_raw,
        canonical_url=str(row[start_index + 1]),
        title=str(row[start_index + 2]) if row[start_index + 2] is not None else None,
        summary=str(row[start_index + 3]) if row[start_index + 3] is not None else None,
        location=str(row[start_index + 4]) if row[start_index + 4] is not None else None,
        category=category,
        event_start_at=str(row[start_index + 6]) if row[start_index + 6] is not None else None,
        event_end_at=str(row[start_index + 7]) if row[start_index + 7] is not None else None,
    )


def _optional_delivered_item_from_row(
    row: tuple[object, ...], start_index: int
) -> DeliveredItem | None:
    if row[start_index] is None:
        return None
    return _delivered_item_from_row(row, start_index)


def _parse_digest_kind(raw: object):
    return DigestKind(str(raw))
