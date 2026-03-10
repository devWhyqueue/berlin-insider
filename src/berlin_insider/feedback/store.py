from __future__ import annotations

from pathlib import Path

from berlin_insider.feedback.models import (
    FeedbackEvent,
    SentMessageRecord,
    TelegramUpdatesState,
)
from berlin_insider.storage.sent_message_serialization import (
    feedback_event_values,
    row_to_sent_record,
    sent_message_values,
)
from berlin_insider.storage.sqlite import ensure_schema, sqlite_connection

_UPSERT_FEEDBACK_SQL = """
INSERT INTO feedback_events (
    message_key,
    digest_kind,
    vote,
    telegram_user_id,
    chat_id,
    message_id,
    voted_at,
    updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(message_key, telegram_user_id) DO UPDATE SET
    digest_kind = excluded.digest_kind,
    vote = excluded.vote,
    chat_id = excluded.chat_id,
    message_id = excluded.message_id,
    voted_at = excluded.voted_at,
    updated_at = excluded.updated_at
"""

_UPSERT_SENT_MESSAGE_SQL = """
INSERT INTO sent_messages (
    message_key,
    digest_kind,
    local_date,
    sent_at,
    telegram_message_id,
    selected_urls_json,
    alternative_item_json
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(message_key) DO UPDATE SET
    digest_kind = excluded.digest_kind,
    local_date = excluded.local_date,
    sent_at = excluded.sent_at,
    telegram_message_id = excluded.telegram_message_id,
    selected_urls_json = excluded.selected_urls_json,
    alternative_item_json = excluded.alternative_item_json
"""

_GET_SENT_MESSAGE_SQL = """
SELECT
    message_key,
    digest_kind,
    local_date,
    sent_at,
    telegram_message_id,
    selected_urls_json,
    alternative_item_json
FROM sent_messages
WHERE message_key = ?
"""


class SqliteFeedbackStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        ensure_schema(self._db_path)

    def upsert(self, event: FeedbackEvent) -> None:
        """Insert or update a vote using (message_key, telegram_user_id) uniqueness."""
        with sqlite_connection(self._db_path) as conn:
            conn.execute(_UPSERT_FEEDBACK_SQL, feedback_event_values(event))
            conn.commit()

    def count(self) -> int:
        """Return number of deduplicated feedback vote rows."""
        with sqlite_connection(self._db_path) as conn:
            row = conn.execute("SELECT COUNT(*) AS count FROM feedback_events").fetchone()
        return int(row[0]) if row is not None else 0


class SqliteTelegramUpdatesStateStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        ensure_schema(self._db_path)

    def load(self) -> TelegramUpdatesState:
        """Load Telegram updates offset state from SQLite."""
        with sqlite_connection(self._db_path) as conn:
            row = conn.execute(
                "SELECT last_update_id FROM telegram_updates_state WHERE id = 1"
            ).fetchone()
        if row is None:
            return TelegramUpdatesState()
        value = row[0]
        return TelegramUpdatesState(last_update_id=value if isinstance(value, int) else None)

    def save(self, state: TelegramUpdatesState) -> None:
        """Persist Telegram updates offset state."""
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


class SqliteSentMessageStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        ensure_schema(self._db_path)

    def upsert(self, record: SentMessageRecord) -> None:
        """Insert or replace metadata for a sent Telegram digest message."""
        with sqlite_connection(self._db_path) as conn:
            conn.execute(_UPSERT_SENT_MESSAGE_SQL, sent_message_values(record))
            conn.commit()

    def get(self, message_key: str) -> SentMessageRecord | None:
        """Return sent message metadata by message key, if present."""
        with sqlite_connection(self._db_path) as conn:
            row = conn.execute(_GET_SENT_MESSAGE_SQL, (message_key,)).fetchone()
        if row is None:
            return None
        return row_to_sent_record(row)
