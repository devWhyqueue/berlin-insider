from __future__ import annotations

import json
from pathlib import Path

from berlin_insider.digest import DigestKind
from berlin_insider.feedback.models import (
    FeedbackEvent,
    SentMessageRecord,
    TelegramUpdatesState,
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
    selected_urls_json
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(message_key) DO UPDATE SET
    digest_kind = excluded.digest_kind,
    local_date = excluded.local_date,
    sent_at = excluded.sent_at,
    telegram_message_id = excluded.telegram_message_id,
    selected_urls_json = excluded.selected_urls_json
"""

_GET_SENT_MESSAGE_SQL = """
SELECT
    message_key,
    digest_kind,
    local_date,
    sent_at,
    telegram_message_id,
    selected_urls_json
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
            conn.execute(_UPSERT_FEEDBACK_SQL, _feedback_event_values(event))
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
            conn.execute(_UPSERT_SENT_MESSAGE_SQL, _sent_message_values(record))
            conn.commit()

    def get(self, message_key: str) -> SentMessageRecord | None:
        """Return sent message metadata by message key, if present."""
        with sqlite_connection(self._db_path) as conn:
            row = conn.execute(_GET_SENT_MESSAGE_SQL, (message_key,)).fetchone()
        if row is None:
            return None
        return _row_to_sent_record(row)


def _row_to_sent_record(row) -> SentMessageRecord | None:  # noqa: ANN001
    message_key, digest_kind_raw, local_date, sent_at, telegram_message_id, selected_urls_json = row
    if not isinstance(message_key, str):
        return None
    if not isinstance(local_date, str):
        return None
    if not isinstance(sent_at, str):
        return None
    if not isinstance(telegram_message_id, str):
        return None
    if not isinstance(selected_urls_json, str):
        return None
    try:
        digest_kind = DigestKind(str(digest_kind_raw))
    except ValueError:
        return None
    try:
        selected_urls = json.loads(selected_urls_json)
    except json.JSONDecodeError:
        return None
    if not isinstance(selected_urls, list) or not all(
        isinstance(url, str) for url in selected_urls
    ):
        return None
    return SentMessageRecord(
        message_key=message_key,
        digest_kind=digest_kind,
        local_date=local_date,
        sent_at=sent_at,
        telegram_message_id=telegram_message_id,
        selected_urls=selected_urls,
    )


def _feedback_event_values(event: FeedbackEvent) -> tuple[str, str, str, int, str, str, str, str]:
    return (
        event.message_key,
        event.digest_kind.value,
        event.vote,
        event.telegram_user_id,
        event.chat_id,
        event.message_id,
        event.voted_at,
        event.updated_at,
    )


def _sent_message_values(record: SentMessageRecord) -> tuple[str, str, str, str, str, str]:
    return (
        record.message_key,
        record.digest_kind.value,
        record.local_date,
        record.sent_at,
        record.telegram_message_id,
        json.dumps(record.selected_urls, ensure_ascii=False),
    )
