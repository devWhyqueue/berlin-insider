from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from berlin_insider.digest import DigestKind
from berlin_insider.feedback.models import DeliveredItem, MessageDeliveryRecord
from berlin_insider.feedback.store import (
    SqliteFeedbackStore,
    SqliteMessageDeliveryStore,
    SqliteTelegramUpdatesStateStore,
)
from berlin_insider.feedback.telegram_poller import poll_feedback_once
from berlin_insider.messenger.models import DeliveryResult
from berlin_insider.parser.models import ParsedCategory
from berlin_insider.storage.sqlite import ensure_schema, sqlite_connection


class _FakeMessenger:
    def __init__(self, updates: list[dict[str, object]]) -> None:
        self._updates = updates
        self.answered: list[str] = []
        self.reply_markup_clears: list[tuple[object, int]] = []
        self.text_updates: list[tuple[object, int, str]] = []
        self.sent_messages: list[dict[str, object]] = []

    def get_updates(self, *, offset: int | None = None, timeout_seconds: int = 0) -> list[dict[str, object]]:  # noqa: ARG002
        return self._updates

    def answer_callback_query(self, *, callback_query_id: str) -> None:
        self.answered.append(callback_query_id)

    def edit_message_reply_markup(self, *, chat_id: object, message_id: int) -> None:
        self.reply_markup_clears.append((chat_id, message_id))

    def edit_message_text(self, *, chat_id: object, message_id: int, text: str) -> None:
        self.text_updates.append((chat_id, message_id, text))

    def send_digest(self, *, text: str, feedback_metadata=None) -> DeliveryResult:  # noqa: ANN001
        self.sent_messages.append({"text": text, "feedback_metadata": feedback_metadata})
        return DeliveryResult(
            delivered_at=datetime(2026, 2, 23, 8, 1, tzinfo=UTC),
            external_message_id="91",
        )


def _insert_item(db_path: Path, *, url: str, title: str) -> DeliveredItem:
    ensure_schema(db_path)
    with sqlite_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO sources (source_id, source_url, adapter_kind, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(source_id) DO NOTHING
            """,
            ("test_source", "https://example.com", "test", "2026-02-23T08:00:00+00:00"),
        )
        conn.execute(
            """
            INSERT INTO items (
                canonical_url, source_id, original_url, title, description, summary,
                event_start_at, event_end_at, location, category, category_confidence,
                weekend_relevance, weekend_confidence, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                url,
                "test_source",
                url,
                title,
                None,
                "Compact alternative summary.",
                None,
                None,
                "Pankow",
                ParsedCategory.EVENT.value,
                0.9,
                "likely_this_weekend",
                0.9,
                "2026-02-23T08:00:00+00:00",
                "2026-02-23T08:00:00+00:00",
            ),
        )
        item_id = int(conn.execute("SELECT item_id FROM items WHERE canonical_url = ?", (url,)).fetchone()[0])
        conn.commit()
    return DeliveredItem(
        item_id=item_id,
        canonical_url=url,
        title=title,
        summary="Compact alternative summary.",
        location="Pankow",
        category=ParsedCategory.EVENT,
        event_start_at=None,
        event_end_at=None,
    )


def _insert_delivery(
    db_path: Path,
    *,
    message_key: str,
    primary_url: str,
    alternative_url: str | None = None,
) -> SqliteMessageDeliveryStore:
    primary_item = _insert_item(db_path, url=primary_url, title="Primary")
    alternative_item = _insert_item(db_path, url=alternative_url, title="Alternative") if alternative_url else None
    store = SqliteMessageDeliveryStore(db_path)
    store.upsert(
        MessageDeliveryRecord(
            message_key=message_key,
            digest_kind=DigestKind.DAILY,
            local_date="2026-02-23",
            sent_at="2026-02-23T08:00:00+00:00",
            telegram_message_id="42",
            primary_item=primary_item,
            alternative_item=alternative_item,
        )
    )
    return store


def test_feedback_poller_persists_and_deduplicates_votes(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    delivery_store = _insert_delivery(
        db_path,
        message_key="daily-2026-02-23-abc",
        primary_url="https://example.com/a",
    )
    updates = [
        {
            "update_id": 100,
            "callback_query": {
                "id": "cb1",
                "data": "fb:v1:daily:daily-2026-02-23-abc:up",
                "from": {"id": 1},
                "message": {"message_id": 42, "chat": {"id": -1000}, "text": "Berlin Insider"},
            },
        },
        {
            "update_id": 101,
            "callback_query": {
                "id": "cb2",
                "data": "fb:v1:daily:daily-2026-02-23-abc:down",
                "from": {"id": 1},
                "message": {"message_id": 42, "chat": {"id": -1000}, "text": "Berlin Insider"},
            },
        },
    ]
    messenger = _FakeMessenger(updates)
    feedback_store = SqliteFeedbackStore(db_path)
    state_store = SqliteTelegramUpdatesStateStore(db_path)
    result = poll_feedback_once(
        messenger=messenger,
        state_store=state_store,
        feedback_store=feedback_store,
        sent_message_store=delivery_store,
    )
    assert result.fetched_updates == 2
    assert result.persisted_votes == 2
    assert result.answered_callbacks == 2
    assert feedback_store.count() == 1
    assert state_store.load().last_update_id == 101
    assert messenger.reply_markup_clears == [(-1000, 42), (-1000, 42)]
    assert messenger.text_updates == []


def test_feedback_poller_ignores_non_feedback_callbacks(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    delivery_store = SqliteMessageDeliveryStore(db_path)
    messenger = _FakeMessenger(
        [
            {"update_id": 1, "message": {"text": "hello"}},
            {"update_id": 2, "callback_query": {"id": "cb-x", "data": "foo:bar"}},
        ]
    )
    feedback_store = SqliteFeedbackStore(db_path)
    state_store = SqliteTelegramUpdatesStateStore(db_path)
    result = poll_feedback_once(
        messenger=messenger,
        state_store=state_store,
        feedback_store=feedback_store,
        sent_message_store=delivery_store,
    )
    assert result.persisted_votes == 0
    assert result.ignored_updates >= 1
    assert state_store.load().last_update_id == 2


def test_feedback_poller_daily_downvote_sends_one_alternative(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    delivery_store = _insert_delivery(
        db_path,
        message_key="daily-2026-02-23-abc",
        primary_url="https://example.com/primary",
        alternative_url="https://example.com/alt",
    )
    messenger = _FakeMessenger(
        [
            {
                "update_id": 100,
                "callback_query": {
                    "id": "cb1",
                    "data": "fb:v1:daily:daily-2026-02-23-abc:down",
                    "from": {"id": 1},
                    "message": {"message_id": 42, "chat": {"id": -1000}, "text": "Berlin Insider"},
                },
            }
        ]
    )
    feedback_store = SqliteFeedbackStore(db_path)
    state_store = SqliteTelegramUpdatesStateStore(db_path)

    result = poll_feedback_once(
        messenger=messenger,
        state_store=state_store,
        feedback_store=feedback_store,
        sent_message_store=delivery_store,
    )

    assert result.persisted_votes == 1
    assert len(messenger.sent_messages) == 1
    sent_text = messenger.sent_messages[0]["text"]
    assert isinstance(sent_text, str)
    assert "Berlin Insider \\| Tip of the Day" in sent_text
    assert delivery_store.get("daily-2026-02-23-abc-alt1") is not None
