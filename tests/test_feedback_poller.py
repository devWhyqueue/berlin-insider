from __future__ import annotations

from pathlib import Path

from berlin_insider.digest import DigestKind
from berlin_insider.feedback.store import (
    SqliteFeedbackStore,
    SqliteSentMessageStore,
    SqliteTelegramUpdatesStateStore,
)
from berlin_insider.feedback.telegram_poller import poll_feedback_once
from berlin_insider.feedback.models import SentMessageRecord


class _FakeMessenger:
    def __init__(self, updates: list[dict[str, object]]) -> None:
        self._updates = updates
        self.answered: list[str] = []
        self.reply_markup_clears: list[tuple[object, int]] = []
        self.text_updates: list[tuple[object, int, str]] = []

    def get_updates(self, *, offset: int | None = None, timeout_seconds: int = 0) -> list[dict[str, object]]:  # noqa: ARG002
        return self._updates

    def answer_callback_query(self, *, callback_query_id: str) -> None:
        self.answered.append(callback_query_id)

    def edit_message_reply_markup(self, *, chat_id: object, message_id: int) -> None:
        self.reply_markup_clears.append((chat_id, message_id))

    def edit_message_text(self, *, chat_id: object, message_id: int, text: str) -> None:
        self.text_updates.append((chat_id, message_id, text))


def test_feedback_poller_persists_and_deduplicates_votes(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    sent_store = SqliteSentMessageStore(db_path)
    sent_store.upsert(
        SentMessageRecord(
            message_key="daily-2026-02-23-abc",
            digest_kind=DigestKind.DAILY,
            local_date="2026-02-23",
            sent_at="2026-02-23T08:00:00+00:00",
            telegram_message_id="42",
            selected_urls=["https://example.com/a"],
        )
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
        sent_message_store=sent_store,
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
    sent_store = SqliteSentMessageStore(db_path)
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
        sent_message_store=sent_store,
    )
    assert result.persisted_votes == 0
    assert result.ignored_updates >= 1
    assert state_store.load().last_update_id == 2
