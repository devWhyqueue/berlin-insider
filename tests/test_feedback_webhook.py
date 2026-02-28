from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from berlin_insider.digest import DigestKind
from berlin_insider.feedback.models import SentMessageRecord
from berlin_insider.feedback.store import SqliteFeedbackStore, SqliteSentMessageStore
from berlin_insider.feedback.webhook import WebhookDependencies, create_webhook_app


class _FakeMessenger:
    def __init__(self) -> None:
        self.answered: list[str] = []

    def answer_callback_query(self, *, callback_query_id: str) -> None:
        self.answered.append(callback_query_id)


def _make_update(*, callback_data: str, callback_id: str = "cb-1") -> dict[str, object]:
    return {
        "update_id": 100,
        "callback_query": {
            "id": callback_id,
            "data": callback_data,
            "from": {"id": 123},
            "message": {"message_id": 42, "chat": {"id": -1000}},
        },
    }


def test_webhook_persists_feedback_and_acks(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    sent_store = SqliteSentMessageStore(db_path)
    sent_store.upsert(
        SentMessageRecord(
            message_key="daily-2026-02-28-abc",
            digest_kind=DigestKind.DAILY,
            local_date="2026-02-28",
            sent_at="2026-02-28T08:00:00+00:00",
            telegram_message_id="42",
            selected_urls=["https://example.com/a"],
        )
    )
    feedback_store = SqliteFeedbackStore(db_path)
    messenger = _FakeMessenger()
    app = create_webhook_app(
        deps=WebhookDependencies(
            messenger=messenger,  # type: ignore[arg-type]
            feedback_store=feedback_store,
            sent_message_store=sent_store,
            secret="secret123",
        )
    )

    client = TestClient(app)
    response = client.post(
        "/telegram/webhook/secret123",
        json=_make_update(callback_data="fb:v1:daily:daily-2026-02-28-abc:up"),
    )

    assert response.status_code == 200
    assert feedback_store.count() == 1
    assert messenger.answered == ["cb-1"]


def test_webhook_unknown_message_key_is_ignored_but_acked(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    sent_store = SqliteSentMessageStore(db_path)
    feedback_store = SqliteFeedbackStore(db_path)
    messenger = _FakeMessenger()
    app = create_webhook_app(
        deps=WebhookDependencies(
            messenger=messenger,  # type: ignore[arg-type]
            feedback_store=feedback_store,
            sent_message_store=sent_store,
            secret="secret123",
        )
    )
    client = TestClient(app)
    response = client.post(
        "/telegram/webhook/secret123",
        json=_make_update(callback_data="fb:v1:daily:missing-key:up", callback_id="cb-x"),
    )

    assert response.status_code == 200
    assert feedback_store.count() == 0
    assert messenger.answered == ["cb-x"]


def test_webhook_rejects_invalid_secret(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    app = create_webhook_app(
        deps=WebhookDependencies(
            messenger=_FakeMessenger(),  # type: ignore[arg-type]
            feedback_store=SqliteFeedbackStore(db_path),
            sent_message_store=SqliteSentMessageStore(db_path),
            secret="secret123",
        )
    )
    client = TestClient(app)
    response = client.post("/telegram/webhook/wrong", json={"update_id": 1})

    assert response.status_code == 404

