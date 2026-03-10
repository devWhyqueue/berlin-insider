from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from berlin_insider.digest import DigestKind
from berlin_insider.feedback.models import SentMessageRecord
from berlin_insider.feedback.store import SqliteFeedbackStore, SqliteSentMessageStore
from berlin_insider.feedback.webhook import WebhookDependencies, create_webhook_app
from berlin_insider.formatter.models import AlternativeDigestItem
from berlin_insider.messenger.models import DeliveryResult
from datetime import UTC, datetime
from berlin_insider.parser.models import ParsedCategory


class _FakeMessenger:
    def __init__(self, *, fail_answer: bool = False) -> None:
        self.answered: list[str] = []
        self.reply_markup_clears: list[tuple[object, int]] = []
        self.text_updates: list[tuple[object, int, str]] = []
        self.sent_messages: list[dict[str, object]] = []
        self._fail_answer = fail_answer

    def answer_callback_query(self, *, callback_query_id: str) -> None:
        if self._fail_answer:
            raise RuntimeError("answer failed")
        self.answered.append(callback_query_id)

    def edit_message_reply_markup(self, *, chat_id: object, message_id: int) -> None:
        self.reply_markup_clears.append((chat_id, message_id))

    def edit_message_text(self, *, chat_id: object, message_id: int, text: str) -> None:
        self.text_updates.append((chat_id, message_id, text))

    def send_digest(self, *, text: str, feedback_metadata=None) -> DeliveryResult:  # noqa: ANN001
        self.sent_messages.append({"text": text, "feedback_metadata": feedback_metadata})
        return DeliveryResult(
            delivered_at=datetime(2026, 2, 28, 8, 1, tzinfo=UTC),
            external_message_id="99",
        )


def _make_update(*, callback_data: str, callback_id: str = "cb-1") -> dict[str, object]:
    return {
        "update_id": 100,
        "callback_query": {
            "id": callback_id,
            "data": callback_data,
            "from": {"id": 123},
            "message": {"message_id": 42, "chat": {"id": -1000}, "text": "Berlin Insider Digest"},
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
    assert messenger.reply_markup_clears == [(-1000, 42)]
    assert messenger.text_updates == []
    assert len(messenger.sent_messages) == 0


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
    assert len(messenger.sent_messages) == 0


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


def test_webhook_stale_callback_ack_error_does_not_fail_request(tmp_path: Path) -> None:
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
    messenger = _FakeMessenger(fail_answer=True)
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


def test_webhook_daily_downvote_sends_single_alternative_follow_up(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    sent_store = SqliteSentMessageStore(db_path)
    sent_store.upsert(
        SentMessageRecord(
            message_key="daily-2026-02-28-abc",
            digest_kind=DigestKind.DAILY,
            local_date="2026-02-28",
            sent_at="2026-02-28T08:00:00+00:00",
            telegram_message_id="42",
            selected_urls=["https://example.com/primary", "https://example.com/alt"],
            alternative_item=AlternativeDigestItem(
                item_url="https://example.com/alt",
                title="Alternative",
                summary="Compact alternative summary.",
                location="Pankow",
                category=ParsedCategory.EVENT,
                event_start_at=None,
                event_end_at=None,
            ),
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
        json=_make_update(callback_data="fb:v1:daily:daily-2026-02-28-abc:down"),
    )

    assert response.status_code == 200
    assert feedback_store.count() == 1
    assert len(messenger.sent_messages) == 1
    sent_text = messenger.sent_messages[0]["text"]
    assert isinstance(sent_text, str)
    assert "Berlin Insider \\| Tip of the Day" in sent_text
    assert "Compact alternative summary\\." in sent_text
    assert sent_store.get("daily-2026-02-28-abc-alt1") is not None


def test_webhook_downvote_on_alt_message_does_not_chain(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    sent_store = SqliteSentMessageStore(db_path)
    sent_store.upsert(
        SentMessageRecord(
            message_key="daily-2026-02-28-abc-alt1",
            digest_kind=DigestKind.DAILY,
            local_date="2026-02-28",
            sent_at="2026-02-28T08:01:00+00:00",
            telegram_message_id="99",
            selected_urls=["https://example.com/alt"],
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
        json=_make_update(callback_data="fb:v1:daily:daily-2026-02-28-abc-alt1:down"),
    )

    assert response.status_code == 200
    assert feedback_store.count() == 1
    assert len(messenger.sent_messages) == 0


def test_webhook_weekend_feedback_is_ignored(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    sent_store = SqliteSentMessageStore(db_path)
    sent_store.upsert(
        SentMessageRecord(
            message_key="weekend-2026-02-27-abc",
            digest_kind=DigestKind.WEEKEND,
            local_date="2026-02-27",
            sent_at="2026-02-27T08:00:00+00:00",
            telegram_message_id="7",
            selected_urls=["https://example.com/weekend"],
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
        json=_make_update(callback_data="fb:v1:weekend:weekend-2026-02-27-abc:down"),
    )

    assert response.status_code == 200
    assert feedback_store.count() == 0
    assert messenger.answered == ["cb-1"]
    assert len(messenger.sent_messages) == 0
