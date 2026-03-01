from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from berlin_insider.digest import DigestKind
from berlin_insider.feedback.models import FeedbackEvent, FeedbackVote
from berlin_insider.feedback.store import SqliteFeedbackStore, SqliteSentMessageStore


class CallbackAcknowledger(Protocol):
    def answer_callback_query(self, *, callback_query_id: str) -> None:
        """Acknowledge callback query events."""
        ...


_FEEDBACK_RECEIVED_SUFFIX = "✅ Feedback received"


@dataclass(slots=True)
class FeedbackIngestResult:
    processed_callback: bool
    persisted_vote: bool
    ignored: bool
    answered_callback: bool


def ingest_feedback_update(
    *,
    update: dict[str, object],
    messenger: CallbackAcknowledger,
    feedback_store: SqliteFeedbackStore,
    sent_message_store: SqliteSentMessageStore,
) -> FeedbackIngestResult:
    """Process one Telegram update and persist thumbs feedback when applicable."""
    callback_query = update.get("callback_query")
    if not isinstance(callback_query, dict):
        return FeedbackIngestResult(
            processed_callback=False,
            persisted_vote=False,
            ignored=True,
            answered_callback=False,
        )

    return _process_callback_query(
        callback_query=callback_query,
        messenger=messenger,
        feedback_store=feedback_store,
        sent_message_store=sent_message_store,
    )


def parse_feedback_callback(
    callback_query: dict[str, object],
) -> tuple[str, DigestKind, FeedbackVote] | None:
    """Return parsed (message_key, digest_kind, vote) tuple for fb:v1 callbacks."""
    data = callback_query.get("data")
    if not isinstance(data, str):
        return None
    parts = data.split(":")
    if len(parts) != 5 or parts[0] != "fb" or parts[1] != "v1":
        return None
    _, _, kind_str, message_key, vote_str = parts
    try:
        digest_kind = DigestKind(kind_str)
    except ValueError:
        return None
    if not message_key or vote_str not in {"up", "down"}:
        return None
    vote: FeedbackVote = "up" if vote_str == "up" else "down"
    return message_key, digest_kind, vote


def _ignore_with_ack(
    *, messenger: CallbackAcknowledger, callback_id: object
) -> FeedbackIngestResult:
    answered = _ack_if_possible(messenger=messenger, callback_id=callback_id)
    return FeedbackIngestResult(
        processed_callback=True,
        persisted_vote=False,
        ignored=True,
        answered_callback=answered,
    )


def _process_callback_query(
    *,
    callback_query: dict[str, object],
    messenger: CallbackAcknowledger,
    feedback_store: SqliteFeedbackStore,
    sent_message_store: SqliteSentMessageStore,
) -> FeedbackIngestResult:
    callback_id = callback_query.get("id")
    parsed = parse_feedback_callback(callback_query)
    if parsed is None:
        return _ignore_with_ack(messenger=messenger, callback_id=callback_id)
    message_key, digest_kind, vote = parsed
    if sent_message_store.get(message_key) is None:
        return _ignore_with_ack(messenger=messenger, callback_id=callback_id)
    event = _event_from_callback(
        callback_query=callback_query,
        message_key=message_key,
        digest_kind=digest_kind,
        vote=vote,
    )
    if event is None:
        return _ignore_with_ack(messenger=messenger, callback_id=callback_id)
    feedback_store.upsert(event)
    _update_feedback_message_ui(messenger=messenger, callback_query=callback_query)
    answered = _ack_if_possible(messenger=messenger, callback_id=callback_id)
    return FeedbackIngestResult(
        processed_callback=True,
        persisted_vote=True,
        ignored=False,
        answered_callback=answered,
    )


def _ack_if_possible(*, messenger: CallbackAcknowledger, callback_id: object) -> bool:
    if not isinstance(callback_id, str):
        return False
    messenger.answer_callback_query(callback_query_id=callback_id)
    return True


def _event_from_callback(
    *,
    callback_query: dict[str, object],
    message_key: str,
    digest_kind: DigestKind,
    vote: FeedbackVote,
) -> FeedbackEvent | None:
    user_obj = callback_query.get("from")
    user_id = user_obj.get("id") if isinstance(user_obj, dict) else None
    message_obj = callback_query.get("message")
    message_id_obj = message_obj.get("message_id") if isinstance(message_obj, dict) else None
    chat_obj = message_obj.get("chat") if isinstance(message_obj, dict) else None
    chat_id_obj = chat_obj.get("id") if isinstance(chat_obj, dict) else None
    if not isinstance(user_id, int) or not isinstance(message_id_obj, int):
        return None
    now_iso = datetime.now(UTC).isoformat()
    return FeedbackEvent(
        message_key=message_key,
        digest_kind=digest_kind,
        vote=vote,
        telegram_user_id=user_id,
        chat_id=str(chat_id_obj) if chat_id_obj is not None else "",
        message_id=str(message_id_obj),
        voted_at=now_iso,
        updated_at=now_iso,
    )


def _update_feedback_message_ui(*, messenger: CallbackAcknowledger, callback_query: dict[str, object]) -> None:
    message_obj = callback_query.get("message")
    if not isinstance(message_obj, dict):
        return
    message_id = message_obj.get("message_id")
    chat_obj = message_obj.get("chat")
    chat_id = chat_obj.get("id") if isinstance(chat_obj, dict) else None
    if not isinstance(message_id, int) or chat_id is None:
        return
    _try_remove_buttons(messenger=messenger, chat_id=chat_id, message_id=message_id)
    message_text = message_obj.get("text")
    if not isinstance(message_text, str):
        return
    if _FEEDBACK_RECEIVED_SUFFIX in message_text:
        return
    updated_text = f"{message_text}\n\n{_FEEDBACK_RECEIVED_SUFFIX}"
    _try_append_confirmation(
        messenger=messenger,
        chat_id=chat_id,
        message_id=message_id,
        text=updated_text,
    )


def _try_remove_buttons(*, messenger: CallbackAcknowledger, chat_id: object, message_id: int) -> None:
    method = getattr(messenger, "edit_message_reply_markup", None)
    if not callable(method):
        return
    try:
        method(chat_id=chat_id, message_id=message_id)
    except Exception:  # noqa: BLE001
        return


def _try_append_confirmation(
    *, messenger: CallbackAcknowledger, chat_id: object, message_id: int, text: str
) -> None:
    method = getattr(messenger, "edit_message_text", None)
    if not callable(method):
        return
    try:
        method(chat_id=chat_id, message_id=message_id, text=text)
    except Exception:  # noqa: BLE001
        return
