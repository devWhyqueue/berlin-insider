from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from berlin_insider.digest import DigestKind
from berlin_insider.feedback.messages import render_daily_alternative_message
from berlin_insider.feedback.models import FeedbackEvent, FeedbackVote, SentMessageRecord
from berlin_insider.feedback.store import SqliteFeedbackStore, SqliteSentMessageStore
from berlin_insider.messenger.models import DeliveryResult, FeedbackMetadata

logger = logging.getLogger(__name__)
_ALTERNATIVE_SUFFIX = "-alt1"


class FeedbackMessenger(Protocol):
    def answer_callback_query(self, *, callback_query_id: str) -> None:
        """Acknowledge callback query events."""
        ...

    def send_digest(
        self,
        *,
        text: str,
        feedback_metadata: FeedbackMetadata | None = None,
    ) -> DeliveryResult:
        """Send follow-up digest text through Telegram."""
        ...


@dataclass(slots=True)
class FeedbackIngestResult:
    processed_callback: bool
    persisted_vote: bool
    ignored: bool
    answered_callback: bool


def ingest_feedback_update(
    *,
    update: dict[str, object],
    messenger: FeedbackMessenger,
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


def _ignore_with_ack(*, messenger: FeedbackMessenger, callback_id: object) -> FeedbackIngestResult:
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
    messenger: FeedbackMessenger,
    feedback_store: SqliteFeedbackStore,
    sent_message_store: SqliteSentMessageStore,
) -> FeedbackIngestResult:
    callback_id = callback_query.get("id")
    parsed = parse_feedback_callback(callback_query)
    if parsed is None:
        return _ignore_with_ack(messenger=messenger, callback_id=callback_id)
    message_key, digest_kind, vote = parsed
    if digest_kind != DigestKind.DAILY:
        return _ignore_with_ack(messenger=messenger, callback_id=callback_id)
    sent_message = sent_message_store.get(message_key)
    if sent_message is None:
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
    if vote == "down":
        _send_alternative_follow_up_if_needed(
            messenger=messenger,
            sent_message_store=sent_message_store,
            sent_message=sent_message,
        )
    _update_feedback_message_ui(messenger=messenger, callback_query=callback_query)
    answered = _ack_if_possible(messenger=messenger, callback_id=callback_id)
    return FeedbackIngestResult(
        processed_callback=True,
        persisted_vote=True,
        ignored=False,
        answered_callback=answered,
    )


def _ack_if_possible(*, messenger: FeedbackMessenger, callback_id: object) -> bool:
    if not isinstance(callback_id, str):
        return False
    try:
        messenger.answer_callback_query(callback_query_id=callback_id)
    except RuntimeError:
        logger.warning("Failed to acknowledge callback query id=%s", callback_id)
        return False
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


def _update_feedback_message_ui(
    *, messenger: FeedbackMessenger, callback_query: dict[str, object]
) -> None:
    message_obj = callback_query.get("message")
    if not isinstance(message_obj, dict):
        return
    message_id = message_obj.get("message_id")
    chat_obj = message_obj.get("chat")
    chat_id = chat_obj.get("id") if isinstance(chat_obj, dict) else None
    if not isinstance(message_id, int) or chat_id is None:
        return
    _try_remove_buttons(messenger=messenger, chat_id=chat_id, message_id=message_id)


def _try_remove_buttons(*, messenger: FeedbackMessenger, chat_id: object, message_id: int) -> None:
    method = getattr(messenger, "edit_message_reply_markup", None)
    if not callable(method):
        return
    try:
        method(chat_id=chat_id, message_id=message_id)
    except RuntimeError:
        return


def _send_alternative_follow_up_if_needed(
    *,
    messenger: FeedbackMessenger,
    sent_message_store: SqliteSentMessageStore,
    sent_message: SentMessageRecord,
) -> None:
    if _is_alternative_message_key(sent_message.message_key):
        return
    alternative_url = _extract_alternative_url(sent_message.selected_urls)
    if alternative_url is None:
        return
    alternative_message_key = f"{sent_message.message_key}{_ALTERNATIVE_SUFFIX}"
    if sent_message_store.get(alternative_message_key) is not None:
        return
    try:
        delivery = messenger.send_digest(
            text=render_daily_alternative_message(alternative_url=alternative_url),
            feedback_metadata=FeedbackMetadata(
                digest_kind=DigestKind.DAILY,
                message_key=alternative_message_key,
            ),
        )
    except RuntimeError:
        logger.warning(
            "Failed to send daily alternative follow-up for message_key=%s",
            sent_message.message_key,
        )
        return
    sent_message_store.upsert(
        SentMessageRecord(
            message_key=alternative_message_key,
            digest_kind=DigestKind.DAILY,
            local_date=sent_message.local_date,
            sent_at=delivery.delivered_at.isoformat(),
            telegram_message_id=delivery.external_message_id,
            selected_urls=[alternative_url],
        )
    )


def _extract_alternative_url(selected_urls: list[str]) -> str | None:
    return (
        selected_urls[1].strip() if len(selected_urls) >= 2 and selected_urls[1].strip() else None
    )


def _is_alternative_message_key(message_key: str) -> bool:
    return message_key.endswith(_ALTERNATIVE_SUFFIX)
