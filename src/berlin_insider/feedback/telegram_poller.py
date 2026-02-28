from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from berlin_insider.digest import DigestKind
from berlin_insider.feedback.models import (
    FeedbackEvent,
    FeedbackPollResult,
    FeedbackVote,
    TelegramUpdatesState,
)
from berlin_insider.feedback.store import (
    JsonFeedbackStore,
    JsonSentMessageStore,
    JsonTelegramUpdatesStateStore,
)


class FeedbackMessenger(Protocol):
    def get_updates(
        self, *, offset: int | None = None, timeout_seconds: int = 0
    ) -> list[dict[str, object]]:
        """Fetch Telegram updates from the Bot API."""
        ...

    def answer_callback_query(self, *, callback_query_id: str) -> None:
        """Acknowledge a Telegram callback query."""
        ...


@dataclass(slots=True)
class _PollCounters:
    processed_callbacks: int = 0
    persisted_votes: int = 0
    ignored_updates: int = 0
    answered_callbacks: int = 0
    max_update_id: int | None = None


def poll_feedback_once(
    *,
    messenger: FeedbackMessenger,
    state_store: JsonTelegramUpdatesStateStore,
    feedback_store: JsonFeedbackStore,
    sent_message_store: JsonSentMessageStore,
    timeout_seconds: int = 0,
) -> FeedbackPollResult:
    """Poll Telegram callbacks once and persist normalized thumbs feedback."""
    state = state_store.load()
    offset = _offset_from_state(state)
    updates = messenger.get_updates(offset=offset, timeout_seconds=timeout_seconds)
    counters = _PollCounters(max_update_id=state.last_update_id)
    for update in updates:
        _apply_update(
            update=update,
            counters=counters,
            messenger=messenger,
            feedback_store=feedback_store,
            sent_message_store=sent_message_store,
        )
    if counters.max_update_id is not None:
        state_store.save(TelegramUpdatesState(last_update_id=counters.max_update_id))
    return _result_from_counters(counters=counters, fetched_updates=len(updates))


def _offset_from_state(state: TelegramUpdatesState) -> int | None:
    if state.last_update_id is None:
        return None
    return state.last_update_id + 1


def _apply_update(
    *,
    update: dict[str, object],
    counters: _PollCounters,
    messenger: FeedbackMessenger,
    feedback_store: JsonFeedbackStore,
    sent_message_store: JsonSentMessageStore,
) -> None:
    _track_update_id(update, counters)
    callback_query = update.get("callback_query")
    if not isinstance(callback_query, dict):
        counters.ignored_updates += 1
        return
    counters.processed_callbacks += 1
    accepted = _process_callback(
        callback_query=callback_query,
        messenger=messenger,
        feedback_store=feedback_store,
        sent_message_store=sent_message_store,
        counters=counters,
    )
    if accepted:
        counters.persisted_votes += 1
        return
    counters.ignored_updates += 1


def _process_callback(
    *,
    callback_query: dict[str, object],
    messenger: FeedbackMessenger,
    feedback_store: JsonFeedbackStore,
    sent_message_store: JsonSentMessageStore,
    counters: _PollCounters,
) -> bool:
    parsed = _parse_feedback_callback(callback_query)
    callback_id = callback_query.get("id")
    if parsed is None:
        _ack_if_possible(messenger, callback_id, counters)
        return False
    message_key, digest_kind, vote = parsed
    if sent_message_store.get(message_key) is None:
        _ack_if_possible(messenger, callback_id, counters)
        return False
    feedback_event = _event_from_callback(
        callback_query=callback_query,
        message_key=message_key,
        digest_kind=digest_kind,
        vote=vote,
    )
    if feedback_event is None:
        _ack_if_possible(messenger, callback_id, counters)
        return False
    feedback_store.upsert(feedback_event)
    _ack_if_possible(messenger, callback_id, counters)
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


def _track_update_id(update: dict[str, object], counters: _PollCounters) -> None:
    update_id = update.get("update_id")
    if not isinstance(update_id, int):
        return
    if counters.max_update_id is None:
        counters.max_update_id = update_id
        return
    counters.max_update_id = max(counters.max_update_id, update_id)


def _ack_if_possible(
    messenger: FeedbackMessenger, callback_id: object, counters: _PollCounters
) -> None:
    if not isinstance(callback_id, str):
        return
    messenger.answer_callback_query(callback_query_id=callback_id)
    counters.answered_callbacks += 1


def _parse_feedback_callback(
    callback_query: dict[str, object],
) -> tuple[str, DigestKind, FeedbackVote] | None:
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


def _result_from_counters(*, counters: _PollCounters, fetched_updates: int) -> FeedbackPollResult:
    next_offset = counters.max_update_id + 1 if counters.max_update_id is not None else None
    return FeedbackPollResult(
        fetched_updates=fetched_updates,
        processed_callbacks=counters.processed_callbacks,
        persisted_votes=counters.persisted_votes,
        ignored_updates=counters.ignored_updates,
        answered_callbacks=counters.answered_callbacks,
        next_offset=next_offset,
        finished_at=datetime.now(UTC),
    )
