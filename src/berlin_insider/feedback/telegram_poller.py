from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from berlin_insider.feedback.ingest import ingest_feedback_update
from berlin_insider.feedback.models import FeedbackPollResult, TelegramUpdatesState
from berlin_insider.feedback.store import (
    SqliteFeedbackStore,
    SqliteSentMessageStore,
    SqliteTelegramUpdatesStateStore,
)
from berlin_insider.messenger.models import DeliveryResult, FeedbackMetadata


class FeedbackMessenger(Protocol):
    def get_updates(
        self, *, offset: int | None = None, timeout_seconds: int = 0
    ) -> list[dict[str, object]]:
        """Fetch Telegram updates from the Bot API."""
        ...

    def answer_callback_query(self, *, callback_query_id: str) -> None:
        """Acknowledge a Telegram callback query."""
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
class _PollCounters:
    processed_callbacks: int = 0
    persisted_votes: int = 0
    ignored_updates: int = 0
    answered_callbacks: int = 0
    max_update_id: int | None = None


def poll_feedback_once(
    *,
    messenger: FeedbackMessenger,
    state_store: SqliteTelegramUpdatesStateStore,
    feedback_store: SqliteFeedbackStore,
    sent_message_store: SqliteSentMessageStore,
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
    feedback_store: SqliteFeedbackStore,
    sent_message_store: SqliteSentMessageStore,
) -> None:
    _track_update_id(update, counters)
    result = ingest_feedback_update(
        update=update,
        messenger=messenger,
        feedback_store=feedback_store,
        sent_message_store=sent_message_store,
    )
    if result.processed_callback:
        counters.processed_callbacks += 1
    if result.persisted_vote:
        counters.persisted_votes += 1
    if result.ignored:
        counters.ignored_updates += 1
    if result.answered_callback:
        counters.answered_callbacks += 1


def _track_update_id(update: dict[str, object], counters: _PollCounters) -> None:
    update_id = update.get("update_id")
    if not isinstance(update_id, int):
        return
    if counters.max_update_id is None:
        counters.max_update_id = update_id
        return
    counters.max_update_id = max(counters.max_update_id, update_id)


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
