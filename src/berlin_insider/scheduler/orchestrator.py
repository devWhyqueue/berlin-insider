from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from berlin_insider.digest import DigestKind
from berlin_insider.feedback.store import SqliteMessageDeliveryStore
from berlin_insider.messenger.models import FeedbackMetadata, Messenger, MessengerError
from berlin_insider.messenger.telegram import TelegramMessenger
from berlin_insider.pipeline import build_fetch_context, run_full_pipeline
from berlin_insider.scheduler.due import expected_digest_kind, is_due, persist_sent_message
from berlin_insider.scheduler.models import ScheduleConfig, SchedulerState, ScheduleRunResult
from berlin_insider.scheduler.result_builders import (
    build_delivery_error_result,
    build_error_result,
    build_message_key,
    build_skip_result,
    build_success_result,
)
from berlin_insider.scheduler.store import SqliteSchedulerStateStore
from berlin_insider.storage.item_store import SqliteItemStore


class Scheduler:
    def run_once(
        self,
        *,
        state_store: SqliteSchedulerStateStore,
        config: ScheduleConfig,
        db_path: Path,
        target_items: int,
        force: bool,
        now_utc: datetime | None = None,
        messenger: Messenger | None = None,
        sent_message_store: SqliteMessageDeliveryStore | None = None,
    ) -> ScheduleRunResult:
        """Run one scheduler cycle with due-check, state write, and pipeline execution."""
        reference_now = now_utc or datetime.now(UTC)
        state = _load_and_mark_attempt(state_store=state_store, reference_now=reference_now)
        return _run_once_cycle(
            state_store=state_store,
            config=config,
            db_path=db_path,
            target_items=target_items,
            force=force,
            reference_now=reference_now,
            state=state,
            messenger=messenger,
            sent_message_store=sent_message_store,
        )


def _run_once_cycle(
    *,
    state_store: SqliteSchedulerStateStore,
    config: ScheduleConfig,
    db_path: Path,
    target_items: int,
    force: bool,
    reference_now: datetime,
    state: SchedulerState,
    messenger: Messenger | None,
    sent_message_store: SqliteMessageDeliveryStore | None,
) -> ScheduleRunResult:
    due_state = _resolve_due_state(
        now_utc=reference_now, config=config, state_store=state_store, state=state, force=force
    )
    if due_state.skip_result is not None:
        return due_state.skip_result
    if due_state.digest_kind is None:
        return build_skip_result(
            state_store=state_store,
            state=state,
            force=force,
            reason="no digest kind scheduled for this local date",
            local_date=due_state.local_date,
            digest_kind=None,
        )
    return _execute_due_run(
        state_store=state_store,
        state=state,
        digest_kind=due_state.digest_kind,
        local_date=due_state.local_date,
        due=due_state.due,
        force=force,
        reference_now=reference_now,
        db_path=db_path,
        target_items=target_items,
        messenger=messenger,
        sent_message_store=sent_message_store,
    )


class _DueState:
    def __init__(
        self,
        *,
        due: bool,
        local_date: str,
        digest_kind: DigestKind | None,
        skip_result: ScheduleRunResult | None,
    ) -> None:
        self.due = due
        self.local_date = local_date
        self.digest_kind = digest_kind
        self.skip_result = skip_result


def _resolve_due_state(
    *,
    now_utc: datetime,
    config: ScheduleConfig,
    state_store: SqliteSchedulerStateStore,
    state: SchedulerState,
    force: bool,
) -> _DueState:
    due, reason, local_date, digest_kind = is_due(now_utc=now_utc, config=config, state=state)
    if force:
        resolved_kind = digest_kind or expected_digest_kind(now_utc=now_utc, config=config)
        if resolved_kind is not None:
            return _DueState(
                due=due, local_date=local_date, digest_kind=resolved_kind, skip_result=None
            )
        skip = build_skip_result(
            state_store=state_store,
            state=state,
            force=True,
            reason="no digest kind scheduled for this local date",
            local_date=local_date,
            digest_kind=None,
        )
        return _DueState(due=False, local_date=local_date, digest_kind=None, skip_result=skip)
    if due and digest_kind is not None:
        return _DueState(due=True, local_date=local_date, digest_kind=digest_kind, skip_result=None)
    skip = build_skip_result(
        state_store=state_store,
        state=state,
        force=False,
        reason=reason,
        local_date=local_date,
        digest_kind=digest_kind,
    )
    return _DueState(due=False, local_date=local_date, digest_kind=digest_kind, skip_result=skip)


def _load_and_mark_attempt(
    *, state_store: SqliteSchedulerStateStore, reference_now: datetime
) -> SchedulerState:
    state = state_store.load()
    state.last_attempt_at = reference_now.isoformat()
    return state


def _execute_due_run(
    *,
    state_store: SqliteSchedulerStateStore,
    state: SchedulerState,
    digest_kind: DigestKind,
    local_date: str,
    due: bool,
    force: bool,
    reference_now: datetime,
    db_path: Path,
    target_items: int,
    messenger: Messenger | None,
    sent_message_store: SqliteMessageDeliveryStore | None,
) -> ScheduleRunResult:
    try:
        pipeline_result = run_full_pipeline(
            context=build_fetch_context(collected_at=reference_now),
            db_path=db_path,
            target_items=target_items,
            digest_kind=digest_kind,
        )
    except Exception as exc:  # noqa: BLE001
        return build_error_result(
            state_store=state_store,
            state=state,
            force=force,
            due=due,
            local_date=local_date,
            digest_kind=digest_kind,
            exc=exc,
        )
    message_key = build_message_key(digest_kind=digest_kind, local_date=local_date)
    messenger_instance = messenger or TelegramMessenger.from_env()
    feedback_metadata = (
        FeedbackMetadata(digest_kind=digest_kind, message_key=message_key)
        if digest_kind == DigestKind.DAILY
        else None
    )
    try:
        delivery = messenger_instance.send_digest(
            text=pipeline_result.digest,
            feedback_metadata=feedback_metadata,
        )
    except MessengerError as exc:
        return build_delivery_error_result(
            state_store=state_store,
            state=state,
            force=force,
            due=due,
            local_date=local_date,
            digest_kind=digest_kind,
            pipeline_result=pipeline_result,
            exc=exc,
        )
    persist_sent_message(
        store=sent_message_store or SqliteMessageDeliveryStore(db_path),
        item_store=SqliteItemStore(db_path),
        message_key=message_key,
        digest_kind=digest_kind,
        local_date=local_date,
        delivered_at=delivery.delivered_at.isoformat(),
        message_id=delivery.external_message_id,
        pipeline_result=pipeline_result,
    )
    return build_success_result(
        state_store=state_store,
        state=state,
        force=force,
        due=due,
        local_date=local_date,
        digest_kind=digest_kind,
        message_key=message_key,
        reference_now=reference_now,
        pipeline_result=pipeline_result,
        delivery_result=delivery,
    )
