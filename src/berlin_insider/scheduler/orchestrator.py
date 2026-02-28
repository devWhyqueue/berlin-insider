from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from berlin_insider.messenger.models import DeliveryResult, Messenger, MessengerError
from berlin_insider.messenger.telegram import TelegramMessenger
from berlin_insider.pipeline import FullPipelineRunResult, build_fetch_context, run_full_pipeline
from berlin_insider.scheduler.due import is_due
from berlin_insider.scheduler.models import (
    ScheduleConfig,
    SchedulerState,
    SchedulerStatus,
    ScheduleRunResult,
)
from berlin_insider.scheduler.store import JsonSchedulerStateStore


class Scheduler:
    def run_once(
        self,
        *,
        state_store: JsonSchedulerStateStore,
        config: ScheduleConfig,
        sent_store_path: Path,
        target_items: int,
        force: bool,
        now_utc: datetime | None = None,
        messenger: Messenger | None = None,
    ) -> ScheduleRunResult:
        """Run one scheduler cycle with due-check, state write, and pipeline execution."""
        reference_now = now_utc or datetime.now(UTC)
        state = _load_and_mark_attempt(state_store, reference_now)
        due, reason, local_date = is_due(now_utc=reference_now, config=config, state=state)
        if not force and not due:
            return _build_skip_result(state_store, state, reason=reason, local_date=local_date)
        return _execute_due_run(
            state_store=state_store,
            state=state,
            local_date=local_date,
            due=due,
            force=force,
            reference_now=reference_now,
            sent_store_path=sent_store_path,
            target_items=target_items,
            messenger=messenger,
        )


def _load_and_mark_attempt(
    state_store: JsonSchedulerStateStore, reference_now: datetime
) -> SchedulerState:
    state = state_store.load()
    state.last_attempt_at = reference_now.isoformat()
    return state


def _build_skip_result(
    state_store: JsonSchedulerStateStore,
    state: SchedulerState,
    *,
    reason: str,
    local_date: str,
) -> ScheduleRunResult:
    state.last_status = SchedulerStatus.SKIPPED
    state_store.save(state)
    return ScheduleRunResult(
        executed=False,
        forced=False,
        due=False,
        reason=reason,
        status=SchedulerStatus.SKIPPED,
        exit_code=0,
        delivered=False,
        digest=None,
        state=state,
        local_date=local_date,
    )


def _execute_due_run(
    *,
    state_store: JsonSchedulerStateStore,
    state: SchedulerState,
    local_date: str,
    due: bool,
    force: bool,
    reference_now: datetime,
    sent_store_path: Path,
    target_items: int,
    messenger: Messenger | None,
) -> ScheduleRunResult:
    try:
        pipeline_result = run_full_pipeline(
            context=build_fetch_context(collected_at=reference_now),
            sent_store_path=sent_store_path,
            target_items=target_items,
        )
    except Exception as exc:  # noqa: BLE001
        return _build_error_result(
            state_store, state, force=force, due=due, local_date=local_date, exc=exc
        )
    messenger_instance = messenger or TelegramMessenger.from_env()
    try:
        delivery_result = messenger_instance.send_digest(text=pipeline_result.digest)
    except MessengerError as exc:
        return _build_delivery_error_result(
            state_store=state_store,
            state=state,
            force=force,
            due=due,
            local_date=local_date,
            pipeline_result=pipeline_result,
            exc=exc,
        )
    return _build_success_result(
        state_store=state_store,
        state=state,
        force=force,
        due=due,
        local_date=local_date,
        reference_now=reference_now,
        pipeline_result=pipeline_result,
        delivery_result=delivery_result,
    )


def _build_error_result(
    state_store: JsonSchedulerStateStore,
    state: SchedulerState,
    *,
    force: bool,
    due: bool,
    local_date: str,
    exc: Exception,
) -> ScheduleRunResult:
    state.last_status = SchedulerStatus.ERROR
    state.last_error_message = str(exc)
    state_store.save(state)
    return ScheduleRunResult(
        executed=True,
        forced=force,
        due=due,
        reason="run failed",
        status=SchedulerStatus.ERROR,
        exit_code=1,
        delivered=False,
        digest=None,
        state=state,
        local_date=local_date,
    )


def _build_delivery_error_result(
    *,
    state_store: JsonSchedulerStateStore,
    state: SchedulerState,
    force: bool,
    due: bool,
    local_date: str,
    pipeline_result: FullPipelineRunResult,
    exc: MessengerError,
) -> ScheduleRunResult:
    _apply_pipeline_state(state, pipeline_result=pipeline_result)
    state.last_status = SchedulerStatus.ERROR
    state.last_error_message = f"delivery failed: {exc}"
    state.last_delivery_error = str(exc)
    state_store.save(state)
    return ScheduleRunResult(
        executed=True,
        forced=force,
        due=due,
        reason="delivery failed",
        status=SchedulerStatus.ERROR,
        exit_code=1,
        delivered=False,
        digest=pipeline_result.digest,
        state=state,
        local_date=local_date,
    )


def _build_success_result(
    *,
    state_store: JsonSchedulerStateStore,
    state: SchedulerState,
    force: bool,
    due: bool,
    local_date: str,
    reference_now: datetime,
    pipeline_result: FullPipelineRunResult,
    delivery_result: DeliveryResult,
) -> ScheduleRunResult:
    has_failures = bool(
        pipeline_result.fetch_result.failed_sources
        or pipeline_result.parse_result.failed_sources
        or pipeline_result.curate_result.failed_sources
    )
    status = SchedulerStatus.PARTIAL if has_failures else SchedulerStatus.SUCCESS
    _apply_pipeline_state(state, pipeline_result=pipeline_result)
    state.last_status = status
    state.last_run_date_local = local_date
    state.last_success_at = reference_now.isoformat()
    state.last_error_message = None
    state.last_delivery_at = delivery_result.delivered_at.isoformat()
    state.last_delivery_message_id = delivery_result.external_message_id
    state.last_delivery_error = delivery_result.warning_message
    state_store.save(state)
    return ScheduleRunResult(
        executed=True,
        forced=force,
        due=due,
        reason="run executed",
        status=status,
        exit_code=0,
        delivered=True,
        digest=pipeline_result.digest,
        state=state,
        local_date=local_date,
    )


def _apply_pipeline_state(state: SchedulerState, *, pipeline_result: FullPipelineRunResult) -> None:
    state.last_digest_length = len(pipeline_result.digest)
    state.last_curated_count = pipeline_result.curate_result.actual_count
    state.last_failed_sources = sorted(
        {
            source_id.value
            for source_id in (
                pipeline_result.fetch_result.failed_sources
                + pipeline_result.parse_result.failed_sources
                + pipeline_result.curate_result.failed_sources
            )
        }
    )
    state.last_source_status = {
        result.source_id.value: result.status.value
        for result in pipeline_result.fetch_result.results
    }
