from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from berlin_insider.pipeline import FullPipelineRunResult, build_fetch_context, run_full_pipeline
from berlin_insider.scheduler.models import (
    ScheduleConfig,
    SchedulerState,
    SchedulerStatus,
    ScheduleRunResult,
)
from berlin_insider.scheduler.store import JsonSchedulerStateStore

_WEEKDAY_TO_INDEX = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}


def is_due(
    *,
    now_utc: datetime,
    config: ScheduleConfig,
    state: SchedulerState,
) -> tuple[bool, str, str]:
    """Return due decision, reason, and local schedule date string."""
    local_now = _local_now(now_utc, timezone_name=config.timezone)
    local_date = local_now.date().isoformat()
    expected_weekday = _WEEKDAY_TO_INDEX.get(config.weekday.lower())
    if expected_weekday is None:
        return False, f"invalid weekday '{config.weekday}'", local_date
    if local_now.weekday() != expected_weekday:
        return False, "today is not the configured weekday", local_date
    if (local_now.hour, local_now.minute) < (config.hour, config.minute):
        return False, "configured send time has not been reached yet", local_date
    if state.last_run_date_local == local_date:
        return False, "already ran for this local date", local_date
    return True, "run is due", local_date


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
        )


def _local_now(now_utc: datetime, *, timezone_name: str) -> datetime:
    try:
        tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        tz = UTC
    return now_utc.astimezone(tz)


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
    return _build_success_result(
        state_store=state_store,
        state=state,
        force=force,
        due=due,
        local_date=local_date,
        reference_now=reference_now,
        pipeline_result=pipeline_result,
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
        digest=None,
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
) -> ScheduleRunResult:
    has_failures = bool(
        pipeline_result.fetch_result.failed_sources
        or pipeline_result.parse_result.failed_sources
        or pipeline_result.curate_result.failed_sources
    )
    status = SchedulerStatus.PARTIAL if has_failures else SchedulerStatus.SUCCESS
    state.last_status = status
    state.last_run_date_local = local_date
    state.last_success_at = reference_now.isoformat()
    state.last_error_message = None
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
    state_store.save(state)
    return ScheduleRunResult(
        executed=True,
        forced=force,
        due=due,
        reason="run executed",
        status=status,
        exit_code=0,
        digest=pipeline_result.digest,
        state=state,
        local_date=local_date,
    )
