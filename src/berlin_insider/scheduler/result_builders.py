from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from berlin_insider.digest import DigestKind
from berlin_insider.messenger.models import DeliveryResult, MessengerError
from berlin_insider.pipeline import FullPipelineRunResult
from berlin_insider.scheduler.models import SchedulerState, SchedulerStatus, ScheduleRunResult
from berlin_insider.scheduler.store import SqliteSchedulerStateStore


def build_skip_result(
    *,
    state_store: SqliteSchedulerStateStore,
    state: SchedulerState,
    force: bool,
    reason: str,
    local_date: str,
    digest_kind: DigestKind | None,
) -> ScheduleRunResult:
    """Build skipped run result and persist skipped status in scheduler state."""
    state.last_status = SchedulerStatus.SKIPPED
    state_store.save(state)
    return ScheduleRunResult(
        executed=False,
        forced=force,
        due=False,
        reason=reason,
        status=SchedulerStatus.SKIPPED,
        exit_code=0,
        delivered=False,
        digest=None,
        state=state,
        local_date=local_date,
        digest_kind=digest_kind,
    )


def build_error_result(
    *,
    state_store: SqliteSchedulerStateStore,
    state: SchedulerState,
    force: bool,
    due: bool,
    local_date: str,
    digest_kind: DigestKind,
    exc: Exception,
) -> ScheduleRunResult:
    """Build pipeline failure run result and persist error state."""
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
        digest_kind=digest_kind,
    )


def build_delivery_error_result(
    *,
    state_store: SqliteSchedulerStateStore,
    state: SchedulerState,
    force: bool,
    due: bool,
    local_date: str,
    digest_kind: DigestKind,
    pipeline_result: FullPipelineRunResult,
    exc: MessengerError,
) -> ScheduleRunResult:
    """Build delivery failure run result while retaining generated digest text."""
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
        digest_kind=digest_kind,
    )


def build_success_result(
    *,
    state_store: SqliteSchedulerStateStore,
    state: SchedulerState,
    force: bool,
    due: bool,
    local_date: str,
    digest_kind: DigestKind,
    message_key: str,
    reference_now: datetime,
    pipeline_result: FullPipelineRunResult,
    delivery_result: DeliveryResult,
) -> ScheduleRunResult:
    """Build success/partial run result and persist updated scheduler state."""
    status = _status_from_pipeline(pipeline_result)
    _apply_pipeline_state(state, pipeline_result=pipeline_result)
    _apply_success_state(
        state=state,
        status=status,
        local_date=local_date,
        digest_kind=digest_kind,
        reference_now=reference_now,
        delivery_result=delivery_result,
    )
    state_store.save(state)
    return _success_run_result(
        force=force,
        due=due,
        status=status,
        digest=pipeline_result.digest,
        state=state,
        local_date=local_date,
        digest_kind=digest_kind,
        message_key=message_key,
    )


def build_message_key(*, digest_kind: DigestKind, local_date: str) -> str:
    """Build unique message key embedded into Telegram callback payloads."""
    return f"{digest_kind.value}-{local_date}-{uuid4().hex[:10]}"


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


def _status_from_pipeline(pipeline_result: FullPipelineRunResult) -> SchedulerStatus:
    has_failures = bool(
        pipeline_result.fetch_result.failed_sources
        or pipeline_result.parse_result.failed_sources
        or pipeline_result.curate_result.failed_sources
    )
    return SchedulerStatus.PARTIAL if has_failures else SchedulerStatus.SUCCESS


def _apply_success_state(
    *,
    state: SchedulerState,
    status: SchedulerStatus,
    local_date: str,
    digest_kind: DigestKind,
    reference_now: datetime,
    delivery_result: DeliveryResult,
) -> None:
    state.last_status = status
    state.last_run_date_local = local_date
    state.last_run_date_by_kind[digest_kind.value] = local_date
    state.last_success_at = reference_now.isoformat()
    state.last_error_message = None
    state.last_delivery_at = delivery_result.delivered_at.isoformat()
    state.last_delivery_message_id = delivery_result.external_message_id
    state.last_delivery_error = delivery_result.warning_message


def _success_run_result(
    *,
    force: bool,
    due: bool,
    status: SchedulerStatus,
    digest: str,
    state: SchedulerState,
    local_date: str,
    digest_kind: DigestKind,
    message_key: str,
) -> ScheduleRunResult:
    return ScheduleRunResult(
        executed=True,
        forced=force,
        due=due,
        reason="run executed",
        status=status,
        exit_code=0,
        delivered=True,
        digest=digest,
        state=state,
        local_date=local_date,
        digest_kind=digest_kind,
        message_key=message_key,
    )
