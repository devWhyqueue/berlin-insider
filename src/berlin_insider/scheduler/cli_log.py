from __future__ import annotations

import json
import logging
from dataclasses import asdict

from berlin_insider.scheduler.models import SchedulerStatus, ScheduleRunResult


def log_schedule_result(
    logger: logging.Logger, result: ScheduleRunResult, *, json_output: bool
) -> None:
    """Render scheduler output for human-readable and JSON CLI modes."""
    if json_output:
        logger.info(json.dumps(asdict(result), default=str, ensure_ascii=False, indent=2))
        return
    _log_schedule_overview(logger, result)
    _log_state_summary(logger, result)
    if result.digest:
        logger.info(result.digest)


def _log_schedule_overview(logger: logging.Logger, result: ScheduleRunResult) -> None:
    logger.info(
        "Schedule run: status=%s | executed=%s | due=%s | forced=%s | delivered=%s | local_date=%s | reason=%s",
        result.status.value,
        result.executed,
        result.due,
        result.forced,
        result.delivered,
        result.local_date,
        result.reason,
    )


def _log_state_summary(logger: logging.Logger, result: ScheduleRunResult) -> None:
    state = result.state
    logger.info(
        "Scheduler state: status=%s | attempt=%s | success=%s | error=%s | digest_len=%s | curated=%s | source_statuses=%s | failed_sources=%s | delivery_at=%s | delivery_message_id=%s | delivery_error=%s",
        state.last_status.value if isinstance(state.last_status, SchedulerStatus) else "n/a",
        state.last_attempt_at or "n/a",
        state.last_success_at or "n/a",
        state.last_error_message or "none",
        state.last_digest_length if state.last_digest_length is not None else "n/a",
        state.last_curated_count if state.last_curated_count is not None else "n/a",
        len(state.last_source_status),
        ", ".join(state.last_failed_sources) if state.last_failed_sources else "none",
        state.last_delivery_at or "n/a",
        state.last_delivery_message_id or "n/a",
        state.last_delivery_error or "none",
    )
