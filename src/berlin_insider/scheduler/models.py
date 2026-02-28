from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class SchedulerStatus(StrEnum):
    SUCCESS = "success"
    PARTIAL = "partial"
    ERROR = "error"
    SKIPPED = "skipped"


@dataclass(slots=True)
class ScheduleConfig:
    timezone: str = "Europe/Berlin"
    weekday: str = "friday"
    hour: int = 8
    minute: int = 0


@dataclass(slots=True)
class SchedulerState:
    last_attempt_at: str | None = None
    last_run_date_local: str | None = None
    last_status: SchedulerStatus | None = None
    last_success_at: str | None = None
    last_error_message: str | None = None
    last_digest_length: int | None = None
    last_curated_count: int | None = None
    last_failed_sources: list[str] = field(default_factory=list)
    last_source_status: dict[str, str] = field(default_factory=dict)
    last_delivery_at: str | None = None
    last_delivery_message_id: str | None = None
    last_delivery_error: str | None = None


@dataclass(slots=True)
class ScheduleRunResult:
    executed: bool
    forced: bool
    due: bool
    reason: str
    status: SchedulerStatus
    exit_code: int
    delivered: bool
    digest: str | None
    state: SchedulerState
    local_date: str
