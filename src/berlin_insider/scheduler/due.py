from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from berlin_insider.digest import DigestKind
from berlin_insider.scheduler.models import ScheduleConfig, SchedulerState

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
) -> tuple[bool, str, str, DigestKind | None]:
    """Return due decision, reason, local date string, and digest kind."""
    local_now = _local_now(now_utc, timezone_name=config.timezone)
    local_date = local_now.date().isoformat()
    expected_weekday = _WEEKDAY_TO_INDEX.get(config.weekend_weekday.lower())
    if expected_weekday is None:
        return False, f"invalid weekday '{config.weekend_weekday}'", local_date, None
    digest_kind = _expected_digest_kind(local_now.weekday(), weekend_weekday=expected_weekday)
    if digest_kind is None:
        return False, "today has no scheduled digest", local_date, None
    scheduled_time = _scheduled_time_for_kind(digest_kind=digest_kind, config=config)
    if (local_now.hour, local_now.minute) < scheduled_time:
        return False, "configured send time has not been reached yet", local_date, digest_kind
    if _last_run_date_for_kind(state, digest_kind=digest_kind) == local_date:
        return False, "already ran for this local date", local_date, digest_kind
    return True, "run is due", local_date, digest_kind


def expected_digest_kind(*, now_utc: datetime, config: ScheduleConfig) -> DigestKind | None:
    """Return scheduled digest kind for local date, ignoring time and run history."""
    local_now = _local_now(now_utc, timezone_name=config.timezone)
    expected_weekday = _WEEKDAY_TO_INDEX.get(config.weekend_weekday.lower())
    if expected_weekday is None:
        return None
    return _expected_digest_kind(local_now.weekday(), weekend_weekday=expected_weekday)


def _scheduled_time_for_kind(*, digest_kind: DigestKind, config: ScheduleConfig) -> tuple[int, int]:
    if digest_kind == DigestKind.DAILY:
        return config.daily_hour, config.daily_minute
    return config.weekend_hour, config.weekend_minute


def _expected_digest_kind(local_weekday: int, *, weekend_weekday: int) -> DigestKind | None:
    if local_weekday == weekend_weekday:
        return DigestKind.WEEKEND
    if local_weekday in {0, 1, 2, 3}:
        return DigestKind.DAILY
    return None


def _last_run_date_for_kind(state: SchedulerState, *, digest_kind: DigestKind) -> str | None:
    current = state.last_run_date_by_kind.get(digest_kind.value)
    if current is not None:
        return current
    if digest_kind == DigestKind.WEEKEND:
        return state.last_run_date_local
    return None


def _local_now(now_utc: datetime, *, timezone_name: str) -> datetime:
    try:
        tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        tz = UTC
    return now_utc.astimezone(tz)
