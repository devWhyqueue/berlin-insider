from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

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


def _local_now(now_utc: datetime, *, timezone_name: str) -> datetime:
    try:
        tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        tz = UTC
    return now_utc.astimezone(tz)
