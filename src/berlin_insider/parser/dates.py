from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from berlin_insider.fetcher.models import FetchedItem
from berlin_insider.fetcher.utils import parse_datetime
from berlin_insider.parser.models import WeekendRelevance

try:
    BERLIN_TZ = ZoneInfo("Europe/Berlin")
except ZoneInfoNotFoundError:
    BERLIN_TZ = UTC

_WEEKDAY_OFFSETS: dict[str, int] = {
    "montag": 0,
    "dienstag": 1,
    "mittwoch": 2,
    "donnerstag": 3,
    "freitag": 4,
    "samstag": 5,
    "sonntag": 6,
}


@dataclass(frozen=True, slots=True)
class WeekendDecision:
    relevance: WeekendRelevance
    confidence: float
    note: str | None


def derive_event_start(
    item: FetchedItem, *, reference_now: datetime, notes: list[str]
) -> datetime | None:
    """Resolve the best event start timestamp from fetched item fields."""
    metadata = item.metadata if isinstance(item.metadata, dict) else {}
    from_metadata = parse_end_date(metadata.get("start_date"))
    if from_metadata is not None:
        notes.append("event_start_at from metadata.start_date")
        return from_metadata
    from_raw = _parse_datetime_utc(item.raw_date_text)
    if from_raw is not None:
        notes.append("event_start_at from raw_date_text via parse_datetime")
        return from_raw
    from_relative = _parse_german_relative_datetime(item.raw_date_text, reference_now)
    if from_relative is not None:
        notes.append("event_start_at from relative German date phrase")
        return from_relative
    if item.published_at is not None:
        notes.append("event_start_at from published_at fallback")
        return to_utc(item.published_at)
    return None


def parse_end_date(value: object) -> datetime | None:
    """Parse optional metadata end-date values into UTC datetimes."""
    if not isinstance(value, str):
        return None
    parsed = parse_datetime(value)
    return to_utc(parsed) if parsed else None


def infer_weekend_relevance(
    event_start: datetime | None, *, reference_now: datetime
) -> WeekendDecision:
    """Tag whether an event date appears to be in or near the target weekend window."""
    if event_start is None:
        return WeekendDecision(
            WeekendRelevance.UNKNOWN, 0.0, "No date available for weekend tagging"
        )
    ref_local = to_utc(reference_now).astimezone(BERLIN_TZ).date()
    event_local = event_start.astimezone(BERLIN_TZ).date()
    saturday, sunday = _target_weekend(ref_local)
    if event_local in {saturday, sunday}:
        return WeekendDecision(
            WeekendRelevance.LIKELY_THIS_WEEKEND, 0.9, "Date falls on target weekend"
        )
    if abs((event_local - saturday).days) <= 1 or abs((event_local - sunday).days) <= 1:
        return WeekendDecision(WeekendRelevance.POSSIBLE, 0.6, "Date near target weekend")
    return WeekendDecision(WeekendRelevance.UNLIKELY, 0.2, "Date outside target weekend")


def to_utc(value: datetime) -> datetime:
    """Return a timezone-aware UTC datetime."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_datetime_utc(value: str | None) -> datetime | None:
    parsed = parse_datetime(value) if value else None
    return to_utc(parsed) if parsed else None


def _parse_german_relative_datetime(value: str | None, reference_now: datetime) -> datetime | None:
    if not value:
        return None
    text = value.lower()
    now_local = to_utc(reference_now).astimezone(BERLIN_TZ)
    date_local = now_local.date()
    target_date = _target_date(text, date_local)
    if target_date is None:
        return None
    hour, minute = _extract_time(text)
    local_dt = datetime.combine(target_date, time(hour=hour, minute=minute), tzinfo=BERLIN_TZ)
    return local_dt.astimezone(UTC)


def _target_date(text: str, date_local: date) -> date | None:
    if "heute" in text:
        return date_local
    if "morgen" in text:
        return date_local + timedelta(days=1)
    return _next_weekday_from_text(text, date_local)


def _next_weekday_from_text(text: str, start: date) -> date | None:
    for label, target_weekday in _WEEKDAY_OFFSETS.items():
        if re.search(rf"\b{label}\b", text):
            delta = (target_weekday - start.weekday()) % 7
            return start + timedelta(days=delta)
    return None


def _extract_time(text: str) -> tuple[int, int]:
    match = re.search(r"(\d{1,2})[:.](\d{2})", text)
    if not match:
        return (12, 0)
    hour = min(max(int(match.group(1)), 0), 23)
    minute = min(max(int(match.group(2)), 0), 59)
    return (hour, minute)


def _target_weekend(reference_date: date) -> tuple[date, date]:
    weekday = reference_date.weekday()
    if weekday == 6:
        return (reference_date - timedelta(days=1), reference_date)
    days_until_saturday = (5 - weekday) % 7
    saturday = reference_date + timedelta(days=days_until_saturday)
    return (saturday, saturday + timedelta(days=1))
