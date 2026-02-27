from __future__ import annotations

from datetime import UTC, datetime
from email.utils import parsedate_to_datetime


def parse_datetime(value: str | None) -> datetime | None:
    """Parse many datetime formats into UTC-aware datetimes."""
    if not value:
        return None
    parsed = _parse_datetime_flexible(value)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def dedupe_urls(urls: list[str]) -> list[str]:
    """Return input URLs in original order without duplicates."""
    seen: set[str] = set()
    unique: list[str] = []
    for url in urls:
        normalized = url.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique.append(normalized)
    return unique


def _parse_datetime_flexible(value: str) -> datetime | None:
    try:
        return parsedate_to_datetime(value)
    except (TypeError, ValueError):
        pass
    normalized = value.strip().replace("Z", "+00:00")
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None
