from __future__ import annotations

from dataclasses import dataclass

from berlin_insider.parser.models import ParsedCategory


@dataclass(slots=True)
class DigestFormatConfig:
    timezone: str = "Europe/Berlin"
    max_items: int | None = None
    show_location_when_present: bool = True


@dataclass(slots=True)
class AlternativeDigestItem:
    item_url: str
    title: str | None
    summary: str | None
    location: str | None
    category: ParsedCategory
    event_start_at: str | None
    event_end_at: str | None
