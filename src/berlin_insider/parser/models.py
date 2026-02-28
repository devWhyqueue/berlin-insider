from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from berlin_insider.fetcher.models import SourceId


class ParsedCategory(StrEnum):
    EVENT = "event"
    FOOD = "food"
    NIGHTLIFE = "nightlife"
    EXHIBITION = "exhibition"
    CULTURE = "culture"
    MISC = "misc"


class WeekendRelevance(StrEnum):
    LIKELY_THIS_WEEKEND = "likely_this_weekend"
    POSSIBLE = "possible"
    UNLIKELY = "unlikely"
    UNKNOWN = "unknown"


class ParseStatus(StrEnum):
    SUCCESS = "success"
    PARTIAL = "partial"
    ERROR = "error"


@dataclass(slots=True)
class ParsedItem:
    source_id: SourceId
    item_url: str
    title: str | None
    description: str | None
    event_start_at: datetime | None
    event_end_at: datetime | None
    location: str | None
    category: ParsedCategory
    category_confidence: float
    weekend_relevance: WeekendRelevance
    weekend_confidence: float
    parse_notes: list[str] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)
    detail_text: str | None = None


@dataclass(slots=True)
class SourceParseResult:
    source_id: SourceId
    status: ParseStatus
    items: list[ParsedItem]
    warnings: list[str]
    error_message: str | None
    duration_ms: int


@dataclass(slots=True)
class ParseRunResult:
    started_at: datetime
    finished_at: datetime
    results: list[SourceParseResult]
    total_items: int
    failed_sources: list[SourceId]
