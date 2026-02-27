from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from berlin_insider.fetcher.models import SourceId
from berlin_insider.parser.models import ParsedCategory, ParsedItem


class CurateStatus(StrEnum):
    SUCCESS = "success"
    PARTIAL = "partial"
    ERROR = "error"


class DropReason(StrEnum):
    MISSING_TITLE = "missing_title"
    MISSING_URL = "missing_url"
    OUTSIDE_WEEKEND_WINDOW = "outside_weekend_window"
    UNKNOWN_WEEKEND_RELEVANCE = "unknown_weekend_relevance"
    DUPLICATE = "duplicate"
    ALREADY_SENT = "already_sent"
    LOW_SCORE = "low_score"


@dataclass(slots=True)
class CuratedItem:
    item: ParsedItem
    score: float
    selection_notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class DroppedItem:
    item: ParsedItem
    reason: DropReason
    details: str | None = None


@dataclass(slots=True)
class SourceCurateResult:
    source_id: SourceId
    status: CurateStatus
    selected_items: list[CuratedItem]
    dropped_items: list[DroppedItem]
    warnings: list[str]
    error_message: str | None
    duration_ms: int


@dataclass(slots=True)
class CurateRunResult:
    started_at: datetime
    finished_at: datetime
    results: list[SourceCurateResult]
    selected_items: list[CuratedItem]
    dropped_count: int
    failed_sources: list[SourceId]
    target_count: int
    actual_count: int
    category_counts: dict[ParsedCategory, int]
    warnings: list[str]
