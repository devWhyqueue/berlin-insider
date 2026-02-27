from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from berlin_insider.parser.models import ParsedCategory, ParsedItem, WeekendRelevance


@dataclass(frozen=True, slots=True)
class ScoreBreakdown:
    total: float
    notes: list[str]


def score_item(item: ParsedItem, *, event_in_window: bool) -> ScoreBreakdown:
    """Compute deterministic curation score from parser confidence and completeness."""
    weekend_points = _weekend_points(item.weekend_relevance, event_in_window)
    category_points = max(0.0, min(item.category_confidence, 1.0)) * 0.30
    quality_points = _quality_points(item)
    penalties = _penalties(item)
    score = weekend_points + category_points + quality_points - penalties
    notes = [
        f"weekend={weekend_points:.2f}",
        f"category={category_points:.2f}",
        f"quality={quality_points:.2f}",
    ]
    if item.category == ParsedCategory.MISC:
        notes.append("penalty=misc")
    if item.weekend_confidence < 0.35:
        notes.append("penalty=weak_weekend")
    return ScoreBreakdown(total=round(score, 4), notes=notes)


def _weekend_points(relevance: WeekendRelevance, event_in_window: bool) -> float:
    if event_in_window:
        return 0.45
    if relevance == WeekendRelevance.LIKELY_THIS_WEEKEND:
        return 0.40
    if relevance == WeekendRelevance.POSSIBLE:
        return 0.24
    if relevance == WeekendRelevance.UNKNOWN:
        return 0.08
    return 0.0


def _quality_points(item: ParsedItem) -> float:
    points = 0.0
    if item.title:
        points += 0.10
    if item.description:
        points += 0.06
    if item.event_start_at:
        points += 0.06
    if item.location:
        points += 0.03
    if item.event_start_at and _is_futureish(item.event_start_at):
        points += 0.02
    return points


def _penalties(item: ParsedItem) -> float:
    penalty = 0.0
    if item.category == ParsedCategory.MISC:
        penalty += 0.07
    if item.weekend_confidence < 0.35:
        penalty += 0.06
    return penalty


def _is_futureish(value: datetime) -> bool:
    dt = value if value.tzinfo else value.replace(tzinfo=UTC)
    return dt.astimezone(UTC) >= datetime.now(UTC)
