from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from difflib import SequenceMatcher
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from berlin_insider.curator.config import CuratorConfig
from berlin_insider.curator.models import CuratedItem, CurateStatus, DroppedItem, SourceCurateResult
from berlin_insider.parser.models import ParsedCategory, ParsedItem, ParseRunResult, ParseStatus

try:
    BERLIN_TZ = ZoneInfo("Europe/Berlin")
except ZoneInfoNotFoundError:
    BERLIN_TZ = UTC


@dataclass(slots=True)
class Candidate:
    item: ParsedItem
    source_index: int
    source_order: int
    score: float
    notes: list[str]
    canonical_url: str
    title_key: str
    tier: int


def candidate_sort_key(candidate: Candidate) -> tuple[float, int, str, int]:
    """Sort candidates by score, tier, source and source order for deterministic output."""
    return (
        -candidate.score,
        candidate.tier,
        candidate.item.source_id.value,
        candidate.source_index,
    )


def normalize_title(title: str | None) -> str:
    """Collapse whitespace and lowercase title for similarity comparisons."""
    return " ".join((title or "").lower().split())


def title_duplicate(candidate: Candidate, kept: list[Candidate], threshold: float) -> bool:
    """Return true when candidate title is near-duplicate to any kept item title."""
    return any(
        SequenceMatcher(None, candidate.title_key, existing.title_key).ratio() >= threshold
        for existing in kept
        if candidate.title_key and existing.title_key
    )


def pick_category_targets(
    primary: list[Candidate], cfg: CuratorConfig
) -> tuple[list[Candidate], set[int]]:
    """Pick top candidates per configured category quotas (soft targets)."""
    by_category: dict[ParsedCategory, list[Candidate]] = defaultdict(list)
    for candidate in primary:
        by_category[candidate.item.category].append(candidate)
    selected: list[Candidate] = []
    selected_ids: set[int] = set()
    for category, target in cfg.category_targets.items():
        for candidate in by_category.get(category, []):
            marker = id(candidate.item)
            if marker in selected_ids:
                continue
            selected.append(candidate)
            selected_ids.add(marker)
            if len(selected) >= cfg.target_count or _count_category(selected, category) >= target:
                break
        if len(selected) >= cfg.target_count:
            break
    return selected, selected_ids


def backfill(
    pool: list[Candidate],
    selected: list[Candidate],
    selected_ids: set[int],
    target_count: int,
) -> tuple[list[Candidate], set[int]]:
    """Fill remaining slots by score while preserving deterministic ordering."""
    for candidate in pool:
        if len(selected) >= target_count:
            break
        marker = id(candidate.item)
        if marker in selected_ids:
            continue
        selected.append(candidate)
        selected_ids.add(marker)
    return selected, selected_ids


def category_counts(selected: list[Candidate]) -> dict[ParsedCategory, int]:
    """Count selected candidates by parsed category."""
    counts: dict[ParsedCategory, int] = {category: 0 for category in ParsedCategory}
    for candidate in selected:
        counts[candidate.item.category] += 1
    return counts


def event_in_window(value: datetime | None, start: datetime, end: datetime) -> bool:
    """Return true when event datetime falls inside the Berlin weekend window."""
    if value is None:
        return False
    event = value if value.tzinfo else value.replace(tzinfo=UTC)
    local = event.astimezone(BERLIN_TZ)
    return start <= local <= end


def weekend_window(reference_now: datetime, friday_hour: int) -> tuple[datetime, datetime]:
    """Compute Friday evening to Sunday night window for the target weekend."""
    local_ref = _to_local(reference_now)
    saturday = _target_saturday(local_ref.date())
    friday = saturday - timedelta(days=1)
    sunday = saturday + timedelta(days=1)
    start = datetime.combine(friday, time(friday_hour, 0), tzinfo=BERLIN_TZ)
    end = datetime.combine(sunday, time(23, 59, 59), tzinfo=BERLIN_TZ)
    return start, end


def build_source_results(
    parse_result: ParseRunResult,
    source_drops: dict[int, list[DroppedItem]],
    selected: list[Candidate],
) -> list[SourceCurateResult]:
    """Build source-level curate summaries preserving parser source order."""
    selected_by_source: dict[int, list[CuratedItem]] = defaultdict(list)
    for candidate in selected:
        selected_by_source[candidate.source_order].append(to_curated(candidate))
    source_results: list[SourceCurateResult] = []
    for source_order, source_result in enumerate(parse_result.results):
        selected_items = selected_by_source[source_order]
        dropped_items = source_drops[source_order]
        warnings = list(source_result.warnings)
        status = curate_status(source_result.status, dropped_items)
        if not selected_items and source_result.items and status != CurateStatus.ERROR:
            warnings.append("No curated items selected from parsed source items")
        source_results.append(
            SourceCurateResult(
                source_id=source_result.source_id,
                status=status,
                selected_items=selected_items,
                dropped_items=dropped_items,
                warnings=warnings,
                error_message=source_result.error_message,
                duration_ms=1,
            )
        )
    return source_results


def curate_status(parse_status: ParseStatus, dropped_items: list[DroppedItem]) -> CurateStatus:
    """Map parser status plus curation drops into final source curate status."""
    if parse_status == ParseStatus.ERROR:
        return CurateStatus.ERROR
    if parse_status == ParseStatus.PARTIAL or dropped_items:
        return CurateStatus.PARTIAL
    return CurateStatus.SUCCESS


def to_curated(candidate: Candidate) -> CuratedItem:
    """Convert internal candidate object into public CuratedItem model."""
    return CuratedItem(item=candidate.item, score=candidate.score, selection_notes=candidate.notes)


def _count_category(selected: list[Candidate], category: ParsedCategory) -> int:
    return sum(1 for candidate in selected if candidate.item.category == category)


def _to_local(value: datetime) -> datetime:
    aware = value if value.tzinfo else value.replace(tzinfo=UTC)
    return aware.astimezone(BERLIN_TZ)


def _target_saturday(ref_date: date) -> date:
    if ref_date.weekday() == 6:
        return ref_date - timedelta(days=1)
    days_until_sat = (5 - ref_date.weekday()) % 7
    return ref_date + timedelta(days=days_until_sat)
