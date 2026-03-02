from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime

from berlin_insider.curator.config import CuratorConfig
from berlin_insider.curator.helpers import (
    BERLIN_TZ,
    Candidate,
    backfill,
    build_run_result,
    candidate_sort_key,
    event_in_window,
    nearest_upcoming_for_local_date,
    normalize_title,
    pick_category_targets,
    title_duplicate,
    weekend_window,
)
from berlin_insider.curator.models import (
    CurateRunResult,
    DroppedItem,
    DropReason,
)
from berlin_insider.curator.scoring import score_item
from berlin_insider.curator.store import SentItemStore, canonicalize_url
from berlin_insider.digest import DigestKind
from berlin_insider.parser.models import ParsedItem, ParseRunResult, WeekendRelevance


class Curator:
    """Select a balanced, deduplicated weekend candidate set from parser output."""

    def run(
        self,
        parse_result: ParseRunResult,
        *,
        reference_now: datetime,
        store: SentItemStore,
        config: CuratorConfig | None = None,
    ) -> CurateRunResult:
        """Run curation pipeline from parsed results to ranked selection."""
        cfg = config or CuratorConfig()
        weekend_start, weekend_end = weekend_window(reference_now, cfg.weekend_start_hour_friday)
        source_drops, candidates = self._collect_candidates(
            parse_result=parse_result,
            weekend_start=weekend_start,
            weekend_end=weekend_end,
            store=store,
            digest_kind=cfg.digest_kind,
        )
        kept = self._dedupe(candidates, cfg, source_drops)
        selected, fallback_warning = self._select(kept, cfg, reference_now=reference_now)
        self._record_non_selected(kept, selected, source_drops)
        store.mark_sent([candidate.canonical_url for candidate in selected])
        return build_run_result(
            parse_result=parse_result,
            source_drops=source_drops,
            selected=selected,
            weekend_start=weekend_start,
            weekend_end=weekend_end,
            fallback_warning=fallback_warning,
            target_count=cfg.target_count,
            digest_kind=cfg.digest_kind,
        )

    def _collect_candidates(
        self,
        *,
        parse_result: ParseRunResult,
        weekend_start: datetime,
        weekend_end: datetime,
        store: SentItemStore,
        digest_kind: DigestKind,
    ) -> tuple[dict[int, list[DroppedItem]], list[Candidate]]:
        source_drops: dict[int, list[DroppedItem]] = defaultdict(list)
        candidates: list[Candidate] = []
        for source_order, source_result in enumerate(parse_result.results):
            for source_index, item in enumerate(source_result.items):
                candidate = self._prepare_candidate(
                    item=item,
                    source_index=source_index,
                    source_order=source_order,
                    weekend_start=weekend_start,
                    weekend_end=weekend_end,
                    store=store,
                    source_drops=source_drops[source_order],
                    digest_kind=digest_kind,
                )
                if candidate is not None:
                    candidates.append(candidate)
        return source_drops, candidates

    def _prepare_candidate(
        self,
        *,
        item: ParsedItem,
        source_index: int,
        source_order: int,
        weekend_start: datetime,
        weekend_end: datetime,
        store: SentItemStore,
        source_drops: list[DroppedItem],
        digest_kind: DigestKind,
    ) -> Candidate | None:
        if not item.item_url.strip():
            source_drops.append(DroppedItem(item=item, reason=DropReason.MISSING_URL))
            return None
        if item.title is None or not item.title.strip():
            source_drops.append(DroppedItem(item=item, reason=DropReason.MISSING_TITLE))
            return None
        canonical_url = canonicalize_url(item.item_url)
        if store.is_sent(canonical_url):
            source_drops.append(DroppedItem(item=item, reason=DropReason.ALREADY_SENT))
            return None
        in_window = event_in_window(item.event_start_at, weekend_start, weekend_end)
        if (
            digest_kind == DigestKind.WEEKEND
            and not in_window
            and item.weekend_relevance == WeekendRelevance.UNLIKELY
        ):
            source_drops.append(DroppedItem(item=item, reason=DropReason.OUTSIDE_WEEKEND_WINDOW))
            return None
        scoring = score_item(item, event_in_window=in_window)
        return Candidate(
            item=item,
            source_index=source_index,
            source_order=source_order,
            score=scoring.total,
            notes=scoring.notes,
            canonical_url=canonical_url,
            title_key=normalize_title(item.title),
            tier=_tier_for_item(item, in_window),
        )

    def _dedupe(
        self,
        candidates: list[Candidate],
        cfg: CuratorConfig,
        source_drops: dict[int, list[DroppedItem]],
    ) -> list[Candidate]:
        ranked = sorted(candidates, key=candidate_sort_key)
        kept: list[Candidate] = []
        winners: set[str] = set()
        for candidate in ranked:
            if candidate.canonical_url in winners:
                _drop(source_drops, candidate, DropReason.DUPLICATE, "canonical URL")
                continue
            if title_duplicate(candidate, kept, cfg.title_similarity_threshold):
                _drop(source_drops, candidate, DropReason.DUPLICATE, "near-duplicate title")
                continue
            winners.add(candidate.canonical_url)
            kept.append(candidate)
        return kept

    def _select(
        self, kept: list[Candidate], cfg: CuratorConfig, *, reference_now: datetime
    ) -> tuple[list[Candidate], str | None]:
        if cfg.digest_kind == DigestKind.DAILY:
            return self._select_daily(kept, reference_now=reference_now)
        pool = sorted(kept, key=candidate_sort_key)
        primary = [candidate for candidate in pool if candidate.tier in {0, 1}]
        unknown = [candidate for candidate in pool if candidate.tier == 2]
        selected, selected_ids = pick_category_targets(primary, cfg)
        selected, selected_ids = backfill(primary, selected, selected_ids, cfg.target_count)
        if len(selected) >= cfg.min_count_fallback:
            return selected[: cfg.target_count], None
        selected, _ = backfill(unknown, selected, selected_ids, cfg.target_count)
        warning = f"Fallback selection active: only {len(selected)} items available after filtering"
        return selected[: cfg.target_count], warning

    def _select_daily(
        self, kept: list[Candidate], *, reference_now: datetime
    ) -> tuple[list[Candidate], str | None]:
        if not kept:
            return [], "Fallback selection active: no candidates available for daily tip"
        pool = sorted(kept, key=candidate_sort_key)
        aware_now = reference_now if reference_now.tzinfo else reference_now.replace(tzinfo=UTC)
        local_date = aware_now.astimezone(BERLIN_TZ).date()
        same_day, _ = nearest_upcoming_for_local_date(pool, local_date=local_date)
        if same_day:
            return [same_day[0]], None
        return [], "Fallback selection active: no same-day items available"

    def _record_non_selected(
        self,
        kept: list[Candidate],
        selected: list[Candidate],
        source_drops: dict[int, list[DroppedItem]],
    ) -> None:
        selected_ids = {id(candidate.item) for candidate in selected}
        for candidate in kept:
            if id(candidate.item) in selected_ids:
                continue
            reason = (
                DropReason.UNKNOWN_WEEKEND_RELEVANCE
                if candidate.tier == 2
                else DropReason.LOW_SCORE
            )
            _drop(source_drops, candidate, reason)


def _drop(
    source_drops: dict[int, list[DroppedItem]],
    candidate: Candidate,
    reason: DropReason,
    details: str | None = None,
) -> None:
    source_drops[candidate.source_order].append(
        DroppedItem(item=candidate.item, reason=reason, details=details)
    )


def _tier_for_item(item: ParsedItem, in_window: bool) -> int:
    if not in_window and item.weekend_relevance == WeekendRelevance.UNKNOWN:
        return 2
    if in_window or item.weekend_relevance == WeekendRelevance.LIKELY_THIS_WEEKEND:
        return 0
    return 1
