from __future__ import annotations

from collections.abc import Iterable

from berlin_insider.curator.models import CuratedItem, CurateRunResult
from berlin_insider.fetcher.models import FetchedItem, FetchRunResult
from berlin_insider.parser.models import ParsedItem, ParseRunResult


def render_summary(result: FetchRunResult) -> str:
    """Render human-readable summary for fetch-only output."""
    lines = [
        f"Fetch run started: {result.started_at.isoformat()}",
        f"Fetch run finished: {result.finished_at.isoformat()}",
        f"Total items: {result.total_items}",
        "",
    ]
    for source_result in _sorted_results(result):
        lines.append(
            f"- {source_result.source_id.value}: {source_result.status.value} | "
            f"items={len(source_result.items)} | duration_ms={source_result.duration_ms}"
        )
        if source_result.warnings:
            lines.append(f"  warnings: {'; '.join(source_result.warnings)}")
        if source_result.error_message:
            lines.append(f"  error: {source_result.error_message}")
        if source_result.items:
            lines.append(f"  sample: {_item_preview(source_result.items[0])}")
    return "\n".join(lines)


def render_summary_with_parse(fetch: FetchRunResult, parse: ParseRunResult) -> str:
    """Render summary for fetch + parse stages."""
    lines = [render_summary(fetch), "", f"Parsed total items: {parse.total_items}", ""]
    for source_result in sorted(parse.results, key=lambda item: item.source_id.value):
        lines.append(
            f"- {source_result.source_id.value}: {source_result.status.value} | "
            f"items={len(source_result.items)} | duration_ms={source_result.duration_ms}"
        )
        if source_result.warnings:
            lines.append(f"  warnings: {'; '.join(source_result.warnings)}")
        if source_result.error_message:
            lines.append(f"  error: {source_result.error_message}")
        if source_result.items:
            lines.append(f"  sample: {_parsed_item_preview(source_result.items[0])}")
    return "\n".join(lines)


def render_summary_with_parse_and_curate(
    fetch: FetchRunResult, parse: ParseRunResult, curate: CurateRunResult
) -> str:
    """Render summary for fetch + parse + curate pipeline output."""
    lines = [
        render_summary_with_parse(fetch, parse),
        "",
        f"Curated total items: {curate.actual_count}/{curate.target_count}",
        f"Curated dropped items: {curate.dropped_count}",
        f"Curator warnings: {'; '.join(curate.warnings) if curate.warnings else 'none'}",
        "",
    ]
    lines.extend(_render_curate_overview(curate))
    lines.extend(_render_curate_sources(curate))
    return "\n".join(lines)


def _render_curate_overview(curate: CurateRunResult) -> list[str]:
    counts = ", ".join(
        f"{category.value}={count}" for category, count in sorted(curate.category_counts.items())
    )
    lines = [f"Category counts: {counts}"]
    if curate.selected_items:
        lines.append(f"Top curated: {_curated_item_preview(curate.selected_items[0])}")
    return lines


def _render_curate_sources(curate: CurateRunResult) -> list[str]:
    lines: list[str] = []
    for source_result in sorted(curate.results, key=lambda item: item.source_id.value):
        lines.append(
            f"- {source_result.source_id.value}: {source_result.status.value} | "
            f"selected={len(source_result.selected_items)} | "
            f"dropped={len(source_result.dropped_items)} | duration_ms={source_result.duration_ms}"
        )
        if source_result.dropped_items:
            first_drop = source_result.dropped_items[0]
            detail = f" ({first_drop.details})" if first_drop.details else ""
            lines.append(f"  dropped_sample: {first_drop.reason.value}{detail}")
        if source_result.warnings:
            lines.append(f"  warnings: {'; '.join(source_result.warnings)}")
        if source_result.error_message:
            lines.append(f"  error: {source_result.error_message}")
    return lines


def _sorted_results(result: FetchRunResult) -> Iterable:
    return sorted(result.results, key=lambda item: item.source_id.value)


def _item_preview(item: FetchedItem) -> str:
    title = item.title or "untitled"
    date = item.raw_date_text or (item.published_at.isoformat() if item.published_at else "n/a")
    location = item.location_hint or "unknown location"
    snippet = (item.snippet or "").strip()
    snippet_text = snippet[:60] + ("..." if len(snippet) > 60 else "")
    return (
        f"{title} | {date} | {location} | method={item.fetch_method} | "
        f"meta={len(item.metadata)} | detail={'yes' if item.detail_text else 'no'} | {snippet_text}"
    )


def _parsed_item_preview(item: ParsedItem) -> str:
    title = item.title or "untitled"
    start = item.event_start_at.isoformat() if item.event_start_at else "n/a"
    end = item.event_end_at.isoformat() if item.event_end_at else "n/a"
    location = item.location or "unknown location"
    notes_count = len(item.parse_notes)
    raw_count = len(item.raw)
    return (
        f"{title} | {start} | end={end} | {location} | category={item.category.value} "
        f"({item.category_confidence:.2f}) | weekend={item.weekend_relevance.value} "
        f"({item.weekend_confidence:.2f}) | notes={notes_count} | raw={raw_count}"
    )


def _curated_item_preview(item: CuratedItem) -> str:
    title = item.item.title or "untitled"
    start = item.item.event_start_at.isoformat() if item.item.event_start_at else "n/a"
    notes_count = len(item.selection_notes)
    return (
        f"{title} | {start} | category={item.item.category.value} | "
        f"weekend={item.item.weekend_relevance.value} | score={item.score:.2f} | notes={notes_count}"
    )
