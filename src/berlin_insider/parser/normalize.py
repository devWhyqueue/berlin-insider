from __future__ import annotations

from datetime import datetime

from berlin_insider.fetcher.models import FetchedItem
from berlin_insider.parser.classify import CategoryDecision, infer_category
from berlin_insider.parser.dates import (
    WeekendDecision,
    derive_event_start,
    infer_weekend_relevance,
    parse_end_date,
)
from berlin_insider.parser.models import ParsedItem

DESCRIPTION_MAX_CHARS = 280


def normalize_fetched_item(item: FetchedItem, *, reference_now: datetime) -> ParsedItem:
    """Convert one fetched item into a normalized parsed item."""
    notes: list[str] = []
    title = _normalize_text(item.title)
    description = _normalize_description(item.snippet)
    location = _normalize_text(item.location_hint)
    event_start_at = derive_event_start(item, reference_now=reference_now, notes=notes)
    category = infer_category(item, title=title, description=description, location=location)
    weekend = infer_weekend_relevance(event_start_at, reference_now=reference_now)
    _append_notes(notes, category.note, weekend.note, title=title, event_start_at=event_start_at)
    return _to_parsed_item(
        item=item,
        title=title,
        description=description,
        location=location,
        event_start_at=event_start_at,
        notes=notes,
        category=category,
        weekend=weekend,
    )


def _append_notes(
    notes: list[str],
    category_note: str | None,
    weekend_note: str | None,
    *,
    title: str | None,
    event_start_at: datetime | None,
) -> None:
    for note in [category_note, weekend_note]:
        if note:
            notes.append(note)
    if title is None:
        notes.append("Missing title")
    if event_start_at is None:
        notes.append("Could not derive event_start_at")


def _to_parsed_item(
    *,
    item: FetchedItem,
    title: str | None,
    description: str | None,
    location: str | None,
    event_start_at: datetime | None,
    notes: list[str],
    category: CategoryDecision,
    weekend: WeekendDecision,
) -> ParsedItem:
    return ParsedItem(
        source_id=item.source_id,
        item_url=item.item_url,
        title=title,
        description=description,
        event_start_at=event_start_at,
        event_end_at=parse_end_date(item.metadata.get("end_date")),
        location=location,
        category=category.category,
        category_confidence=category.confidence,
        weekend_relevance=weekend.relevance,
        weekend_confidence=weekend.confidence,
        parse_notes=notes,
        raw={
            "raw_date_text": item.raw_date_text,
            "metadata": item.metadata,
            "fetch_method": item.fetch_method.value,
        },
    )


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    collapsed = " ".join(value.split())
    return collapsed or None


def _normalize_description(value: str | None) -> str | None:
    normalized = _normalize_text(value)
    if normalized is None or len(normalized) <= DESCRIPTION_MAX_CHARS:
        return normalized
    return normalized[: DESCRIPTION_MAX_CHARS - 1].rstrip() + "…"
