from __future__ import annotations

from datetime import datetime
from typing import Any

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
    metadata = item.metadata if isinstance(item.metadata, dict) else {}
    title, detail_text, clean_text, description = _text_fields(item)
    location = _normalize_text(item.location_hint) or _normalize_text(
        _as_optional_str(metadata.get("location"))
    )
    event_start_at = derive_event_start(item, reference_now=reference_now, notes=notes)
    category = infer_category(item, title=title, description=description, location=location)
    weekend = infer_weekend_relevance(event_start_at, reference_now=reference_now)
    _append_notes(notes, category.note, weekend.note, title=title, event_start_at=event_start_at)
    return _to_parsed_item(
        item=item,
        title=title,
        description=description,
        detail_text=detail_text,
        clean_text=clean_text,
        location=location,
        event_start_at=event_start_at,
        notes=notes,
        category=category,
        weekend=weekend,
        metadata=metadata,
    )


def _text_fields(item: FetchedItem) -> tuple[str | None, str | None, str | None, str | None]:
    title = _normalize_text(item.title)
    detail_text = _normalize_detail_text(item.detail_text)
    body = detail_text or item.snippet
    return title, detail_text, _normalize_clean_text(body), _normalize_description(body)


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
    detail_text: str | None,
    clean_text: str | None,
    location: str | None,
    event_start_at: datetime | None,
    notes: list[str],
    category: CategoryDecision,
    weekend: WeekendDecision,
    metadata: dict[str, Any],
) -> ParsedItem:
    cached_summary = _as_optional_str(metadata.get("cached_summary"))
    return ParsedItem(
        source_id=item.source_id,
        item_url=item.item_url,
        title=title,
        description=description,
        detail_text=detail_text,
        clean_text=clean_text,
        event_start_at=event_start_at,
        event_end_at=parse_end_date(metadata.get("end_date")),
        location=location,
        category=category.category,
        category_confidence=category.confidence,
        weekend_relevance=weekend.relevance,
        weekend_confidence=weekend.confidence,
        parse_notes=notes,
        raw={
            "raw_date_text": item.raw_date_text,
            "metadata": metadata,
            "fetch_method": item.fetch_method.value,
            "detail_status": item.detail_status,
        },
        summary=cached_summary,
        price_text=_as_optional_str(metadata.get("price_text")),
        price_amount=_as_optional_float(metadata.get("price_amount")),
        price_currency=_as_optional_str(metadata.get("price_currency")),
        is_free=_as_optional_bool(metadata.get("is_free")),
        event_date_source=_event_date_source(notes),
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


def _normalize_detail_text(value: str | None) -> str | None:
    return _normalize_text(value)


def _normalize_clean_text(value: str | None) -> str | None:
    normalized = _normalize_text(value)
    if normalized is None:
        return None
    cleaned = normalized
    for marker in ("Cookie", "Datenschutz", "Newsletter abonnieren", "Mehr Infos"):
        cleaned = cleaned.split(marker, 1)[0].strip()
    return cleaned or normalized


def _as_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "."))
    except ValueError:
        return None


def _as_optional_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    normalized = str(value).strip().casefold()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    return None


def _event_date_source(notes: list[str]) -> str | None:
    for note in notes:
        if note.startswith("event_start_at from "):
            return note.removeprefix("event_start_at from ")
    return None
