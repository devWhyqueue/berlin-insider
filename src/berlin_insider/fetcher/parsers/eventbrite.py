from __future__ import annotations

import json

from bs4 import BeautifulSoup

from berlin_insider.fetcher.base import SourceDefinition
from berlin_insider.fetcher.models import FetchContext, FetchedItem, FetchMethod
from berlin_insider.fetcher.parsers.common import aware
from berlin_insider.fetcher.utils import parse_datetime


def parse_eventbrite_jsonld(
    html: str, definition: SourceDefinition, context: FetchContext
) -> list[FetchedItem]:
    """Parse Eventbrite weekend events from JSON-LD ItemList payloads."""
    soup = BeautifulSoup(html, "html.parser")
    items: list[FetchedItem] = []
    for payload in _iter_payloads(soup):
        _append_events(payload, items, definition, context)
        if len(items) >= context.max_items_per_source:
            break
    return _dedupe_items(items, context.max_items_per_source)


def _iter_payloads(soup: BeautifulSoup):
    for script in soup.select("script[type='application/ld+json']"):
        content = script.string or script.get_text()
        if not content:
            continue
        yield from _json_documents(content)


def _append_events(payload, items, definition: SourceDefinition, context: FetchContext) -> None:
    for event in _iter_event_nodes(payload):
        item = _event_to_item(event, definition, context)
        if item is None:
            continue
        items.append(item)
        if len(items) >= context.max_items_per_source:
            return


def _iter_event_nodes(payload):
    if isinstance(payload, dict):
        payload_type = _normalized_type_names(payload.get("@type"))
        if "ItemList" in payload_type:
            elements = payload.get("itemListElement")
            if isinstance(elements, list):
                for element in elements:
                    item = element.get("item") if isinstance(element, dict) else None
                    if isinstance(item, dict):
                        yield from _iter_event_nodes(item)
        if "Event" in payload_type:
            yield payload
        for value in payload.values():
            yield from _iter_event_nodes(value)
        return
    if isinstance(payload, list):
        for value in payload:
            yield from _iter_event_nodes(value)


def _event_to_item(
    event, definition: SourceDefinition, context: FetchContext
) -> FetchedItem | None:
    if not isinstance(event, dict):
        return None
    link = _as_string(event.get("url"))
    if not link:
        return None
    location = event.get("location")
    location_name = location.get("name") if isinstance(location, dict) else None
    return FetchedItem(
        source_id=definition.source_id,
        source_url=definition.source_url,
        item_url=link,
        title=_as_string(event.get("name")),
        published_at=parse_datetime(_as_string(event.get("startDate"))),
        raw_date_text=_as_string(event.get("startDate")),
        snippet=_as_string(event.get("description")),
        location_hint=_as_string(location_name),
        fetch_method=FetchMethod.JSONLD,
        collected_at=aware(context.collected_at),
        metadata={"end_date": _as_string(event.get("endDate"))},
    )


def _json_documents(content: str) -> list[object]:
    cleaned = content.strip()
    if not cleaned:
        return []
    try:
        payload = json.loads(cleaned)
        return payload if isinstance(payload, list) else [payload]
    except json.JSONDecodeError:
        return _decode_many_json_values(cleaned)


def _decode_many_json_values(text: str) -> list[object]:
    docs: list[object] = []
    decoder = json.JSONDecoder()
    index = 0
    length = len(text)
    while index < length:
        while index < length and text[index] in {" ", "\t", "\r", "\n", ";"}:
            index += 1
        if index >= length:
            break
        try:
            payload, end = decoder.raw_decode(text, index)
        except json.JSONDecodeError:
            index += 1
            continue
        if isinstance(payload, list):
            docs.extend(payload)
        else:
            docs.append(payload)
        index = end
    return docs


def _dedupe_items(items: list[FetchedItem], max_items: int) -> list[FetchedItem]:
    deduped: list[FetchedItem] = []
    seen: set[str] = set()
    for item in items:
        if item.item_url in seen:
            continue
        seen.add(item.item_url)
        deduped.append(item)
        if len(deduped) >= max_items:
            break
    return deduped


def _as_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _normalized_type_names(value: object) -> set[str]:
    if isinstance(value, str):
        return {value}
    if isinstance(value, list):
        return {item for item in value if isinstance(item, str)}
    return set()
