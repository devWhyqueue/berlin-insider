from __future__ import annotations

import json
import re

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
        _append_item_list_events(payload, items, definition, context)
        if len(items) >= context.max_items_per_source:
            break
    return _dedupe_items(items, context.max_items_per_source)


def _iter_payloads(soup: BeautifulSoup):
    for script in soup.select("script[type='application/ld+json']"):
        content = script.string or script.get_text()
        if not content:
            continue
        yield from _json_documents(content)


def _append_item_list_events(
    payload, items, definition: SourceDefinition, context: FetchContext
) -> None:
    if not isinstance(payload, dict) or payload.get("@type") != "ItemList":
        return
    elements = payload.get("itemListElement")
    if not isinstance(elements, list):
        return
    for element in elements:
        item = _eventbrite_item(element, definition, context)
        if item is None:
            continue
        items.append(item)
        if len(items) >= context.max_items_per_source:
            return


def _eventbrite_item(
    element, definition: SourceDefinition, context: FetchContext
) -> FetchedItem | None:
    item = element.get("item") if isinstance(element, dict) else None
    if not isinstance(item, dict):
        return None
    link = _as_string(item.get("url"))
    if not link:
        return None
    location = item.get("location")
    location_name = location.get("name") if isinstance(location, dict) else None
    return FetchedItem(
        source_id=definition.source_id,
        source_url=definition.source_url,
        item_url=link,
        title=_as_string(item.get("name")),
        published_at=parse_datetime(_as_string(item.get("startDate"))),
        raw_date_text=_as_string(item.get("startDate")),
        snippet=_as_string(item.get("description")),
        location_hint=_as_string(location_name),
        fetch_method=FetchMethod.JSONLD,
        collected_at=aware(context.collected_at),
        metadata={"end_date": _as_string(item.get("endDate"))},
    )


def _json_documents(content: str) -> list[object]:
    cleaned = content.strip()
    if not cleaned:
        return []
    try:
        payload = json.loads(cleaned)
        return payload if isinstance(payload, list) else [payload]
    except json.JSONDecodeError:
        docs: list[object] = []
        for candidate in re.findall(r"\{.*\}", cleaned, flags=re.DOTALL):
            try:
                docs.append(json.loads(candidate))
            except json.JSONDecodeError:
                continue
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
