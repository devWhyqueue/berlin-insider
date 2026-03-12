from __future__ import annotations

from bs4 import BeautifulSoup

from berlin_insider.fetcher.base import SourceDefinition
from berlin_insider.fetcher.models import FetchContext, FetchedItem, FetchMethod
from berlin_insider.fetcher.parsers.common import absolute_url, aware
from berlin_insider.fetcher.utils import dedupe_urls


def parse_visit_berlin_daily(
    html: str, definition: SourceDefinition, context: FetchContext
) -> list[FetchedItem]:
    """Parse visitBerlin day tip cards from the event finder listing."""
    soup = BeautifulSoup(html, "html.parser")
    items: list[FetchedItem] = []
    for article in soup.select("main article"):
        item = _visit_berlin_item(article, definition, context)
        if item is None:
            continue
        items.append(item)
        if len(items) >= context.max_items_per_source:
            break
    return _dedupe_items_by_url(items)


def _visit_berlin_item(
    article, definition: SourceDefinition, context: FetchContext
) -> FetchedItem | None:
    link = article.select_one("a[href*='/event/']")
    heading = article.select_one("h2")
    if link is None or heading is None:
        return None
    href = link.get("href", "").strip()
    if not href or "/event/" not in href or "/en/event/" in href:
        return None
    return FetchedItem(
        source_id=definition.source_id,
        source_url=definition.source_url,
        item_url=absolute_url(definition.source_url, href),
        title=heading.get_text(" ", strip=True) or None,
        published_at=None,
        raw_date_text=_visit_berlin_date_text(article),
        snippet=_visit_berlin_snippet(article),
        location_hint=None,
        fetch_method=FetchMethod.HTML,
        collected_at=aware(context.collected_at),
        metadata={},
    )


def _visit_berlin_date_text(article) -> str | None:
    text = " ".join(node.get_text(" ", strip=True) for node in article.select("time"))
    return text or None


def _visit_berlin_snippet(article) -> str | None:
    node = article.select_one("div:not(:has(*))")
    if node is None:
        return None
    text = node.get_text(" ", strip=True)
    return text or None


def parse_berlin_de_tickets_heute(
    html: str, definition: SourceDefinition, context: FetchContext
) -> list[FetchedItem]:
    """Parse Berlin.de 'Heute in Berlin' editorial picks."""
    soup = BeautifulSoup(html, "html.parser")
    items: list[FetchedItem] = []
    for article in soup.select("main article"):
        item = _berlin_de_tickets_item(article, definition, context)
        if item is None:
            continue
        items.append(item)
        if len(items) >= context.max_items_per_source:
            break
    return _dedupe_items_by_url(items)


def parse_ra_berlin(
    html: str, definition: SourceDefinition, context: FetchContext
) -> list[FetchedItem]:
    """Parse Resident Advisor Berlin event listings."""
    soup = BeautifulSoup(html, "html.parser")
    items: list[FetchedItem] = []
    for anchor in soup.select("main h3 a[href*='/events/']"):
        item = _ra_item(anchor, definition, context)
        if item is None:
            continue
        items.append(item)
        if len(items) >= context.max_items_per_source:
            break
    return _dedupe_items_by_url(items)


def _berlin_de_tickets_item(
    article, definition: SourceDefinition, context: FetchContext
) -> FetchedItem | None:
    heading = article.select_one("h3")
    link = heading.select_one("a") if heading else None
    if link is None:
        return None
    href = link.get("href", "").strip()
    if not href or "/tickets/" not in href:
        return None
    return FetchedItem(
        source_id=definition.source_id,
        source_url=definition.source_url,
        item_url=absolute_url(definition.source_url, href),
        title=link.get_text(" ", strip=True) or None,
        published_at=None,
        raw_date_text=None,
        snippet=_berlin_de_teaser_text(article),
        location_hint=None,
        fetch_method=FetchMethod.HTML,
        collected_at=aware(context.collected_at),
        metadata={},
    )


def _ra_item(anchor, definition: SourceDefinition, context: FetchContext) -> FetchedItem | None:
    href = anchor.get("href", "").strip()
    if not href or "#tickets" in href:
        return None
    return FetchedItem(
        source_id=definition.source_id,
        source_url=definition.source_url,
        item_url=absolute_url(definition.source_url, href),
        title=anchor.get_text(" ", strip=True) or None,
        published_at=None,
        raw_date_text=_ra_date_text(anchor),
        snippet=None,
        location_hint=_ra_location(anchor),
        fetch_method=FetchMethod.PLAYWRIGHT_HTML,
        collected_at=aware(context.collected_at),
        metadata={},
    )


def _berlin_de_teaser_text(article) -> str | None:
    for paragraph in article.select("p"):
        text = paragraph.get_text(" ", strip=True)
        if not text or text.startswith("©"):
            continue
        return text.removesuffix(" mehr").strip() or None
    return None


def _ra_date_text(anchor) -> str | None:
    day_map = {
        "Mo": "Montag",
        "Di": "Dienstag",
        "Mi": "Mittwoch",
        "Do": "Donnerstag",
        "Fr": "Freitag",
        "Sa": "Samstag",
        "So": "Sonntag",
    }
    for text in anchor.parent.stripped_strings:
        normalized = text.replace("̸", "").strip()
        for short, full in day_map.items():
            if normalized.startswith(f"{short}."):
                return full
    for text in anchor.find_all_previous(string=True, limit=30):
        normalized = " ".join(str(text).replace("̸", " ").split())
        for short, full in day_map.items():
            if normalized.startswith(f"{short}."):
                return full
    return None


def _ra_location(anchor) -> str | None:
    container = anchor.parent.parent if anchor.parent is not None else None
    if container is None:
        return None
    club_link = container.select_one("a[href^='/clubs/']")
    if club_link is not None:
        text = club_link.get_text(" ", strip=True)
        return text or None
    for text in container.stripped_strings:
        normalized = " ".join(text.split())
        if "Berlin" in normalized or normalized.startswith("TBA"):
            return normalized
    return None


def _dedupe_items_by_url(items: list[FetchedItem]) -> list[FetchedItem]:
    unique_urls = set(dedupe_urls([item.item_url for item in items]))
    deduped: list[FetchedItem] = []
    seen: set[str] = set()
    for item in items:
        if item.item_url not in unique_urls or item.item_url in seen:
            continue
        seen.add(item.item_url)
        deduped.append(item)
    return deduped
