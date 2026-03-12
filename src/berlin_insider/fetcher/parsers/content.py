from __future__ import annotations

from bs4 import BeautifulSoup

from berlin_insider.fetcher.base import SourceDefinition
from berlin_insider.fetcher.models import FetchContext, FetchedItem, FetchMethod
from berlin_insider.fetcher.parsers.common import absolute_url, aware
from berlin_insider.fetcher.utils import dedupe_urls, parse_datetime


def parse_berlin_food_stories(
    html: str, definition: SourceDefinition, context: FetchContext
) -> list[FetchedItem]:
    """Parse Berlin Food Stories teaser links from listing pages."""
    soup = BeautifulSoup(html, "html.parser")
    items: list[FetchedItem] = []
    for headline in soup.select("h3.article-teaser__headline a"):
        item = _food_story_item(headline, definition, context)
        if item is None:
            continue
        items.append(item)
        if len(items) >= context.max_items_per_source:
            break
    return items


def _food_story_item(
    headline, definition: SourceDefinition, context: FetchContext
) -> FetchedItem | None:
    link = headline.get("href", "").strip()
    if not link:
        return None
    return FetchedItem(
        source_id=definition.source_id,
        source_url=definition.source_url,
        item_url=absolute_url(definition.source_url, link),
        title=headline.get_text(strip=True) or None,
        published_at=None,
        raw_date_text=None,
        snippet=None,
        location_hint=None,
        fetch_method=FetchMethod.HTML,
        collected_at=aware(context.collected_at),
        metadata={},
    )


def parse_rausgegangen(
    html: str, definition: SourceDefinition, context: FetchContext
) -> list[FetchedItem]:
    """Parse event cards from Rausgegangen weekend listing pages."""
    soup = BeautifulSoup(html, "html.parser")
    items: list[FetchedItem] = []
    for card in soup.select("a.event-tile"):
        item = _rausgegangen_item(card, definition, context)
        if item is None:
            continue
        items.append(item)
        if len(items) >= context.max_items_per_source:
            break
    return items


def parse_rausgegangen_daily(
    html: str, definition: SourceDefinition, context: FetchContext
) -> list[FetchedItem]:
    """Parse event cards from the main Rausgegangen Berlin day tips page."""
    soup = BeautifulSoup(html, "html.parser")
    items: list[FetchedItem] = []
    for card in soup.select("a[href^='/events/']"):
        item = _rausgegangen_item(card, definition, context)
        if item is None:
            continue
        items.append(item)
        if len(items) >= context.max_items_per_source:
            break
    return _dedupe_rausgegangen_items(items)


def _rausgegangen_item(
    card, definition: SourceDefinition, context: FetchContext
) -> FetchedItem | None:
    href = card.get("href", "").strip()
    if not href:
        return None
    title = card.select_one("h4, [role='heading'], heading, h3")
    date_hint = _rausgegangen_date_hint(card)
    venue_hint = _rausgegangen_venue_hint(card, title=title)
    return FetchedItem(
        source_id=definition.source_id,
        source_url=definition.source_url,
        item_url=absolute_url(definition.source_url, href),
        title=title.get_text(" ", strip=True) if title else None,
        published_at=None,
        raw_date_text=date_hint,
        snippet=None,
        location_hint=venue_hint,
        fetch_method=FetchMethod.HTML,
        collected_at=aware(context.collected_at),
        metadata={},
    )


def _rausgegangen_date_hint(card) -> str | None:
    node = card.select_one("span.text-sm, div[class*='date'], p[class*='date']")
    if node is not None:
        return node.get_text(" ", strip=True) or None
    for candidate in card.select("div, span, p"):
        text = candidate.get_text(" ", strip=True)
        if "|" in text and "Uhr" in text:
            return text
    return None


def _rausgegangen_venue_hint(card, *, title) -> str | None:
    node = card.select_one("span.text-sm.pr-1.opacity-70, [class*='venue'], [class*='location']")
    if node is not None:
        return node.get_text(" ", strip=True) or None
    title_text = title.get_text(" ", strip=True) if title else None
    date_text = _rausgegangen_date_hint(card)
    for candidate in card.select("div, span, p"):
        text = candidate.get_text(" ", strip=True)
        if not text or text == title_text or text == date_text:
            continue
        return text
    return None


def _dedupe_rausgegangen_items(items: list[FetchedItem]) -> list[FetchedItem]:
    unique_urls = set(dedupe_urls([item.item_url for item in items]))
    deduped: list[FetchedItem] = []
    seen: set[str] = set()
    for item in items:
        if item.item_url not in unique_urls or item.item_url in seen:
            continue
        seen.add(item.item_url)
        deduped.append(item)
    return deduped


def parse_gratis_in_berlin(
    html: str, definition: SourceDefinition, context: FetchContext
) -> list[FetchedItem]:
    """Parse featured listings from gratis-in-berlin homepage blocks."""
    soup = BeautifulSoup(html, "html.parser")
    items: list[FetchedItem] = []
    for anchor in soup.select(".tipp_wrapper h2.overviewcontentheading a"):
        item = _gratis_item(anchor, definition, context)
        if item is None:
            continue
        items.append(item)
        if len(items) >= context.max_items_per_source:
            break
    return items


def _gratis_item(anchor, definition: SourceDefinition, context: FetchContext) -> FetchedItem | None:
    href = anchor.get("href", "").strip()
    if not href:
        return None
    wrapper = anchor.find_parent(class_="tipp_wrapper")
    date_hint = _date_text(wrapper)
    return FetchedItem(
        source_id=definition.source_id,
        source_url=definition.source_url,
        item_url=absolute_url(definition.source_url, href),
        title=anchor.get_text(" ", strip=True) or None,
        published_at=None,
        raw_date_text=date_hint,
        snippet=None,
        location_hint=None,
        fetch_method=FetchMethod.HTML,
        collected_at=aware(context.collected_at),
        metadata={},
    )


def _date_text(wrapper) -> str | None:
    if wrapper is None:
        return None
    date_node = wrapper.select_one(".dateTipp")
    if date_node is None:
        return None
    return date_node.get_text(" ", strip=True)


def parse_telegram(
    html: str, definition: SourceDefinition, context: FetchContext
) -> list[FetchedItem]:
    """Parse Telegram channel post snippets from `t.me/s/...` pages."""
    soup = BeautifulSoup(html, "html.parser")
    items: list[FetchedItem] = []
    for wrapper in soup.select(".tgme_widget_message_wrap"):
        item = _telegram_item(wrapper, definition, context)
        if item is None:
            continue
        items.append(item)
        if len(items) >= context.max_items_per_source:
            break
    return items


def _telegram_item(
    wrapper, definition: SourceDefinition, context: FetchContext
) -> FetchedItem | None:
    post = wrapper.select_one(".tgme_widget_message")
    post_id = post.get("data-post", "").strip() if post else ""
    if not post_id:
        return None
    text_node = wrapper.select_one(".tgme_widget_message_text")
    time_node = wrapper.select_one("time")
    href_node = wrapper.select_one(".tgme_widget_message_date")
    link = href_node.get("href", "").strip() if href_node else f"https://t.me/{post_id}"
    return FetchedItem(
        source_id=definition.source_id,
        source_url=definition.source_url,
        item_url=link,
        title=None,
        published_at=parse_datetime(time_node.get("datetime") if time_node else None),
        raw_date_text=time_node.get_text(" ", strip=True) if time_node else None,
        snippet=text_node.get_text(" ", strip=True) if text_node else None,
        location_hint=None,
        fetch_method=FetchMethod.TELEGRAM_HTML,
        collected_at=aware(context.collected_at),
        metadata={"post_id": post_id},
    )
