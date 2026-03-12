from __future__ import annotations

from bs4 import BeautifulSoup

from berlin_insider.fetcher.base import SourceDefinition
from berlin_insider.fetcher.models import FetchContext, FetchedItem, FetchMethod
from berlin_insider.fetcher.parsers.common import absolute_url, aware
from berlin_insider.fetcher.utils import dedupe_urls


def extract_tip_berlin_items_from_html(
    html: str,
    definition: SourceDefinition,
    context: FetchContext,
) -> list[FetchedItem]:
    """Extract tip-berlin event links and best-effort titles from rendered HTML."""
    soup = BeautifulSoup(html, "html.parser")
    hrefs = _tip_event_urls(soup, definition.source_url)
    selected = hrefs[: context.max_items_per_source]
    return [_tip_item(soup, href, definition, context) for href in selected]


def _tip_event_urls(soup: BeautifulSoup, source_url: str) -> list[str]:
    urls: list[str] = []
    for anchor in soup.select("main a[href*='/event/']"):
        raw_href = anchor.get("href")
        href = raw_href if isinstance(raw_href, str) else ""
        if href.rstrip("/") == "/event":
            continue
        urls.append(absolute_url(source_url, href))
    return dedupe_urls(urls)


def _tip_item(
    soup: BeautifulSoup, href: str, definition: SourceDefinition, context: FetchContext
) -> FetchedItem:
    anchor = _tip_anchor_for_href(soup, href, definition.source_url)
    title = _tip_title(anchor)
    location = _tip_location(anchor)
    raw_date_text = _tip_date(anchor)
    return FetchedItem(
        source_id=definition.source_id,
        source_url=definition.source_url,
        item_url=href,
        title=title,
        published_at=None,
        raw_date_text=raw_date_text,
        snippet=None,
        location_hint=location,
        fetch_method=FetchMethod.PLAYWRIGHT_HTML,
        collected_at=aware(context.collected_at),
        metadata={},
    )


def _tip_title(anchor) -> str | None:
    if anchor is None:
        return None
    image = anchor.select_one("img[alt]")
    heading = anchor.select_one("h2")
    text = (
        heading.get_text(" ", strip=True)
        if heading
        else image.get("alt", "").strip()
        if image
        else anchor.get_text(" ", strip=True)
    )
    return text or None


def _tip_location(anchor) -> str | None:
    if anchor is None:
        return None
    candidates = anchor.select("h3, div, p")
    for node in candidates:
        text = node.get_text(" ", strip=True)
        if text and "Berlin" in text:
            return text
    return None


def _tip_date(anchor) -> str | None:
    if anchor is None:
        return None
    for node in anchor.select("p, h3"):
        text = node.get_text(" ", strip=True)
        if "." in text and any(
            token in text for token in ("Mo", "Di", "Mi", "Do", "Fr", "Sa", "So")
        ):
            return text
    return None


def _tip_anchor_for_href(soup: BeautifulSoup, href: str, source_url: str):
    for anchor in soup.select("main a[href*='/event/']"):
        raw_href = anchor.get("href")
        candidate = raw_href if isinstance(raw_href, str) else ""
        if absolute_url(source_url, candidate) == href:
            return anchor
    return None
