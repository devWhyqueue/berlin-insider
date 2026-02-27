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
    for anchor in soup.select("a[href*='/event/'][href*='1465.']"):
        raw_href = anchor.get("href")
        href = raw_href if isinstance(raw_href, str) else ""
        urls.append(absolute_url(source_url, href))
    return dedupe_urls(urls)


def _tip_item(
    soup: BeautifulSoup, href: str, definition: SourceDefinition, context: FetchContext
) -> FetchedItem:
    anchor = soup.select_one(f"a[href='{href}']")
    title = _tip_title(anchor)
    return FetchedItem(
        source_id=definition.source_id,
        source_url=definition.source_url,
        item_url=href,
        title=title,
        published_at=None,
        raw_date_text=None,
        snippet=None,
        location_hint=None,
        fetch_method=FetchMethod.PLAYWRIGHT_HTML,
        collected_at=aware(context.collected_at),
        metadata={},
    )


def _tip_title(anchor) -> str | None:
    if anchor is None:
        return None
    image = anchor.select_one("img[alt]")
    text = image.get("alt", "").strip() if image else anchor.get_text(" ", strip=True)
    return text or None
