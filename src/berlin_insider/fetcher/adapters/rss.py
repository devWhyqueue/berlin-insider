from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from time import perf_counter
from xml.etree import ElementTree

from berlin_insider.fetcher.base import SourceAdapter, SourceDefinition
from berlin_insider.fetcher.http import get_text_with_retries
from berlin_insider.fetcher.models import (
    FetchContext,
    FetchedItem,
    FetchMethod,
    FetchStatus,
    SourceFetchResult,
)
from berlin_insider.fetcher.utils import parse_datetime


@dataclass(slots=True)
class RssAdapter(SourceAdapter):
    """Fetch and parse one RSS feed into canonical fetched items."""

    definition: SourceDefinition
    feed_url: str

    def fetch(self, context: FetchContext) -> SourceFetchResult:
        """Return a source result derived from the adapter's RSS feed URL."""
        started = perf_counter()
        warnings: list[str] = []
        try:
            items = self._fetch_items(context, warnings)
            return _success_result(self.definition.source_id, items, warnings, started)
        except Exception as exc:  # noqa: BLE001
            return SourceFetchResult(
                source_id=self.definition.source_id,
                status=FetchStatus.ERROR,
                items=[],
                warnings=warnings,
                error_message=str(exc),
                duration_ms=_duration_ms(started),
            )

    def _fetch_items(self, context: FetchContext, warnings: list[str]) -> list[FetchedItem]:
        xml_text = get_text_with_retries(
            self.feed_url,
            user_agent=context.user_agent,
            timeout_seconds=context.timeout_seconds,
            on_retry=warnings.append,
        )
        return parse_rss_items(
            xml_text=xml_text,
            source_id=self.definition.source_id,
            source_url=self.definition.source_url,
            collected_at=context.collected_at,
            max_items=context.max_items_per_source,
        )


def parse_rss_items(
    *,
    xml_text: str,
    source_id,
    source_url: str,
    collected_at: datetime,
    max_items: int,
) -> list[FetchedItem]:
    """Parse RSS XML into canonical fetched items."""
    root = ElementTree.fromstring(xml_text)
    raw_items = root.findall("./channel/item")
    parsed: list[FetchedItem] = []
    for item in raw_items:
        parsed_item = _from_rss_xml_item(item, source_id, source_url, collected_at)
        if parsed_item is None:
            continue
        parsed.append(parsed_item)
        if len(parsed) >= max_items:
            break
    return parsed


def _text(node: ElementTree.Element | None) -> str | None:
    if node is None or node.text is None:
        return None
    value = node.text.strip()
    return value if value else None


def _duration_ms(started: float) -> int:
    return int((perf_counter() - started) * 1000)


def _success_result(
    source_id, items: list[FetchedItem], warnings: list[str], started: float
) -> SourceFetchResult:
    status = FetchStatus.SUCCESS if items else FetchStatus.PARTIAL
    if not items:
        warnings.append("No RSS entries were parsed")
    return SourceFetchResult(
        source_id=source_id,
        status=status,
        items=items,
        warnings=warnings,
        error_message=None,
        duration_ms=_duration_ms(started),
    )


def _from_rss_xml_item(
    item, source_id, source_url: str, collected_at: datetime
) -> FetchedItem | None:
    link = _text(item.find("link"))
    if not link:
        return None
    return FetchedItem(
        source_id=source_id,
        source_url=source_url,
        item_url=link,
        title=_text(item.find("title")),
        published_at=parse_datetime(_text(item.find("pubDate"))),
        raw_date_text=_text(item.find("pubDate")),
        snippet=_text(item.find("description")),
        location_hint=None,
        fetch_method=FetchMethod.RSS,
        collected_at=collected_at if collected_at.tzinfo else collected_at.replace(tzinfo=UTC),
        metadata={},
    )
