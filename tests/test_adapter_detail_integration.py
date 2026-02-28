from __future__ import annotations

from datetime import UTC, datetime

from berlin_insider.fetcher.adapters.html import HtmlAdapter
from berlin_insider.fetcher.adapters.rss import RssAdapter
from berlin_insider.fetcher.base import SourceDefinition
from berlin_insider.fetcher.models import FetchContext, FetchedItem, FetchMethod, SourceId


def _context() -> FetchContext:
    return FetchContext(
        user_agent="test-agent",
        timeout_seconds=1.0,
        max_items_per_source=5,
        collected_at=datetime.now(UTC),
    )


def test_rss_adapter_enriches_detail_text(monkeypatch) -> None:
    feed_xml = """<?xml version="1.0"?>
    <rss><channel><item>
      <title>Weekend Pick</title>
      <link>https://example.com/a</link>
      <description>Listing text</description>
    </item></channel></rss>"""

    def _feed_get(url: str, **kwargs):  # noqa: ANN003, ARG001
        return feed_xml

    def _detail_get(url: str, **kwargs):  # noqa: ANN003, ARG001
        return "<html><body><article>This detail page has enough content to be captured as full detail text for parsing.</article></body></html>"

    monkeypatch.setattr("berlin_insider.fetcher.adapters.rss.get_text_with_retries", _feed_get)
    monkeypatch.setattr("berlin_insider.fetcher.utils.get_text_with_retries", _detail_get)
    adapter = RssAdapter(
        definition=SourceDefinition(SourceId.MITVERGNUEGEN, "https://example.com/source"),
        feed_url="https://example.com/feed.xml",
    )
    result = adapter.fetch(_context())
    assert len(result.items) == 1
    assert result.items[0].detail_text is not None
    assert result.items[0].detail_status == "ok"


def test_html_adapter_enriches_detail_text(monkeypatch) -> None:
    def _listing_get(url: str, **kwargs):  # noqa: ANN003, ARG001
        return "<html><body><a href='/item'>Item</a></body></html>"

    def _detail_get(url: str, **kwargs):  # noqa: ANN003, ARG001
        if url.endswith("/item"):
            return "<html><body><main>Detailed venue and event content that should be extracted from the detail page.</main></body></html>"
        return _listing_get(url)

    def _parser(html: str, definition: SourceDefinition, context: FetchContext) -> list[FetchedItem]:
        return [
            FetchedItem(
                source_id=definition.source_id,
                source_url=definition.source_url,
                item_url="https://example.com/item",
                title="Item",
                published_at=None,
                raw_date_text=None,
                snippet="Listing snippet",
                location_hint=None,
                fetch_method=FetchMethod.HTML,
                collected_at=context.collected_at if context.collected_at.tzinfo else context.collected_at.replace(tzinfo=UTC),
                metadata={},
            )
        ]

    monkeypatch.setattr("berlin_insider.fetcher.adapters.html.get_text_with_retries", _listing_get)
    monkeypatch.setattr("berlin_insider.fetcher.utils.get_text_with_retries", _detail_get)
    adapter = HtmlAdapter(
        definition=SourceDefinition(SourceId.GRATIS_IN_BERLIN, "https://example.com/list"),
        parser=_parser,
    )
    result = adapter.fetch(_context())
    assert len(result.items) == 1
    assert result.items[0].detail_text is not None
    assert result.items[0].detail_status == "ok"
