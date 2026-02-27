from datetime import UTC, datetime

from berlin_insider.fetcher.adapters.rss import parse_rss_items
from berlin_insider.fetcher.models import SourceId


def test_parse_rss_items_extracts_entries() -> None:
    xml = """<?xml version="1.0"?>
    <rss>
      <channel>
        <item>
          <title>Weekend Market</title>
          <link>https://example.com/a</link>
          <pubDate>Fri, 27 Feb 2026 10:00:00 +0000</pubDate>
          <description>Fresh food and music.</description>
        </item>
      </channel>
    </rss>"""
    items = parse_rss_items(
        xml_text=xml,
        source_id=SourceId.MITVERGNUEGEN,
        source_url="https://example.com",
        collected_at=datetime.now(UTC),
        max_items=10,
    )
    assert len(items) == 1
    assert items[0].title == "Weekend Market"
    assert items[0].item_url == "https://example.com/a"
    assert items[0].snippet == "Fresh food and music."
