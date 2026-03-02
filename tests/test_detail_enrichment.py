from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from berlin_insider.fetcher.utils import enrich_items_with_detail, extract_detail_text
from berlin_insider.fetcher.models import FetchContext, FetchedItem, FetchMethod, SourceId
from berlin_insider.storage.detail_cache import SqliteDetailCacheStore


def _context() -> FetchContext:
    return FetchContext(
        user_agent="test-agent",
        timeout_seconds=1.0,
        max_items_per_source=5,
        collected_at=datetime.now(UTC),
    )


def _item(url: str) -> FetchedItem:
    return FetchedItem(
        source_id=SourceId.MITVERGNUEGEN,
        source_url="https://example.com/source",
        item_url=url,
        title="Item",
        published_at=None,
        raw_date_text=None,
        snippet="Listing snippet",
        location_hint=None,
        fetch_method=FetchMethod.RSS,
        collected_at=datetime.now(UTC),
        metadata={},
    )


def test_extract_detail_text_prefers_jsonld() -> None:
    html = """
    <html>
      <body>
        <script type="application/ld+json">
          {"@type":"Article","articleBody":"This is a long detailed body text that clearly passes the configured minimum detail length threshold."}
        </script>
        <article>Fallback article text that should not be used.</article>
      </body>
    </html>
    """
    detail = extract_detail_text(html)
    assert detail is not None
    assert detail.startswith("This is a long detailed body text")


def test_extract_detail_text_falls_back_to_article_and_main() -> None:
    article_html = "<html><body><article>Detailed article copy with enough words to be useful for parsing and classification.</article></body></html>"
    main_html = "<html><body><main>Detailed main copy with enough words to be useful for parsing and classification.</main></body></html>"
    assert extract_detail_text(article_html) is not None
    assert extract_detail_text(main_html) is not None


def test_extract_detail_text_falls_back_to_body_content() -> None:
    html = """
    <html><body>
      <nav>Top menu</nav>
      <div>Long body copy that remains after boilerplate removal and is still meaningful for parser enrichment and storage.</div>
    </body></html>
    """
    detail = extract_detail_text(html)
    assert detail is not None
    assert detail.startswith("Long body copy")


def test_extract_detail_text_returns_none_for_boilerplate() -> None:
    html = """
    <html><body>
      <nav>Main menu links</nav>
      <footer>Footer links and copyright</footer>
      <script>console.log("tracking")</script>
    </body></html>
    """
    assert extract_detail_text(html) is None


def test_enrich_items_with_detail_keeps_item_on_fetch_error(monkeypatch) -> None:
    def _raise(*args, **kwargs):  # noqa: ANN002, ANN003, ARG001
        raise RuntimeError("network down")

    monkeypatch.setattr("berlin_insider.fetcher.utils.get_text_with_retries", _raise)
    enriched_items, warnings = enrich_items_with_detail([_item("https://example.com/a")], context=_context())
    assert len(enriched_items) == 1
    assert enriched_items[0].detail_text == "Listing snippet"
    assert enriched_items[0].detail_status == "fallback_listing"
    assert any("Detail enrich failed" in warning for warning in warnings)


def test_extract_detail_text_handles_decomposed_nodes_without_crash() -> None:
    html = """
    <html><body>
      <nav class="menu"><span class="menu-item">Link</span></nav>
      <article>Real detail article content long enough to pass the minimum threshold for extraction logic.</article>
    </body></html>
    """
    detail = extract_detail_text(html)
    assert detail is not None
    assert detail.startswith("Real detail article content")


def test_enrich_items_with_detail_uses_listing_fallback_when_detail_empty(monkeypatch) -> None:
    def _minimal(*args, **kwargs):  # noqa: ANN002, ANN003, ARG001
        return "<html><body><nav>menu</nav></body></html>"

    monkeypatch.setattr("berlin_insider.fetcher.utils.get_text_with_retries", _minimal)
    enriched_items, warnings = enrich_items_with_detail([_item("https://example.com/b")], context=_context())
    assert len(enriched_items) == 1
    assert enriched_items[0].detail_text == "Listing snippet"
    assert enriched_items[0].detail_status == "fallback_listing"
    assert any("used listing fallback" in warning for warning in warnings)


def test_enrich_items_with_detail_falls_back_to_title_when_snippet_missing(monkeypatch) -> None:
    def _minimal(*args, **kwargs):  # noqa: ANN002, ANN003, ARG001
        return "<html><body></body></html>"

    item = _item("https://example.com/c")
    item.snippet = None
    item.title = "Fallback Title"
    monkeypatch.setattr("berlin_insider.fetcher.utils.get_text_with_retries", _minimal)
    enriched_items, _ = enrich_items_with_detail([item], context=_context())
    assert enriched_items[0].detail_text == "Fallback Title"
    assert enriched_items[0].detail_status == "fallback_listing"


def test_enrich_items_with_detail_retries_tip_pages_with_playwright(monkeypatch) -> None:
    def _http_get(url: str, **kwargs):  # noqa: ARG001, ANN003
        return "<html><body><div class='verification-container'>Checking your browser...</div></body></html>"

    def _pw_get(url: str, **kwargs):  # noqa: ARG001, ANN003
        return "<html><body><article>Tip Berlin detail content loaded in browser rendering path with enough text to pass extraction.</article></body></html>"

    monkeypatch.setattr("berlin_insider.fetcher.utils.get_text_with_retries", _http_get)
    monkeypatch.setattr("berlin_insider.fetcher.utils.get_text_with_playwright", _pw_get)
    item = _item("https://www.tip-berlin.de/event/sonstiges/1465.2875139721/")
    enriched_items, warnings = enrich_items_with_detail([item], context=_context())
    assert enriched_items[0].detail_status == "ok"
    assert enriched_items[0].detail_text is not None
    assert warnings == []


def test_enrich_items_with_detail_uses_cache_hit(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "cache.db"
    cache = SqliteDetailCacheStore(db_path)
    cache.upsert_detail(
        url="https://example.com/a?utm_source=test",
        source_id=SourceId.MITVERGNUEGEN.value,
        detail_text="Cached detail text",
        detail_hash="hash-a",
        detail_status="ok",
    )
    cache.upsert_summary(
        url="https://example.com/a",
        detail_hash="hash-a",
        summary="Cached summary",
    )

    def _raise(*args, **kwargs):  # noqa: ANN002, ANN003, ARG001
        raise AssertionError("detail network fetch should not run on cache hit")

    monkeypatch.setattr("berlin_insider.fetcher.utils.get_text_with_retries", _raise)
    context = _context()
    context.detail_cache_db_path = db_path
    enriched_items, warnings = enrich_items_with_detail([_item("https://example.com/a")], context=context)
    assert warnings == []
    assert enriched_items[0].detail_status == "cache_hit"
    assert enriched_items[0].detail_text == "Cached detail text"
    assert enriched_items[0].metadata.get("detail_cache_hit") is True
    assert enriched_items[0].metadata.get("cached_summary") == "Cached summary"


def test_enrich_items_with_detail_cache_miss_writes_cache(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "cache.db"

    def _detail_get(url: str, **kwargs):  # noqa: ANN003, ARG001
        return "<html><body><article>Fresh detail body text long enough to pass extraction and be cached for future runs.</article></body></html>"

    monkeypatch.setattr("berlin_insider.fetcher.utils.get_text_with_retries", _detail_get)
    context = _context()
    context.detail_cache_db_path = db_path
    enriched_items, warnings = enrich_items_with_detail([_item("https://example.com/new")], context=context)
    assert warnings == []
    assert enriched_items[0].detail_status == "ok"
    detail_hash = enriched_items[0].metadata.get("detail_hash")
    assert isinstance(detail_hash, str)
    cached = SqliteDetailCacheStore(db_path).get("https://example.com/new")
    assert cached is not None
    assert cached.detail_hash == detail_hash


def test_enrich_items_with_detail_refresh_bypasses_cache(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "cache.db"
    cache = SqliteDetailCacheStore(db_path)
    cache.upsert_detail(
        url="https://example.com/refresh",
        source_id=SourceId.MITVERGNUEGEN.value,
        detail_text="Old detail",
        detail_hash="old-hash",
        detail_status="ok",
    )
    calls = {"count": 0}

    def _detail_get(url: str, **kwargs):  # noqa: ANN003, ARG001
        calls["count"] += 1
        return "<html><body><article>Refreshed detail text with enough content to be extracted and replace cache value.</article></body></html>"

    monkeypatch.setattr("berlin_insider.fetcher.utils.get_text_with_retries", _detail_get)
    context = _context()
    context.detail_cache_db_path = db_path
    context.refresh_detail_cache = True
    enriched_items, warnings = enrich_items_with_detail(
        [_item("https://example.com/refresh")], context=context
    )
    assert warnings == []
    assert calls["count"] == 1
    assert enriched_items[0].detail_status == "ok"
    cached = SqliteDetailCacheStore(db_path).get("https://example.com/refresh")
    assert cached is not None
    assert cached.detail_text != "Old detail"
