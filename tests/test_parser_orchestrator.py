from datetime import UTC, datetime
from pathlib import Path

import berlin_insider.parser.orchestrator as parser_module
from berlin_insider.fetcher.models import (
    FetchedItem,
    FetchMethod,
    FetchRunResult,
    FetchStatus,
    SourceFetchResult,
    SourceId,
)
from berlin_insider.parser.models import ParseStatus
from berlin_insider.parser.orchestrator import Parser
from berlin_insider.storage.detail_cache import SqliteDetailCacheStore


def _fetched_item(source_id: SourceId, url: str) -> FetchedItem:
    return FetchedItem(
        source_id=source_id,
        source_url="https://example.com",
        item_url=url,
        title="Title",
        published_at=datetime(2026, 2, 28, 10, 0, tzinfo=UTC),
        raw_date_text=None,
        snippet="Snippet",
        location_hint="Berlin",
        fetch_method=FetchMethod.HTML,
        collected_at=datetime(2026, 2, 27, 9, 0, tzinfo=UTC),
        metadata={},
    )


def test_parser_keeps_source_order_and_marks_fetch_errors() -> None:
    fetch_result = FetchRunResult(
        started_at=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        finished_at=datetime(2026, 2, 27, 9, 0, tzinfo=UTC),
        results=[
            SourceFetchResult(
                source_id=SourceId.MITVERGNUEGEN,
                status=FetchStatus.SUCCESS,
                items=[_fetched_item(SourceId.MITVERGNUEGEN, "https://example.com/a")],
                warnings=[],
                error_message=None,
                duration_ms=1,
            ),
            SourceFetchResult(
                source_id=SourceId.TIP_BERLIN_HOME,
                status=FetchStatus.ERROR,
                items=[],
                warnings=[],
                error_message="boom",
                duration_ms=1,
            ),
        ],
        total_items=1,
        failed_sources=[SourceId.TIP_BERLIN_HOME],
    )

    parsed = Parser().run(fetch_result)
    assert [result.source_id for result in parsed.results] == [
        SourceId.MITVERGNUEGEN,
        SourceId.TIP_BERLIN_HOME,
    ]
    assert parsed.results[0].status == ParseStatus.SUCCESS
    assert parsed.results[1].status == ParseStatus.ERROR
    assert parsed.failed_sources == [SourceId.TIP_BERLIN_HOME]


def test_parser_marks_partial_when_single_item_fails(monkeypatch) -> None:
    original = parser_module.normalize_fetched_item

    def _maybe_fail(item, *, reference_now):  # noqa: ANN001, ANN202
        if item.item_url.endswith("/bad"):
            raise ValueError("bad item")
        return original(item, reference_now=reference_now)

    monkeypatch.setattr(parser_module, "normalize_fetched_item", _maybe_fail)
    fetch_result = FetchRunResult(
        started_at=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        finished_at=datetime(2026, 2, 27, 9, 0, tzinfo=UTC),
        results=[
            SourceFetchResult(
                source_id=SourceId.GRATIS_IN_BERLIN,
                status=FetchStatus.SUCCESS,
                items=[
                    _fetched_item(SourceId.GRATIS_IN_BERLIN, "https://example.com/good"),
                    _fetched_item(SourceId.GRATIS_IN_BERLIN, "https://example.com/bad"),
                ],
                warnings=[],
                error_message=None,
                duration_ms=1,
            )
        ],
        total_items=2,
        failed_sources=[],
    )

    parsed = Parser().run(fetch_result)
    assert parsed.results[0].status == ParseStatus.PARTIAL
    assert len(parsed.results[0].items) == 1
    assert any("Failed to parse item" in warning for warning in parsed.results[0].warnings)


def test_parser_adds_summary_when_generator_returns_text() -> None:
    class _FakeSummaryGenerator:
        def summarize(self, item):  # noqa: ANN001, ANN202
            return "One sentence summary."

    fetch_result = FetchRunResult(
        started_at=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        finished_at=datetime(2026, 2, 27, 9, 0, tzinfo=UTC),
        results=[
            SourceFetchResult(
                source_id=SourceId.MITVERGNUEGEN,
                status=FetchStatus.SUCCESS,
                items=[_fetched_item(SourceId.MITVERGNUEGEN, "https://example.com/a")],
                warnings=[],
                error_message=None,
                duration_ms=1,
            )
        ],
        total_items=1,
        failed_sources=[],
    )

    parsed = Parser(summary_generator=_FakeSummaryGenerator()).run(fetch_result)
    assert parsed.results[0].items[0].summary == "One sentence summary."


def test_parser_keeps_item_when_summary_generator_fails() -> None:
    class _FailingSummaryGenerator:
        def summarize(self, item):  # noqa: ANN001, ANN202
            raise RuntimeError("summary api down")

    fetch_result = FetchRunResult(
        started_at=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        finished_at=datetime(2026, 2, 27, 9, 0, tzinfo=UTC),
        results=[
            SourceFetchResult(
                source_id=SourceId.MITVERGNUEGEN,
                status=FetchStatus.SUCCESS,
                items=[_fetched_item(SourceId.MITVERGNUEGEN, "https://example.com/a")],
                warnings=[],
                error_message=None,
                duration_ms=1,
            )
        ],
        total_items=1,
        failed_sources=[],
    )

    parsed = Parser(summary_generator=_FailingSummaryGenerator()).run(fetch_result)
    assert len(parsed.results[0].items) == 1
    assert parsed.results[0].items[0].summary is None
    assert any("Failed to summarize item" in warning for warning in parsed.results[0].warnings)


def test_parser_skips_summary_call_when_cached_summary_present() -> None:
    calls = {"count": 0}

    class _CountingSummaryGenerator:
        def summarize(self, item):  # noqa: ANN001, ANN202
            calls["count"] += 1
            return "Generated summary should not be used."

    item = _fetched_item(SourceId.MITVERGNUEGEN, "https://example.com/cached")
    item.metadata = {"cached_summary": "Cached summary from detail cache", "detail_hash": "hash-cached"}
    fetch_result = FetchRunResult(
        started_at=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        finished_at=datetime(2026, 2, 27, 9, 0, tzinfo=UTC),
        results=[
            SourceFetchResult(
                source_id=SourceId.MITVERGNUEGEN,
                status=FetchStatus.SUCCESS,
                items=[item],
                warnings=[],
                error_message=None,
                duration_ms=1,
            )
        ],
        total_items=1,
        failed_sources=[],
    )

    parsed = Parser(summary_generator=_CountingSummaryGenerator()).run(fetch_result)
    assert calls["count"] == 0
    assert parsed.results[0].items[0].summary == "Cached summary from detail cache"


def test_parser_persists_generated_summary_to_detail_cache(tmp_path: Path) -> None:
    class _FakeSummaryGenerator:
        def summarize(self, item):  # noqa: ANN001, ANN202
            return "Generated summary"

    db_path = tmp_path / "cache.db"
    cache = SqliteDetailCacheStore(db_path)
    cache.upsert_detail(
        url="https://example.com/summary-target",
        source_id=SourceId.MITVERGNUEGEN.value,
        detail_text="Detail body",
        detail_hash="hash-target",
        detail_status="ok",
    )
    item = _fetched_item(SourceId.MITVERGNUEGEN, "https://example.com/summary-target")
    item.metadata = {"detail_hash": "hash-target"}
    fetch_result = FetchRunResult(
        started_at=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        finished_at=datetime(2026, 2, 27, 9, 0, tzinfo=UTC),
        results=[
            SourceFetchResult(
                source_id=SourceId.MITVERGNUEGEN,
                status=FetchStatus.SUCCESS,
                items=[item],
                warnings=[],
                error_message=None,
                duration_ms=1,
            )
        ],
        total_items=1,
        failed_sources=[],
    )

    parsed = Parser(summary_generator=_FakeSummaryGenerator(), detail_cache_store=cache).run(fetch_result)
    assert parsed.results[0].items[0].summary == "Generated summary"
    refreshed = cache.get("https://example.com/summary-target")
    assert refreshed is not None
    assert refreshed.summary == "Generated summary"
