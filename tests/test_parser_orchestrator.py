from datetime import UTC, datetime

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
