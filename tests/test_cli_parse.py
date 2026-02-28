from datetime import UTC, datetime

import berlin_insider.cli as cli
from berlin_insider.curator.models import CuratedItem, CurateRunResult, CurateStatus, SourceCurateResult
from berlin_insider.fetcher.models import (
    FetchedItem,
    FetchMethod,
    FetchRunResult,
    FetchStatus,
    SourceFetchResult,
    SourceId,
)
from berlin_insider.parser.models import (
    ParsedCategory,
    ParsedItem,
    ParseRunResult,
    ParseStatus,
    SourceParseResult,
    WeekendRelevance,
)


def _fetch_result() -> FetchRunResult:
    return FetchRunResult(
        started_at=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        finished_at=datetime(2026, 2, 27, 8, 1, tzinfo=UTC),
        results=[
            SourceFetchResult(
                source_id=SourceId.MITVERGNUEGEN,
                status=FetchStatus.SUCCESS,
                items=[
                    FetchedItem(
                        source_id=SourceId.MITVERGNUEGEN,
                        source_url="https://example.com",
                        item_url="https://example.com/item",
                        title="Title",
                        published_at=datetime(2026, 2, 28, 8, 0, tzinfo=UTC),
                        raw_date_text=None,
                        snippet="Snippet",
                        location_hint="Berlin",
                        fetch_method=FetchMethod.RSS,
                        collected_at=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
                        metadata={},
                    )
                ],
                warnings=[],
                error_message=None,
                duration_ms=10,
            )
        ],
        total_items=1,
        failed_sources=[],
    )


def _parse_result() -> ParseRunResult:
    return ParseRunResult(
        started_at=datetime(2026, 2, 27, 8, 1, tzinfo=UTC),
        finished_at=datetime(2026, 2, 27, 8, 1, tzinfo=UTC),
        results=[
            SourceParseResult(
                source_id=SourceId.MITVERGNUEGEN,
                status=ParseStatus.SUCCESS,
                items=[
                    ParsedItem(
                        source_id=SourceId.MITVERGNUEGEN,
                        item_url="https://example.com/item",
                        title="Title",
                        description="Snippet",
                        event_start_at=datetime(2026, 2, 28, 8, 0, tzinfo=UTC),
                        event_end_at=None,
                        location="Berlin",
                        category=ParsedCategory.EVENT,
                        category_confidence=0.65,
                        weekend_relevance=WeekendRelevance.LIKELY_THIS_WEEKEND,
                        weekend_confidence=0.9,
                        parse_notes=[],
                        raw={},
                    )
                ],
                warnings=[],
                error_message=None,
                duration_ms=5,
            )
        ],
        total_items=1,
        failed_sources=[],
    )


def _curate_result() -> CurateRunResult:
    parsed = _parse_result().results[0].items[0]
    curated = CuratedItem(item=parsed, score=0.9, selection_notes=["weekend=0.45"])
    return CurateRunResult(
        started_at=datetime(2026, 2, 27, 8, 1, tzinfo=UTC),
        finished_at=datetime(2026, 2, 27, 8, 1, tzinfo=UTC),
        results=[
            SourceCurateResult(
                source_id=SourceId.MITVERGNUEGEN,
                status=CurateStatus.SUCCESS,
                selected_items=[curated],
                dropped_items=[],
                warnings=[],
                error_message=None,
                duration_ms=1,
            )
        ],
        selected_items=[curated],
        dropped_count=0,
        failed_sources=[],
        target_count=7,
        actual_count=1,
        category_counts={category: 0 for category in ParsedCategory} | {ParsedCategory.EVENT: 1},
        warnings=[],
    )


def test_cli_fetch_runs_parser_by_default(monkeypatch, caplog) -> None:
    caplog.set_level("INFO")

    class _FakeFetcher:
        def run(self, *, context, source_ids=None):  # noqa: ANN001, ANN202
            return _fetch_result()

    class _FakeParser:
        called = False

        def run(self, fetch_result):  # noqa: ANN001, ANN202
            _FakeParser.called = True
            return _parse_result()

    class _FakeCurator:
        called = False

        def run(self, parse_result, *, reference_now, store, config):  # noqa: ANN001, ANN202
            _FakeCurator.called = True
            return _curate_result()

    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(cli, "Parser", _FakeParser)
    monkeypatch.setattr(cli, "Curator", _FakeCurator)
    monkeypatch.setattr("sys.argv", ["berlin-insider", "fetch"])

    cli.main()

    assert _FakeParser.called is True
    assert _FakeCurator.called is True
    assert "Curated total items: 1/7" in caplog.text


def test_cli_fetch_only_skips_parser(monkeypatch, caplog) -> None:
    caplog.set_level("INFO")

    class _FakeFetcher:
        def run(self, *, context, source_ids=None):  # noqa: ANN001, ANN202
            return _fetch_result()

    class _FailIfCalledParser:
        def run(self, fetch_result):  # noqa: ANN001, ANN202
            raise AssertionError("parser should not run for --fetch-only")

    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(cli, "Parser", _FailIfCalledParser)
    monkeypatch.setattr("sys.argv", ["berlin-insider", "fetch", "--fetch-only"])

    cli.main()

    assert "Parsed total items" not in caplog.text
    assert "Total items: 1" in caplog.text


def test_cli_parse_only_skips_curator(monkeypatch, caplog) -> None:
    caplog.set_level("INFO")

    class _FakeFetcher:
        def run(self, *, context, source_ids=None):  # noqa: ANN001, ANN202
            return _fetch_result()

    class _FakeParser:
        def run(self, fetch_result):  # noqa: ANN001, ANN202
            return _parse_result()

    class _FailIfCalledCurator:
        def run(self, parse_result, *, reference_now, store, config):  # noqa: ANN001, ANN202
            raise AssertionError("curator should not run for --parse-only")

    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(cli, "Parser", _FakeParser)
    monkeypatch.setattr(cli, "Curator", _FailIfCalledCurator)
    monkeypatch.setattr("sys.argv", ["berlin-insider", "fetch", "--parse-only"])

    cli.main()

    assert "Parsed total items: 1" in caplog.text
    assert "Curated total items" not in caplog.text


def test_cli_parse_only_ignores_digest_flag(monkeypatch, caplog) -> None:
    caplog.set_level("INFO")

    class _FakeFetcher:
        def run(self, *, context, source_ids=None):  # noqa: ANN001, ANN202
            return _fetch_result()

    class _FakeParser:
        def run(self, fetch_result):  # noqa: ANN001, ANN202
            return _parse_result()

    class _FailIfCalledCurator:
        def run(self, parse_result, *, reference_now, store, config):  # noqa: ANN001, ANN202
            raise AssertionError("curator should not run for --parse-only")

    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(cli, "Parser", _FakeParser)
    monkeypatch.setattr(cli, "Curator", _FailIfCalledCurator)
    monkeypatch.setattr("sys.argv", ["berlin-insider", "fetch", "--parse-only", "--digest"])

    cli.main()

    assert "Parsed total items: 1" in caplog.text
    assert "Curated total items" not in caplog.text
    assert "Berlin Insider" not in caplog.text
