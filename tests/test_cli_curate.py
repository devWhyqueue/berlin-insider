from datetime import UTC, datetime
import json

import berlin_insider.cli as cli
from berlin_insider.curator.models import CurateRunResult
from berlin_insider.fetcher.models import FetchRunResult
from berlin_insider.parser.models import ParseRunResult


def _empty_fetch() -> FetchRunResult:
    return FetchRunResult(
        started_at=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        finished_at=datetime(2026, 2, 27, 8, 1, tzinfo=UTC),
        results=[],
        total_items=0,
        failed_sources=[],
    )


def _empty_parse() -> ParseRunResult:
    return ParseRunResult(
        started_at=datetime(2026, 2, 27, 8, 1, tzinfo=UTC),
        finished_at=datetime(2026, 2, 27, 8, 1, tzinfo=UTC),
        results=[],
        total_items=0,
        failed_sources=[],
    )


def _empty_curate() -> CurateRunResult:
    return CurateRunResult(
        started_at=datetime(2026, 2, 27, 8, 1, tzinfo=UTC),
        finished_at=datetime(2026, 2, 27, 8, 1, tzinfo=UTC),
        results=[],
        selected_items=[],
        dropped_count=0,
        failed_sources=[],
        target_count=7,
        actual_count=0,
        category_counts={},
        warnings=["Fallback selection active: only 0 items available after filtering"],
    )


def test_cli_json_includes_curate_payload(monkeypatch, caplog) -> None:
    caplog.set_level("INFO")

    class _FakeFetcher:
        def run(self, *, context, source_ids=None):  # noqa: ANN001, ANN202
            return _empty_fetch()

    class _FakeParser:
        def run(self, fetch_result):  # noqa: ANN001, ANN202
            return _empty_parse()

    class _FakeCurator:
        def run(self, parse_result, *, reference_now, store, config):  # noqa: ANN001, ANN202
            return _empty_curate()

    monkeypatch.setattr(cli, "Fetcher", _FakeFetcher)
    monkeypatch.setattr(cli, "Parser", _FakeParser)
    monkeypatch.setattr(cli, "Curator", _FakeCurator)
    monkeypatch.setattr("sys.argv", ["berlin-insider", "fetch", "--json"])

    cli.main()

    payload = json.loads(caplog.records[-1].message)
    assert set(payload.keys()) == {"fetch", "parse", "curate"}
    assert payload["curate"]["target_count"] == 7
