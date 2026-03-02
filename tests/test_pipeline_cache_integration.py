from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import berlin_insider.pipeline as pipeline_module
from berlin_insider.fetcher.models import (
    FetchContext,
    FetchedItem,
    FetchMethod,
    FetchStatus,
    SourceFetchResult,
    SourceId,
)
from berlin_insider.fetcher.utils import enrich_items_with_detail
from berlin_insider.pipeline import run_fetch_parse_pipeline


class _CountingSummaryGenerator:
    def __init__(self) -> None:
        self.calls = 0

    def summarize(self, item):  # noqa: ANN001, ANN202
        self.calls += 1
        return "Generated summary once"


class _FakeAdapter:
    def __init__(self) -> None:
        self.definition = type(
            "_Definition",
            (),
            {"source_id": SourceId.MITVERGNUEGEN, "source_url": "https://example.com/listing"},
        )()

    def fetch(self, context: FetchContext) -> SourceFetchResult:
        item = FetchedItem(
            source_id=SourceId.MITVERGNUEGEN,
            source_url="https://example.com/listing",
            item_url="https://example.com/detail",
            title="Cached candidate",
            published_at=None,
            raw_date_text=None,
            snippet="Listing snippet",
            location_hint=None,
            fetch_method=FetchMethod.HTML,
            collected_at=context.collected_at,
            metadata={},
        )
        enriched_items, warnings = enrich_items_with_detail([item], context=context)
        return SourceFetchResult(
            source_id=SourceId.MITVERGNUEGEN,
            status=FetchStatus.SUCCESS,
            items=enriched_items,
            warnings=warnings,
            error_message=None,
            duration_ms=1,
        )


def test_pipeline_reuses_detail_and_summary_cache(monkeypatch, tmp_path: Path) -> None:
    summary = _CountingSummaryGenerator()
    detail_calls = {"count": 0}

    def _detail_get(url: str, **kwargs):  # noqa: ANN003, ARG001
        detail_calls["count"] += 1
        return "<html><body><article>Integration detail text with enough content for extraction and cache persistence.</article></body></html>"

    monkeypatch.setattr("berlin_insider.fetcher.utils.get_text_with_retries", _detail_get)
    monkeypatch.setattr(
        "berlin_insider.parser.orchestrator.OpenAISummaryGenerator.from_env",
        lambda env=None: summary,  # noqa: ARG005
    )
    monkeypatch.setattr(
        "berlin_insider.fetcher.orchestrator.SOURCES",
        {SourceId.MITVERGNUEGEN: _FakeAdapter()},
    )
    context = pipeline_module.build_fetch_context(
        collected_at=datetime(2026, 3, 2, 8, 0, tzinfo=UTC),
        max_items_per_source=1,
    )
    db_path = tmp_path / "cache.db"

    _, parse_first = run_fetch_parse_pipeline(context=context, db_path=db_path)
    _, parse_second = run_fetch_parse_pipeline(context=context, db_path=db_path)

    assert detail_calls["count"] == 1
    assert summary.calls == 1
    assert parse_first.total_items == 1
    assert parse_second.total_items == 1
    assert parse_second.results[0].items[0].summary == "Generated summary once"
