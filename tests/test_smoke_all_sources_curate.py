from datetime import UTC, datetime

from berlin_insider.curator.config import CuratorConfig
from berlin_insider.curator.orchestrator import Curator
from berlin_insider.curator.store import NoOpSentItemStore
from berlin_insider.fetcher.base import SourceDefinition
from berlin_insider.fetcher.models import (
    FetchedItem,
    FetchContext,
    FetchMethod,
    FetchStatus,
    SourceFetchResult,
    SourceId,
)
from berlin_insider.fetcher.orchestrator import Fetcher
from berlin_insider.parser.orchestrator import Parser


class _SmokeAdapter:
    def __init__(self, source_id: SourceId) -> None:
        self.definition = SourceDefinition(source_id=source_id, source_url=f"https://{source_id.value}.example")

    def fetch(self, context: FetchContext) -> SourceFetchResult:  # noqa: ARG002
        item = FetchedItem(
            source_id=self.definition.source_id,
            source_url=self.definition.source_url,
            item_url=f"https://example.com/{self.definition.source_id.value}",
            title=f"{self.definition.source_id.value} Weekend Event",
            published_at=datetime(2026, 2, 28, 12, 0, tzinfo=UTC),
            raw_date_text=None,
            snippet="Berlin weekend pick",
            location_hint="Berlin",
            fetch_method=FetchMethod.HTML,
            collected_at=datetime(2026, 2, 27, 9, 0, tzinfo=UTC),
            metadata={},
        )
        return SourceFetchResult(
            source_id=self.definition.source_id,
            status=FetchStatus.SUCCESS,
            items=[item],
            warnings=[],
            error_message=None,
            duration_ms=1,
        )


def test_smoke_all_sources_reach_curate_stage() -> None:
    fetcher = Fetcher()
    fetcher._sources = {source_id: _SmokeAdapter(source_id) for source_id in SourceId}  # noqa: SLF001
    fetch = fetcher.run(
        context=FetchContext(
            user_agent="test-agent",
            timeout_seconds=1.0,
            max_items_per_source=1,
            collected_at=datetime(2026, 2, 27, 9, 0, tzinfo=UTC),
        )
    )
    parse = Parser().run(fetch)
    curate = Curator().run(
        parse,
        reference_now=fetch.finished_at,
        store=NoOpSentItemStore(),
        config=CuratorConfig(target_count=7, min_count_fallback=5),
    )

    assert len(curate.results) == len(SourceId)
    assert curate.actual_count > 0 or any("Fallback selection active" in warning for warning in curate.warnings)
