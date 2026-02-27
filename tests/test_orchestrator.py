from datetime import UTC, datetime

from berlin_insider.fetcher.base import SourceDefinition
from berlin_insider.fetcher.models import (
    FetchContext,
    FetchStatus,
    SourceFetchResult,
    SourceId,
)
from berlin_insider.fetcher.orchestrator import Fetcher


class _FakeAdapter:
    def __init__(self, source_id: SourceId) -> None:
        self.definition = SourceDefinition(source_id=source_id, source_url="https://example.com")

    def fetch(self, context: FetchContext) -> SourceFetchResult:
        return SourceFetchResult(
            source_id=self.definition.source_id,
            status=FetchStatus.SUCCESS,
            items=[],
            warnings=[],
            error_message=None,
            duration_ms=1,
        )


def test_fetcher_runs_with_selected_source() -> None:
    fetcher = Fetcher()
    source_id = SourceId.MITVERGNUEGEN
    fetcher._sources = {source_id: _FakeAdapter(source_id)}  # noqa: SLF001
    result = fetcher.run(
        context=FetchContext(
            user_agent="test-agent",
            timeout_seconds=1.0,
            max_items_per_source=1,
            collected_at=datetime.now(UTC),
        ),
        source_ids=[source_id],
    )
    assert len(result.results) == 1
    assert result.results[0].source_id == source_id
