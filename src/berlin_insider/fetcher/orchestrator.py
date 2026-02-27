from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

from berlin_insider.fetcher.base import SourceAdapter
from berlin_insider.fetcher.models import (
    FetchContext,
    FetchRunResult,
    FetchStatus,
    SourceFetchResult,
    SourceId,
)
from berlin_insider.fetcher.sources import SOURCES


class Fetcher:
    """Run source adapters and aggregate their results."""

    def __init__(self) -> None:
        self._sources = SOURCES

    def run(
        self, *, context: FetchContext, source_ids: list[SourceId] | None = None
    ) -> FetchRunResult:
        """Run one fetch cycle for all or selected source IDs."""
        started = datetime.now(UTC)
        selected = self._resolve_selected_sources(source_ids)
        results = self._run_sources_parallel(selected, context)
        finished = datetime.now(UTC)
        failed_sources = [
            result.source_id
            for result in results
            if result.status in {FetchStatus.ERROR, FetchStatus.BLOCKED}
        ]
        total_items = sum(len(result.items) for result in results)
        return FetchRunResult(
            started_at=started,
            finished_at=finished,
            results=results,
            total_items=total_items,
            failed_sources=failed_sources,
        )

    def _resolve_selected_sources(self, source_ids: list[SourceId] | None) -> list[SourceAdapter]:
        if source_ids is None:
            return list(self._sources.values())
        return [self._sources[source_id] for source_id in source_ids]

    def _run_sources_parallel(
        self,
        selected_sources: list[SourceAdapter],
        context: FetchContext,
    ) -> list[SourceFetchResult]:
        ordered_ids = [adapter.definition.source_id for adapter in selected_sources]
        results_by_id: dict[SourceId, SourceFetchResult] = {}
        workers = min(4, max(1, len(selected_sources)))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            tasks = {
                executor.submit(adapter.fetch, context): adapter.definition.source_id
                for adapter in selected_sources
            }
            for task in as_completed(tasks):
                source_id = tasks[task]
                try:
                    results_by_id[source_id] = task.result()
                except Exception as exc:  # noqa: BLE001
                    results_by_id[source_id] = SourceFetchResult(
                        source_id=source_id,
                        status=FetchStatus.ERROR,
                        items=[],
                        warnings=[],
                        error_message=str(exc),
                        duration_ms=0,
                    )
        return [results_by_id[source_id] for source_id in ordered_ids]
