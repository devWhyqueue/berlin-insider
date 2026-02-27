from __future__ import annotations

from datetime import UTC, datetime
from time import perf_counter

from berlin_insider.fetcher.models import FetchRunResult, FetchStatus, SourceFetchResult
from berlin_insider.parser.models import ParseRunResult, ParseStatus, SourceParseResult
from berlin_insider.parser.normalize import normalize_fetched_item


class Parser:
    """Normalize fetched items into parse-ready canonical objects."""

    def run(self, fetch_result: FetchRunResult) -> ParseRunResult:
        """Transform a fetch run into normalized parse results per source."""
        started = datetime.now(UTC)
        parse_results = [
            self._parse_source_result(result, reference_now=fetch_result.finished_at)
            for result in fetch_result.results
        ]
        finished = datetime.now(UTC)
        failed_sources = [
            source_result.source_id
            for source_result in parse_results
            if source_result.status == ParseStatus.ERROR
        ]
        total_items = sum(len(result.items) for result in parse_results)
        return ParseRunResult(
            started_at=started,
            finished_at=finished,
            results=parse_results,
            total_items=total_items,
            failed_sources=failed_sources,
        )

    def _parse_source_result(
        self, source_result: SourceFetchResult, *, reference_now: datetime
    ) -> SourceParseResult:
        started = perf_counter()
        warnings = list(source_result.warnings)
        try:
            items = []
            item_failures = 0
            for fetched_item in source_result.items:
                try:
                    items.append(normalize_fetched_item(fetched_item, reference_now=reference_now))
                except Exception as exc:  # noqa: BLE001
                    item_failures += 1
                    warnings.append(f"Failed to parse item {fetched_item.item_url}: {exc}")
            status = self._status_for_source(source_result, len(items), item_failures)
            if not items:
                warnings.append("No items produced by parser")
            return SourceParseResult(
                source_id=source_result.source_id,
                status=status,
                items=items,
                warnings=warnings,
                error_message=None,
                duration_ms=_duration_ms(started),
            )
        except Exception as exc:  # noqa: BLE001
            return SourceParseResult(
                source_id=source_result.source_id,
                status=ParseStatus.ERROR,
                items=[],
                warnings=warnings,
                error_message=str(exc),
                duration_ms=_duration_ms(started),
            )

    def _status_for_source(
        self, source_result: SourceFetchResult, parsed_item_count: int, item_failures: int
    ) -> ParseStatus:
        if source_result.status in {FetchStatus.ERROR, FetchStatus.BLOCKED}:
            return ParseStatus.ERROR
        if parsed_item_count == 0 or item_failures > 0:
            return ParseStatus.PARTIAL
        return ParseStatus.SUCCESS


def _duration_ms(started: float) -> int:
    return int((perf_counter() - started) * 1000)
