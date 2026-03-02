from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from time import perf_counter

from berlin_insider.fetcher.models import FetchRunResult, FetchStatus, SourceFetchResult
from berlin_insider.parser.models import ParsedItem, ParseRunResult, ParseStatus, SourceParseResult
from berlin_insider.parser.normalize import normalize_fetched_item
from berlin_insider.parser.summarizer import OpenAISummaryGenerator, SummaryGenerator
from berlin_insider.storage.detail_cache import SqliteDetailCacheStore


class Parser:
    """Normalize fetched items into parse-ready canonical objects."""

    def __init__(
        self,
        *,
        summary_generator: SummaryGenerator | None = None,
        detail_cache_store: SqliteDetailCacheStore | None = None,
    ) -> None:
        self._summary_generator = summary_generator or OpenAISummaryGenerator.from_env()
        self._detail_cache_store = detail_cache_store

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
                    parsed_item = normalize_fetched_item(fetched_item, reference_now=reference_now)
                    items.append(
                        self._with_summary(
                            parsed_item, warnings=warnings, item_url=fetched_item.item_url
                        )
                    )
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

    def _with_summary(
        self, parsed_item: ParsedItem, *, warnings: list[str], item_url: str
    ) -> ParsedItem:
        if parsed_item.summary is not None:
            return parsed_item
        try:
            summary = self._summary_generator.summarize(parsed_item)
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"Failed to summarize item {item_url}: {exc}")
            return parsed_item
        if summary is None:
            return parsed_item
        self._persist_summary_to_cache(
            parsed_item, summary=summary, warnings=warnings, item_url=item_url
        )
        return replace(parsed_item, summary=summary)

    def _persist_summary_to_cache(
        self, parsed_item: ParsedItem, *, summary: str, warnings: list[str], item_url: str
    ) -> None:
        if self._detail_cache_store is None:
            return
        detail_hash = _detail_hash_from_raw(parsed_item)
        if detail_hash is None:
            return
        try:
            self._detail_cache_store.upsert_summary(
                url=item_url,
                detail_hash=detail_hash,
                summary=summary,
            )
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"Failed to store cached summary for {item_url}: {exc}")


def _duration_ms(started: float) -> int:
    return int((perf_counter() - started) * 1000)


def _detail_hash_from_raw(item: ParsedItem) -> str | None:
    metadata = item.raw.get("metadata")
    if not isinstance(metadata, dict):
        return None
    value = metadata.get("detail_hash")
    if value is None:
        return None
    detail_hash = str(value).strip()
    return detail_hash or None
