from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter

from berlin_insider.fetcher.base import SourceAdapter, SourceDefinition
from berlin_insider.fetcher.http import get_text_with_playwright
from berlin_insider.fetcher.models import FetchContext, FetchStatus, SourceFetchResult
from berlin_insider.fetcher.parsers.common import Parser
from berlin_insider.fetcher.utils import enrich_items_with_detail


@dataclass(slots=True)
class PlaywrightHtmlAdapter(SourceAdapter):
    """Fetch HTML with Playwright, then parse with the provided parser."""

    definition: SourceDefinition
    parser: Parser

    def fetch(self, context: FetchContext) -> SourceFetchResult:
        """Return parsed items or a blocked/error status for this source."""
        started = perf_counter()
        warnings: list[str] = []
        try:
            items = self._fetch_and_parse(context, warnings)
            status = FetchStatus.SUCCESS if items else FetchStatus.PARTIAL
            if not items:
                warnings.append("No items parsed from Playwright HTML source")
            return _success_result(self.definition.source_id, status, items, warnings, started)
        except Exception as exc:  # noqa: BLE001
            return _error_result(self.definition.source_id, str(exc), warnings, started)

    def _fetch_and_parse(self, context: FetchContext, warnings: list[str]):
        html = get_text_with_playwright(
            self.definition.source_url,
            timeout_seconds=context.timeout_seconds,
        )
        items = self.parser(html, self.definition, context)
        selected_items = items[: context.max_items_per_source]
        enriched_items, detail_warnings = enrich_items_with_detail(selected_items, context=context)
        warnings.extend(detail_warnings)
        return enriched_items


def _success_result(source_id, status, items, warnings, started: float) -> SourceFetchResult:
    return SourceFetchResult(
        source_id=source_id,
        status=status,
        items=items,
        warnings=warnings,
        error_message=None,
        duration_ms=_duration_ms(started),
    )


def _error_result(
    source_id, message: str, warnings: list[str], started: float
) -> SourceFetchResult:
    blocked = "403" in message or "forbidden" in message.lower()
    return SourceFetchResult(
        source_id=source_id,
        status=FetchStatus.BLOCKED if blocked else FetchStatus.ERROR,
        items=[],
        warnings=warnings,
        error_message=message,
        duration_ms=_duration_ms(started),
    )


def _duration_ms(started: float) -> int:
    return int((perf_counter() - started) * 1000)
