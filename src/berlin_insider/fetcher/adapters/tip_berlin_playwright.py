from __future__ import annotations

from dataclasses import dataclass
from time import perf_counter

from playwright.sync_api import sync_playwright

from berlin_insider.fetcher.base import SourceAdapter, SourceDefinition
from berlin_insider.fetcher.models import FetchContext, FetchStatus, SourceFetchResult
from berlin_insider.fetcher.parsers.tip_berlin import extract_tip_berlin_items_from_html
from berlin_insider.fetcher.utils import enrich_items_with_detail


@dataclass(slots=True)
class TipBerlinPlaywrightAdapter(SourceAdapter):
    """Fetch tip-berlin pages with Playwright and parse rendered event links."""

    definition: SourceDefinition

    def fetch(self, context: FetchContext) -> SourceFetchResult:
        """Return parsed tip-berlin events or a blocked status with the root cause."""
        started = perf_counter()
        try:
            return _fetch_success(self.definition, context, started)
        except Exception as exc:  # noqa: BLE001
            return SourceFetchResult(
                source_id=self.definition.source_id,
                status=FetchStatus.BLOCKED,
                items=[],
                warnings=[],
                error_message=str(exc),
                duration_ms=_duration_ms(started),
            )


def _fetch_with_playwright(*, url: str, timeout_seconds: float) -> str:
    timeout_ms = int(timeout_seconds * 1000)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle", timeout=max(timeout_ms, 45000))
        page.wait_for_timeout(2500)
        html = page.content()
        browser.close()
    return html


def _duration_ms(started: float) -> int:
    return int((perf_counter() - started) * 1000)


def _fetch_success(
    definition: SourceDefinition, context: FetchContext, started: float
) -> SourceFetchResult:
    html = _fetch_with_playwright(
        url=definition.source_url,
        timeout_seconds=context.timeout_seconds,
    )
    items = extract_tip_berlin_items_from_html(html, definition, context)
    warnings = [] if items else ["Playwright rendered page, but no event links were parsed"]
    items, detail_warnings = enrich_items_with_detail(items, context=context)
    warnings.extend(detail_warnings)
    status = FetchStatus.SUCCESS if items else FetchStatus.PARTIAL
    return SourceFetchResult(
        source_id=definition.source_id,
        status=status,
        items=items,
        warnings=warnings,
        error_message=None,
        duration_ms=_duration_ms(started),
    )
