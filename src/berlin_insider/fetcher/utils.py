from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

from berlin_insider.fetcher.http import get_text_with_playwright, get_text_with_retries
from berlin_insider.fetcher.models import FetchContext, FetchedItem
from berlin_insider.fetcher.parsers.detail_extract import extract_detail_payload
from berlin_insider.storage.detail_cache_enrichment import enrich_one_with_cache

MAX_DETAIL_WORKERS = 4


def parse_datetime(value: str | None) -> datetime | None:
    """Parse many datetime formats into UTC-aware datetimes."""
    if not value:
        return None
    parsed = _parse_datetime_flexible(value)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def dedupe_urls(urls: list[str]) -> list[str]:
    """Return input URLs in original order without duplicates."""
    seen: set[str] = set()
    unique: list[str] = []
    for url in urls:
        normalized = url.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique.append(normalized)
    return unique


def enrich_items_with_detail(
    items: list[FetchedItem], *, context: FetchContext
) -> tuple[list[FetchedItem], list[str]]:
    """Fetch and attach cleaned detail-page text for each item URL."""
    if not items:
        return items, []
    worker = _enrich_one if context.detail_cache_db_path is None else _enrich_one_cached_wrapper
    return _enrich_items_parallel(items, context=context, worker=worker)


def _enrich_items_parallel(
    items: list[FetchedItem],
    *,
    context: FetchContext,
    worker: Callable[..., tuple[FetchedItem, str | None]],
) -> tuple[list[FetchedItem], list[str]]:
    warnings: list[str] = []
    enriched_by_index: dict[int, FetchedItem] = {}
    workers = min(MAX_DETAIL_WORKERS, len(items))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        tasks = {
            executor.submit(worker, item, context=context): idx for idx, item in enumerate(items)
        }
        for task in as_completed(tasks):
            idx = tasks[task]
            item = items[idx]
            try:
                enriched, warning = task.result()
            except Exception as exc:  # noqa: BLE001
                fallback = _fallback_detail_text(item)
                warning = f"Detail enrich failed for {item.item_url}: {exc}"
                if fallback is None:
                    enriched = replace(item, detail_status="fetch_error")
                else:
                    enriched = replace(item, detail_text=fallback, detail_status="fallback_listing")
            enriched_by_index[idx] = enriched
            if warning:
                warnings.append(warning)
    return [enriched_by_index[idx] for idx in range(len(items))], warnings


def _enrich_one(item: FetchedItem, *, context: FetchContext) -> tuple[FetchedItem, str | None]:
    html = get_text_with_retries(
        item.item_url,
        user_agent=context.user_agent,
        timeout_seconds=context.timeout_seconds,
    )
    detail_text, detail_metadata = extract_detail_payload(html)
    if detail_text is None and _needs_playwright_retry(item.item_url, html):
        html = get_text_with_playwright(item.item_url, timeout_seconds=context.timeout_seconds)
        detail_text, detail_metadata = extract_detail_payload(html)
    enriched_metadata = dict(item.metadata)
    enriched_metadata.update(detail_metadata)
    if detail_text is None:
        fallback = _fallback_detail_text(item)
        if fallback is None:
            warning = f"Detail content empty for {item.item_url}"
            return replace(item, detail_status="extract_empty", metadata=enriched_metadata), warning
        warning = f"Detail content empty for {item.item_url}; used listing fallback"
        return (
            replace(
                item,
                detail_text=fallback,
                detail_status="fallback_listing",
                metadata=enriched_metadata,
            ),
            warning,
        )
    return replace(
        item, detail_text=detail_text, detail_status="ok", metadata=enriched_metadata
    ), None


def _enrich_one_cached_wrapper(
    item: FetchedItem, *, context: FetchContext
) -> tuple[FetchedItem, str | None]:
    return enrich_one_with_cache(item, context=context, enrich_one=_enrich_one)


def _parse_datetime_flexible(value: str) -> datetime | None:
    try:
        return parsedate_to_datetime(value)
    except (TypeError, ValueError):
        pass
    normalized = value.strip().replace("Z", "+00:00")
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    collapsed = " ".join(value.split())
    return collapsed or None


def _fallback_detail_text(item: FetchedItem) -> str | None:
    return _normalize_text(item.snippet) or _normalize_text(item.title)


def _needs_playwright_retry(url: str, html: str) -> bool:
    lowered = html.lower()
    return "tip-berlin.de" in url and (
        "verification-container" in lowered or "enable javascript" in lowered
    )
