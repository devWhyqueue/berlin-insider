from __future__ import annotations

import json
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

from bs4 import BeautifulSoup

from berlin_insider.fetcher.http import get_text_with_playwright, get_text_with_retries
from berlin_insider.fetcher.models import FetchContext, FetchedItem
from berlin_insider.storage.detail_cache_enrichment import enrich_one_with_cache

MAX_DETAIL_WORKERS = 4
MIN_DETAIL_LENGTH = 60
JSONLD_BODY_KEYS = ("articleBody", "text", "description")
BOILERPLATE_SELECTORS = (
    "script",
    "style",
    "noscript",
    "nav",
    "footer",
    "header",
    "aside",
    "form",
    "svg",
    "iframe",
    "button",
)


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


def extract_detail_text(html: str) -> str | None:
    """Extract best-effort readable text from a detail page."""
    soup = BeautifulSoup(html, "html.parser")
    jsonld_text = _extract_jsonld_text(soup)
    if _is_meaningful(jsonld_text):
        return jsonld_text
    _strip_boilerplate(soup)
    for selector in ("article", "main", "body"):
        node = soup.select_one(selector)
        candidate = _normalize_text(node.get_text(" ", strip=True)) if node else None
        if _is_meaningful(candidate):
            return candidate
    return None


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
    detail_text = extract_detail_text(html)
    if detail_text is None and _needs_playwright_retry(item.item_url, html):
        html = get_text_with_playwright(item.item_url, timeout_seconds=context.timeout_seconds)
        detail_text = extract_detail_text(html)
    if detail_text is None:
        fallback = _fallback_detail_text(item)
        if fallback is None:
            warning = f"Detail content empty for {item.item_url}"
            return replace(item, detail_status="extract_empty"), warning
        warning = f"Detail content empty for {item.item_url}; used listing fallback"
        return replace(item, detail_text=fallback, detail_status="fallback_listing"), warning
    return replace(item, detail_text=detail_text, detail_status="ok"), None


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


def _extract_jsonld_text(soup: BeautifulSoup) -> str | None:
    for script in soup.select("script[type='application/ld+json']"):
        content = script.string or script.get_text()
        if not content:
            continue
        for payload in _json_documents(content):
            candidate = _extract_text_from_payload(payload)
            if _is_meaningful(candidate):
                return candidate
    return None


def _json_documents(content: str) -> list[Any]:
    cleaned = content.strip()
    if not cleaned:
        return []
    try:
        payload = json.loads(cleaned)
    except json.JSONDecodeError:
        return []
    return payload if isinstance(payload, list) else [payload]


def _extract_text_from_payload(payload: Any) -> str | None:
    if isinstance(payload, dict):
        for key in JSONLD_BODY_KEYS:
            candidate = _normalize_text(_coerce_string(payload.get(key)))
            if _is_meaningful(candidate):
                return candidate
        graph = payload.get("@graph")
        if isinstance(graph, list):
            for node in graph:
                candidate = _extract_text_from_payload(node)
                if _is_meaningful(candidate):
                    return candidate
    if isinstance(payload, list):
        for node in payload:
            candidate = _extract_text_from_payload(node)
            if _is_meaningful(candidate):
                return candidate
    return None


def _strip_boilerplate(soup: BeautifulSoup) -> None:
    for selector in BOILERPLATE_SELECTORS:
        for node in soup.select(selector):
            node.decompose()


def _coerce_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    collapsed = " ".join(value.split())
    return collapsed or None


def _fallback_detail_text(item: FetchedItem) -> str | None:
    return _normalize_text(item.snippet) or _normalize_text(item.title)


def _is_meaningful(value: str | None) -> bool:
    return value is not None and len(value) >= MIN_DETAIL_LENGTH


def _needs_playwright_retry(url: str, html: str) -> bool:
    lowered = html.lower()
    return "tip-berlin.de" in url and (
        "verification-container" in lowered or "enable javascript" in lowered
    )
