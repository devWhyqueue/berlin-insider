from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import replace

from berlin_insider.fetcher.models import FetchContext, FetchedItem
from berlin_insider.storage.detail_cache import SqliteDetailCacheStore


def enrich_one_with_cache(
    item: FetchedItem,
    *,
    context: FetchContext,
    enrich_one: Callable[..., tuple[FetchedItem, str | None]],
) -> tuple[FetchedItem, str | None]:
    """Resolve one item's detail content with SQLite cache reuse."""
    if context.detail_cache_db_path is None:
        return enrich_one(item, context=context)
    cache = SqliteDetailCacheStore(context.detail_cache_db_path)
    if not context.refresh_detail_cache:
        cached_result = _cached_hit(item, cache=cache)
        if cached_result is not None:
            return cached_result
    enriched, warning = enrich_one(item, context=context)
    return _store_and_attach_hash(item, enriched=enriched, warning=warning, cache=cache)


def _cached_hit(
    item: FetchedItem, *, cache: SqliteDetailCacheStore
) -> tuple[FetchedItem, str | None] | None:
    try:
        cached = cache.get(item.item_url)
    except Exception as exc:  # noqa: BLE001
        return item, f"Detail cache read failed for {item.item_url}: {exc}"
    if cached is None:
        return None
    warning = _touch_cache(item, cache=cache)
    metadata = dict(item.metadata)
    metadata["detail_cache_hit"] = True
    metadata["detail_hash"] = cached.detail_hash
    if cached.summary:
        metadata["cached_summary"] = cached.summary
    return (
        replace(item, detail_text=cached.detail_text, detail_status="cache_hit", metadata=metadata),
        warning,
    )


def _store_and_attach_hash(
    item: FetchedItem,
    *,
    enriched: FetchedItem,
    warning: str | None,
    cache: SqliteDetailCacheStore,
) -> tuple[FetchedItem, str | None]:
    if enriched.detail_status != "ok" or enriched.detail_text is None:
        return enriched, warning
    detail_hash = hashlib.sha256(enriched.detail_text.encode("utf-8")).hexdigest()
    metadata = dict(enriched.metadata)
    metadata["detail_hash"] = detail_hash
    try:
        cache.upsert_detail(
            url=item.item_url,
            source_id=item.source_id.value,
            detail_text=enriched.detail_text,
            detail_hash=detail_hash,
            detail_status="ok",
        )
        cached_after_write = cache.get(item.item_url)
    except Exception as exc:  # noqa: BLE001
        cache_warning = f"Detail cache write failed for {item.item_url}: {exc}"
        warning = f"{warning}; {cache_warning}" if warning is not None else cache_warning
    else:
        if cached_after_write is not None and cached_after_write.summary:
            metadata["cached_summary"] = cached_after_write.summary
    return replace(enriched, metadata=metadata), warning


def _touch_cache(item: FetchedItem, *, cache: SqliteDetailCacheStore) -> str | None:
    try:
        cache.touch_used(item.item_url)
    except Exception as exc:  # noqa: BLE001
        return f"Detail cache touch failed for {item.item_url}: {exc}"
    return None
