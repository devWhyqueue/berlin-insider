from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Literal

from berlin_insider.web.models import (
    DeliveryItem,
    DeliveryRow,
    DetailCacheEntryView,
    DetailCacheSummary,
    FeedbackAggregateRow,
    ItemCard,
    SourceStatus,
    WorkerStateView,
)

_WORKER_STATE_FIELDS = (
    "last_attempt_at",
    "last_run_date_local",
    "last_status",
    "last_success_at",
    "last_error_message",
    "last_digest_length",
    "last_curated_count",
    "last_failed_sources_json",
    "last_source_status_json",
    "last_delivery_at",
    "last_delivery_message_id",
    "last_delivery_error",
    "last_run_date_by_kind_json",
)


def _delivery_from_row(row: tuple[object, ...]) -> DeliveryRow:
    primary_item = _delivery_item(row, 5)
    if primary_item is None:
        raise ValueError("expected delivery item row")
    return DeliveryRow(
        message_key=str(row[0]),
        digest_kind=str(row[1]),
        local_date=str(row[2]),
        sent_at=str(row[3]),
        telegram_message_id=str(row[4]),
        primary_item=primary_item,
        alternative_item=_delivery_item(row, 12, optional=True),
    )


def _delivery_item(
    row: Sequence[object], start: int, optional: bool = False
) -> DeliveryItem | None:
    if optional and row[start] is None:
        return None
    return DeliveryItem(
        item_id=_require_int(row[start]),
        title=_opt_str(row[start + 1]),
        canonical_url=str(row[start + 2]),
        summary=_opt_str(row[start + 3]),
        location=_opt_str(row[start + 4]),
        category=_opt_str(row[start + 5]),
        event_start_at=_opt_str(row[start + 6]),
    )


def _detail_cache_entry(row: tuple[object, ...]) -> DetailCacheEntryView:
    return DetailCacheEntryView(
        canonical_url=str(row[0]),
        source_id=_opt_str(row[1]),
        detail_status=str(row[2]),
        summary=_opt_str(row[3]),
        first_fetched_at=str(row[4]),
        last_fetched_at=str(row[5]),
        last_used_at=str(row[6]),
        updated_at=str(row[7]),
        detail_length=_int_or_zero(row[8]),
        metadata_keys=sorted(_json_keys(row[9])),
    )


def _detail_cache_summary(rows: list[tuple[object, ...]], total_entries: int) -> DetailCacheSummary:
    return DetailCacheSummary(
        total_entries=total_entries,
        recent_entries=[_detail_cache_entry(row) for row in rows],
    )


def _feedback_from_row(row: tuple[object, ...]) -> FeedbackAggregateRow:
    return FeedbackAggregateRow(
        message_key=str(row[0]),
        digest_kind=str(row[1]),
        local_date=str(row[2]),
        up_votes=_int_or_zero(row[3]),
        down_votes=_int_or_zero(row[4]),
        total_votes=_int_or_zero(row[5]),
    )


def _item_filters(
    *,
    source: str | None,
    category: str | None,
    has_summary: bool | None,
    timing: Literal["upcoming", "undated"] | None,
    search_text: str | None,
) -> tuple[str, tuple[object, ...]]:
    clauses: list[str] = []
    params: list[object] = []
    if source:
        clauses.append("source_id = ?")
        params.append(source)
    if category:
        clauses.append("category = ?")
        params.append(category)
    if has_summary is not None:
        summary_clause = "TRIM(COALESCE(summary, '')) != ''"
        clauses.append(summary_clause if has_summary else summary_clause.replace("!=", "="))
    if timing == "upcoming":
        clauses.append("event_start_at IS NOT NULL")
    if timing == "undated":
        clauses.append("event_start_at IS NULL")
    if search_text:
        clauses.append(_search_clause())
        params.extend(_search_params(search_text))
    prefix = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return prefix, tuple(params)


def _item_from_row(row: tuple[object, ...]) -> ItemCard:
    summary = _opt_str(row[2])
    return ItemCard(
        item_id=_require_int(row[0]),
        title=_opt_str(row[1]),
        summary=summary,
        source_id=str(row[3]),
        category=_opt_str(row[4]),
        event_start_at=_opt_str(row[5]),
        location=_opt_str(row[6]),
        canonical_url=str(row[7]),
        has_summary=summary is not None,
        timing="upcoming" if row[5] is not None else "undated",
    )


def _json_dict(value: object) -> dict[str, str]:
    payload = _json_value(value)
    if not isinstance(payload, dict):
        return {}
    return {
        key: item for key, item in payload.items() if isinstance(key, str) and isinstance(item, str)
    }


def _json_keys(value: object) -> list[str]:
    payload = _json_value(value)
    if not isinstance(payload, dict):
        return []
    return [key for key in payload if isinstance(key, str)]


def _json_list(value: object) -> list[str]:
    payload = _json_value(value)
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, str)]


def _json_value(value: object) -> object:
    if not isinstance(value, str):
        return {}
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return {}


def _opt_str(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _require_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("expected integer value")
    return value


def _int_or_zero(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    return value


def _row_int(row: dict[str, object], key: str) -> int | None:
    value = row.get(key)
    return value if isinstance(value, int) else None


def _row_str(row: dict[str, object], key: str) -> str | None:
    value = row.get(key)
    return value if isinstance(value, str) and value else None


def _search_clause() -> str:
    return (
        "(LOWER(COALESCE(title, '')) LIKE ? OR LOWER(COALESCE(summary, '')) LIKE ? "
        "OR LOWER(COALESCE(location, '')) LIKE ?)"
    )


def _search_params(search_text: str) -> tuple[str, str, str]:
    needle = f"%{search_text.lower()}%"
    return needle, needle, needle


def _source_from_row(row: tuple[object, ...]) -> SourceStatus:
    return SourceStatus(
        source_id=str(row[0]),
        source_url=str(row[1]),
        adapter_kind=str(row[2]),
        updated_at=str(row[3]),
    )


def _telegram_update_id(row: tuple[object, ...] | None) -> int | None:
    if row is None or not isinstance(row[0], int):
        return None
    return row[0]


def _worker_state_view(worker: dict[str, object]) -> WorkerStateView:
    return WorkerStateView(
        last_attempt_at=_row_str(worker, "last_attempt_at"),
        last_run_date_local=_row_str(worker, "last_run_date_local"),
        last_status=_row_str(worker, "last_status"),
        last_success_at=_row_str(worker, "last_success_at"),
        last_error_message=_row_str(worker, "last_error_message"),
        last_digest_length=_row_int(worker, "last_digest_length"),
        last_curated_count=_row_int(worker, "last_curated_count"),
        last_failed_sources=_json_list(worker.get("last_failed_sources_json")),
        last_source_status=_json_dict(worker.get("last_source_status_json")),
        last_delivery_at=_row_str(worker, "last_delivery_at"),
        last_delivery_message_id=_row_str(worker, "last_delivery_message_id"),
        last_delivery_error=_row_str(worker, "last_delivery_error"),
        last_run_date_by_kind=_json_dict(worker.get("last_run_date_by_kind_json")),
    )
