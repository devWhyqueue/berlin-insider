from __future__ import annotations

from pathlib import Path
from typing import Literal

from berlin_insider.storage.sqlite import now_utc_iso, sqlite_connection
from berlin_insider.web.models import (
    DeliveriesResponse,
    FeedbackResponse,
    ItemsResponse,
    OpsResponse,
    OverviewCounts,
    OverviewResponse,
    OverviewWorker,
    TelegramUpdatesStateView,
)
from berlin_insider.web.query_helpers import (
    _WORKER_STATE_FIELDS,
    _delivery_from_row,
    _detail_cache_summary,
    _feedback_from_row,
    _item_filters,
    _item_from_row,
    _row_int,
    _row_str,
    _source_from_row,
    _telegram_update_id,
    _worker_state_view,
)

_DELIVERIES_SQL = """
SELECT
    deliveries.message_key,
    deliveries.digest_kind,
    deliveries.local_date,
    deliveries.sent_at,
    deliveries.telegram_message_id,
    primary_item.item_id,
    primary_item.title,
    primary_item.canonical_url,
    primary_item.summary,
    primary_item.location,
    primary_item.category,
    primary_item.event_start_at,
    alternative_item.item_id,
    alternative_item.title,
    alternative_item.canonical_url,
    alternative_item.summary,
    alternative_item.location,
    alternative_item.category,
    alternative_item.event_start_at
FROM message_deliveries deliveries
JOIN items primary_item ON primary_item.item_id = deliveries.primary_item_id
LEFT JOIN items alternative_item ON alternative_item.item_id = deliveries.alternative_item_id
ORDER BY deliveries.sent_at DESC, deliveries.message_key DESC
LIMIT 40
"""

_FEEDBACK_SQL = """
SELECT
    deliveries.message_key,
    deliveries.digest_kind,
    deliveries.local_date,
    SUM(CASE WHEN feedback.vote = 'up' THEN 1 ELSE 0 END),
    SUM(CASE WHEN feedback.vote = 'down' THEN 1 ELSE 0 END),
    COUNT(*)
FROM feedback_events feedback
JOIN message_deliveries deliveries ON deliveries.message_key = feedback.message_key
GROUP BY deliveries.message_key, deliveries.digest_kind, deliveries.local_date
ORDER BY deliveries.local_date DESC, deliveries.message_key DESC
LIMIT 40
"""


class _PublicSiteRepository:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path

    def _overview(self) -> OverviewResponse:
        worker = self._worker_state()
        return OverviewResponse(
            generated_at=now_utc_iso(),
            counts=OverviewCounts(
                items=self._count("items"),
                message_deliveries=self._count("message_deliveries"),
                feedback_events=self._count("feedback_events"),
                sources=self._count("sources"),
                detail_cache_entries=self._count("detail_cache"),
            ),
            worker=OverviewWorker(
                last_status=_row_str(worker, "last_status"),
                last_attempt_at=_row_str(worker, "last_attempt_at"),
                last_success_at=_row_str(worker, "last_success_at"),
                last_delivery_at=_row_str(worker, "last_delivery_at"),
                last_curated_count=_row_int(worker, "last_curated_count"),
                last_error_message=_row_str(worker, "last_error_message"),
            ),
        )

    def _items(
        self,
        *,
        source: str | None,
        category: str | None,
        has_summary: bool | None,
        timing: Literal["upcoming", "undated"] | None,
        search_text: str | None,
    ) -> ItemsResponse:
        where_clause, params = _item_filters(
            source=source,
            category=category,
            has_summary=has_summary,
            timing=timing,
            search_text=search_text,
        )
        rows = self._fetchall(_items_sql(where_clause), params)
        return ItemsResponse(
            items=[_item_from_row(row) for row in rows],
            available_sources=self._distinct("items", "source_id"),
            available_categories=self._distinct("items", "category"),
            total=len(rows),
        )

    def _deliveries(self) -> DeliveriesResponse:
        return DeliveriesResponse(
            deliveries=[_delivery_from_row(row) for row in self._fetchall(_DELIVERIES_SQL)]
        )

    def _feedback(self) -> FeedbackResponse:
        return FeedbackResponse(
            feedback=[_feedback_from_row(row) for row in self._fetchall(_FEEDBACK_SQL)]
        )

    def _ops(self) -> OpsResponse:
        worker = self._worker_state()
        return OpsResponse(
            sources=[_source_from_row(row) for row in self._source_rows()],
            detail_cache=_detail_cache_summary(
                self._detail_cache_rows(), self._count("detail_cache")
            ),
            worker_state=_worker_state_view(worker),
            telegram_updates_state=TelegramUpdatesStateView(
                last_update_id=_telegram_update_id(self._telegram_state_row())
            ),
        )

    def _count(self, table_name: str) -> int:
        row = self._fetchone(f"SELECT COUNT(*) FROM {table_name}")
        return _count_value(row)

    def _detail_cache_rows(self) -> list[tuple[object, ...]]:
        return self._fetchall(
            """
            SELECT canonical_url, source_id, detail_status, summary, first_fetched_at,
                   last_fetched_at, last_used_at, updated_at, LENGTH(detail_text), detail_metadata_json
            FROM detail_cache
            ORDER BY last_used_at DESC, updated_at DESC
            LIMIT 12
            """
        )

    def _distinct(self, table_name: str, column_name: str) -> list[str]:
        rows = self._fetchall(
            f"SELECT DISTINCT {column_name} FROM {table_name} "
            f"WHERE TRIM(COALESCE({column_name}, '')) != '' ORDER BY 1"
        )
        return [str(row[0]) for row in rows if row[0] is not None]

    def _source_rows(self) -> list[tuple[object, ...]]:
        return self._fetchall(
            "SELECT source_id, source_url, adapter_kind, updated_at FROM sources ORDER BY source_id"
        )

    def _telegram_state_row(self) -> tuple[object, ...] | None:
        return self._fetchone("SELECT last_update_id FROM telegram_updates_state WHERE id = 1")

    def _worker_state(self) -> dict[str, object]:
        row = self._fetchone(
            "SELECT " + ", ".join(_WORKER_STATE_FIELDS) + " FROM worker_state WHERE id = 1"
        )
        return dict(
            zip(_WORKER_STATE_FIELDS, row or [None] * len(_WORKER_STATE_FIELDS), strict=True)
        )

    def _fetchall(self, sql: str, params: tuple[object, ...] = ()) -> list[tuple[object, ...]]:
        with sqlite_connection(self._db_path) as conn:
            return conn.execute(sql, params).fetchall()

    def _fetchone(self, sql: str, params: tuple[object, ...] = ()) -> tuple[object, ...] | None:
        with sqlite_connection(self._db_path) as conn:
            return conn.execute(sql, params).fetchone()


def _items_sql(where_clause: str) -> str:
    return f"""
    SELECT item_id, title, summary, source_id, category, event_start_at, location, canonical_url
    FROM items
    {where_clause}
    ORDER BY event_start_at IS NULL, event_start_at, updated_at DESC
    LIMIT 120
    """


def _count_value(row: tuple[object, ...] | None) -> int:
    if row is None:
        return 0
    value = row[0]
    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    return value
