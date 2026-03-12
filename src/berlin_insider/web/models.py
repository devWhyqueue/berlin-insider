from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass(slots=True)
class OverviewCounts:
    items: int
    message_deliveries: int
    feedback_events: int
    sources: int
    detail_cache_entries: int


@dataclass(slots=True)
class OverviewWorker:
    last_status: str | None
    last_attempt_at: str | None
    last_success_at: str | None
    last_delivery_at: str | None
    last_curated_count: int | None
    last_error_message: str | None


@dataclass(slots=True)
class OverviewResponse:
    generated_at: str
    counts: OverviewCounts
    worker: OverviewWorker


@dataclass(slots=True)
class ItemCard:
    item_id: int
    title: str | None
    summary: str | None
    source_id: str
    category: str | None
    event_start_at: str | None
    location: str | None
    canonical_url: str
    has_summary: bool
    timing: Literal["upcoming", "undated"]


@dataclass(slots=True)
class ItemsResponse:
    items: list[ItemCard]
    available_sources: list[str]
    available_categories: list[str]
    total: int


@dataclass(slots=True)
class DeliveryItem:
    item_id: int
    title: str | None
    canonical_url: str
    summary: str | None
    location: str | None
    category: str | None
    event_start_at: str | None


@dataclass(slots=True)
class DeliveryRow:
    message_key: str
    digest_kind: str
    local_date: str
    sent_at: str
    telegram_message_id: str
    primary_item: DeliveryItem
    alternative_item: DeliveryItem | None


@dataclass(slots=True)
class DeliveriesResponse:
    deliveries: list[DeliveryRow]


@dataclass(slots=True)
class FeedbackAggregateRow:
    message_key: str
    digest_kind: str
    local_date: str
    up_votes: int
    down_votes: int
    total_votes: int


@dataclass(slots=True)
class FeedbackResponse:
    feedback: list[FeedbackAggregateRow]


@dataclass(slots=True)
class SourceStatus:
    source_id: str
    source_url: str
    adapter_kind: str
    updated_at: str


@dataclass(slots=True)
class DetailCacheEntryView:
    canonical_url: str
    source_id: str | None
    detail_status: str
    summary: str | None
    first_fetched_at: str
    last_fetched_at: str
    last_used_at: str
    updated_at: str
    detail_length: int
    metadata_keys: list[str] = field(default_factory=list)


@dataclass(slots=True)
class DetailCacheSummary:
    total_entries: int
    recent_entries: list[DetailCacheEntryView]


@dataclass(slots=True)
class WorkerStateView:
    last_attempt_at: str | None
    last_run_date_local: str | None
    last_status: str | None
    last_success_at: str | None
    last_error_message: str | None
    last_digest_length: int | None
    last_curated_count: int | None
    last_failed_sources: list[str]
    last_source_status: dict[str, str]
    last_delivery_at: str | None
    last_delivery_message_id: str | None
    last_delivery_error: str | None
    last_run_date_by_kind: dict[str, str]


@dataclass(slots=True)
class TelegramUpdatesStateView:
    last_update_id: int | None


@dataclass(slots=True)
class OpsResponse:
    sources: list[SourceStatus]
    detail_cache: DetailCacheSummary
    worker_state: WorkerStateView
    telegram_updates_state: TelegramUpdatesStateView
