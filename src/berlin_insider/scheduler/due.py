from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from berlin_insider.curator.models import DropReason
from berlin_insider.digest import DigestKind
from berlin_insider.feedback.models import DeliveredItem, MessageDeliveryRecord
from berlin_insider.feedback.store import SqliteMessageDeliveryStore
from berlin_insider.parser.models import ParsedItem
from berlin_insider.pipeline import FullPipelineRunResult
from berlin_insider.scheduler.models import ScheduleConfig, SchedulerState
from berlin_insider.storage.item_store import SqliteItemStore

_WEEKDAY_TO_INDEX = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}


def is_due(
    *,
    now_utc: datetime,
    config: ScheduleConfig,
    state: SchedulerState,
) -> tuple[bool, str, str, DigestKind | None]:
    """Return due state, reason, local date, and digest kind for the current time."""
    local_now = _local_now(now_utc, timezone_name=config.timezone)
    local_date = local_now.date().isoformat()
    expected_weekday = _WEEKDAY_TO_INDEX.get(config.weekend_weekday.lower())
    if expected_weekday is None:
        return False, f"invalid weekday '{config.weekend_weekday}'", local_date, None
    digest_kind = _expected_digest_kind(local_now.weekday(), weekend_weekday=expected_weekday)
    if digest_kind is None:
        return False, "today has no scheduled digest", local_date, None
    scheduled_time = _scheduled_time_for_kind(digest_kind=digest_kind, config=config)
    if (local_now.hour, local_now.minute) < scheduled_time:
        return False, "configured send time has not been reached yet", local_date, digest_kind
    if _last_run_date_for_kind(state, digest_kind=digest_kind) == local_date:
        return False, "already ran for this local date", local_date, digest_kind
    return True, "run is due", local_date, digest_kind


def expected_digest_kind(*, now_utc: datetime, config: ScheduleConfig) -> DigestKind | None:
    """Return the scheduled digest kind for the local date, ignoring run history."""
    local_now = _local_now(now_utc, timezone_name=config.timezone)
    expected_weekday = _WEEKDAY_TO_INDEX.get(config.weekend_weekday.lower())
    if expected_weekday is None:
        return None
    return _expected_digest_kind(local_now.weekday(), weekend_weekday=expected_weekday)


def persist_sent_message(
    *,
    store: SqliteMessageDeliveryStore,
    item_store: SqliteItemStore,
    message_key: str,
    digest_kind: DigestKind,
    local_date: str,
    delivered_at: str,
    message_id: str,
    pipeline_result: FullPipelineRunResult,
) -> None:
    """Persist one delivered message with direct durable item references."""
    selected_items = pipeline_result.curate_result.selected_items
    if not selected_items:
        return
    primary_item = _to_delivered_item(_get_or_persist_item(item_store, selected_items[0].item))
    alternative = alternative_item_for_sent_message(
        digest_kind=digest_kind,
        pipeline_result=pipeline_result,
        item_store=item_store,
    )
    store.upsert(
        MessageDeliveryRecord(
            message_key=message_key,
            digest_kind=digest_kind,
            local_date=local_date,
            sent_at=delivered_at,
            telegram_message_id=message_id,
            primary_item=primary_item,
            alternative_item=alternative,
        )
    )


def alternative_item_for_sent_message(
    *,
    digest_kind: DigestKind,
    pipeline_result: FullPipelineRunResult,
    item_store: SqliteItemStore,
) -> DeliveredItem | None:
    """Resolve one persisted alternative item for daily follow-up messaging."""
    if digest_kind != DigestKind.DAILY:
        return None
    selected_urls = [item.item.item_url for item in pipeline_result.curate_result.selected_items]
    if not selected_urls:
        return None
    alternative_item = _first_alternative_parsed_item(
        pipeline_result=pipeline_result,
        excluded_urls={selected_urls[0]},
    )
    if alternative_item is None:
        return None
    return _to_delivered_item(_get_or_persist_item(item_store, alternative_item))


def _first_alternative_parsed_item(
    *,
    pipeline_result: FullPipelineRunResult,
    excluded_urls: set[str],
) -> ParsedItem | None:
    for source_result in pipeline_result.curate_result.results:
        for dropped in source_result.dropped_items:
            if dropped.reason not in {DropReason.LOW_SCORE, DropReason.UNKNOWN_WEEKEND_RELEVANCE}:
                continue
            item = dropped.item
            url = item.item_url.strip()
            if not url or url in excluded_urls:
                continue
            return item
    return None


def _get_or_persist_item(item_store: SqliteItemStore, item: ParsedItem):
    record = item_store.get_by_url(item.item_url)
    if record is not None:
        return record
    item_store.upsert_item(item)
    reloaded = item_store.get_by_url(item.item_url)
    if reloaded is None:
        raise ValueError(f"failed to persist item for {item.item_url}")
    return reloaded


def _to_delivered_item(item_record) -> DeliveredItem:  # noqa: ANN001
    return DeliveredItem(
        item_id=item_record.item_id,
        canonical_url=item_record.canonical_url,
        title=item_record.title,
        summary=item_record.summary,
        location=item_record.location,
        category=item_record.category,
        event_start_at=item_record.event_start_at,
        event_end_at=item_record.event_end_at,
    )


def _scheduled_time_for_kind(*, digest_kind: DigestKind, config: ScheduleConfig) -> tuple[int, int]:
    if digest_kind == DigestKind.DAILY:
        return config.daily_hour, config.daily_minute
    return config.weekend_hour, config.weekend_minute


def _expected_digest_kind(local_weekday: int, *, weekend_weekday: int) -> DigestKind | None:
    if local_weekday == weekend_weekday:
        return DigestKind.WEEKEND
    return DigestKind.DAILY


def _last_run_date_for_kind(state: SchedulerState, *, digest_kind: DigestKind) -> str | None:
    current = state.last_run_date_by_kind.get(digest_kind.value)
    if current is not None:
        return current
    if digest_kind == DigestKind.WEEKEND:
        return state.last_run_date_local
    return None


def _local_now(now_utc: datetime, *, timezone_name: str) -> datetime:
    try:
        tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        tz = UTC
    return now_utc.astimezone(tz)
