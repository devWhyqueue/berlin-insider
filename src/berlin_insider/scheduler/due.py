from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from berlin_insider.curator.models import DropReason
from berlin_insider.digest import DigestKind
from berlin_insider.feedback.models import SentMessageRecord
from berlin_insider.feedback.store import SqliteSentMessageStore
from berlin_insider.formatter.models import AlternativeDigestItem
from berlin_insider.pipeline import FullPipelineRunResult
from berlin_insider.scheduler.models import ScheduleConfig, SchedulerState

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
    """Return due decision, reason, local date string, and digest kind."""
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
    """Return scheduled digest kind for local date, ignoring time and run history."""
    local_now = _local_now(now_utc, timezone_name=config.timezone)
    expected_weekday = _WEEKDAY_TO_INDEX.get(config.weekend_weekday.lower())
    if expected_weekday is None:
        return None
    return _expected_digest_kind(local_now.weekday(), weekend_weekday=expected_weekday)


def persist_sent_message(
    *,
    store: SqliteSentMessageStore,
    message_key: str,
    digest_kind: DigestKind,
    local_date: str,
    delivered_at: str,
    message_id: str,
    pipeline_result: FullPipelineRunResult,
) -> None:
    """Persist sent-message metadata and include one daily fallback URL when available."""
    store.upsert(
        SentMessageRecord(
            message_key=message_key,
            digest_kind=digest_kind,
            local_date=local_date,
            sent_at=delivered_at,
            telegram_message_id=message_id,
            selected_urls=selected_urls_for_sent_message(
                digest_kind=digest_kind,
                pipeline_result=pipeline_result,
            ),
            alternative_item=alternative_item_for_sent_message(
                digest_kind=digest_kind,
                pipeline_result=pipeline_result,
            ),
        )
    )


def selected_urls_for_sent_message(
    *,
    digest_kind: DigestKind,
    pipeline_result: FullPipelineRunResult,
) -> list[str]:
    """Return URLs to store for sent-message feedback workflows."""
    selected_urls = [item.item.item_url for item in pipeline_result.curate_result.selected_items]
    if digest_kind != DigestKind.DAILY:
        return selected_urls
    if not selected_urls:
        return []
    primary_url = selected_urls[0]
    alternative_url = daily_alternative_url(
        excluded_urls={primary_url},
        pipeline_result=pipeline_result,
    )
    if alternative_url is None:
        return [primary_url]
    return [primary_url, alternative_url]


def alternative_item_for_sent_message(
    *,
    digest_kind: DigestKind,
    pipeline_result: FullPipelineRunResult,
) -> AlternativeDigestItem | None:
    """Return one persisted alternative daily item for feedback follow-ups."""
    if digest_kind != DigestKind.DAILY:
        return None
    selected_urls = [item.item.item_url for item in pipeline_result.curate_result.selected_items]
    if not selected_urls:
        return None
    return _first_alternative_item(
        pipeline_result=pipeline_result,
        excluded_urls={selected_urls[0]},
    )


def _first_alternative_item(
    *,
    pipeline_result: FullPipelineRunResult,
    excluded_urls: set[str],
) -> AlternativeDigestItem | None:
    for source_result in pipeline_result.curate_result.results:
        for dropped in source_result.dropped_items:
            if not _is_allowed_alternative_drop(dropped.reason):
                continue
            item = dropped.item
            url = item.item_url.strip()
            if not url or url in excluded_urls:
                continue
            return _to_alternative_item(url=url, dropped_item=item)
    return None


def _is_allowed_alternative_drop(reason: DropReason) -> bool:
    return reason in {DropReason.LOW_SCORE, DropReason.UNKNOWN_WEEKEND_RELEVANCE}


def _to_alternative_item(*, url: str, dropped_item) -> AlternativeDigestItem:  # noqa: ANN001
    return AlternativeDigestItem(
        item_url=url,
        title=dropped_item.title,
        summary=dropped_item.summary,
        location=dropped_item.location,
        category=dropped_item.category,
        event_start_at=dropped_item.event_start_at.isoformat()
        if dropped_item.event_start_at is not None
        else None,
        event_end_at=dropped_item.event_end_at.isoformat()
        if dropped_item.event_end_at is not None
        else None,
    )


def daily_alternative_url(
    *,
    excluded_urls: set[str],
    pipeline_result: FullPipelineRunResult,
) -> str | None:
    """Choose one non-selected daily fallback URL from dropped low-priority candidates."""
    allowed_drop_reasons = {DropReason.LOW_SCORE, DropReason.UNKNOWN_WEEKEND_RELEVANCE}
    for source_result in pipeline_result.curate_result.results:
        for dropped in source_result.dropped_items:
            if dropped.reason not in allowed_drop_reasons:
                continue
            url = dropped.item.item_url.strip()
            if not url or url in excluded_urls:
                continue
            return url
    return None


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
