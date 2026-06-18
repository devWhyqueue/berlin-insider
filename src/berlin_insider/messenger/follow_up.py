from __future__ import annotations

import logging
from datetime import UTC, datetime
from difflib import SequenceMatcher
from typing import Protocol
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from berlin_insider.digest import DigestKind
from berlin_insider.feedback.messages import render_daily_alternative_message
from berlin_insider.feedback.models import MessageDeliveryRecord
from berlin_insider.feedback.store import SqliteMessageDeliveryStore
from berlin_insider.formatter.models import AlternativeDigestItem
from berlin_insider.messenger.models import DeliveryResult, FeedbackMetadata
from berlin_insider.parser.models import ParsedCategory

logger = logging.getLogger(__name__)
_ALTERNATIVE_SUFFIX = "-alt1"
try:
    _BERLIN_TZ = ZoneInfo("Europe/Berlin")
except ZoneInfoNotFoundError:
    _BERLIN_TZ = UTC


class AlternativeMessenger(Protocol):
    def send_digest(
        self,
        *,
        text: str,
        feedback_metadata: FeedbackMetadata | None = None,
    ) -> DeliveryResult:
        """Send one follow-up digest message."""
        ...


def send_alternative_follow_up_if_needed(
    *,
    messenger: AlternativeMessenger,
    sent_message_store: SqliteMessageDeliveryStore,
    sent_message: MessageDeliveryRecord,
) -> None:
    """Send one alternative daily tip after a downvote when available."""
    alternative_message_key = _alternative_message_key(sent_message)
    if alternative_message_key is None:
        return
    if sent_message_store.get(alternative_message_key) is not None:
        return
    alternative_item = _alternative_for_follow_up(
        sent_message_store=sent_message_store,
        sent_message=sent_message,
    )
    if alternative_item is None:
        return
    _send_and_persist_alternative(
        messenger=messenger,
        sent_message_store=sent_message_store,
        sent_message=sent_message,
        alternative_item=alternative_item,
        message_key=alternative_message_key,
    )


def _send_and_persist_alternative(
    *,
    messenger: AlternativeMessenger,
    sent_message_store: SqliteMessageDeliveryStore,
    sent_message: MessageDeliveryRecord,
    alternative_item,
    message_key: str,
) -> None:
    delivery = _deliver_alternative_message(
        messenger=messenger,
        message_key=message_key,
        sent_message=sent_message,
        alternative_item=alternative_item,
    )
    if delivery is None:
        return
    _persist_alternative_message(
        sent_message_store=sent_message_store,
        sent_message=sent_message,
        alternative_item=alternative_item,
        delivery=delivery,
        message_key=message_key,
    )


def _alternative_message_key(sent_message: MessageDeliveryRecord) -> str | None:
    if sent_message.message_key.endswith(_ALTERNATIVE_SUFFIX):
        return None
    if sent_message.alternative_item is None:
        return None
    return f"{sent_message.message_key}{_ALTERNATIVE_SUFFIX}"


def _alternative_for_follow_up(*, sent_message_store, sent_message: MessageDeliveryRecord):
    alternative = sent_message.alternative_item
    if alternative is not None and _alternative_is_usable(
        sent_message_store=sent_message_store,
        sent_message=sent_message,
        alternative=alternative,
    ):
        return alternative
    return sent_message_store.find_daily_alternative(
        local_date=sent_message.local_date,
        excluded_urls={sent_message.primary_item.canonical_url},
        excluded_title=sent_message.primary_item.title,
    )


def _alternative_is_usable(
    *,
    sent_message_store: SqliteMessageDeliveryStore,
    sent_message: MessageDeliveryRecord,
    alternative,
) -> bool:
    if alternative.canonical_url == sent_message.primary_item.canonical_url:
        return False
    if sent_message_store.has_primary_delivery(
        canonical_url=alternative.canonical_url,
        digest_kind=DigestKind.DAILY,
    ):
        return False
    if _same_title(alternative.title, sent_message.primary_item.title):
        return False
    return _local_date(alternative.event_start_at) == sent_message.local_date


def _local_date(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(_BERLIN_TZ).date().isoformat()


def _title_key(value: str | None) -> str:
    return " ".join((value or "").casefold().split())


def _same_title(left: str | None, right: str | None) -> bool:
    left_key = _title_key(left)
    right_key = _title_key(right)
    if not left_key or not right_key:
        return False
    return SequenceMatcher(None, left_key, right_key).ratio() >= 0.88


def _deliver_alternative_message(
    *,
    messenger: AlternativeMessenger,
    message_key: str,
    sent_message: MessageDeliveryRecord,
    alternative_item,
) -> DeliveryResult | None:
    try:
        return messenger.send_digest(
            text=render_daily_alternative_message(
                alternative_item=_renderable_alternative_item(alternative_item)
            ),
            feedback_metadata=FeedbackMetadata(
                digest_kind=DigestKind.DAILY,
                message_key=message_key,
            ),
        )
    except RuntimeError:
        logger.warning(
            "Failed to send daily alternative follow-up for message_key=%s",
            sent_message.message_key,
        )
        return None


def _persist_alternative_message(
    *,
    sent_message_store: SqliteMessageDeliveryStore,
    sent_message: MessageDeliveryRecord,
    alternative_item,
    delivery: DeliveryResult,
    message_key: str,
) -> None:
    sent_message_store.upsert(
        MessageDeliveryRecord(
            message_key=message_key,
            digest_kind=DigestKind.DAILY,
            local_date=sent_message.local_date,
            sent_at=delivery.delivered_at.isoformat(),
            telegram_message_id=delivery.external_message_id,
            primary_item=alternative_item,
            alternative_item=None,
        )
    )


def _renderable_alternative_item(alternative_item) -> AlternativeDigestItem:
    return AlternativeDigestItem(
        item_url=alternative_item.canonical_url,
        title=alternative_item.title,
        summary=alternative_item.summary,
        location=alternative_item.location,
        category=alternative_item.category or ParsedCategory.MISC,
        event_start_at=alternative_item.event_start_at,
        event_end_at=alternative_item.event_end_at,
    )
