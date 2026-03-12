from __future__ import annotations

import logging
from typing import Protocol

from berlin_insider.digest import DigestKind
from berlin_insider.feedback.messages import render_daily_alternative_message
from berlin_insider.feedback.models import MessageDeliveryRecord
from berlin_insider.feedback.store import SqliteMessageDeliveryStore
from berlin_insider.formatter.models import AlternativeDigestItem
from berlin_insider.messenger.models import DeliveryResult, FeedbackMetadata
from berlin_insider.parser.models import ParsedCategory

logger = logging.getLogger(__name__)
_ALTERNATIVE_SUFFIX = "-alt1"


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
    delivery = _deliver_alternative_message(
        messenger=messenger,
        message_key=alternative_message_key,
        sent_message=sent_message,
    )
    if delivery is None:
        return
    _persist_alternative_message(
        sent_message_store=sent_message_store,
        sent_message=sent_message,
        delivery=delivery,
        message_key=alternative_message_key,
    )


def _alternative_message_key(sent_message: MessageDeliveryRecord) -> str | None:
    if sent_message.message_key.endswith(_ALTERNATIVE_SUFFIX):
        return None
    if sent_message.alternative_item is None:
        return None
    return f"{sent_message.message_key}{_ALTERNATIVE_SUFFIX}"


def _deliver_alternative_message(
    *,
    messenger: AlternativeMessenger,
    message_key: str,
    sent_message: MessageDeliveryRecord,
) -> DeliveryResult | None:
    alternative_item = sent_message.alternative_item
    if alternative_item is None:
        return None
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
    delivery: DeliveryResult,
    message_key: str,
) -> None:
    alternative_item = sent_message.alternative_item
    if alternative_item is None:
        return
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
