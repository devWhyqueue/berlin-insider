from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from berlin_insider.digest import DigestKind
from berlin_insider.parser.models import ParsedCategory

FeedbackVote = Literal["up", "down"]


@dataclass(slots=True)
class FeedbackEvent:
    message_key: str
    vote: FeedbackVote
    telegram_user_id: int
    voted_at: str
    updated_at: str


@dataclass(slots=True)
class TelegramUpdatesState:
    last_update_id: int | None = None


@dataclass(slots=True)
class DeliveredItem:
    item_id: int
    canonical_url: str
    title: str | None
    summary: str | None
    location: str | None
    category: ParsedCategory | None
    event_start_at: str | None
    event_end_at: str | None


@dataclass(slots=True)
class MessageDeliveryRecord:
    message_key: str
    digest_kind: DigestKind
    local_date: str
    sent_at: str
    telegram_message_id: str
    primary_item: DeliveredItem
    alternative_item: DeliveredItem | None = None


@dataclass(slots=True)
class FeedbackPollResult:
    fetched_updates: int
    processed_callbacks: int
    persisted_votes: int
    ignored_updates: int
    answered_callbacks: int
    next_offset: int | None
    finished_at: datetime
