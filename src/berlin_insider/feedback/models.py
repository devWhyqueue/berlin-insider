from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from berlin_insider.digest import DigestKind
from berlin_insider.formatter.models import AlternativeDigestItem

FeedbackVote = Literal["up", "down"]


@dataclass(slots=True)
class FeedbackEvent:
    message_key: str
    digest_kind: DigestKind
    vote: FeedbackVote
    telegram_user_id: int
    chat_id: str
    message_id: str
    voted_at: str
    updated_at: str


@dataclass(slots=True)
class TelegramUpdatesState:
    last_update_id: int | None = None


@dataclass(slots=True)
class SentMessageRecord:
    message_key: str
    digest_kind: DigestKind
    local_date: str
    sent_at: str
    telegram_message_id: str
    selected_urls: list[str]
    alternative_item: AlternativeDigestItem | None = None


@dataclass(slots=True)
class FeedbackPollResult:
    fetched_updates: int
    processed_callbacks: int
    persisted_votes: int
    ignored_updates: int
    answered_callbacks: int
    next_offset: int | None
    finished_at: datetime
