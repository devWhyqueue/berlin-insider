from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from berlin_insider.digest import DigestKind


class MessengerError(RuntimeError):
    """Raised when a messenger provider fails to deliver a digest."""


@dataclass(slots=True)
class DeliveryResult:
    delivered_at: datetime
    external_message_id: str
    warning_message: str | None = None


@dataclass(slots=True, frozen=True)
class FeedbackMetadata:
    digest_kind: DigestKind
    message_key: str


class Messenger(Protocol):
    def send_digest(
        self,
        *,
        text: str,
        feedback_metadata: FeedbackMetadata | None = None,
    ) -> DeliveryResult:
        """Deliver digest text to an external messaging channel."""
        ...
