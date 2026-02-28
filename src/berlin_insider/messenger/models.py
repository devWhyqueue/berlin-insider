from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


class MessengerError(RuntimeError):
    """Raised when a messenger provider fails to deliver a digest."""


@dataclass(slots=True)
class DeliveryResult:
    delivered_at: datetime
    external_message_id: str
    warning_message: str | None = None


class Messenger(Protocol):
    def send_digest(self, *, text: str) -> DeliveryResult:
        """Deliver digest text to an external messaging channel."""
        ...
