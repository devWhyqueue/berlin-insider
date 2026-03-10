from __future__ import annotations

from berlin_insider.formatter.models import AlternativeDigestItem
from berlin_insider.formatter.telegram import render_daily_telegram_alternative


def render_daily_alternative_message(*, alternative_item: AlternativeDigestItem) -> str:
    """Render one alternative tip using the same daily Telegram digest format."""
    return render_daily_telegram_alternative(alternative_item)
