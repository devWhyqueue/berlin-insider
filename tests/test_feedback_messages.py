from __future__ import annotations

from berlin_insider.feedback.messages import render_daily_alternative_message
from berlin_insider.formatter.models import AlternativeDigestItem
from berlin_insider.parser.models import ParsedCategory


def test_render_daily_alternative_message_matches_daily_tip_format() -> None:
    text = render_daily_alternative_message(
        alternative_item=AlternativeDigestItem(
            item_url="https://example.com/a_(b)",
            title="Alternative Pick",
            summary="Compact follow-up summary.",
            location="Pankow",
            category=ParsedCategory.CULTURE,
            event_start_at=None,
            event_end_at=None,
        )
    )

    assert "Berlin Insider \\| Tip of the Day" in text
    assert "Alternative Pick" in text
    assert "Compact follow\\-up summary\\." in text
    assert "\\(" in text
    assert "\\)" in text
