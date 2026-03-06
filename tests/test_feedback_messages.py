from __future__ import annotations

from berlin_insider.feedback.messages import render_daily_alternative_message


def test_render_daily_alternative_message_contains_one_markdown_link() -> None:
    text = render_daily_alternative_message(alternative_url="https://example.com/a_(b)")

    assert text.count("](https://") == 1
    assert "Open alternative tip" in text
    assert "\\(" in text
    assert "\\)" in text
