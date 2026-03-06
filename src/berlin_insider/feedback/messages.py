from __future__ import annotations

import re

_MARKDOWN_V2_SPECIALS = re.compile(r"([\\_*\[\]()~`>#+\-=|{}.!])")


def render_daily_alternative_message(*, alternative_url: str) -> str:
    """Render a short Telegram MarkdownV2 message with one alternative tip link."""
    safe_url = _escape_url(alternative_url)
    lines = [
        _escape_text("Thanks for the feedback."),
        "",
        _escape_text("Here is one alternative tip for today:"),
        f"\\- [Open alternative tip]({safe_url})",
    ]
    return "\n".join(lines)


def _escape_text(value: str) -> str:
    return _MARKDOWN_V2_SPECIALS.sub(r"\\\1", value)


def _escape_url(url: str) -> str:
    return url.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
