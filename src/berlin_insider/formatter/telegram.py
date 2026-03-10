from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime

from berlin_insider.curator.models import CuratedItem, CurateRunResult
from berlin_insider.digest import DigestKind
from berlin_insider.formatter.models import AlternativeDigestItem, DigestFormatConfig
from berlin_insider.parser.models import ParsedCategory, ParsedItem

_CATEGORY_ORDER = [
    ParsedCategory.EVENT,
    ParsedCategory.FOOD,
    ParsedCategory.NIGHTLIFE,
    ParsedCategory.EXHIBITION,
    ParsedCategory.CULTURE,
    ParsedCategory.MISC,
]

_CATEGORY_LABELS = {
    ParsedCategory.EVENT: "Event",
    ParsedCategory.FOOD: "Food",
    ParsedCategory.NIGHTLIFE: "Nightlife",
    ParsedCategory.EXHIBITION: "Exhibition",
    ParsedCategory.CULTURE: "Culture",
    ParsedCategory.MISC: "Misc",
}

_MARKDOWN_V2_SPECIALS = re.compile(r"([\\_*\[\]()~`>#+\-=|{}.!])")


def render_telegram_digest(
    curate: CurateRunResult,
    *,
    reference_now: datetime,
    digest_kind: DigestKind = DigestKind.WEEKEND,
    config: DigestFormatConfig | None = None,
) -> str:
    """Render curated picks as a Telegram MarkdownV2 digest."""
    if digest_kind == DigestKind.DAILY:
        return render_daily_telegram_digest(curate, reference_now=reference_now, config=config)
    return render_weekend_telegram_digest(curate, reference_now=reference_now, config=config)


def render_weekend_telegram_digest(
    curate: CurateRunResult,
    *,
    reference_now: datetime,  # noqa: ARG001
    config: DigestFormatConfig | None = None,
) -> str:
    """Render curated weekend picks as a Telegram MarkdownV2 digest."""
    cfg = config or DigestFormatConfig()
    items = (
        curate.selected_items[: cfg.max_items]
        if cfg.max_items is not None
        else curate.selected_items
    )
    lines = _build_header()
    if not items:
        return _render_empty(lines)
    if _needs_fallback_note(curate):
        lines.extend(_fallback_lines())
    lines.extend(_render_sections(items, cfg=cfg))
    lines.extend(_render_footer(items))
    return "\n".join(lines)


def render_daily_telegram_digest(
    curate: CurateRunResult,
    *,
    reference_now: datetime,  # noqa: ARG001
    config: DigestFormatConfig | None = None,
) -> str:
    """Render one daily curated recommendation in Telegram MarkdownV2 format."""
    cfg = config or DigestFormatConfig()
    lines = [
        _escape_text("Berlin Insider | Tip of the Day"),
        "",
    ]
    items = curate.selected_items[:1]
    if not items:
        lines.append(_escape_text("No strong tip found today."))
        lines.append(_escape_text("We will share a fresh pick on the next run."))
        return "\n".join(lines)
    lines.append(_render_bullet(items[0], cfg=cfg))
    lines.append("")
    lines.extend(_render_footer(items))
    return "\n".join(lines)


def render_daily_telegram_alternative(
    item: AlternativeDigestItem,
    *,
    config: DigestFormatConfig | None = None,
) -> str:
    """Render one persisted alternative item in daily Telegram format."""
    cfg = config or DigestFormatConfig()
    lines = [
        _escape_text("Berlin Insider | Tip of the Day"),
        "",
        _render_persisted_item(item, cfg=cfg),
        "",
    ]
    return "\n".join(lines)


def _build_header() -> list[str]:
    return [
        _escape_text("Berlin Insider | Weekend Picks"),
        "",
    ]


def _render_empty(lines: list[str]) -> str:
    lines.append(_escape_text("No strong picks found this weekend."))
    lines.append(_escape_text("We will share fresh picks on the next run."))
    return "\n".join(lines)


def _fallback_lines() -> list[str]:
    return [
        _escape_text("Note: fewer strong picks this weekend; showing the best available."),
        "",
    ]


def _render_sections(items: list[CuratedItem], *, cfg: DigestFormatConfig) -> list[str]:
    lines: list[str] = []
    by_category = _group_items(items)
    for category in _CATEGORY_ORDER:
        grouped = by_category.get(category, [])
        if not grouped:
            continue
        lines.append(f"*{_escape_text(_CATEGORY_LABELS[category])}*")
        for item in grouped:
            lines.append(_render_bullet(item, cfg=cfg))
        lines.append("")
    return lines


def _render_footer(items: list[CuratedItem]) -> list[str]:  # noqa: ARG001
    return []


def _group_items(items: list[CuratedItem]) -> dict[ParsedCategory, list[CuratedItem]]:
    grouped: dict[ParsedCategory, list[CuratedItem]] = defaultdict(list)
    for item in items:
        grouped[item.item.category].append(item)
    return grouped


def _render_bullet(curated_item: CuratedItem, *, cfg: DigestFormatConfig) -> str:
    item: ParsedItem = curated_item.item
    return _render_item_fields(
        title=item.title,
        item_url=item.item_url,
        location=item.location,
        summary=item.summary,
        cfg=cfg,
    )


def _render_persisted_item(item: AlternativeDigestItem, *, cfg: DigestFormatConfig) -> str:
    return _render_item_fields(
        title=item.title,
        item_url=item.item_url,
        location=item.location,
        summary=item.summary,
        cfg=cfg,
    )


def _render_item_fields(
    *,
    title: str | None,
    item_url: str,
    location: str | None,
    summary: str | None,
    cfg: DigestFormatConfig,
) -> str:
    title = _escape_text(title or "Untitled")
    url = _escape_url(item_url)
    parts = [f"\\- [{title}]({url})"]
    if cfg.show_location_when_present and location:
        parts.append(_escape_text(location))
    base_line = " \\| ".join(parts)
    if summary is None:
        return base_line
    return f"{base_line}\n{_escape_text(summary)}"


def _needs_fallback_note(curate: CurateRunResult) -> bool:
    if curate.actual_count < 5:
        return True
    return any("Fallback selection active" in warning for warning in curate.warnings)


def _escape_text(value: str) -> str:
    return _MARKDOWN_V2_SPECIALS.sub(r"\\\1", value)


def _escape_url(url: str) -> str:
    return url.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
