from __future__ import annotations

import re
from collections import defaultdict
from datetime import UTC, datetime, timedelta, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from berlin_insider.curator.models import CuratedItem, CurateRunResult
from berlin_insider.digest import DigestKind
from berlin_insider.formatter.models import DigestFormatConfig
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

_WEEKDAY_ABBR = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
_MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
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
    reference_now: datetime,
    config: DigestFormatConfig | None = None,
) -> str:
    """Render curated weekend picks as a Telegram MarkdownV2 digest."""
    cfg = config or DigestFormatConfig()
    tz = _timezone_or_utc(cfg.timezone)
    local_now = reference_now.astimezone(tz)
    weekend_start, weekend_end = _weekend_bounds(local_now)
    items = (
        curate.selected_items[: cfg.max_items]
        if cfg.max_items is not None
        else curate.selected_items
    )
    lines = _build_header(weekend_start, weekend_end, cfg=cfg)
    if not items:
        return _render_empty(lines)
    if _needs_fallback_note(curate):
        lines.extend(_fallback_lines())
    lines.extend(_render_sections(items, cfg=cfg, tz=tz))
    lines.extend(_render_footer(items))
    return "\n".join(lines)


def render_daily_telegram_digest(
    curate: CurateRunResult,
    *,
    reference_now: datetime,
    config: DigestFormatConfig | None = None,
) -> str:
    """Render one daily curated recommendation in Telegram MarkdownV2 format."""
    cfg = config or DigestFormatConfig()
    tz = _timezone_or_utc(cfg.timezone)
    local_now = reference_now.astimezone(tz)
    lines = [
        _escape_text(f"Berlin Insider | Tip of the Day ({_format_day(local_now)}, {cfg.timezone})"),
        "",
    ]
    items = curate.selected_items[:1]
    if not items:
        lines.append(_escape_text("No strong tip found today."))
        lines.append(_escape_text("We will share a fresh pick on the next run."))
        return "\n".join(lines)
    lines.append(_render_bullet(items[0], cfg=cfg, tz=tz))
    lines.append("")
    lines.extend(_render_footer(items))
    return "\n".join(lines)


def _build_header(
    weekend_start: datetime, weekend_end: datetime, *, cfg: DigestFormatConfig
) -> list[str]:
    return [
        _escape_text(
            f"Berlin Insider | Weekend Picks ({_format_day(weekend_start)} - {_format_day(weekend_end)}, {cfg.timezone})"
        ),
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


def _render_sections(items: list[CuratedItem], *, cfg: DigestFormatConfig, tz: tzinfo) -> list[str]:
    lines: list[str] = []
    by_category = _group_items(items)
    for category in _CATEGORY_ORDER:
        grouped = by_category.get(category, [])
        if not grouped:
            continue
        lines.append(f"*{_escape_text(_CATEGORY_LABELS[category])}*")
        for item in grouped:
            lines.append(_render_bullet(item, cfg=cfg, tz=tz))
        lines.append("")
    return lines


def _render_footer(items: list[CuratedItem]) -> list[str]:  # noqa: ARG001
    return []


def _group_items(items: list[CuratedItem]) -> dict[ParsedCategory, list[CuratedItem]]:
    grouped: dict[ParsedCategory, list[CuratedItem]] = defaultdict(list)
    for item in items:
        grouped[item.item.category].append(item)
    return grouped


def _render_bullet(curated_item: CuratedItem, *, cfg: DigestFormatConfig, tz: tzinfo) -> str:
    item: ParsedItem = curated_item.item
    title = _escape_text(item.title or "Untitled")
    url = _escape_url(item.item_url)
    date_text = _escape_text(_format_item_date(item.event_start_at, tz=tz))
    parts = [f"\\- [{title}]({url})", date_text]
    if cfg.show_location_when_present and item.location:
        parts.append(_escape_text(item.location))
    base_line = " \\| ".join(parts)
    if item.summary is None:
        return base_line
    return f"{base_line}\n{_escape_text(item.summary)}"


def _timezone_or_utc(timezone_name: str) -> tzinfo:
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        return UTC


def _weekend_bounds(reference_now: datetime) -> tuple[datetime, datetime]:
    weekday = reference_now.weekday()
    if weekday <= 4:
        days_to_friday = 4 - weekday
    else:
        days_to_friday = -(weekday - 4)
    friday = (reference_now + timedelta(days=days_to_friday)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    sunday = friday + timedelta(days=2)
    return friday, sunday


def _format_day(value: datetime) -> str:
    return f"{_WEEKDAY_ABBR[value.weekday()]} {value.day:02d} {_MONTH_ABBR[value.month - 1]}"


def _format_item_date(value: datetime | None, *, tz: tzinfo) -> str:
    if value is None:
        return "Date TBA"
    local = value.astimezone(tz)
    day_text = _format_day(local)
    if local.hour == 0 and local.minute == 0 and local.second == 0:
        return day_text
    return f"{day_text} {local.hour:02d}:{local.minute:02d}"


def _needs_fallback_note(curate: CurateRunResult) -> bool:
    if curate.actual_count < 5:
        return True
    return any("Fallback selection active" in warning for warning in curate.warnings)


def _escape_text(value: str) -> str:
    return _MARKDOWN_V2_SPECIALS.sub(r"\\\1", value)


def _escape_url(url: str) -> str:
    return url.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
