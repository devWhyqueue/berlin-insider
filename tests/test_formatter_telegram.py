from datetime import UTC, datetime

from berlin_insider.curator.models import CuratedItem, CurateRunResult
from berlin_insider.digest import DigestKind
from berlin_insider.fetcher.models import SourceId
from berlin_insider.formatter import DigestFormatConfig, render_telegram_digest
from berlin_insider.parser.models import ParsedCategory, ParsedItem, WeekendRelevance


def _parsed_item(
    *,
    title: str,
    url: str,
    category: ParsedCategory,
    start_at: datetime | None,
    location: str | None = "Berlin",
    source_id: SourceId = SourceId.MITVERGNUEGEN,
    summary: str | None = None,
) -> ParsedItem:
    return ParsedItem(
        source_id=source_id,
        item_url=url,
        title=title,
        description="desc",
        event_start_at=start_at,
        event_end_at=None,
        location=location,
        category=category,
        category_confidence=0.8,
        weekend_relevance=WeekendRelevance.LIKELY_THIS_WEEKEND,
        weekend_confidence=0.9,
        parse_notes=[],
        raw={},
        summary=summary,
    )


def _curate_result(items: list[CuratedItem], *, warnings: list[str] | None = None) -> CurateRunResult:
    return CurateRunResult(
        started_at=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        finished_at=datetime(2026, 2, 27, 8, 1, tzinfo=UTC),
        results=[],
        selected_items=items,
        dropped_count=0,
        failed_sources=[],
        target_count=7,
        actual_count=len(items),
        category_counts={},
        warnings=warnings or [],
    )


def test_formatter_groups_by_category_without_item_dates() -> None:
    nightlife = CuratedItem(
        item=_parsed_item(
            title="Late Rave",
            url="https://example.com/night",
            category=ParsedCategory.NIGHTLIFE,
            start_at=datetime(2026, 2, 28, 20, 30, tzinfo=UTC),
        ),
        score=0.9,
    )
    event = CuratedItem(
        item=_parsed_item(
            title="Gallery Walk",
            url="https://example.com/event",
            category=ParsedCategory.EVENT,
            start_at=datetime(2026, 2, 28, 18, 30, tzinfo=UTC),
        ),
        score=0.95,
    )
    food = CuratedItem(
        item=_parsed_item(
            title="Street Food",
            url="https://example.com/food",
            category=ParsedCategory.FOOD,
            start_at=datetime(2026, 3, 1, 0, 0, tzinfo=UTC),
        ),
        score=0.88,
    )

    text = render_telegram_digest(
        _curate_result([nightlife, event, food]),
        reference_now=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        config=DigestFormatConfig(timezone="UTC"),
    )

    assert "Berlin Insider \\| Weekend Picks" in text
    assert text.index("*Event*") < text.index("*Food*") < text.index("*Nightlife*")
    assert "Sat 28 Feb" not in text
    assert "Sun 01 Mar" not in text


def test_formatter_emits_fallback_note_when_too_few_items() -> None:
    item = CuratedItem(
        item=_parsed_item(
            title="Single Pick",
            url="https://example.com/one",
            category=ParsedCategory.EVENT,
            start_at=datetime(2026, 2, 28, 12, 0, tzinfo=UTC),
        ),
        score=0.8,
    )
    text = render_telegram_digest(
        _curate_result([item], warnings=["Fallback selection active: only 1 items available after filtering"]),
        reference_now=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        config=DigestFormatConfig(timezone="UTC"),
    )
    assert "fewer strong picks this weekend" in text


def test_formatter_escapes_markdown_v2_specials() -> None:
    item = CuratedItem(
        item=_parsed_item(
            title="Rock [n] Roll (Live)!",
            url="https://example.com/path(test)",
            category=ParsedCategory.EVENT,
            start_at=datetime(2026, 2, 28, 12, 0, tzinfo=UTC),
            location="A_B",
            summary="Best [party] in Mitte!",
        ),
        score=0.8,
    )
    text = render_telegram_digest(
        _curate_result([item]),
        reference_now=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        config=DigestFormatConfig(timezone="UTC"),
    )
    assert "Rock \\[n\\] Roll \\(Live\\)\\!" in text
    assert "A\\_B" in text
    assert "Best \\[party\\] in Mitte\\!" in text
    assert "https://example.com/path\\(test\\)" in text


def test_formatter_handles_empty_selection() -> None:
    text = render_telegram_digest(
        _curate_result([]),
        reference_now=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        config=DigestFormatConfig(timezone="UTC"),
    )
    assert "No strong picks found this weekend" in text


def test_formatter_renders_daily_tip() -> None:
    item = CuratedItem(
        item=_parsed_item(
            title="Daily Pick",
            url="https://example.com/daily",
            category=ParsedCategory.EVENT,
            start_at=datetime(2026, 2, 23, 18, 0, tzinfo=UTC),
            summary="One-sentence daily summary.",
        ),
        score=0.9,
    )
    text = render_telegram_digest(
        _curate_result([item]),
        reference_now=datetime(2026, 2, 23, 8, 0, tzinfo=UTC),
        digest_kind=DigestKind.DAILY,
        config=DigestFormatConfig(timezone="UTC"),
    )
    assert "Berlin Insider \\| Tip of the Day" in text
    assert "Tip of the Day \\(" not in text
    assert "Daily Pick" in text
    assert "One\\-sentence daily summary\\." in text


def test_formatter_renders_weekend_summary_line() -> None:
    item = CuratedItem(
        item=_parsed_item(
            title="Weekend Pick",
            url="https://example.com/weekend",
            category=ParsedCategory.EVENT,
            start_at=datetime(2026, 2, 28, 12, 0, tzinfo=UTC),
            summary="One-sentence weekend summary.",
        ),
        score=0.85,
    )
    text = render_telegram_digest(
        _curate_result([item]),
        reference_now=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        config=DigestFormatConfig(timezone="UTC"),
    )
    assert "One\\-sentence weekend summary\\." in text


def test_formatter_omits_footer_metadata_lines() -> None:
    item = CuratedItem(
        item=_parsed_item(
            title="Footer Check",
            url="https://example.com/footer",
            category=ParsedCategory.EVENT,
            start_at=datetime(2026, 2, 28, 12, 0, tzinfo=UTC),
        ),
        score=0.85,
    )
    text = render_telegram_digest(
        _curate_result([item]),
        reference_now=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        config=DigestFormatConfig(timezone="UTC"),
    )
    assert "Sources covered" not in text
    assert "Generated:" not in text
