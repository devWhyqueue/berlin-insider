from datetime import UTC, datetime

from berlin_insider.curator.config import CuratorConfig
from berlin_insider.curator.models import DropReason
from berlin_insider.curator.orchestrator import Curator
from berlin_insider.curator.store import NoOpSentItemStore
from berlin_insider.digest import DigestKind
from berlin_insider.fetcher.models import SourceId
from berlin_insider.parser.models import (
    ParseRunResult,
    ParseStatus,
    ParsedCategory,
    ParsedItem,
    SourceParseResult,
    WeekendRelevance,
)


def _parsed_item(
    url: str,
    *,
    source_id: SourceId = SourceId.MITVERGNUEGEN,
    title: str | None = None,
    category: ParsedCategory = ParsedCategory.EVENT,
    weekend: WeekendRelevance = WeekendRelevance.LIKELY_THIS_WEEKEND,
    weekend_confidence: float = 0.9,
    category_confidence: float = 0.7,
    start: datetime | None = datetime(2026, 2, 28, 19, 0, tzinfo=UTC),
) -> ParsedItem:
    resolved_title = title or url.rsplit("/", 1)[-1]
    return ParsedItem(
        source_id=source_id,
        item_url=url,
        title=resolved_title,
        description="desc",
        event_start_at=start,
        event_end_at=None,
        location="Berlin",
        category=category,
        category_confidence=category_confidence,
        weekend_relevance=weekend,
        weekend_confidence=weekend_confidence,
        parse_notes=[],
        raw={},
    )


def _parse_result(items: list[ParsedItem]) -> ParseRunResult:
    return ParseRunResult(
        started_at=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        finished_at=datetime(2026, 2, 27, 8, 1, tzinfo=UTC),
        results=[
            SourceParseResult(
                source_id=SourceId.MITVERGNUEGEN,
                status=ParseStatus.SUCCESS,
                items=items,
                warnings=[],
                error_message=None,
                duration_ms=1,
            )
        ],
        total_items=len(items),
        failed_sources=[],
    )


def test_curator_dedupes_url_and_title() -> None:
    parse = _parse_result(
        [
            _parsed_item("https://example.com/item?utm_source=x", title="Warehouse Rave"),
            _parsed_item("https://example.com/item", title="Warehouse Rave Berlin"),
            _parsed_item("https://example.com/other", title="Warehouse Rave"),
        ]
    )
    result = Curator().run(
        parse,
        reference_now=datetime(2026, 2, 27, 9, 0, tzinfo=UTC),
        store=NoOpSentItemStore(),
        config=CuratorConfig(target_count=7),
    )
    assert result.actual_count == 1
    dropped = result.results[0].dropped_items
    assert sum(1 for item in dropped if item.reason == DropReason.DUPLICATE) == 2


def test_curator_uses_soft_category_fill_and_backfill() -> None:
    items = [
        _parsed_item("https://example.com/event-1", category=ParsedCategory.EVENT),
        _parsed_item("https://example.com/event-2", category=ParsedCategory.EVENT),
        _parsed_item("https://example.com/event-3", category=ParsedCategory.EVENT),
        _parsed_item("https://example.com/food-1", category=ParsedCategory.FOOD),
        _parsed_item("https://example.com/night-1", category=ParsedCategory.NIGHTLIFE),
    ]
    parse = _parse_result(items)
    result = Curator().run(
        parse,
        reference_now=datetime(2026, 2, 27, 9, 0, tzinfo=UTC),
        store=NoOpSentItemStore(),
        config=CuratorConfig(target_count=4),
    )
    assert result.actual_count == 4
    assert result.category_counts[ParsedCategory.EVENT] >= 2


def test_curator_filters_unlikely_weekend() -> None:
    in_item = _parsed_item("https://example.com/in", weekend=WeekendRelevance.LIKELY_THIS_WEEKEND)
    out_item = _parsed_item(
        "https://example.com/out",
        weekend=WeekendRelevance.UNLIKELY,
        start=datetime(2026, 3, 5, 10, 0, tzinfo=UTC),
    )
    parse = _parse_result([in_item, out_item])
    result = Curator().run(
        parse,
        reference_now=datetime(2026, 2, 27, 9, 0, tzinfo=UTC),
        store=NoOpSentItemStore(),
    )
    assert result.actual_count == 1
    assert any(
        dropped.reason == DropReason.OUTSIDE_WEEKEND_WINDOW
        for dropped in result.results[0].dropped_items
    )


def test_curator_allows_unknown_as_fallback() -> None:
    parse = _parse_result(
        [
            _parsed_item("https://example.com/known"),
            _parsed_item(
                "https://example.com/unknown",
                weekend=WeekendRelevance.UNKNOWN,
                start=None,
                weekend_confidence=0.0,
            ),
        ]
    )
    result = Curator().run(
        parse,
        reference_now=datetime(2026, 2, 27, 9, 0, tzinfo=UTC),
        store=NoOpSentItemStore(),
        config=CuratorConfig(target_count=7, min_count_fallback=2),
    )
    assert result.actual_count == 2
    assert any("Fallback selection active" in warning for warning in result.warnings)


def test_curator_daily_selects_single_same_day_item() -> None:
    parse = _parse_result(
        [
            _parsed_item(
                "https://example.com/today",
                start=datetime(2026, 2, 23, 12, 0, tzinfo=UTC),
            ),
            _parsed_item(
                "https://example.com/tomorrow",
                start=datetime(2026, 2, 24, 12, 0, tzinfo=UTC),
            ),
        ]
    )
    result = Curator().run(
        parse,
        reference_now=datetime(2026, 2, 23, 8, 0, tzinfo=UTC),
        store=NoOpSentItemStore(),
        config=CuratorConfig(target_count=1, digest_kind=DigestKind.DAILY),
    )
    assert result.actual_count == 1
    assert result.selected_items[0].item.item_url == "https://example.com/today"


def test_curator_daily_accepts_unlikely_same_day_item() -> None:
    parse = _parse_result(
        [
            _parsed_item(
                "https://example.com/today-unlikely",
                weekend=WeekendRelevance.UNLIKELY,
                start=datetime(2026, 2, 23, 12, 0, tzinfo=UTC),
            ),
            _parsed_item(
                "https://example.com/tomorrow-likely",
                start=datetime(2026, 2, 24, 12, 0, tzinfo=UTC),
            ),
        ]
    )
    result = Curator().run(
        parse,
        reference_now=datetime(2026, 2, 23, 8, 0, tzinfo=UTC),
        store=NoOpSentItemStore(),
        config=CuratorConfig(target_count=1, digest_kind=DigestKind.DAILY),
    )
    assert result.actual_count == 1
    assert result.selected_items[0].item.item_url == "https://example.com/today-unlikely"


def test_curator_daily_returns_empty_when_only_upcoming_items_exist() -> None:
    parse = _parse_result(
        [
            _parsed_item(
                "https://example.com/upcoming",
                start=datetime(2026, 2, 24, 12, 0, tzinfo=UTC),
            ),
            _parsed_item(
                "https://example.com/later",
                start=datetime(2026, 2, 25, 12, 0, tzinfo=UTC),
            ),
        ]
    )
    result = Curator().run(
        parse,
        reference_now=datetime(2026, 2, 23, 8, 0, tzinfo=UTC),
        store=NoOpSentItemStore(),
        config=CuratorConfig(target_count=1, digest_kind=DigestKind.DAILY),
    )
    assert result.actual_count == 0
    assert not result.selected_items
    assert any("Fallback selection active" in warning for warning in result.warnings)
