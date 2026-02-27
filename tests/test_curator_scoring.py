from datetime import UTC, datetime

from berlin_insider.curator.scoring import score_item
from berlin_insider.fetcher.models import SourceId
from berlin_insider.parser.models import ParsedCategory, ParsedItem, WeekendRelevance


def _item(
    *,
    weekend: WeekendRelevance = WeekendRelevance.LIKELY_THIS_WEEKEND,
    weekend_confidence: float = 0.9,
    category: ParsedCategory = ParsedCategory.EVENT,
    category_confidence: float = 0.8,
    title: str | None = "Item",
    description: str | None = "Desc",
    location: str | None = "Berlin",
    event_start_at: datetime | None = datetime(2026, 2, 28, 18, 0, tzinfo=UTC),
) -> ParsedItem:
    return ParsedItem(
        source_id=SourceId.MITVERGNUEGEN,
        item_url="https://example.com/item",
        title=title,
        description=description,
        event_start_at=event_start_at,
        event_end_at=None,
        location=location,
        category=category,
        category_confidence=category_confidence,
        weekend_relevance=weekend,
        weekend_confidence=weekend_confidence,
        parse_notes=[],
        raw={},
    )


def test_score_rewards_confidence_and_completeness() -> None:
    strong = score_item(_item(), event_in_window=True)
    weak = score_item(
        _item(
            weekend=WeekendRelevance.POSSIBLE,
            weekend_confidence=0.2,
            category_confidence=0.2,
            description=None,
            location=None,
            event_start_at=None,
        ),
        event_in_window=False,
    )
    assert strong.total > weak.total


def test_score_penalizes_misc_category() -> None:
    misc = score_item(_item(category=ParsedCategory.MISC), event_in_window=True)
    event = score_item(_item(category=ParsedCategory.EVENT), event_in_window=True)
    assert event.total > misc.total
