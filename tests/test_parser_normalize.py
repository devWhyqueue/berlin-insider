from datetime import UTC, datetime

from berlin_insider.fetcher.models import FetchedItem, FetchMethod, SourceId
from berlin_insider.parser.models import ParsedCategory, WeekendRelevance
from berlin_insider.parser.normalize import normalize_fetched_item


def _item(
    *,
    source_id: SourceId = SourceId.BERLIN_FOOD_STORIES_NEWS,
    title: str | None = "  Great   Food   Spot  ",
    snippet: str | None = "  Amazing   brunch place  ",
    raw_date_text: str | None = None,
    published_at: datetime | None = None,
) -> FetchedItem:
    return FetchedItem(
        source_id=source_id,
        source_url="https://example.com/source",
        item_url="https://example.com/item",
        title=title,
        published_at=published_at,
        raw_date_text=raw_date_text,
        snippet=snippet,
        location_hint="  Berlin Mitte  ",
        fetch_method=FetchMethod.HTML,
        collected_at=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        metadata={},
    )


def test_normalize_cleans_fields_and_uses_source_prior() -> None:
    parsed = normalize_fetched_item(
        _item(published_at=datetime(2026, 2, 28, 10, 0, tzinfo=UTC)),
        reference_now=datetime(2026, 2, 27, 10, 0, tzinfo=UTC),
    )
    assert parsed.title == "Great Food Spot"
    assert parsed.description == "Amazing brunch place"
    assert parsed.location == "Berlin Mitte"
    assert parsed.category == ParsedCategory.FOOD
    assert parsed.category_confidence == 0.8
    assert parsed.weekend_relevance == WeekendRelevance.LIKELY_THIS_WEEKEND


def test_normalize_keyword_override_conflict() -> None:
    parsed = normalize_fetched_item(
        _item(
            source_id=SourceId.BERLIN_FOOD_STORIES_EDITORIALS,
            title="Warehouse Rave Party",
            snippet=None,
            published_at=datetime(2026, 3, 1, 0, 0, tzinfo=UTC),
        ),
        reference_now=datetime(2026, 2, 27, 10, 0, tzinfo=UTC),
    )
    assert parsed.category == ParsedCategory.NIGHTLIFE
    assert parsed.category_confidence == 0.55


def test_normalize_handles_relative_german_date_text() -> None:
    parsed = normalize_fetched_item(
        _item(
            source_id=SourceId.RAUSGEGANGEN_WEEKEND,
            published_at=None,
            raw_date_text="Heute 19:00",
        ),
        reference_now=datetime(2026, 2, 27, 10, 0, tzinfo=UTC),
    )
    assert parsed.event_start_at is not None
    assert parsed.event_start_at.date().isoformat() == "2026-02-27"
    assert parsed.weekend_relevance == WeekendRelevance.POSSIBLE


def test_normalize_unknown_date_marks_unknown_weekend() -> None:
    parsed = normalize_fetched_item(
        _item(
            source_id=SourceId.BLOG_IN_BERLIN,
            published_at=None,
            raw_date_text="unknown date text",
        ),
        reference_now=datetime(2026, 2, 27, 10, 0, tzinfo=UTC),
    )
    assert parsed.event_start_at is None
    assert parsed.weekend_relevance == WeekendRelevance.UNKNOWN
