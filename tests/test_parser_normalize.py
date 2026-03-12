from datetime import UTC, datetime

from berlin_insider.fetcher.models import FetchedItem, FetchMethod, SourceId
from berlin_insider.parser.models import ParsedCategory, WeekendRelevance
from berlin_insider.parser.normalize import normalize_fetched_item


def _item(
    *,
    source_id: SourceId = SourceId.BERLIN_FOOD_STORIES_NEWS,
    title: str | None = "  Great   Food   Spot  ",
    snippet: str | None = "  Amazing   brunch place  ",
    detail_text: str | None = None,
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
        detail_text=detail_text,
        detail_status=None,
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


def test_normalize_prefers_metadata_start_date_over_published_at() -> None:
    item = _item(
        source_id=SourceId.BERLIN_DE_WOCHENEND_TIPPS,
        published_at=datetime(2026, 2, 27, 10, 29, 38, tzinfo=UTC),
    )
    item.metadata = {"start_date": "2026-03-14", "end_date": "2026-03-15"}
    parsed = normalize_fetched_item(
        item,
        reference_now=datetime(2026, 3, 12, 10, 0, tzinfo=UTC),
    )
    assert parsed.event_start_at is not None
    assert parsed.event_start_at.date().isoformat() == "2026-03-14"
    assert parsed.event_end_at is not None
    assert parsed.event_end_at.date().isoformat() == "2026-03-15"
    assert parsed.weekend_relevance == WeekendRelevance.LIKELY_THIS_WEEKEND


def test_normalize_prefers_detail_text_over_snippet() -> None:
    parsed = normalize_fetched_item(
        _item(
            title="Weekend Art Night",
            snippet="short snippet",
            detail_text="Detailed exhibition and concert guide in Berlin this weekend.",
            published_at=datetime(2026, 2, 28, 10, 0, tzinfo=UTC),
        ),
        reference_now=datetime(2026, 2, 27, 10, 0, tzinfo=UTC),
    )
    assert parsed.detail_text == "Detailed exhibition and concert guide in Berlin this weekend."
    assert parsed.description == "Detailed exhibition and concert guide in Berlin this weekend."


def test_normalize_falls_back_to_snippet_when_detail_missing() -> None:
    parsed = normalize_fetched_item(
        _item(
            snippet="Listing-only summary",
            detail_text=None,
            published_at=datetime(2026, 2, 28, 10, 0, tzinfo=UTC),
        ),
        reference_now=datetime(2026, 2, 27, 10, 0, tzinfo=UTC),
    )
    assert parsed.detail_text is None
    assert parsed.description == "Listing-only summary"


def test_normalize_prefers_page_date_metadata_over_published_at() -> None:
    item = _item(
        source_id=SourceId.VISIT_BERLIN_BLOG,
        published_at=datetime(2017, 2, 28, 6, 26, 24, tzinfo=UTC),
    )
    item.metadata = {"page_date": "2026-03-10T08:00:00Z"}
    parsed = normalize_fetched_item(
        item,
        reference_now=datetime(2026, 3, 12, 10, 0, tzinfo=UTC),
    )
    assert parsed.event_start_at is not None
    assert parsed.event_start_at.date().isoformat() == "2026-03-10"


def test_normalize_uses_detail_location_when_listing_missing() -> None:
    item = _item(published_at=datetime(2026, 2, 28, 10, 0, tzinfo=UTC))
    item.location_hint = None
    item.metadata = {"location": "Maybachufer, Neukölln"}
    parsed = normalize_fetched_item(
        item,
        reference_now=datetime(2026, 2, 27, 10, 0, tzinfo=UTC),
    )
    assert parsed.location == "Maybachufer, Neukölln"
