from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class SourceId(StrEnum):
    BERLIN_DE_WOCHENEND_TIPPS = "berlin_de_wochenend_tipps"
    BLOG_IN_BERLIN = "blog_in_berlin"
    IHEART_BERLIN = "iheart_berlin"
    MITVERGNUEGEN = "mitvergnuegen"
    TIP_BERLIN_HOME = "tip_berlin_home"
    TIP_BERLIN_WEEKEND = "tip_berlin_weekend"
    VISIT_BERLIN_BLOG = "visit_berlin_blog"
    BERLIN_FOOD_STORIES_EDITORIALS = "berlin_food_stories_editorials"
    BERLIN_FOOD_STORIES_NEWS = "berlin_food_stories_news"
    RAUSGEGANGEN_WEEKEND = "rausgegangen_weekend"
    GRATIS_IN_BERLIN = "gratis_in_berlin"
    TELEGRAM_NIGHTDRIVE = "telegram_nightdrive"
    EVENTBRITE_BERLIN_WEEKEND = "eventbrite_berlin_weekend"


class FetchMethod(StrEnum):
    RSS = "rss"
    HTML = "html"
    JSONLD = "jsonld"
    TELEGRAM_HTML = "telegram_html"
    PLAYWRIGHT_HTML = "playwright_html"


class FetchStatus(StrEnum):
    SUCCESS = "success"
    PARTIAL = "partial"
    BLOCKED = "blocked"
    ERROR = "error"


@dataclass(slots=True)
class FetchContext:
    user_agent: str
    timeout_seconds: float
    max_items_per_source: int
    collected_at: datetime


@dataclass(slots=True)
class FetchedItem:
    source_id: SourceId
    source_url: str
    item_url: str
    title: str | None
    published_at: datetime | None
    raw_date_text: str | None
    snippet: str | None
    location_hint: str | None
    fetch_method: FetchMethod
    collected_at: datetime
    metadata: dict[str, Any] = field(default_factory=dict)
    detail_text: str | None = None
    detail_status: str | None = None


@dataclass(slots=True)
class SourceFetchResult:
    source_id: SourceId
    status: FetchStatus
    items: list[FetchedItem]
    warnings: list[str]
    error_message: str | None
    duration_ms: int


@dataclass(slots=True)
class FetchRunResult:
    started_at: datetime
    finished_at: datetime
    results: list[SourceFetchResult]
    total_items: int
    failed_sources: list[SourceId]
