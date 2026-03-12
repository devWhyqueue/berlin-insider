from __future__ import annotations

import re
from dataclasses import dataclass

from berlin_insider.fetcher.models import FetchedItem, SourceId
from berlin_insider.parser.models import ParsedCategory

_SOURCE_PRIORS: dict[SourceId, ParsedCategory] = {
    SourceId.BERLIN_DE_TICKETS_HEUTE: ParsedCategory.EVENT,
    SourceId.BERLIN_FOOD_STORIES_EDITORIALS: ParsedCategory.FOOD,
    SourceId.BERLIN_FOOD_STORIES_NEWS: ParsedCategory.FOOD,
    SourceId.TELEGRAM_NIGHTDRIVE: ParsedCategory.NIGHTLIFE,
    SourceId.EVENTBRITE_BERLIN_WEEKEND: ParsedCategory.EVENT,
    SourceId.RA_BERLIN: ParsedCategory.EVENT,
    SourceId.RAUSGEGANGEN_DAILY: ParsedCategory.EVENT,
    SourceId.RAUSGEGANGEN_WEEKEND: ParsedCategory.EVENT,
    SourceId.TIP_BERLIN_DAILY: ParsedCategory.EVENT,
    SourceId.TIP_BERLIN_HOME: ParsedCategory.EVENT,
    SourceId.TIP_BERLIN_WEEKEND: ParsedCategory.EVENT,
    SourceId.GRATIS_IN_BERLIN: ParsedCategory.EVENT,
    SourceId.VISIT_BERLIN_DAILY: ParsedCategory.EVENT,
}

_KEYWORDS: dict[ParsedCategory, set[str]] = {
    ParsedCategory.FOOD: {
        "food",
        "restaurant",
        "dinner",
        "brunch",
        "breakfast",
        "cafe",
        "kitchen",
        "menu",
        "eat",
        "taste",
    },
    ParsedCategory.NIGHTLIFE: {"club", "dj", "party", "rave", "nightlife", "dance"},
    ParsedCategory.EXHIBITION: {"museum", "gallery", "exhibition", "vernissage"},
    ParsedCategory.CULTURE: {"concert", "theater", "theatre", "performance", "cinema", "film"},
}


@dataclass(frozen=True, slots=True)
class CategoryDecision:
    category: ParsedCategory
    confidence: float
    note: str | None


def infer_category(
    item: FetchedItem, *, title: str | None, description: str | None, location: str | None
) -> CategoryDecision:
    """Infer normalized category and confidence from source prior and text signals."""
    prior = _SOURCE_PRIORS.get(item.source_id)
    inferred = _keyword_category(title, description, location)
    if prior and inferred and prior != inferred:
        return CategoryDecision(inferred, 0.55, "Category from keywords overrides source prior")
    if inferred:
        return CategoryDecision(inferred, 0.8, "Category inferred from keywords")
    if prior:
        return CategoryDecision(prior, 0.65, "Category from source prior")
    return CategoryDecision(ParsedCategory.MISC, 0.3, "Category fallback to misc")


def _keyword_category(
    title: str | None, description: str | None, location: str | None
) -> ParsedCategory | None:
    haystack = " ".join(filter(None, [title, description, location])).lower()
    if not haystack:
        return None
    scores: dict[ParsedCategory, int] = {}
    for category, words in _KEYWORDS.items():
        score = sum(1 for word in words if re.search(rf"\b{re.escape(word)}\b", haystack))
        if score:
            scores[category] = score
    if not scores:
        return None
    return max(scores.items(), key=lambda pair: pair[1])[0]
