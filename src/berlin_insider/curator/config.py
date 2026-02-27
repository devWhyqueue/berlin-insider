from __future__ import annotations

from dataclasses import dataclass, field

from berlin_insider.parser.models import ParsedCategory


@dataclass(slots=True)
class CuratorConfig:
    target_count: int = 7
    min_count_fallback: int = 5
    weekend_start_hour_friday: int = 18
    title_similarity_threshold: float = 0.88
    category_targets: dict[ParsedCategory, int] = field(
        default_factory=lambda: {
            ParsedCategory.EVENT: 2,
            ParsedCategory.FOOD: 2,
            ParsedCategory.NIGHTLIFE: 2,
            ParsedCategory.EXHIBITION: 1,
        }
    )
