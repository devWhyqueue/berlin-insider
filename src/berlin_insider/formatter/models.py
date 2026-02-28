from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class DigestFormatConfig:
    timezone: str = "Europe/Berlin"
    max_items: int | None = None
    show_location_when_present: bool = True
