from __future__ import annotations

from dataclasses import dataclass

from berlin_insider.parser.models import ParsedCategory


@dataclass(slots=True)
class ItemRecord:
    item_id: int
    canonical_url: str
    source_id: str
    title: str | None
    description: str | None
    clean_text: str | None
    summary: str | None
    event_start_at: str | None
    event_end_at: str | None
    event_date_source: str | None
    location: str | None
    price_text: str | None
    price_amount: float | None
    price_currency: str | None
    is_free: bool | None
    category: ParsedCategory | None


def row_to_item_record(row: tuple[object, ...] | None) -> ItemRecord | None:
    """Convert one item row into an item record."""
    if row is None or not isinstance(row[0], int):
        return None
    category_raw = str(row[16]).strip() if row[16] is not None else None
    return ItemRecord(
        item_id=row[0],
        canonical_url=str(row[1]),
        source_id=str(row[2]),
        title=_str(row[4]),
        description=_str(row[5]),
        clean_text=_str(row[6]),
        summary=_str(row[7]),
        event_start_at=_str(row[8]),
        event_end_at=_str(row[9]),
        event_date_source=_str(row[10]),
        location=_str(row[11]),
        price_text=_str(row[12]),
        price_amount=_float(row[13]),
        price_currency=_str(row[14]),
        is_free=bool(row[15]) if row[15] is not None else None,
        category=ParsedCategory(category_raw) if category_raw else None,
    )


def _str(value: object) -> str | None:
    return str(value) if value is not None else None


def _float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value))
    except ValueError:
        return None
