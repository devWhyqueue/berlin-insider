from __future__ import annotations

import re


def price_metadata_from_offer(offers: object) -> dict[str, str]:
    """Extract price metadata from a JSON-LD Offer or Offer list."""
    offer = offers[0] if isinstance(offers, list) and offers else offers
    if not isinstance(offer, dict):
        return {}
    metadata: dict[str, str] = {}
    price = _coerce_string(offer.get("price"))
    currency = _coerce_string(offer.get("priceCurrency"))
    if price:
        metadata["price_amount"] = price
        metadata["price_text"] = price if currency is None else f"{price} {currency}"
        metadata["is_free"] = str(_is_zero_price(price)).lower()
    if currency:
        metadata["price_currency"] = currency
    return metadata


def extract_price_metadata(detail_text: str) -> dict[str, str]:
    """Extract obvious free/euro price metadata from visible text."""
    metadata: dict[str, str] = {}
    if re.search(r"\b(kostenlos|eintritt frei|free entry|free)\b", detail_text, re.I):
        metadata["is_free"] = "true"
        metadata["price_text"] = "free"
    match = re.search(r"(\d+(?:[,.]\d{1,2})?)\s*(?:€|EUR)\b", detail_text, re.I)
    if match:
        amount = match.group(1).replace(",", ".")
        metadata["price_amount"] = amount
        metadata["price_currency"] = "EUR"
        metadata["price_text"] = f"{amount} EUR"
        metadata["is_free"] = str(_is_zero_price(amount)).lower()
    return metadata


def _coerce_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _is_zero_price(value: str) -> bool:
    try:
        return float(value.replace(",", ".")) == 0
    except ValueError:
        return False
