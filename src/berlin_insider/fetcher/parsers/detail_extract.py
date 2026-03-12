from __future__ import annotations

import json
import re
from typing import Any

from bs4 import BeautifulSoup

MIN_DETAIL_LENGTH = 60
JSONLD_BODY_KEYS = ("articleBody", "text", "description")
BOILERPLATE_SELECTORS = (
    "script",
    "style",
    "noscript",
    "nav",
    "footer",
    "header",
    "aside",
    "form",
    "svg",
    "iframe",
    "button",
)


def extract_detail_payload(html: str) -> tuple[str | None, dict[str, str]]:
    """Extract best-effort readable text plus structured event metadata."""
    soup = BeautifulSoup(html, "html.parser")
    detail_metadata = _extract_jsonld_event_metadata(soup)
    detail_metadata.update(_extract_page_date_metadata(soup))
    jsonld_text = _extract_jsonld_text(soup)
    if _is_meaningful(jsonld_text):
        detail_metadata.update(_extract_location_metadata(_require_text(jsonld_text)))
        return jsonld_text, detail_metadata
    _strip_boilerplate(soup)
    for selector in ("article", "main", "body"):
        node = soup.select_one(selector)
        candidate = _normalize_text(node.get_text(" ", strip=True)) if node else None
        if _is_meaningful(candidate):
            detail_metadata.update(_extract_location_metadata(_require_text(candidate)))
            return candidate, detail_metadata
    return None, detail_metadata


def _extract_jsonld_text(soup: BeautifulSoup) -> str | None:
    for script in soup.select("script[type='application/ld+json']"):
        content = script.string or script.get_text()
        if not content:
            continue
        for payload in _json_documents(content):
            candidate = _extract_text_from_payload(payload)
            if _is_meaningful(candidate):
                return candidate
    return None


def _json_documents(content: str) -> list[Any]:
    cleaned = content.strip()
    if not cleaned:
        return []
    try:
        payload = json.loads(cleaned)
    except json.JSONDecodeError:
        return []
    return payload if isinstance(payload, list) else [payload]


def _extract_text_from_payload(payload: Any) -> str | None:
    if isinstance(payload, dict):
        for key in JSONLD_BODY_KEYS:
            candidate = _normalize_text(_coerce_string(payload.get(key)))
            if _is_meaningful(candidate):
                return candidate
        graph = payload.get("@graph")
        if isinstance(graph, list):
            for node in graph:
                candidate = _extract_text_from_payload(node)
                if _is_meaningful(candidate):
                    return candidate
    if isinstance(payload, list):
        for node in payload:
            candidate = _extract_text_from_payload(node)
            if _is_meaningful(candidate):
                return candidate
    return None


def _extract_jsonld_event_metadata(soup: BeautifulSoup) -> dict[str, str]:
    for script in soup.select("script[type='application/ld+json']"):
        content = script.string or script.get_text()
        if not content:
            continue
        for payload in _json_documents(content):
            event_metadata = _extract_event_metadata_from_payload(payload)
            if event_metadata:
                return event_metadata
    return {}


def _extract_page_date_metadata(soup: BeautifulSoup) -> dict[str, str]:
    metadata = _visible_time_metadata(soup)
    if metadata:
        return metadata
    return _jsonld_page_date_metadata(soup)


def _extract_event_metadata_from_payload(payload: Any) -> dict[str, str]:
    if isinstance(payload, dict):
        extracted = _event_metadata_from_mapping(payload)
        if extracted:
            return extracted
        graph = payload.get("@graph")
        if isinstance(graph, list):
            for node in graph:
                extracted = _extract_event_metadata_from_payload(node)
                if extracted:
                    return extracted
    if isinstance(payload, list):
        for node in payload:
            extracted = _extract_event_metadata_from_payload(node)
            if extracted:
                return extracted
    return {}


def _jsonld_page_date_metadata(soup: BeautifulSoup) -> dict[str, str]:
    for script in soup.select("script[type='application/ld+json']"):
        content = script.string or script.get_text()
        if not content:
            continue
        for payload in _json_documents(content):
            date_metadata = _extract_page_date_from_payload(payload)
            if date_metadata:
                return date_metadata
    return {}


def _extract_page_date_from_payload(payload: Any) -> dict[str, str]:
    if isinstance(payload, dict):
        date_modified = _coerce_string(payload.get("dateModified"))
        date_published = _coerce_string(payload.get("datePublished"))
        if date_modified:
            return {"page_date": date_modified}
        if date_published:
            return {"page_date": date_published}
        graph = payload.get("@graph")
        if isinstance(graph, list):
            for node in graph:
                extracted = _extract_page_date_from_payload(node)
                if extracted:
                    return extracted
    if isinstance(payload, list):
        for node in payload:
            extracted = _extract_page_date_from_payload(node)
            if extracted:
                return extracted
    return {}


def _event_metadata_from_mapping(payload: dict[str, Any]) -> dict[str, str]:
    type_value = payload.get("@type")
    if not _is_event_type(type_value):
        return {}
    metadata: dict[str, str] = {}
    start_date = _coerce_string(payload.get("startDate"))
    end_date = _coerce_string(payload.get("endDate"))
    if start_date:
        metadata["start_date"] = start_date
    if end_date:
        metadata["end_date"] = end_date
    return metadata


def _visible_time_metadata(soup: BeautifulSoup) -> dict[str, str]:
    for node in soup.select("time[datetime]"):
        text = _normalize_text(node.get_text(" ", strip=True))
        if not text:
            continue
        value = _coerce_string(node.get("datetime"))
        if value:
            return {"page_date": value}
    return {}


def _is_event_type(type_value: object) -> bool:
    if isinstance(type_value, str):
        normalized = type_value.lower()
        return normalized == "event" or normalized.endswith("event")
    if isinstance(type_value, list):
        return any(_is_event_type(entry) for entry in type_value)
    return False


def _strip_boilerplate(soup: BeautifulSoup) -> None:
    for selector in BOILERPLATE_SELECTORS:
        for node in soup.select(selector):
            node.decompose()


def _extract_location_metadata(detail_text: str) -> dict[str, str]:
    location = _location_from_text(detail_text)
    return {"location": location} if location else {}


def _location_from_text(detail_text: str) -> str | None:
    pattern = r"(?:^|\b)Wo:\s*(.+?)(?:\s+(?:Mehr Infos|Mehr Informationen)\b|$)"
    match = re.search(pattern, detail_text)
    if not match:
        return None
    location = match.group(1).strip(" .,:;")
    return _normalize_text(location)


def _coerce_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    collapsed = " ".join(value.split())
    return collapsed or None


def _is_meaningful(value: str | None) -> bool:
    return value is not None and len(value) >= MIN_DETAIL_LENGTH


def _require_text(value: str | None) -> str:
    if value is None:
        raise ValueError("expected text value")
    return value
