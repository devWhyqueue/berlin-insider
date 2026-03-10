from __future__ import annotations

import json
from typing import cast

from berlin_insider.digest import DigestKind
from berlin_insider.feedback.models import FeedbackEvent, SentMessageRecord
from berlin_insider.formatter.models import AlternativeDigestItem
from berlin_insider.parser.models import ParsedCategory


def feedback_event_values(event: FeedbackEvent) -> tuple[str, str, str, int, str, str, str, str]:
    """Build one SQLite parameter tuple for a feedback event row."""
    return (
        event.message_key,
        event.digest_kind.value,
        event.vote,
        event.telegram_user_id,
        event.chat_id,
        event.message_id,
        event.voted_at,
        event.updated_at,
    )


def sent_message_values(record: SentMessageRecord) -> tuple[str, str, str, str, str, str, str]:
    """Build one SQLite parameter tuple for a sent-message row."""
    return (
        record.message_key,
        record.digest_kind.value,
        record.local_date,
        record.sent_at,
        record.telegram_message_id,
        json.dumps(record.selected_urls, ensure_ascii=False),
        json.dumps(_alternative_item_payload(record.alternative_item), ensure_ascii=False),
    )


def row_to_sent_record(row: tuple[object, ...]) -> SentMessageRecord | None:
    """Convert one SQLite sent-message row into a typed record."""
    parsed = _parsed_sent_message_fields(row)
    if parsed is None:
        return None
    return SentMessageRecord(
        message_key=parsed[0],
        digest_kind=parsed[1],
        local_date=parsed[2],
        sent_at=parsed[3],
        telegram_message_id=parsed[4],
        selected_urls=parsed[5],
        alternative_item=_alternative_item_from_json(parsed[6]),
    )


def _row_fields(row: tuple[object, ...]) -> tuple[str, object, str, str, str, str, object] | None:
    if len(row) != 7:
        return None
    (
        message_key,
        digest_kind_raw,
        local_date,
        sent_at,
        telegram_message_id,
        selected_urls_json,
        alternative_item_json,
    ) = row
    required_strings = [message_key, local_date, sent_at, telegram_message_id, selected_urls_json]
    if not all(isinstance(value, str) for value in required_strings):
        return None
    parsed_message_key = cast(str, message_key)
    parsed_local_date = cast(str, local_date)
    parsed_sent_at = cast(str, sent_at)
    parsed_telegram_message_id = cast(str, telegram_message_id)
    parsed_selected_urls_json = cast(str, selected_urls_json)
    return (
        parsed_message_key,
        digest_kind_raw,
        parsed_local_date,
        parsed_sent_at,
        parsed_telegram_message_id,
        parsed_selected_urls_json,
        alternative_item_json,
    )


def _parsed_sent_message_fields(
    row: tuple[object, ...],
) -> tuple[str, DigestKind, str, str, str, list[str], object] | None:
    raw_fields = _row_fields(row)
    if raw_fields is None:
        return None
    digest_kind = _parse_digest_kind(raw_fields[1])
    selected_urls = _parse_selected_urls(raw_fields[5])
    if digest_kind is None or selected_urls is None:
        return None
    return (
        raw_fields[0],
        digest_kind,
        raw_fields[2],
        raw_fields[3],
        raw_fields[4],
        selected_urls,
        raw_fields[6],
    )


def _parse_digest_kind(raw: object) -> DigestKind | None:
    try:
        return DigestKind(str(raw))
    except ValueError:
        return None


def _parse_selected_urls(raw: str) -> list[str] | None:
    try:
        selected_urls = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(selected_urls, list) or not all(
        isinstance(url, str) for url in selected_urls
    ):
        return None
    return selected_urls


def _alternative_item_payload(item: AlternativeDigestItem | None) -> dict[str, str | None] | None:
    if item is None:
        return None
    return {
        "item_url": item.item_url,
        "title": item.title,
        "summary": item.summary,
        "location": item.location,
        "category": item.category.value,
        "event_start_at": item.event_start_at,
        "event_end_at": item.event_end_at,
    }


def _alternative_item_from_json(raw: object) -> AlternativeDigestItem | None:
    payload = _parse_json_dict(raw)
    if payload is None:
        return None
    item_url = payload.get("item_url")
    category_raw = payload.get("category")
    if not isinstance(item_url, str):
        return None
    try:
        category = ParsedCategory(str(category_raw))
    except ValueError:
        return None
    return AlternativeDigestItem(
        item_url=item_url,
        title=_optional_str(payload.get("title")),
        summary=_optional_str(payload.get("summary")),
        location=_optional_str(payload.get("location")),
        category=category,
        event_start_at=_optional_str(payload.get("event_start_at")),
        event_end_at=_optional_str(payload.get("event_end_at")),
    )


def _parse_json_dict(raw: object) -> dict[str, object] | None:
    if raw is None or not isinstance(raw, str):
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if payload is None or not isinstance(payload, dict):
        return None
    return payload


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
