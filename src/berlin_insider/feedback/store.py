from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from berlin_insider.digest import DigestKind
from berlin_insider.feedback.models import (
    FeedbackEvent,
    FeedbackVote,
    SentMessageRecord,
    TelegramUpdatesState,
)


class JsonFeedbackStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._events = self._load()

    def upsert(self, event: FeedbackEvent) -> None:
        """Insert or update a vote using (message_key, telegram_user_id) uniqueness."""
        key = (event.message_key, event.telegram_user_id)
        self._events[key] = event
        self._save()

    def count(self) -> int:
        """Return number of deduplicated feedback vote rows."""
        return len(self._events)

    def _load(self) -> dict[tuple[str, int], FeedbackEvent]:
        payload = _read_json(self._path)
        if not isinstance(payload, list):
            return {}
        events: dict[tuple[str, int], FeedbackEvent] = {}
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            event = _feedback_event_from_payload(entry)
            if event is None:
                continue
            events[(event.message_key, event.telegram_user_id)] = event
        return events

    def _save(self) -> None:
        rows = [asdict(event) for event in self._events.values()]
        rows.sort(key=lambda item: (item["message_key"], item["telegram_user_id"]))
        _atomic_write_json(self._path, rows)


class JsonTelegramUpdatesStateStore:
    def __init__(self, path: Path) -> None:
        self._path = path

    def load(self) -> TelegramUpdatesState:
        """Load Telegram updates offset state from JSON."""
        payload = _read_json(self._path)
        if not isinstance(payload, dict):
            return TelegramUpdatesState()
        value = payload.get("last_update_id")
        return TelegramUpdatesState(last_update_id=value if isinstance(value, int) else None)

    def save(self, state: TelegramUpdatesState) -> None:
        """Persist Telegram updates offset state atomically."""
        _atomic_write_json(self._path, asdict(state))


class JsonSentMessageStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._items = self._load()

    def upsert(self, record: SentMessageRecord) -> None:
        """Insert or replace metadata for a sent Telegram digest message."""
        self._items[record.message_key] = record
        self._save()

    def get(self, message_key: str) -> SentMessageRecord | None:
        """Return sent message metadata by message key, if present."""
        return self._items.get(message_key)

    def _load(self) -> dict[str, SentMessageRecord]:
        payload = _read_json(self._path)
        if not isinstance(payload, list):
            return {}
        out: dict[str, SentMessageRecord] = {}
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            record = _sent_record_from_payload(entry)
            if record is not None:
                out[record.message_key] = record
        return out

    def _save(self) -> None:
        rows = [asdict(item) for item in self._items.values()]
        rows.sort(key=lambda item: item["message_key"])
        _atomic_write_json(self._path, rows)


def _feedback_event_from_payload(payload: dict[str, object]) -> FeedbackEvent | None:
    try:
        digest_kind = DigestKind(str(payload.get("digest_kind")))
    except ValueError:
        return None
    vote_raw = str(payload.get("vote"))
    if vote_raw not in {"up", "down"}:
        return None
    vote: FeedbackVote = "up" if vote_raw == "up" else "down"
    user_id = payload.get("telegram_user_id")
    if not isinstance(user_id, int):
        return None
    message_key = payload.get("message_key")
    if not isinstance(message_key, str):
        return None
    chat_id = payload.get("chat_id")
    message_id = payload.get("message_id")
    voted_at = payload.get("voted_at")
    updated_at = payload.get("updated_at")
    if not isinstance(chat_id, str):
        return None
    if not isinstance(message_id, str):
        return None
    if not isinstance(voted_at, str):
        return None
    if not isinstance(updated_at, str):
        return None
    return FeedbackEvent(
        message_key=message_key,
        digest_kind=digest_kind,
        vote=vote,
        telegram_user_id=user_id,
        chat_id=chat_id,
        message_id=message_id,
        voted_at=voted_at,
        updated_at=updated_at,
    )


def _sent_record_from_payload(payload: dict[str, object]) -> SentMessageRecord | None:
    try:
        digest_kind = DigestKind(str(payload.get("digest_kind")))
    except ValueError:
        return None
    message_key = payload.get("message_key")
    local_date = payload.get("local_date")
    sent_at = payload.get("sent_at")
    telegram_message_id = payload.get("telegram_message_id")
    selected_urls = payload.get("selected_urls")
    if not isinstance(message_key, str):
        return None
    if not isinstance(local_date, str):
        return None
    if not isinstance(sent_at, str):
        return None
    if not isinstance(telegram_message_id, str):
        return None
    if not isinstance(selected_urls, list) or not all(
        isinstance(url, str) for url in selected_urls
    ):
        return None
    return SentMessageRecord(
        message_key=message_key,
        digest_kind=digest_kind,
        local_date=local_date,
        sent_at=sent_at,
        telegram_message_id=telegram_message_id,
        selected_urls=selected_urls,
    )


def _read_json(path: Path) -> object | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _atomic_write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(f"{path.suffix}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
