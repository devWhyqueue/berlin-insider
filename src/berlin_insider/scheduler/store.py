from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from berlin_insider.scheduler.models import SchedulerState, SchedulerStatus


class JsonSchedulerStateStore:
    def __init__(self, path: Path) -> None:
        self._path = path

    def load(self) -> SchedulerState:
        """Load persisted scheduler state; return defaults on read/parse errors."""
        payload = _read_payload(self._path)
        if payload is None:
            return SchedulerState()
        return _state_from_payload(payload)

    def save(self, state: SchedulerState) -> None:
        """Persist scheduler state atomically to disk."""
        payload = asdict(state)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self._path.with_suffix(f"{self._path.suffix}.tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(self._path)


def _opt_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _opt_int(value: object) -> int | None:
    return value if isinstance(value, int) else None


def _str_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _str_map(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {
        key: item for key, item in value.items() if isinstance(key, str) and isinstance(item, str)
    }


def _read_payload(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _state_from_payload(payload: dict[str, object]) -> SchedulerState:
    raw_status = payload.get("last_status")
    parsed_status: SchedulerStatus | None = None
    if isinstance(raw_status, str):
        try:
            parsed_status = SchedulerStatus(raw_status)
        except ValueError:
            parsed_status = None
    return SchedulerState(
        last_attempt_at=_opt_str(payload.get("last_attempt_at")),
        last_run_date_local=_opt_str(payload.get("last_run_date_local")),
        last_status=parsed_status,
        last_success_at=_opt_str(payload.get("last_success_at")),
        last_error_message=_opt_str(payload.get("last_error_message")),
        last_digest_length=_opt_int(payload.get("last_digest_length")),
        last_curated_count=_opt_int(payload.get("last_curated_count")),
        last_failed_sources=_str_list(payload.get("last_failed_sources")),
        last_source_status=_str_map(payload.get("last_source_status")),
        last_delivery_at=_opt_str(payload.get("last_delivery_at")),
        last_delivery_message_id=_opt_str(payload.get("last_delivery_message_id")),
        last_delivery_error=_opt_str(payload.get("last_delivery_error")),
        last_run_date_by_kind=_run_dates_by_kind(
            payload.get("last_run_date_by_kind"), legacy=payload.get("last_run_date_local")
        ),
    )


def _run_dates_by_kind(value: object, *, legacy: object) -> dict[str, str]:
    parsed = _str_map(value)
    if parsed:
        return parsed
    if isinstance(legacy, str):
        return {"weekend": legacy}
    return {}
