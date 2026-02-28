from __future__ import annotations

import json
from pathlib import Path

from berlin_insider.scheduler.models import SchedulerState, SchedulerStatus
from berlin_insider.storage.sqlite import ensure_schema, sqlite_connection

_SELECT_SCHEDULER_STATE_SQL = """
SELECT
    last_attempt_at,
    last_run_date_local,
    last_status,
    last_success_at,
    last_error_message,
    last_digest_length,
    last_curated_count,
    last_failed_sources_json,
    last_source_status_json,
    last_delivery_at,
    last_delivery_message_id,
    last_delivery_error,
    last_run_date_by_kind_json
FROM scheduler_state
WHERE id = 1
"""

_UPSERT_SCHEDULER_STATE_SQL = """
INSERT INTO scheduler_state (
    id,
    last_attempt_at,
    last_run_date_local,
    last_status,
    last_success_at,
    last_error_message,
    last_digest_length,
    last_curated_count,
    last_failed_sources_json,
    last_source_status_json,
    last_delivery_at,
    last_delivery_message_id,
    last_delivery_error,
    last_run_date_by_kind_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    last_attempt_at = excluded.last_attempt_at,
    last_run_date_local = excluded.last_run_date_local,
    last_status = excluded.last_status,
    last_success_at = excluded.last_success_at,
    last_error_message = excluded.last_error_message,
    last_digest_length = excluded.last_digest_length,
    last_curated_count = excluded.last_curated_count,
    last_failed_sources_json = excluded.last_failed_sources_json,
    last_source_status_json = excluded.last_source_status_json,
    last_delivery_at = excluded.last_delivery_at,
    last_delivery_message_id = excluded.last_delivery_message_id,
    last_delivery_error = excluded.last_delivery_error,
    last_run_date_by_kind_json = excluded.last_run_date_by_kind_json
"""


class SqliteSchedulerStateStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        ensure_schema(self._db_path)

    def load(self) -> SchedulerState:
        """Load persisted scheduler state; return defaults when row is absent."""
        with sqlite_connection(self._db_path) as conn:
            row = conn.execute(_SELECT_SCHEDULER_STATE_SQL).fetchone()
        if row is None:
            return SchedulerState()
        return _state_from_row(row)

    def save(self, state: SchedulerState) -> None:
        """Persist scheduler state."""
        with sqlite_connection(self._db_path) as conn:
            conn.execute(_UPSERT_SCHEDULER_STATE_SQL, _state_payload(state))
            conn.commit()


def _state_from_row(row) -> SchedulerState:  # noqa: ANN001
    (
        last_attempt_at,
        last_run_date_local,
        raw_status,
        last_success_at,
        last_error_message,
        last_digest_length,
        last_curated_count,
        last_failed_sources_json,
        last_source_status_json,
        last_delivery_at,
        last_delivery_message_id,
        last_delivery_error,
        last_run_date_by_kind_json,
    ) = row
    parsed_status: SchedulerStatus | None = None
    if isinstance(raw_status, str):
        try:
            parsed_status = SchedulerStatus(raw_status)
        except ValueError:
            parsed_status = None
    return SchedulerState(
        last_attempt_at=_opt_str(last_attempt_at),
        last_run_date_local=_opt_str(last_run_date_local),
        last_status=parsed_status,
        last_success_at=_opt_str(last_success_at),
        last_error_message=_opt_str(last_error_message),
        last_digest_length=_opt_int(last_digest_length),
        last_curated_count=_opt_int(last_curated_count),
        last_failed_sources=_str_list_from_json(last_failed_sources_json),
        last_source_status=_str_map_from_json(last_source_status_json),
        last_delivery_at=_opt_str(last_delivery_at),
        last_delivery_message_id=_opt_str(last_delivery_message_id),
        last_delivery_error=_opt_str(last_delivery_error),
        last_run_date_by_kind=_str_map_from_json(last_run_date_by_kind_json),
    )


def _opt_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _opt_int(value: object) -> int | None:
    return value if isinstance(value, int) else None


def _str_list_from_json(value: object) -> list[str]:
    if not isinstance(value, str):
        return []
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, str)]


def _str_map_from_json(value: object) -> dict[str, str]:
    if not isinstance(value, str):
        return {}
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        key: item for key, item in payload.items() if isinstance(key, str) and isinstance(item, str)
    }


def _state_payload(
    state: SchedulerState,
) -> tuple[
    int,
    str | None,
    str | None,
    str | None,
    str | None,
    str | None,
    int | None,
    int | None,
    str,
    str,
    str | None,
    str | None,
    str | None,
    str,
]:
    return (
        1,
        state.last_attempt_at,
        state.last_run_date_local,
        state.last_status.value if state.last_status is not None else None,
        state.last_success_at,
        state.last_error_message,
        state.last_digest_length,
        state.last_curated_count,
        json.dumps(state.last_failed_sources, ensure_ascii=False),
        json.dumps(state.last_source_status, ensure_ascii=False),
        state.last_delivery_at,
        state.last_delivery_message_id,
        state.last_delivery_error,
        json.dumps(state.last_run_date_by_kind, ensure_ascii=False),
    )
