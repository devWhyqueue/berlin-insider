from pathlib import Path

from berlin_insider.scheduler.models import SchedulerState, SchedulerStatus
from berlin_insider.scheduler.store import SqliteSchedulerStateStore


def test_scheduler_store_returns_default_for_empty_db(tmp_path: Path) -> None:
    store = SqliteSchedulerStateStore(tmp_path / "berlin_insider.db")
    state = store.load()
    assert state == SchedulerState()


def test_scheduler_store_persists_state(tmp_path: Path) -> None:
    store = SqliteSchedulerStateStore(tmp_path / "berlin_insider.db")
    state = SchedulerState(
        last_attempt_at="2026-02-27T07:00:00+00:00",
        last_run_date_local="2026-02-27",
        last_status=SchedulerStatus.SUCCESS,
        last_success_at="2026-02-27T07:00:02+00:00",
        last_error_message=None,
        last_digest_length=123,
        last_curated_count=7,
        last_failed_sources=["tip_berlin_home"],
        last_source_status={"mitvergnuegen": "success"},
        last_delivery_at="2026-02-27T07:00:03+00:00",
        last_delivery_message_id="42",
        last_delivery_error=None,
        last_run_date_by_kind={"weekend": "2026-02-27"},
    )

    store.save(state)
    reloaded = store.load()
    assert reloaded.last_run_date_local == "2026-02-27"
    assert reloaded.last_status == SchedulerStatus.SUCCESS
    assert reloaded.last_digest_length == 123
    assert reloaded.last_delivery_message_id == "42"
    assert reloaded.last_run_date_by_kind["weekend"] == "2026-02-27"
