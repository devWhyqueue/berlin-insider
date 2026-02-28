import json
from pathlib import Path

from berlin_insider.scheduler.models import SchedulerState, SchedulerStatus
from berlin_insider.scheduler.store import JsonSchedulerStateStore


def test_scheduler_store_returns_default_for_missing_file(tmp_path: Path) -> None:
    store = JsonSchedulerStateStore(tmp_path / "missing.json")
    state = store.load()
    assert state == SchedulerState()


def test_scheduler_store_handles_corrupt_json(tmp_path: Path) -> None:
    path = tmp_path / "scheduler_state.json"
    path.write_text("{not json", encoding="utf-8")
    state = JsonSchedulerStateStore(path).load()
    assert state == SchedulerState()


def test_scheduler_store_persists_state_atomically(tmp_path: Path) -> None:
    path = tmp_path / "scheduler_state.json"
    store = JsonSchedulerStateStore(path)
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
    )

    store.save(state)

    assert path.exists()
    assert (tmp_path / "scheduler_state.json.tmp").exists() is False
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["last_status"] == "success"
    reloaded = store.load()
    assert reloaded.last_run_date_local == "2026-02-27"
    assert reloaded.last_status == SchedulerStatus.SUCCESS
    assert reloaded.last_digest_length == 123

