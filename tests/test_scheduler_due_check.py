from datetime import UTC, datetime

from berlin_insider.scheduler.models import ScheduleConfig, SchedulerState
from berlin_insider.scheduler.orchestrator import is_due


def test_due_check_not_due_before_time() -> None:
    due, reason, local_date = is_due(
        now_utc=datetime(2026, 2, 27, 7, 59, tzinfo=UTC),
        config=ScheduleConfig(timezone="UTC", weekday="friday", hour=8, minute=0),
        state=SchedulerState(),
    )
    assert due is False
    assert "time" in reason
    assert local_date == "2026-02-27"


def test_due_check_due_at_target_time() -> None:
    due, reason, local_date = is_due(
        now_utc=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        config=ScheduleConfig(timezone="UTC", weekday="friday", hour=8, minute=0),
        state=SchedulerState(),
    )
    assert due is True
    assert reason == "run is due"
    assert local_date == "2026-02-27"


def test_due_check_not_due_if_already_ran_today() -> None:
    due, reason, _ = is_due(
        now_utc=datetime(2026, 2, 27, 9, 0, tzinfo=UTC),
        config=ScheduleConfig(timezone="UTC", weekday="friday", hour=8, minute=0),
        state=SchedulerState(last_run_date_local="2026-02-27"),
    )
    assert due is False
    assert "already ran" in reason


def test_due_check_not_due_on_saturday_no_catch_up() -> None:
    due, reason, local_date = is_due(
        now_utc=datetime(2026, 2, 28, 8, 0, tzinfo=UTC),
        config=ScheduleConfig(timezone="UTC", weekday="friday", hour=8, minute=0),
        state=SchedulerState(),
    )
    assert due is False
    assert "not the configured weekday" in reason
    assert local_date == "2026-02-28"
