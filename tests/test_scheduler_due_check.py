from datetime import UTC, datetime

from berlin_insider.digest import DigestKind
from berlin_insider.scheduler.models import ScheduleConfig, SchedulerState
from berlin_insider.scheduler.orchestrator import is_due


def test_due_check_daily_not_due_before_time() -> None:
    due, reason, local_date, digest_kind = is_due(
        now_utc=datetime(2026, 2, 23, 7, 59, tzinfo=UTC),
        config=ScheduleConfig(timezone="UTC"),
        state=SchedulerState(),
    )
    assert due is False
    assert "time" in reason
    assert local_date == "2026-02-23"
    assert digest_kind == DigestKind.DAILY


def test_due_check_daily_due_at_target_time() -> None:
    due, reason, local_date, digest_kind = is_due(
        now_utc=datetime(2026, 2, 23, 8, 0, tzinfo=UTC),
        config=ScheduleConfig(timezone="UTC"),
        state=SchedulerState(),
    )
    assert due is True
    assert reason == "run is due"
    assert local_date == "2026-02-23"
    assert digest_kind == DigestKind.DAILY


def test_due_check_weekend_not_due_before_time() -> None:
    due, reason, local_date, digest_kind = is_due(
        now_utc=datetime(2026, 2, 27, 7, 59, tzinfo=UTC),
        config=ScheduleConfig(timezone="UTC", weekend_weekday="friday", weekend_hour=8, weekend_minute=0),
        state=SchedulerState(),
    )
    assert due is False
    assert "time" in reason
    assert local_date == "2026-02-27"
    assert digest_kind == DigestKind.WEEKEND


def test_due_check_weekend_due_at_target_time() -> None:
    due, reason, local_date, digest_kind = is_due(
        now_utc=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        config=ScheduleConfig(timezone="UTC"),
        state=SchedulerState(),
    )
    assert due is True
    assert reason == "run is due"
    assert local_date == "2026-02-27"
    assert digest_kind == DigestKind.WEEKEND


def test_due_check_not_due_if_already_ran_today_for_kind() -> None:
    due, reason, _, digest_kind = is_due(
        now_utc=datetime(2026, 2, 27, 9, 0, tzinfo=UTC),
        config=ScheduleConfig(timezone="UTC", weekend_weekday="friday", weekend_hour=8, weekend_minute=0),
        state=SchedulerState(last_run_date_by_kind={"weekend": "2026-02-27"}),
    )
    assert due is False
    assert "already ran" in reason
    assert digest_kind == DigestKind.WEEKEND


def test_due_check_daily_due_on_saturday() -> None:
    due, reason, local_date, digest_kind = is_due(
        now_utc=datetime(2026, 2, 28, 8, 0, tzinfo=UTC),
        config=ScheduleConfig(timezone="UTC"),
        state=SchedulerState(),
    )
    assert due is True
    assert reason == "run is due"
    assert local_date == "2026-02-28"
    assert digest_kind == DigestKind.DAILY
