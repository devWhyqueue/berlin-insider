from datetime import UTC, datetime
import json

import berlin_insider.cli as cli
from berlin_insider.scheduler.models import ScheduleRunResult, SchedulerState, SchedulerStatus


def _result(*, executed: bool, forced: bool, due: bool, status: SchedulerStatus) -> ScheduleRunResult:
    return ScheduleRunResult(
        executed=executed,
        forced=forced,
        due=due,
        reason="ok",
        status=status,
        exit_code=0,
        digest="Berlin Insider",
        state=SchedulerState(
            last_attempt_at=datetime(2026, 2, 27, 7, 0, tzinfo=UTC).isoformat(),
            last_run_date_local="2026-02-27",
            last_status=status,
            last_success_at=datetime(2026, 2, 27, 7, 1, tzinfo=UTC).isoformat(),
            last_error_message=None,
            last_digest_length=13,
            last_curated_count=7,
            last_failed_sources=[],
            last_source_status={},
        ),
        local_date="2026-02-27",
    )


def test_cli_schedule_default_invocation(monkeypatch, caplog) -> None:
    caplog.set_level("INFO")

    class _FakeScheduler:
        def run_once(self, **kwargs):  # noqa: ANN003, ANN202
            return _result(executed=False, forced=False, due=False, status=SchedulerStatus.SKIPPED)

    monkeypatch.setattr(cli, "Scheduler", _FakeScheduler)
    monkeypatch.setattr("sys.argv", ["berlin-insider", "schedule"])
    cli.main()

    assert "Schedule run: status=skipped" in caplog.text


def test_cli_schedule_force_passed(monkeypatch, caplog) -> None:
    caplog.set_level("INFO")

    class _FakeScheduler:
        force_value = False

        def run_once(self, **kwargs):  # noqa: ANN003, ANN202
            _FakeScheduler.force_value = kwargs["force"]
            return _result(executed=True, forced=True, due=False, status=SchedulerStatus.SUCCESS)

    monkeypatch.setattr(cli, "Scheduler", _FakeScheduler)
    monkeypatch.setattr("sys.argv", ["berlin-insider", "schedule", "--force"])
    cli.main()

    assert _FakeScheduler.force_value is True


def test_cli_schedule_json_schema(monkeypatch, caplog) -> None:
    caplog.set_level("INFO")

    class _FakeScheduler:
        def run_once(self, **kwargs):  # noqa: ANN003, ANN202
            return _result(executed=True, forced=False, due=True, status=SchedulerStatus.SUCCESS)

    monkeypatch.setattr(cli, "Scheduler", _FakeScheduler)
    monkeypatch.setattr("sys.argv", ["berlin-insider", "schedule", "--json"])
    cli.main()

    payload = json.loads(caplog.records[-1].message)
    assert set(payload.keys()) == {
        "executed",
        "forced",
        "due",
        "reason",
        "status",
        "exit_code",
        "digest",
        "state",
        "local_date",
    }
    assert payload["status"] == "success"

