from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

import berlin_insider.cli as cli
from berlin_insider.digest import DigestKind
from berlin_insider.scheduler.models import SchedulerState, SchedulerStatus, ScheduleRunResult
from berlin_insider.cli_parser import build_parser


def test_cli_worker_runs_with_config(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class _FakeWorker:
        def __init__(self, *, config):  # noqa: ANN001
            captured["config"] = config

        def run(self) -> None:
            captured["run_called"] = True

    monkeypatch.setattr(cli, "Worker", _FakeWorker)
    monkeypatch.setattr(cli, "_load_dotenv_defaults", lambda path=None: None)
    monkeypatch.setattr(
        "sys.argv",
        [
            "berlin-insider",
            "worker",
            "--db-path",
            ".data/test.db",
            "--webhook-public-base-url",
            "https://example.com",
            "--telegram-webhook-secret",
            "secret123",
            "--port",
            "9090",
        ],
    )

    cli.main()

    assert captured["run_called"] is True
    cfg = cast(cli.WorkerConfig, captured["config"])
    assert cfg.db_path == Path(".data/test.db")
    assert cfg.webhook_public_base_url == "https://example.com"
    assert cfg.telegram_webhook_secret == "secret123"
    assert cfg.port == 9090
    assert cfg.telegram_webhook_cert_path == Path("/etc/nginx/ssl/berlin-insider.crt")
    assert cfg.telegram_webhook_ip is None


def test_cli_worker_requires_webhook_base_url(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_load_dotenv_defaults", lambda path=None: None)
    monkeypatch.delenv("WEBHOOK_PUBLIC_BASE_URL", raising=False)
    monkeypatch.setattr(
        "sys.argv",
        ["berlin-insider", "worker", "--telegram-webhook-secret", "secret123"],
    )
    with pytest.raises(SystemExit, match="WEBHOOK_PUBLIC_BASE_URL"):
        cli.main()


def test_cli_worker_requires_webhook_secret(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_load_dotenv_defaults", lambda path=None: None)
    monkeypatch.delenv("TELEGRAM_WEBHOOK_SECRET", raising=False)
    monkeypatch.setattr(
        "sys.argv",
        ["berlin-insider", "worker", "--webhook-public-base-url", "https://example.com"],
    )
    with pytest.raises(SystemExit, match="TELEGRAM_WEBHOOK_SECRET"):
        cli.main()


def test_cli_worker_accepts_webhook_ip(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class _FakeWorker:
        def __init__(self, *, config):  # noqa: ANN001
            captured["config"] = config

        def run(self) -> None:
            captured["run_called"] = True

    monkeypatch.setattr(cli, "Worker", _FakeWorker)
    monkeypatch.setattr(cli, "_load_dotenv_defaults", lambda path=None: None)
    monkeypatch.setattr(
        "sys.argv",
        [
            "berlin-insider",
            "worker",
            "--webhook-public-base-url",
            "https://example.com",
            "--telegram-webhook-secret",
            "secret123",
            "--telegram-webhook-ip",
            "203.0.113.10",
        ],
    )

    cli.main()

    assert captured["run_called"] is True
    cfg = cast(cli.WorkerConfig, captured["config"])
    assert cfg.telegram_webhook_ip == "203.0.113.10"


def test_cli_worker_run_once_executes_scheduler_force(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class _FakeScheduler:
        def run_once(self, **kwargs):  # noqa: ANN003, ANN202
            captured["kwargs"] = kwargs
            return ScheduleRunResult(
                executed=True,
                forced=True,
                due=False,
                reason="forced run",
                status=SchedulerStatus.SUCCESS,
                exit_code=0,
                delivered=True,
                digest="Berlin Insider",
                state=SchedulerState(last_status=SchedulerStatus.SUCCESS),
                local_date="2026-03-02",
                digest_kind=DigestKind.DAILY,
                message_key="daily:2026-03-02",
            )

    class _FakeTelegramMessenger:
        @staticmethod
        def from_env():  # noqa: ANN205
            return object()

    class _FailWorkerInit:
        def __init__(self, **kwargs):  # noqa: ANN003
            raise AssertionError("Worker runtime must not start for --run-once")

    monkeypatch.setattr(cli, "Scheduler", _FakeScheduler)
    monkeypatch.setattr(cli, "TelegramMessenger", _FakeTelegramMessenger)
    monkeypatch.setattr(cli, "Worker", _FailWorkerInit)
    monkeypatch.setattr(cli, "_load_dotenv_defaults", lambda path=None: None)
    monkeypatch.setattr(
        "sys.argv",
        [
            "berlin-insider",
            "worker",
            "--run-once",
            "--db-path",
            ".data/test.db",
            "--timezone",
            "UTC",
            "--weekend-weekday",
            "sunday",
        ],
    )

    with pytest.raises(SystemExit) as exc:
        cli.main()

    assert exc.value.code == 0
    kwargs = cast(dict[str, Any], captured["kwargs"])
    assert kwargs["force"] is True
    assert kwargs["db_path"] == Path(".data/test.db")
    config = cast(cli.ScheduleConfig, kwargs["config"])
    assert config.timezone == "UTC"
    assert config.weekend_weekday == "sunday"


def test_cli_worker_run_once_does_not_require_webhook_values(monkeypatch) -> None:
    class _FakeScheduler:
        def run_once(self, **kwargs):  # noqa: ANN003, ANN202
            return ScheduleRunResult(
                executed=False,
                forced=True,
                due=False,
                reason="forced no-op",
                status=SchedulerStatus.SKIPPED,
                exit_code=0,
                delivered=False,
                digest=None,
                state=SchedulerState(last_status=SchedulerStatus.SKIPPED),
                local_date="2026-03-02",
                digest_kind=None,
                message_key=None,
            )

    class _FakeTelegramMessenger:
        @staticmethod
        def from_env():  # noqa: ANN205
            return object()

    monkeypatch.setattr(cli, "Scheduler", _FakeScheduler)
    monkeypatch.setattr(cli, "TelegramMessenger", _FakeTelegramMessenger)
    monkeypatch.setattr(cli, "_load_dotenv_defaults", lambda path=None: None)
    monkeypatch.delenv("WEBHOOK_PUBLIC_BASE_URL", raising=False)
    monkeypatch.delenv("TELEGRAM_WEBHOOK_SECRET", raising=False)
    monkeypatch.setattr("sys.argv", ["berlin-insider", "worker", "--run-once"])

    with pytest.raises(SystemExit) as exc:
        cli.main()

    assert exc.value.code == 0


def test_cli_worker_run_once_exits_with_scheduler_exit_code(monkeypatch) -> None:
    class _FakeScheduler:
        def run_once(self, **kwargs):  # noqa: ANN003, ANN202
            return ScheduleRunResult(
                executed=True,
                forced=True,
                due=True,
                reason="delivery failed",
                status=SchedulerStatus.ERROR,
                exit_code=1,
                delivered=False,
                digest=None,
                state=SchedulerState(last_status=SchedulerStatus.ERROR),
                local_date="2026-03-02",
                digest_kind=DigestKind.DAILY,
                message_key=None,
            )

    class _FakeTelegramMessenger:
        @staticmethod
        def from_env():  # noqa: ANN205
            return object()

    monkeypatch.setattr(cli, "Scheduler", _FakeScheduler)
    monkeypatch.setattr(cli, "TelegramMessenger", _FakeTelegramMessenger)
    monkeypatch.setattr(cli, "_load_dotenv_defaults", lambda path=None: None)
    monkeypatch.setattr("sys.argv", ["berlin-insider", "worker", "--run-once"])

    with pytest.raises(SystemExit) as exc:
        cli.main()

    assert exc.value.code == 1


def test_worker_parser_run_once_flag_defaults_and_parses() -> None:
    parser = build_parser()

    args_default = parser.parse_args(["worker"])
    args_true = parser.parse_args(["worker", "--run-once"])

    assert args_default.run_once is False
    assert args_true.run_once is True
