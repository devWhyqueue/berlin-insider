from __future__ import annotations

from pathlib import Path

import berlin_insider.worker as worker_module
from berlin_insider.scheduler.models import ScheduleConfig, SchedulerState, SchedulerStatus


class _FakeMessenger:
    def __init__(self) -> None:
        self.webhook_urls: list[str] = []
        self.webhook_cert_paths: list[object] = []
        self.webhook_ips: list[object] = []

    def set_webhook(self, *, url: str, certificate_path=None, ip_address=None) -> None:  # noqa: ANN001
        self.webhook_urls.append(url)
        self.webhook_cert_paths.append(certificate_path)
        self.webhook_ips.append(ip_address)

    def send_digest(self, *, text: str, feedback_metadata=None):  # noqa: ANN001, ANN201
        raise AssertionError("send_digest should not be called in this test")

    def answer_callback_query(self, *, callback_query_id: str) -> None:  # noqa: ARG002
        return


class _FakeScheduler:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def run_once(self, **kwargs):  # noqa: ANN003, ANN201
        self.calls.append(kwargs)
        return type(
            "_Result",
            (),
            {
                "status": SchedulerStatus.SKIPPED,
                "due": False,
                "executed": False,
                "delivered": False,
                "reason": "not due",
                "state": SchedulerState(),
            },
        )()


class _FakeBackgroundScheduler:
    def __init__(self) -> None:
        self.started = False
        self.shutdown_called = False
        self.jobs: list[dict[str, object]] = []

    def add_job(self, func, **kwargs):  # noqa: ANN001
        self.jobs.append({"func": func, **kwargs})

    def start(self) -> None:
        self.started = True

    def shutdown(self, *, wait: bool = False) -> None:  # noqa: ARG002
        self.shutdown_called = True


def test_worker_registers_webhook_and_runs_startup_cycle(monkeypatch, tmp_path: Path) -> None:
    fake_bg = _FakeBackgroundScheduler()
    cert_path = tmp_path / "webhook.crt"
    cert_path.write_text("dummy", encoding="utf-8")

    def _fake_build_scheduler(**kwargs):  # noqa: ANN003, ANN202
        return fake_bg

    monkeypatch.setattr(worker_module, "_build_scheduler", _fake_build_scheduler)
    monkeypatch.setattr(worker_module.uvicorn, "run", lambda *args, **kwargs: None)
    fake_scheduler = _FakeScheduler()
    fake_messenger = _FakeMessenger()
    worker = worker_module.Worker(
        config=worker_module.WorkerConfig(
            db_path=tmp_path / "berlin_insider.db",
            target_items=7,
            schedule=ScheduleConfig(timezone="UTC"),
            host="127.0.0.1",
            port=8080,
            webhook_public_base_url="https://example.com",
            telegram_webhook_secret="secret123",
            telegram_webhook_cert_path=cert_path,
        ),
        scheduler=fake_scheduler,  # type: ignore[arg-type]
        messenger=fake_messenger,  # type: ignore[arg-type]
    )

    worker.run()

    assert fake_messenger.webhook_urls == ["https://example.com/telegram/webhook/secret123"]
    assert fake_messenger.webhook_cert_paths == [cert_path]
    assert fake_messenger.webhook_ips == [None]
    assert len(fake_scheduler.calls) == 1
    assert fake_scheduler.calls[0]["force"] is False
    assert fake_bg.started is True
    assert fake_bg.shutdown_called is True


def test_worker_overlap_skips_cycle(tmp_path: Path) -> None:
    fake_scheduler = _FakeScheduler()
    worker = worker_module.Worker(
        config=worker_module.WorkerConfig(
            db_path=tmp_path / "berlin_insider.db",
            target_items=7,
            schedule=ScheduleConfig(timezone="UTC"),
            host="127.0.0.1",
            port=8080,
            webhook_public_base_url="https://example.com",
            telegram_webhook_secret="secret123",
        ),
        scheduler=fake_scheduler,  # type: ignore[arg-type]
        messenger=_FakeMessenger(),  # type: ignore[arg-type]
    )
    state_store = worker_module.SqliteSchedulerStateStore(tmp_path / "berlin_insider.db")
    sent_store = worker_module.SqliteMessageDeliveryStore(tmp_path / "berlin_insider.db")

    acquired = worker._run_lock.acquire(blocking=False)
    assert acquired is True
    try:
        worker._try_run_cycle(
            reason="test overlap", state_store=state_store, sent_message_store=sent_store
        )
    finally:
        worker._run_lock.release()

    assert len(fake_scheduler.calls) == 0


def test_build_scheduler_creates_daily_and_weekend_jobs(tmp_path: Path) -> None:
    worker = worker_module.Worker(
        config=worker_module.WorkerConfig(
            db_path=tmp_path / "berlin_insider.db",
            target_items=7,
            schedule=ScheduleConfig(
                timezone="UTC",
                weekend_weekday="friday",
                daily_hour=8,
                daily_minute=0,
                weekend_hour=9,
                weekend_minute=30,
            ),
            host="127.0.0.1",
            port=8080,
            webhook_public_base_url="https://example.com",
            telegram_webhook_secret="secret123",
        ),
        scheduler=_FakeScheduler(),  # type: ignore[arg-type]
        messenger=_FakeMessenger(),  # type: ignore[arg-type]
    )

    scheduler = worker_module._build_scheduler(
        worker=worker,
        state_store=worker_module.SqliteSchedulerStateStore(tmp_path / "berlin_insider.db"),
        sent_message_store=worker_module.SqliteMessageDeliveryStore(tmp_path / "berlin_insider.db"),
    )
    jobs = scheduler.get_jobs()

    assert len(jobs) == 2
    assert {job.id for job in jobs} == {"daily-digest", "weekend-digest"}


def test_worker_passes_configured_webhook_ip(monkeypatch, tmp_path: Path) -> None:
    fake_bg = _FakeBackgroundScheduler()

    def _fake_build_scheduler(**kwargs):  # noqa: ANN003, ANN202
        return fake_bg

    monkeypatch.setattr(worker_module, "_build_scheduler", _fake_build_scheduler)
    monkeypatch.setattr(worker_module.uvicorn, "run", lambda *args, **kwargs: None)
    fake_scheduler = _FakeScheduler()
    fake_messenger = _FakeMessenger()
    worker = worker_module.Worker(
        config=worker_module.WorkerConfig(
            db_path=tmp_path / "berlin_insider.db",
            target_items=7,
            schedule=ScheduleConfig(timezone="UTC"),
            host="127.0.0.1",
            port=8080,
            webhook_public_base_url="https://example.com",
            telegram_webhook_secret="secret123",
            telegram_webhook_ip="203.0.113.10",
        ),
        scheduler=fake_scheduler,  # type: ignore[arg-type]
        messenger=fake_messenger,  # type: ignore[arg-type]
    )

    worker.run()

    assert fake_messenger.webhook_ips == ["203.0.113.10"]
