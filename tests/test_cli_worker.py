from __future__ import annotations

from pathlib import Path

import pytest

import berlin_insider.cli as cli


def test_cli_worker_runs_with_config(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeWorker:
        def __init__(self, *, config):  # noqa: ANN001
            captured["config"] = config

        def run(self) -> None:
            captured["run_called"] = True

    monkeypatch.setattr(cli, "Worker", _FakeWorker)
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
    cfg = captured["config"]
    assert cfg.db_path == Path(".data/test.db")
    assert cfg.webhook_public_base_url == "https://example.com"
    assert cfg.telegram_webhook_secret == "secret123"
    assert cfg.port == 9090


def test_cli_worker_requires_webhook_base_url(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        ["berlin-insider", "worker", "--telegram-webhook-secret", "secret123"],
    )
    with pytest.raises(SystemExit, match="WEBHOOK_PUBLIC_BASE_URL"):
        cli.main()


def test_cli_worker_requires_webhook_secret(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        ["berlin-insider", "worker", "--webhook-public-base-url", "https://example.com"],
    )
    with pytest.raises(SystemExit, match="TELEGRAM_WEBHOOK_SECRET"):
        cli.main()

