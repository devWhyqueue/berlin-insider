from __future__ import annotations

from datetime import UTC, datetime

import berlin_insider.cli as cli
from berlin_insider.feedback.models import FeedbackPollResult


def test_cli_feedback_runs_and_logs(monkeypatch, caplog) -> None:
    caplog.set_level("INFO")

    class _FakeMessenger:
        @classmethod
        def from_env(cls):  # noqa: ANN206
            return cls()

    def _fake_poll_feedback_once(**kwargs):  # noqa: ANN003, ANN202
        return FeedbackPollResult(
            fetched_updates=3,
            processed_callbacks=2,
            persisted_votes=2,
            ignored_updates=1,
            answered_callbacks=2,
            next_offset=11,
            finished_at=datetime(2026, 2, 28, 8, 0, tzinfo=UTC),
        )

    monkeypatch.setattr(cli, "TelegramMessenger", _FakeMessenger)
    monkeypatch.setattr(cli, "poll_feedback_once", _fake_poll_feedback_once)
    monkeypatch.setattr("sys.argv", ["berlin-insider", "feedback"])
    cli.main()
    assert "Feedback poll: fetched=3 processed=2 persisted=2" in caplog.text
