from datetime import UTC, datetime
from pathlib import Path

from berlin_insider.messenger.models import DeliveryResult, MessengerError
import berlin_insider.scheduler.orchestrator as scheduler_module
from berlin_insider.curator.models import CurateRunResult
from berlin_insider.fetcher.models import FetchRunResult
from berlin_insider.parser.models import ParseRunResult
from berlin_insider.pipeline import FullPipelineRunResult
from berlin_insider.scheduler.models import ScheduleConfig, SchedulerState, SchedulerStatus


class _MemoryStateStore:
    def __init__(self, state: SchedulerState | None = None) -> None:
        self.state = state or SchedulerState()

    def load(self) -> SchedulerState:
        return self.state

    def save(self, state: SchedulerState) -> None:
        self.state = state


class _FakeMessenger:
    def send_digest(self, *, text: str) -> DeliveryResult:
        return DeliveryResult(
            delivered_at=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
            external_message_id="42",
        )


def _empty_fetch() -> FetchRunResult:
    return FetchRunResult(
        started_at=datetime(2026, 2, 27, 7, 0, tzinfo=UTC),
        finished_at=datetime(2026, 2, 27, 7, 1, tzinfo=UTC),
        results=[],
        total_items=0,
        failed_sources=[],
    )


def _empty_parse() -> ParseRunResult:
    return ParseRunResult(
        started_at=datetime(2026, 2, 27, 7, 1, tzinfo=UTC),
        finished_at=datetime(2026, 2, 27, 7, 1, tzinfo=UTC),
        results=[],
        total_items=0,
        failed_sources=[],
    )


def _empty_curate() -> CurateRunResult:
    return CurateRunResult(
        started_at=datetime(2026, 2, 27, 7, 1, tzinfo=UTC),
        finished_at=datetime(2026, 2, 27, 7, 1, tzinfo=UTC),
        results=[],
        selected_items=[],
        dropped_count=0,
        failed_sources=[],
        target_count=7,
        actual_count=0,
        category_counts={},
        warnings=[],
    )


def test_scheduler_skips_when_not_due() -> None:
    store = _MemoryStateStore()
    result = scheduler_module.Scheduler().run_once(
        state_store=store,  # type: ignore[arg-type]
        config=ScheduleConfig(timezone="UTC", weekday="friday", hour=8, minute=0),
        sent_store_path=Path(".data/sent_links.json"),
        target_items=7,
        force=False,
        now_utc=datetime(2026, 2, 27, 7, 59, tzinfo=UTC),
    )
    assert result.executed is False
    assert result.status == SchedulerStatus.SKIPPED
    assert result.exit_code == 0
    assert store.state.last_status == SchedulerStatus.SKIPPED


def test_scheduler_executes_due_run_success(monkeypatch) -> None:
    def _fake_run_full_pipeline(**kwargs):  # noqa: ANN003, ANN202
        return FullPipelineRunResult(
            fetch_result=_empty_fetch(),
            parse_result=_empty_parse(),
            curate_result=_empty_curate(),
            digest="Berlin Insider",
        )

    monkeypatch.setattr(scheduler_module, "run_full_pipeline", _fake_run_full_pipeline)
    store = _MemoryStateStore()
    result = scheduler_module.Scheduler().run_once(
        state_store=store,  # type: ignore[arg-type]
        config=ScheduleConfig(timezone="UTC", weekday="friday", hour=8, minute=0),
        sent_store_path=Path(".data/sent_links.json"),
        target_items=7,
        force=False,
        now_utc=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        messenger=_FakeMessenger(),
    )
    assert result.executed is True
    assert result.status == SchedulerStatus.SUCCESS
    assert result.exit_code == 0
    assert result.delivered is True
    assert result.digest == "Berlin Insider"
    assert store.state.last_run_date_local == "2026-02-27"
    assert store.state.last_delivery_message_id == "42"


def test_scheduler_returns_error_when_pipeline_raises(monkeypatch) -> None:
    def _boom(**kwargs):  # noqa: ANN003, ANN202
        raise RuntimeError("boom")

    monkeypatch.setattr(scheduler_module, "run_full_pipeline", _boom)
    store = _MemoryStateStore()
    result = scheduler_module.Scheduler().run_once(
        state_store=store,  # type: ignore[arg-type]
        config=ScheduleConfig(timezone="UTC", weekday="friday", hour=8, minute=0),
        sent_store_path=Path(".data/sent_links.json"),
        target_items=7,
        force=True,
        now_utc=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
    )
    assert result.executed is True
    assert result.status == SchedulerStatus.ERROR
    assert result.exit_code == 1
    assert result.delivered is False
    assert store.state.last_error_message == "boom"


def test_scheduler_returns_error_when_delivery_fails(monkeypatch) -> None:
    class _FailingMessenger:
        def send_digest(self, *, text: str) -> DeliveryResult:
            raise MessengerError("forbidden")

    def _fake_run_full_pipeline(**kwargs):  # noqa: ANN003, ANN202
        return FullPipelineRunResult(
            fetch_result=_empty_fetch(),
            parse_result=_empty_parse(),
            curate_result=_empty_curate(),
            digest="Berlin Insider",
        )

    monkeypatch.setattr(scheduler_module, "run_full_pipeline", _fake_run_full_pipeline)
    store = _MemoryStateStore()
    result = scheduler_module.Scheduler().run_once(
        state_store=store,  # type: ignore[arg-type]
        config=ScheduleConfig(timezone="UTC", weekday="friday", hour=8, minute=0),
        sent_store_path=Path(".data/sent_links.json"),
        target_items=7,
        force=True,
        now_utc=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        messenger=_FailingMessenger(),
    )
    assert result.executed is True
    assert result.status == SchedulerStatus.ERROR
    assert result.exit_code == 1
    assert result.delivered is False
    assert result.reason == "delivery failed"
    assert store.state.last_delivery_error == "forbidden"
