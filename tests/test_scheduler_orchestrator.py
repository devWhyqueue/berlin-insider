from datetime import UTC, datetime
from pathlib import Path

from berlin_insider.curator.models import (
    CuratedItem,
    CurateRunResult,
    CurateStatus,
    DroppedItem,
    DropReason,
    SourceCurateResult,
)
from berlin_insider.digest import DigestKind
from berlin_insider.fetcher.models import SourceId
from berlin_insider.messenger.models import DeliveryResult, MessengerError
import berlin_insider.scheduler.orchestrator as scheduler_module
from berlin_insider.feedback.store import SqliteSentMessageStore
from berlin_insider.fetcher.models import FetchRunResult
from berlin_insider.parser.models import ParsedCategory, ParsedItem, ParseRunResult, WeekendRelevance
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
    def __init__(self) -> None:
        self.feedback_metadata = None

    def send_digest(self, *, text: str, feedback_metadata=None) -> DeliveryResult:  # noqa: ANN001
        self.feedback_metadata = feedback_metadata
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


def _parsed_item(url: str) -> ParsedItem:
    return ParsedItem(
        source_id=SourceId.TIP_BERLIN_HOME,
        item_url=url,
        title="Title",
        description=None,
        event_start_at=datetime(2026, 2, 27, 12, 0, tzinfo=UTC),
        event_end_at=None,
        location=None,
        category=ParsedCategory.EVENT,
        category_confidence=0.9,
        weekend_relevance=WeekendRelevance.LIKELY_THIS_WEEKEND,
        weekend_confidence=0.9,
    )


def _daily_curate_with_alternative() -> CurateRunResult:
    return CurateRunResult(
        started_at=datetime(2026, 2, 27, 7, 1, tzinfo=UTC),
        finished_at=datetime(2026, 2, 27, 7, 1, tzinfo=UTC),
        results=[
            SourceCurateResult(
                source_id=SourceId.TIP_BERLIN_HOME,
                status=CurateStatus.PARTIAL,
                selected_items=[],
                dropped_items=[
                    DroppedItem(item=_parsed_item("https://example.com/alternative"), reason=DropReason.LOW_SCORE)
                ],
                warnings=[],
                error_message=None,
                duration_ms=1,
            )
        ],
        selected_items=[CuratedItem(item=_parsed_item("https://example.com/primary"), score=9.0)],
        dropped_count=1,
        failed_sources=[],
        target_count=1,
        actual_count=1,
        category_counts={ParsedCategory.EVENT: 1},
        warnings=["Daily selection mode active"],
    )


def test_scheduler_skips_when_not_due(tmp_path: Path) -> None:
    store = _MemoryStateStore()
    result = scheduler_module.Scheduler().run_once(
        state_store=store,  # type: ignore[arg-type]
        config=ScheduleConfig(timezone="UTC"),
        db_path=tmp_path / "berlin_insider.db",
        target_items=7,
        force=False,
        now_utc=datetime(2026, 2, 28, 7, 59, tzinfo=UTC),
    )
    assert result.executed is False
    assert result.status == SchedulerStatus.SKIPPED
    assert result.exit_code == 0
    assert store.state.last_status == SchedulerStatus.SKIPPED


def test_scheduler_executes_due_run_success_daily(monkeypatch, tmp_path: Path) -> None:
    def _fake_run_full_pipeline(**kwargs):  # noqa: ANN003, ANN202
        return FullPipelineRunResult(
            fetch_result=_empty_fetch(),
            parse_result=_empty_parse(),
            curate_result=_daily_curate_with_alternative(),
            digest="Berlin Insider",
            digest_kind=DigestKind.DAILY,
        )

    monkeypatch.setattr(scheduler_module, "run_full_pipeline", _fake_run_full_pipeline)
    store = _MemoryStateStore()
    messenger = _FakeMessenger()
    sent_message_store = SqliteSentMessageStore(tmp_path / "berlin_insider.db")
    result = scheduler_module.Scheduler().run_once(
        state_store=store,  # type: ignore[arg-type]
        config=ScheduleConfig(timezone="UTC"),
        db_path=tmp_path / "berlin_insider.db",
        target_items=7,
        force=False,
        now_utc=datetime(2026, 2, 23, 8, 0, tzinfo=UTC),
        messenger=messenger,
        sent_message_store=sent_message_store,
    )
    assert result.executed is True
    assert result.status == SchedulerStatus.SUCCESS
    assert result.exit_code == 0
    assert result.delivered is True
    assert result.digest == "Berlin Insider"
    assert store.state.last_run_date_local == "2026-02-23"
    assert store.state.last_run_date_by_kind["daily"] == "2026-02-23"
    assert result.digest_kind == DigestKind.DAILY
    assert store.state.last_delivery_message_id == "42"
    assert messenger.feedback_metadata is not None
    saved = sent_message_store.get(result.message_key or "")
    assert saved is not None
    assert saved.selected_urls == [
        "https://example.com/primary",
        "https://example.com/alternative",
    ]
    assert saved.alternative_item is not None
    assert saved.alternative_item.item_url == "https://example.com/alternative"


def test_scheduler_returns_error_when_pipeline_raises(monkeypatch, tmp_path: Path) -> None:
    def _boom(**kwargs):  # noqa: ANN003, ANN202
        raise RuntimeError("boom")

    monkeypatch.setattr(scheduler_module, "run_full_pipeline", _boom)
    store = _MemoryStateStore()
    result = scheduler_module.Scheduler().run_once(
        state_store=store,  # type: ignore[arg-type]
        config=ScheduleConfig(timezone="UTC"),
        db_path=tmp_path / "berlin_insider.db",
        target_items=7,
        force=True,
        now_utc=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
    )
    assert result.executed is True
    assert result.status == SchedulerStatus.ERROR
    assert result.exit_code == 1
    assert result.delivered is False
    assert store.state.last_error_message == "boom"


def test_scheduler_returns_error_when_delivery_fails(monkeypatch, tmp_path: Path) -> None:
    class _FailingMessenger:
        def send_digest(self, *, text: str, feedback_metadata=None) -> DeliveryResult:  # noqa: ANN001
            raise MessengerError("forbidden")

    def _fake_run_full_pipeline(**kwargs):  # noqa: ANN003, ANN202
        return FullPipelineRunResult(
            fetch_result=_empty_fetch(),
            parse_result=_empty_parse(),
            curate_result=_empty_curate(),
            digest="Berlin Insider",
            digest_kind=DigestKind.WEEKEND,
        )

    monkeypatch.setattr(scheduler_module, "run_full_pipeline", _fake_run_full_pipeline)
    store = _MemoryStateStore()
    result = scheduler_module.Scheduler().run_once(
        state_store=store,  # type: ignore[arg-type]
        config=ScheduleConfig(timezone="UTC"),
        db_path=tmp_path / "berlin_insider.db",
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


def test_scheduler_omits_feedback_metadata_for_weekend(monkeypatch, tmp_path: Path) -> None:
    def _fake_run_full_pipeline(**kwargs):  # noqa: ANN003, ANN202
        return FullPipelineRunResult(
            fetch_result=_empty_fetch(),
            parse_result=_empty_parse(),
            curate_result=_empty_curate(),
            digest="Berlin Insider",
            digest_kind=DigestKind.WEEKEND,
        )

    monkeypatch.setattr(scheduler_module, "run_full_pipeline", _fake_run_full_pipeline)
    store = _MemoryStateStore()
    messenger = _FakeMessenger()
    result = scheduler_module.Scheduler().run_once(
        state_store=store,  # type: ignore[arg-type]
        config=ScheduleConfig(timezone="UTC"),
        db_path=tmp_path / "berlin_insider.db",
        target_items=7,
        force=True,
        now_utc=datetime(2026, 2, 27, 8, 0, tzinfo=UTC),
        messenger=messenger,
    )
    assert result.executed is True
    assert messenger.feedback_metadata is None
