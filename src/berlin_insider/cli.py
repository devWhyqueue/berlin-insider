from __future__ import annotations

import json
import logging
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from berlin_insider.cli_parser import build_parser
from berlin_insider.cli_render import (
    render_summary,
    render_summary_with_parse,
    render_summary_with_parse_and_curate,
)
from berlin_insider.curator.config import CuratorConfig
from berlin_insider.curator.models import CurateRunResult
from berlin_insider.curator.orchestrator import Curator
from berlin_insider.curator.store import JsonSentItemStore
from berlin_insider.digest import DigestKind
from berlin_insider.feedback.store import (
    JsonFeedbackStore,
    JsonSentMessageStore,
    JsonTelegramUpdatesStateStore,
)
from berlin_insider.feedback.telegram_poller import poll_feedback_once
from berlin_insider.fetcher.models import FetchContext, FetchRunResult, SourceId
from berlin_insider.fetcher.orchestrator import Fetcher
from berlin_insider.formatter import render_telegram_digest
from berlin_insider.messenger.telegram import TelegramMessenger
from berlin_insider.parser.models import ParseRunResult
from berlin_insider.parser.orchestrator import Parser
from berlin_insider.scheduler.cli_log import log_schedule_result
from berlin_insider.scheduler.models import ScheduleConfig
from berlin_insider.scheduler.orchestrator import Scheduler
from berlin_insider.scheduler.store import JsonSchedulerStateStore

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")


def main() -> None:
    """Run the command-line interface."""
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "fetch":
        _run_fetch_command(args)
    elif args.command == "schedule":
        _exit_on_nonzero(_run_schedule_command(args))
    elif args.command == "feedback":
        _exit_on_nonzero(_run_feedback_command(args))
    else:
        parser.print_help()


def _run_fetch_command(args) -> None:  # noqa: ANN001
    source_ids = [SourceId(value) for value in args.source] if args.source else None
    fetch_result = Fetcher().run(context=_fetch_context(args), source_ids=source_ids)
    if args.fetch_only:
        _log_fetch_only(fetch_result, json_output=args.json)
        return
    parse_result = Parser().run(fetch_result)
    if args.parse_only:
        _log_fetch_with_parse(fetch_result, parse_result, json_output=args.json)
        return
    digest_kind = DigestKind(args.digest_kind)
    curate_result = Curator().run(
        parse_result,
        reference_now=fetch_result.finished_at,
        store=JsonSentItemStore(Path(args.sent_store_path)),
        config=CuratorConfig(
            target_count=1 if digest_kind == DigestKind.DAILY else args.target_items,
            digest_kind=digest_kind,
        ),
    )
    _log_fetch_with_parse_and_curate(
        fetch_result=fetch_result,
        parse_result=parse_result,
        curate_result=curate_result,
        reference_now=fetch_result.finished_at,
        json_output=args.json,
        digest_output=args.digest,
        digest_kind=digest_kind,
    )


def _run_schedule_command(args) -> int:  # noqa: ANN001
    result = Scheduler().run_once(
        state_store=JsonSchedulerStateStore(Path(args.state_path)),
        config=ScheduleConfig(
            timezone=args.timezone,
            daily_hour=args.daily_hour,
            daily_minute=args.daily_minute,
            weekend_weekday=args.weekend_weekday,
            weekend_hour=args.weekend_hour,
            weekend_minute=args.weekend_minute,
        ),
        sent_store_path=Path(args.sent_store_path),
        target_items=args.target_items,
        force=args.force,
        sent_message_store=JsonSentMessageStore(Path(args.sent_message_store_path)),
    )
    log_schedule_result(logger, result, json_output=args.json)
    return result.exit_code


def _run_feedback_command(args) -> int:  # noqa: ANN001
    result = poll_feedback_once(
        messenger=TelegramMessenger.from_env(),
        state_store=JsonTelegramUpdatesStateStore(Path(args.updates_state_path)),
        feedback_store=JsonFeedbackStore(Path(args.feedback_store_path)),
        sent_message_store=JsonSentMessageStore(Path(args.sent_message_store_path)),
        timeout_seconds=args.poll_timeout_seconds,
    )
    if args.json:
        logger.info(json.dumps(asdict(result), default=str, ensure_ascii=False, indent=2))
    else:
        logger.info(
            "Feedback poll: fetched=%s processed=%s persisted=%s ignored=%s answered=%s next_offset=%s",
            result.fetched_updates,
            result.processed_callbacks,
            result.persisted_votes,
            result.ignored_updates,
            result.answered_callbacks,
            result.next_offset if result.next_offset is not None else "n/a",
        )
    return 0


def _fetch_context(args) -> FetchContext:  # noqa: ANN001
    return FetchContext(
        user_agent=args.user_agent,
        timeout_seconds=args.timeout,
        max_items_per_source=args.max_items_per_source,
        collected_at=datetime.now(UTC),
    )


def _log_fetch_only(fetch_result: FetchRunResult, *, json_output: bool) -> None:
    if json_output:
        logger.info(json.dumps(asdict(fetch_result), default=str, ensure_ascii=False, indent=2))
        return
    logger.info(render_summary(fetch_result))


def _log_fetch_with_parse(
    fetch_result: FetchRunResult, parse_result: ParseRunResult, *, json_output: bool
) -> None:
    if json_output:
        logger.info(
            json.dumps(
                {"fetch": asdict(fetch_result), "parse": asdict(parse_result)},
                default=str,
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    logger.info(render_summary_with_parse(fetch_result, parse_result))


def _log_fetch_with_parse_and_curate(
    *,
    fetch_result: FetchRunResult,
    parse_result: ParseRunResult,
    curate_result: CurateRunResult,
    reference_now: datetime,
    json_output: bool,
    digest_output: bool,
    digest_kind: DigestKind,
) -> None:
    if json_output:
        payload: dict[str, object] = {
            "fetch": asdict(fetch_result),
            "parse": asdict(parse_result),
            "curate": asdict(curate_result),
        }
        if digest_output:
            payload["digest"] = render_telegram_digest(
                curate_result,
                reference_now=reference_now,
                digest_kind=digest_kind,
            )
        logger.info(json.dumps(payload, default=str, ensure_ascii=False, indent=2))
        return
    if digest_output:
        logger.info(
            render_telegram_digest(
                curate_result, reference_now=reference_now, digest_kind=digest_kind
            )
        )
        return
    logger.info(render_summary_with_parse_and_curate(fetch_result, parse_result, curate_result))


def _exit_on_nonzero(exit_code: int) -> None:
    if exit_code != 0:
        raise SystemExit(exit_code)
