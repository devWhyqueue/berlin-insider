from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from berlin_insider.cli_render import (
    render_summary,
    render_summary_with_parse,
    render_summary_with_parse_and_curate,
)
from berlin_insider.curator.config import CuratorConfig
from berlin_insider.curator.models import CurateRunResult
from berlin_insider.curator.orchestrator import Curator
from berlin_insider.curator.store import JsonSentItemStore
from berlin_insider.fetcher.models import FetchContext, FetchRunResult, SourceId
from berlin_insider.fetcher.orchestrator import Fetcher
from berlin_insider.formatter import render_telegram_digest
from berlin_insider.parser.models import ParseRunResult
from berlin_insider.parser.orchestrator import Parser
from berlin_insider.pipeline import DEFAULT_USER_AGENT
from berlin_insider.scheduler.cli_log import log_schedule_result
from berlin_insider.scheduler.models import ScheduleConfig
from berlin_insider.scheduler.orchestrator import Scheduler
from berlin_insider.scheduler.store import JsonSchedulerStateStore

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")


def main() -> None:
    """Run the command-line interface."""
    parser = _build_parser()
    args = parser.parse_args()
    if args.command == "fetch":
        _run_fetch_command(args)
        return
    if args.command == "schedule":
        exit_code = _run_schedule_command(args)
        if exit_code != 0:
            raise SystemExit(exit_code)
        return
    parser.print_help()


def _run_fetch_command(args: argparse.Namespace) -> None:
    source_ids = [SourceId(value) for value in args.source] if args.source else None
    context = _fetch_context(args)
    fetch_result = Fetcher().run(context=context, source_ids=source_ids)
    if args.fetch_only:
        _log_fetch_only(fetch_result, json_output=args.json)
        return
    parse_result = Parser().run(fetch_result)
    if args.parse_only:
        _log_fetch_with_parse(fetch_result, parse_result, json_output=args.json)
        return
    cur_config = CuratorConfig(target_count=args.target_items)
    sent_store = JsonSentItemStore(Path(args.sent_store_path))
    curate_result = Curator().run(
        parse_result,
        reference_now=fetch_result.finished_at,
        store=sent_store,
        config=cur_config,
    )
    _log_fetch_with_parse_and_curate(
        fetch_result,
        parse_result,
        curate_result,
        reference_now=fetch_result.finished_at,
        json_output=args.json,
        digest_output=args.digest,
    )


def _run_schedule_command(args: argparse.Namespace) -> int:
    result = Scheduler().run_once(
        state_store=JsonSchedulerStateStore(Path(args.state_path)),
        config=ScheduleConfig(
            timezone=args.timezone,
            weekday=args.weekday,
            hour=args.hour,
            minute=args.minute,
        ),
        sent_store_path=Path(args.sent_store_path),
        target_items=args.target_items,
        force=args.force,
    )
    log_schedule_result(logger, result, json_output=args.json)
    return result.exit_code


def _fetch_context(args: argparse.Namespace) -> FetchContext:
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
        payload = {"fetch": asdict(fetch_result), "parse": asdict(parse_result)}
        logger.info(json.dumps(payload, default=str, ensure_ascii=False, indent=2))
        return
    logger.info(render_summary_with_parse(fetch_result, parse_result))


def _log_fetch_with_parse_and_curate(
    fetch_result: FetchRunResult,
    parse_result: ParseRunResult,
    curate_result: CurateRunResult,
    *,
    reference_now: datetime,
    json_output: bool,
    digest_output: bool,
) -> None:
    if json_output:
        payload: dict[str, object] = {
            "fetch": asdict(fetch_result),
            "parse": asdict(parse_result),
            "curate": asdict(curate_result),
        }
        if digest_output:
            payload["digest"] = render_telegram_digest(curate_result, reference_now=reference_now)
        logger.info(json.dumps(payload, default=str, ensure_ascii=False, indent=2))
        return
    if digest_output:
        logger.info(render_telegram_digest(curate_result, reference_now=reference_now))
        return
    logger.info(render_summary_with_parse_and_curate(fetch_result, parse_result, curate_result))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="berlin-insider")
    sub = parser.add_subparsers(dest="command")
    fetch = sub.add_parser("fetch", help="Fetch items from all configured sources")
    fetch.add_argument(
        "--source",
        action="append",
        default=[],
        choices=[source.value for source in SourceId],
        help="Restrict run to one or more source IDs",
    )
    fetch.add_argument("--json", action="store_true", help="Print full JSON output")
    fetch.add_argument(
        "--digest",
        action="store_true",
        help="Render Telegram MarkdownV2 digest text from curated output",
    )
    fetch.add_argument(
        "--fetch-only",
        action="store_true",
        help="Skip parser stage and only return raw fetch results",
    )
    fetch.add_argument(
        "--parse-only",
        action="store_true",
        help="Skip curator stage and only return fetch + parser results",
    )
    fetch.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout in seconds")
    fetch.add_argument(
        "--max-items-per-source",
        type=int,
        default=30,
        help="Maximum number of collected items per source",
    )
    fetch.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="HTTP user-agent string used for requests",
    )
    fetch.add_argument("--target-items", type=int, default=7, help="Target number of curated items")
    fetch.add_argument(
        "--sent-store-path",
        default=".data/sent_links.json",
        help="Path to sent-links JSON store for curation dedupe",
    )
    schedule = sub.add_parser("schedule", help="Run scheduler once (for cron/task scheduler)")
    schedule.add_argument("--json", action="store_true", help="Print full JSON output")
    schedule.add_argument(
        "--timezone", default="Europe/Berlin", help="IANA timezone used for due checks"
    )
    schedule.add_argument(
        "--weekday",
        default="friday",
        help="Lowercase weekday name for scheduled run (for example: friday)",
    )
    schedule.add_argument("--hour", type=int, default=8, help="Scheduled hour in local timezone")
    schedule.add_argument(
        "--minute", type=int, default=0, help="Scheduled minute in local timezone"
    )
    schedule.add_argument(
        "--state-path",
        default=".data/scheduler_state.json",
        help="Path to scheduler state JSON file",
    )
    schedule.add_argument(
        "--sent-store-path",
        default=".data/sent_links.json",
        help="Path to sent-links JSON store for curation dedupe",
    )
    schedule.add_argument(
        "--target-items", type=int, default=7, help="Target number of curated items"
    )
    schedule.add_argument(
        "--force", action="store_true", help="Bypass due check and run immediately"
    )
    return parser
