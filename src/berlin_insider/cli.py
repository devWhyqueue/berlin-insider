from __future__ import annotations

import json
import logging
import os
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
from berlin_insider.curator.store import SqliteSentItemStore
from berlin_insider.digest import DigestKind
from berlin_insider.feedback.store import SqliteMessageDeliveryStore
from berlin_insider.fetcher.models import FetchContext, FetchRunResult, SourceId
from berlin_insider.fetcher.orchestrator import Fetcher
from berlin_insider.formatter import render_telegram_digest
from berlin_insider.messenger.telegram import TelegramMessenger
from berlin_insider.parser.models import ParseRunResult
from berlin_insider.parser.orchestrator import Parser
from berlin_insider.scheduler.cli_log import log_schedule_result
from berlin_insider.scheduler.models import ScheduleConfig
from berlin_insider.scheduler.orchestrator import Scheduler
from berlin_insider.scheduler.store import SqliteSchedulerStateStore
from berlin_insider.storage.item_store import persist_items, upsert_source_websites
from berlin_insider.worker import Worker, WorkerConfig

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")


def main() -> None:
    """Run the command-line interface."""
    _load_dotenv_defaults()
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "fetch":
        _run_fetch_command(args)
    elif args.command == "worker":
        _run_worker_command(args)
    else:
        parser.print_help()


def _run_fetch_command(args) -> None:  # noqa: ANN001
    db_path = Path(args.db_path)
    upsert_source_websites(db_path)
    source_ids = [SourceId(value) for value in args.source] if args.source else None
    fetch_result = Fetcher().run(context=_fetch_context(args), source_ids=source_ids)
    if args.fetch_only:
        _log_fetch_only(fetch_result, json_output=args.json)
        return
    parse_result = Parser().run(fetch_result)
    persist_items(db_path, parse_result)
    if args.parse_only:
        _log_fetch_with_parse(fetch_result, parse_result, json_output=args.json)
        return
    digest_kind = DigestKind(args.digest_kind)
    curate_result = Curator().run(
        parse_result,
        reference_now=fetch_result.finished_at,
        store=SqliteSentItemStore(db_path, digest_kind=digest_kind),
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


def _run_worker_command(args) -> None:  # noqa: ANN001
    db_path = Path(args.db_path)
    schedule = _build_schedule_config(args)
    if args.run_once:
        result = Scheduler().run_once(
            state_store=SqliteSchedulerStateStore(db_path),
            config=schedule,
            db_path=db_path,
            target_items=args.target_items,
            force=True,
            messenger=TelegramMessenger.from_env(),
            sent_message_store=SqliteMessageDeliveryStore(db_path),
        )
        log_schedule_result(logger, result, json_output=False)
        raise SystemExit(result.exit_code)
    if not args.webhook_public_base_url:
        raise SystemExit("WEBHOOK_PUBLIC_BASE_URL is required (or use --webhook-public-base-url).")
    if not args.telegram_webhook_secret:
        raise SystemExit("TELEGRAM_WEBHOOK_SECRET is required (or use --telegram-webhook-secret).")
    Worker(
        config=WorkerConfig(
            db_path=db_path,
            target_items=args.target_items,
            schedule=schedule,
            host=args.host,
            port=args.port,
            webhook_public_base_url=args.webhook_public_base_url,
            telegram_webhook_secret=args.telegram_webhook_secret,
            telegram_webhook_cert_path=(
                Path(args.telegram_webhook_cert_path) if args.telegram_webhook_cert_path else None
            ),
            telegram_webhook_ip=args.telegram_webhook_ip,
        )
    ).run()


def _build_schedule_config(args) -> ScheduleConfig:  # noqa: ANN001
    return ScheduleConfig(
        timezone=args.timezone,
        daily_hour=args.daily_hour,
        daily_minute=args.daily_minute,
        weekend_weekday=args.weekend_weekday,
        weekend_hour=args.weekend_hour,
        weekend_minute=args.weekend_minute,
    )


def _fetch_context(args) -> FetchContext:  # noqa: ANN001
    return FetchContext(
        user_agent=args.user_agent,
        timeout_seconds=args.timeout,
        max_items_per_source=args.max_items_per_source,
        collected_at=datetime.now(UTC),
        detail_cache_db_path=Path(args.db_path),
        refresh_detail_cache=args.refresh_detail_cache,
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


def _load_dotenv_defaults(path: Path | None = None) -> None:
    """Load KEY=VALUE pairs from .env into process environment without overriding existing values."""
    env_path = path or Path(".env")
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        parsed = _parse_dotenv_value(value)
        os.environ.setdefault(key, parsed)


def _parse_dotenv_value(raw_value: str) -> str:
    value = raw_value.strip()
    if not value:
        return ""
    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        return value[1:-1]
    return value.split(" #", 1)[0].strip()
