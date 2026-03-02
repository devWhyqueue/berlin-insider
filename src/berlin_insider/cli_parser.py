from __future__ import annotations

import argparse
import os

from berlin_insider.digest import DigestKind
from berlin_insider.fetcher.models import SourceId
from berlin_insider.pipeline import DEFAULT_USER_AGENT


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser with fetch and worker commands."""
    parser = argparse.ArgumentParser(prog="berlin-insider")
    sub = parser.add_subparsers(dest="command")
    _add_fetch_parser(sub)
    _add_worker_parser(sub)
    return parser


def _add_fetch_parser(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
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
        "--digest-kind",
        choices=[kind.value for kind in DigestKind],
        default=DigestKind.WEEKEND.value,
        help="Digest mode used for curation and formatter output",
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
        "--refresh-detail-cache",
        action="store_true",
        help="Force re-fetch detail pages and refresh cache for this run",
    )
    fetch.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="HTTP user-agent string used for requests",
    )
    fetch.add_argument("--target-items", type=int, default=7, help="Target number of curated items")
    fetch.add_argument(
        "--db-path",
        default=".data/berlin_insider.db",
        help="Path to SQLite database file",
    )


def _add_worker_parser(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    worker = sub.add_parser("worker", help="Run always-on worker with scheduler and webhook")
    worker.add_argument(
        "--timezone",
        default=os.getenv("WORKER_TIMEZONE", "Europe/Berlin"),
        help="IANA timezone used for schedule rules",
    )
    worker.add_argument(
        "--weekend-weekday",
        default=os.getenv("WORKER_WEEKEND_WEEKDAY", "friday"),
        help="Lowercase weekday name for weekend digest (for example: friday)",
    )
    worker.add_argument(
        "--weekend-hour",
        type=int,
        default=int(os.getenv("WORKER_WEEKEND_HOUR", "8")),
        help="Weekend digest scheduled hour in local timezone",
    )
    worker.add_argument(
        "--weekend-minute",
        type=int,
        default=int(os.getenv("WORKER_WEEKEND_MINUTE", "0")),
        help="Weekend digest scheduled minute in local timezone",
    )
    worker.add_argument(
        "--daily-hour",
        type=int,
        default=int(os.getenv("WORKER_DAILY_HOUR", "8")),
        help="Daily tip scheduled hour in local timezone",
    )
    worker.add_argument(
        "--daily-minute",
        type=int,
        default=int(os.getenv("WORKER_DAILY_MINUTE", "0")),
        help="Daily tip scheduled minute in local timezone",
    )
    worker.add_argument(
        "--db-path",
        default=os.getenv("WORKER_DB_PATH", ".data/berlin_insider.db"),
        help="Path to SQLite database file",
    )
    worker.add_argument(
        "--target-items",
        type=int,
        default=int(os.getenv("WORKER_TARGET_ITEMS", "7")),
        help="Weekend target item count",
    )
    worker.add_argument(
        "--host",
        default=os.getenv("WORKER_HOST", "0.0.0.0"),
        help="Host address for webhook HTTP server",
    )
    worker.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("WORKER_PORT", "8080")),
        help="Port for webhook HTTP server",
    )
    worker.add_argument(
        "--webhook-public-base-url",
        default=os.getenv("WEBHOOK_PUBLIC_BASE_URL"),
        help="Public base URL Telegram should call for webhook delivery",
    )
    worker.add_argument(
        "--telegram-webhook-secret",
        default=os.getenv("TELEGRAM_WEBHOOK_SECRET"),
        help="Secret token embedded in webhook path",
    )
    worker.add_argument(
        "--telegram-webhook-cert-path",
        default=os.getenv("TELEGRAM_WEBHOOK_CERT_PATH", "/etc/nginx/ssl/berlin-insider.crt"),
        help="Certificate path uploaded to Telegram for self-signed webhook TLS",
    )
    worker.add_argument(
        "--telegram-webhook-ip",
        default=os.getenv("TELEGRAM_WEBHOOK_IP"),
        help="Optional static IP for Telegram webhook delivery to avoid stale DNS resolution",
    )
