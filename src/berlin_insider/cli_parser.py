from __future__ import annotations

import argparse

from berlin_insider.digest import DigestKind
from berlin_insider.fetcher.models import SourceId
from berlin_insider.pipeline import DEFAULT_USER_AGENT


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser with fetch, schedule, and feedback commands."""
    parser = argparse.ArgumentParser(prog="berlin-insider")
    sub = parser.add_subparsers(dest="command")
    _add_fetch_parser(sub)
    _add_schedule_parser(sub)
    _add_feedback_parser(sub)
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


def _add_schedule_parser(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    schedule = sub.add_parser("schedule", help="Run scheduler once (for cron/task scheduler)")
    schedule.add_argument("--json", action="store_true", help="Print full JSON output")
    schedule.add_argument(
        "--timezone", default="Europe/Berlin", help="IANA timezone used for due checks"
    )
    schedule.add_argument(
        "--weekend-weekday",
        default="friday",
        help="Lowercase weekday name for weekend digest (for example: friday)",
    )
    schedule.add_argument(
        "--weekend-hour",
        type=int,
        default=8,
        help="Weekend digest scheduled hour in local timezone",
    )
    schedule.add_argument(
        "--weekend-minute",
        type=int,
        default=0,
        help="Weekend digest scheduled minute in local timezone",
    )
    schedule.add_argument(
        "--daily-hour", type=int, default=8, help="Daily tip scheduled hour in local timezone"
    )
    schedule.add_argument(
        "--daily-minute", type=int, default=0, help="Daily tip scheduled minute in local timezone"
    )
    schedule.add_argument(
        "--db-path",
        default=".data/berlin_insider.db",
        help="Path to SQLite database file",
    )
    schedule.add_argument("--target-items", type=int, default=7, help="Weekend target item count")
    schedule.add_argument(
        "--force", action="store_true", help="Bypass due check and run immediately"
    )


def _add_feedback_parser(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    feedback = sub.add_parser(
        "feedback", help="Poll Telegram callback updates and persist thumbs feedback"
    )
    feedback.add_argument("--json", action="store_true", help="Print full JSON output")
    feedback.add_argument(
        "--db-path",
        default=".data/berlin_insider.db",
        help="Path to SQLite database file",
    )
    feedback.add_argument(
        "--poll-timeout-seconds",
        type=int,
        default=0,
        help="Telegram getUpdates timeout seconds",
    )
