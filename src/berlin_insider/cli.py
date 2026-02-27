from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Iterable
from dataclasses import asdict
from datetime import UTC, datetime

from berlin_insider.fetcher.models import FetchContext, FetchedItem, FetchRunResult, SourceId
from berlin_insider.fetcher.orchestrator import Fetcher
from berlin_insider.parser.models import ParsedItem, ParseRunResult
from berlin_insider.parser.orchestrator import Parser

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")


def main() -> None:
    """Run the command-line interface."""
    parser = _build_parser()
    args = parser.parse_args()
    if args.command != "fetch":
        parser.print_help()
        return
    _run_fetch_command(args)


def _run_fetch_command(args: argparse.Namespace) -> None:
    source_ids = [SourceId(value) for value in args.source] if args.source else None
    context = _fetch_context(args)
    fetch_result = Fetcher().run(context=context, source_ids=source_ids)
    if args.fetch_only:
        _log_fetch_only(fetch_result, json_output=args.json)
        return
    parse_result = Parser().run(fetch_result)
    _log_fetch_with_parse(fetch_result, parse_result, json_output=args.json)


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
    logger.info(_render_summary(fetch_result))


def _log_fetch_with_parse(
    fetch_result: FetchRunResult, parse_result: ParseRunResult, *, json_output: bool
) -> None:
    if json_output:
        payload = {"fetch": asdict(fetch_result), "parse": asdict(parse_result)}
        logger.info(json.dumps(payload, default=str, ensure_ascii=False, indent=2))
        return
    logger.info(_render_summary_with_parse(fetch_result, parse_result))


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
        "--fetch-only",
        action="store_true",
        help="Skip parser stage and only return raw fetch results",
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
        default=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        help="HTTP user-agent string used for requests",
    )
    return parser


def _render_summary(result: FetchRunResult) -> str:
    lines = [
        f"Fetch run started: {result.started_at.isoformat()}",
        f"Fetch run finished: {result.finished_at.isoformat()}",
        f"Total items: {result.total_items}",
        "",
    ]
    for source_result in _sorted_results(result):
        lines.append(
            f"- {source_result.source_id.value}: {source_result.status.value} | "
            f"items={len(source_result.items)} | duration_ms={source_result.duration_ms}"
        )
        if source_result.warnings:
            lines.append(f"  warnings: {'; '.join(source_result.warnings)}")
        if source_result.error_message:
            lines.append(f"  error: {source_result.error_message}")
        if source_result.items:
            lines.append(f"  sample: {_item_preview(source_result.items[0])}")
    return "\n".join(lines)


def _sorted_results(result: FetchRunResult) -> Iterable:
    return sorted(result.results, key=lambda item: item.source_id.value)


def _item_preview(item: FetchedItem) -> str:
    title = item.title or "untitled"
    date = item.raw_date_text or (item.published_at.isoformat() if item.published_at else "n/a")
    location = item.location_hint or "unknown location"
    snippet = (item.snippet or "").strip()
    snippet_text = snippet[:60] + ("..." if len(snippet) > 60 else "")
    return (
        f"{title} | {date} | {location} | method={item.fetch_method} | "
        f"meta={len(item.metadata)} | {snippet_text}"
    )


def _render_summary_with_parse(fetch: FetchRunResult, parse: ParseRunResult) -> str:
    lines = [_render_summary(fetch), "", f"Parsed total items: {parse.total_items}", ""]
    for source_result in sorted(parse.results, key=lambda item: item.source_id.value):
        lines.append(
            f"- {source_result.source_id.value}: {source_result.status.value} | "
            f"items={len(source_result.items)} | duration_ms={source_result.duration_ms}"
        )
        if source_result.warnings:
            lines.append(f"  warnings: {'; '.join(source_result.warnings)}")
        if source_result.error_message:
            lines.append(f"  error: {source_result.error_message}")
        if source_result.items:
            lines.append(f"  sample: {_parsed_item_preview(source_result.items[0])}")
    return "\n".join(lines)


def _parsed_item_preview(item: ParsedItem) -> str:
    title = item.title or "untitled"
    start = item.event_start_at.isoformat() if item.event_start_at else "n/a"
    end = item.event_end_at.isoformat() if item.event_end_at else "n/a"
    location = item.location or "unknown location"
    notes_count = len(item.parse_notes)
    raw_count = len(item.raw)
    return (
        f"{title} | {start} | end={end} | {location} | category={item.category.value} "
        f"({item.category_confidence:.2f}) | weekend={item.weekend_relevance.value} "
        f"({item.weekend_confidence:.2f}) | notes={notes_count} | raw={raw_count}"
    )
