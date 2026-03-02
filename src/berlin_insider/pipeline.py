from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from berlin_insider.curator.config import CuratorConfig
from berlin_insider.curator.models import CurateRunResult
from berlin_insider.curator.orchestrator import Curator
from berlin_insider.curator.store import SqliteSentItemStore
from berlin_insider.digest import DigestKind
from berlin_insider.fetcher.models import FetchContext, FetchRunResult, SourceId
from berlin_insider.fetcher.orchestrator import Fetcher
from berlin_insider.formatter import render_telegram_digest
from berlin_insider.parser.models import ParseRunResult
from berlin_insider.parser.orchestrator import Parser
from berlin_insider.storage.content_store import persist_parse_run, upsert_source_websites
from berlin_insider.storage.detail_cache import SqliteDetailCacheStore

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
DEFAULT_TIMEOUT_SECONDS = 20.0
DEFAULT_MAX_ITEMS_PER_SOURCE = 30


@dataclass(slots=True)
class FullPipelineRunResult:
    fetch_result: FetchRunResult
    parse_result: ParseRunResult
    curate_result: CurateRunResult
    digest: str
    digest_kind: DigestKind


def build_fetch_context(
    *,
    user_agent: str = DEFAULT_USER_AGENT,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    max_items_per_source: int = DEFAULT_MAX_ITEMS_PER_SOURCE,
    collected_at: datetime | None = None,
    detail_cache_db_path: Path | None = None,
    refresh_detail_cache: bool = False,
) -> FetchContext:
    """Build a fetch context using stable CLI/scheduler defaults."""
    return FetchContext(
        user_agent=user_agent,
        timeout_seconds=timeout_seconds,
        max_items_per_source=max_items_per_source,
        collected_at=collected_at or datetime.now(UTC),
        detail_cache_db_path=detail_cache_db_path,
        refresh_detail_cache=refresh_detail_cache,
    )


def run_fetch_parse_pipeline(
    *,
    context: FetchContext,
    db_path: Path,
    source_ids: list[SourceId] | None = None,
) -> tuple[FetchRunResult, ParseRunResult]:
    """Run fetch and parse stages and return both run results."""
    upsert_source_websites(db_path)
    effective_context = context
    if context.detail_cache_db_path is None:
        effective_context = FetchContext(
            user_agent=context.user_agent,
            timeout_seconds=context.timeout_seconds,
            max_items_per_source=context.max_items_per_source,
            collected_at=context.collected_at,
            detail_cache_db_path=db_path,
            refresh_detail_cache=context.refresh_detail_cache,
        )
    fetch_result = Fetcher().run(context=effective_context, source_ids=source_ids)
    parse_result = Parser(detail_cache_store=SqliteDetailCacheStore(db_path)).run(fetch_result)
    persist_parse_run(db_path, parse_result)
    return fetch_result, parse_result


def run_full_pipeline(
    *,
    context: FetchContext,
    db_path: Path,
    target_items: int,
    digest_kind: DigestKind = DigestKind.WEEKEND,
    source_ids: list[SourceId] | None = None,
) -> FullPipelineRunResult:
    """Run fetch, parse, curate, and digest formatting in one call."""
    fetch_result, parse_result = run_fetch_parse_pipeline(
        context=context, db_path=db_path, source_ids=source_ids
    )
    curate_result = Curator().run(
        parse_result,
        reference_now=fetch_result.finished_at,
        store=SqliteSentItemStore(db_path, digest_kind=digest_kind),
        config=CuratorConfig(
            target_count=1 if digest_kind == DigestKind.DAILY else target_items,
            digest_kind=digest_kind,
        ),
    )
    digest = render_telegram_digest(
        curate_result,
        reference_now=fetch_result.finished_at,
        digest_kind=digest_kind,
    )
    return FullPipelineRunResult(
        fetch_result=fetch_result,
        parse_result=parse_result,
        curate_result=curate_result,
        digest=digest,
        digest_kind=digest_kind,
    )
