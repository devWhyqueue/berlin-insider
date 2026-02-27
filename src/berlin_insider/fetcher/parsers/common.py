from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from urllib.parse import urljoin

from berlin_insider.fetcher.base import SourceDefinition
from berlin_insider.fetcher.models import FetchContext, FetchedItem

Parser = Callable[[str, SourceDefinition, FetchContext], list[FetchedItem]]


def absolute_url(base: str, href: str) -> str:
    """Return an absolute URL by joining base and href."""
    return urljoin(base, href)


def aware(value: datetime) -> datetime:
    """Return a timezone-aware UTC datetime."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value
