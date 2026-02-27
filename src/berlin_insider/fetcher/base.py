from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from berlin_insider.fetcher.models import FetchContext, SourceFetchResult, SourceId


@dataclass(frozen=True, slots=True)
class SourceDefinition:
    source_id: SourceId
    source_url: str


class SourceAdapter(Protocol):
    definition: SourceDefinition

    def fetch(self, context: FetchContext) -> SourceFetchResult:
        """Fetch one source and return canonical raw items plus status metadata."""
        ...
