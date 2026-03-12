from __future__ import annotations

from pathlib import Path
from typing import Protocol

from berlin_insider.digest import DigestKind
from berlin_insider.storage.sqlite import ensure_schema, sqlite_connection
from berlin_insider.storage.url_normalize import canonicalize_url


class SentItemStore(Protocol):
    def is_sent(self, url: str) -> bool:
        """Return true when this canonical URL was already sent previously."""
        ...

    def mark_sent(self, urls: list[str]) -> None:
        """Retained for interface compatibility; durable delivery persistence happens elsewhere."""
        ...


class NoOpSentItemStore:
    def is_sent(self, url: str) -> bool:  # noqa: ARG002
        """Always report items as unsent."""
        return False

    def mark_sent(self, urls: list[str]) -> None:  # noqa: ARG002
        """Ignore sent-item bookkeeping in stateless scenarios."""
        return


class SqliteSentItemStore:
    def __init__(self, db_path: Path, *, digest_kind: DigestKind) -> None:
        self._db_path = db_path
        self._digest_kind = digest_kind.value
        ensure_schema(self._db_path)

    def is_sent(self, url: str) -> bool:
        """Return true when the canonical URL already exists as a delivered primary item."""
        canonical_url = canonicalize_url(url)
        with sqlite_connection(self._db_path) as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM message_deliveries deliveries
                JOIN items delivered_item ON delivered_item.item_id = deliveries.primary_item_id
                WHERE deliveries.digest_kind = ? AND delivered_item.canonical_url = ?
                LIMIT 1
                """,
                (self._digest_kind, canonical_url),
            ).fetchone()
        return row is not None

    def mark_sent(self, urls: list[str]) -> None:  # noqa: ARG002
        """Keep the compatibility hook while durable delivery persistence happens elsewhere."""
        return
