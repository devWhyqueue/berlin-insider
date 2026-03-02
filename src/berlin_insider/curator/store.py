from __future__ import annotations

from pathlib import Path
from typing import Protocol

from berlin_insider.digest import DigestKind
from berlin_insider.storage.sqlite import ensure_schema, now_utc_iso, sqlite_connection
from berlin_insider.storage.url_normalize import canonicalize_url


class SentItemStore(Protocol):
    def is_sent(self, url: str) -> bool:
        """Return true when this canonical URL was already sent previously."""
        ...

    def mark_sent(self, urls: list[str]) -> None:
        """Persist canonical URLs that were selected in this run."""
        ...


class NoOpSentItemStore:
    def is_sent(self, url: str) -> bool:  # noqa: ARG002
        """Always report unsent for tests and stateless runs."""
        return False

    def mark_sent(self, urls: list[str]) -> None:  # noqa: ARG002
        """Intentionally ignore persisted links."""
        return


class SqliteSentItemStore:
    def __init__(self, db_path: Path, *, digest_kind: DigestKind) -> None:
        self._db_path = db_path
        self._digest_kind = digest_kind.value
        ensure_schema(self._db_path)

    def is_sent(self, url: str) -> bool:
        """Return true when the canonical URL already exists in local store for this digest kind."""
        canonical_url = canonicalize_url(url)
        with sqlite_connection(self._db_path) as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM sent_links
                WHERE digest_kind = ? AND canonical_url = ?
                LIMIT 1
                """,
                (self._digest_kind, canonical_url),
            ).fetchone()
        return row is not None

    def mark_sent(self, urls: list[str]) -> None:
        """Persist selected canonical URLs into sent_links table."""
        rows = [(self._digest_kind, canonicalize_url(url), now_utc_iso()) for url in urls]
        if not rows:
            return
        with sqlite_connection(self._db_path) as conn:
            conn.executemany(
                """
                INSERT OR IGNORE INTO sent_links (digest_kind, canonical_url, first_sent_at)
                VALUES (?, ?, ?)
                """,
                rows,
            )
            conn.commit()
