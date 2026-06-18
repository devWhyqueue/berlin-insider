from __future__ import annotations

from pathlib import Path
from typing import Protocol

from berlin_insider.feedback.messenger.formatter.digest import DigestKind
from berlin_insider.parser.models import ParsedItem
from berlin_insider.storage.sqlite import ensure_schema, sqlite_connection
from berlin_insider.storage.url_normalize import canonicalize_url


class SentItemStore(Protocol):
    def is_sent(self, url: str) -> bool:
        """Return true when this canonical URL was already sent previously."""
        ...

    def mark_sent(self, urls: list[str]) -> None:
        """Retained for interface compatibility; durable delivery persistence happens elsewhere."""
        ...

    def source_delivery_count(self, source_id: str) -> int:
        """Return prior primary deliveries for one source and digest kind."""
        ...

    def feedback_adjustment(self, item: ParsedItem) -> float:
        """Return bounded score adjustment from aggregate feedback."""
        ...


class NoOpSentItemStore:
    def is_sent(self, url: str) -> bool:  # noqa: ARG002
        """Always report items as unsent."""
        return False

    def mark_sent(self, urls: list[str]) -> None:  # noqa: ARG002
        """Ignore sent-item bookkeeping in stateless scenarios."""
        return

    def source_delivery_count(self, source_id: str) -> int:  # noqa: ARG002
        """Return zero source history for stateless curation."""
        return 0

    def feedback_adjustment(self, item: ParsedItem) -> float:  # noqa: ARG002
        """Return zero feedback adjustment for stateless curation."""
        return 0.0


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

    def source_delivery_count(self, source_id: str) -> int:
        """Return prior primary deliveries for one source and digest kind."""
        with sqlite_connection(self._db_path) as conn:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM message_deliveries deliveries
                JOIN items delivered_item ON delivered_item.item_id = deliveries.primary_item_id
                WHERE deliveries.digest_kind = ? AND delivered_item.source_id = ?
                """,
                (self._digest_kind, source_id),
            ).fetchone()
        return int(row[0]) if row is not None else 0

    def feedback_adjustment(self, item: ParsedItem) -> float:
        """Return bounded score adjustment from aggregate feedback."""
        signals: list[tuple[str, tuple[object, ...]]] = [
            ("delivered_item.source_id = ?", (item.source_id.value,)),
            ("delivered_item.category = ?", (item.category.value,)),
        ]
        if item.is_free is not None:
            signals.append(("delivered_item.is_free = ?", (int(item.is_free),)))
        if item.event_start_at is not None:
            hour = f"{item.event_start_at.hour:02d}"
            signals.append(("SUBSTR(delivered_item.event_start_at, 12, 2) = ?", (hour,)))
        adjustment = sum(self._feedback_signal(*signal) for signal in signals)
        return max(-0.08, min(adjustment, 0.08))

    def _feedback_signal(self, where_clause: str, params: tuple[object, ...]) -> float:
        with sqlite_connection(self._db_path) as conn:
            row = conn.execute(
                f"""
                SELECT
                    SUM(CASE WHEN feedback.vote = 'up' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN feedback.vote = 'down' THEN 1 ELSE 0 END)
                FROM feedback_events feedback
                JOIN message_deliveries deliveries
                  ON deliveries.message_key = feedback.message_key
                JOIN items delivered_item ON delivered_item.item_id = deliveries.primary_item_id
                WHERE deliveries.digest_kind = ? AND {where_clause}
                """,
                (self._digest_kind, *params),
            ).fetchone()
        if row is None:
            return 0.0
        up_votes = int(row[0] or 0)
        down_votes = int(row[1] or 0)
        total = up_votes + down_votes
        if total < 3:
            return 0.0
        return (up_votes - down_votes) / total * 0.02
