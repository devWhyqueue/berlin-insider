from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from berlin_insider.curator.store import SqliteSentItemStore
from berlin_insider.digest import DigestKind
from berlin_insider.feedback.models import FeedbackEvent, SentMessageRecord
from berlin_insider.feedback.store import SqliteFeedbackStore, SqliteSentMessageStore
from berlin_insider.storage.sqlite import sqlite_connection


def test_sent_links_dedupe_is_safe_on_insert_conflict(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    store_a = SqliteSentItemStore(db_path, digest_kind=DigestKind.WEEKEND)
    store_b = SqliteSentItemStore(db_path, digest_kind=DigestKind.WEEKEND)
    url = "https://example.com/event?utm_source=test"

    def _mark(store: SqliteSentItemStore) -> None:
        store.mark_sent([url])

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(_mark, store_a), pool.submit(_mark, store_b)]
        for future in futures:
            future.result()

    with sqlite_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM sent_links
            WHERE digest_kind = ? AND canonical_url = ?
            """,
            (DigestKind.WEEKEND.value, "https://example.com/event"),
        ).fetchone()
    assert row is not None
    assert row[0] == 1


def test_feedback_events_foreign_key_requires_sent_message(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    feedback_store = SqliteFeedbackStore(db_path)

    with pytest.raises(sqlite3.IntegrityError):
        feedback_store.upsert(
            FeedbackEvent(
                message_key="missing-message",
                digest_kind=DigestKind.DAILY,
                vote="up",
                telegram_user_id=123,
                chat_id="-1000",
                message_id="42",
                voted_at="2026-02-28T08:00:00+00:00",
                updated_at="2026-02-28T08:00:00+00:00",
            )
        )


def test_feedback_events_upsert_deduplicates_by_message_and_user(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    sent_message_store = SqliteSentMessageStore(db_path)
    feedback_store = SqliteFeedbackStore(db_path)
    sent_message_store.upsert(
        SentMessageRecord(
            message_key="daily-2026-02-28-abc",
            digest_kind=DigestKind.DAILY,
            local_date="2026-02-28",
            sent_at="2026-02-28T08:00:00+00:00",
            telegram_message_id="42",
            selected_urls=["https://example.com/a"],
        )
    )

    feedback_store.upsert(
        FeedbackEvent(
            message_key="daily-2026-02-28-abc",
            digest_kind=DigestKind.DAILY,
            vote="up",
            telegram_user_id=123,
            chat_id="-1000",
            message_id="42",
            voted_at="2026-02-28T08:00:00+00:00",
            updated_at="2026-02-28T08:00:00+00:00",
        )
    )
    feedback_store.upsert(
        FeedbackEvent(
            message_key="daily-2026-02-28-abc",
            digest_kind=DigestKind.DAILY,
            vote="down",
            telegram_user_id=123,
            chat_id="-1000",
            message_id="42",
            voted_at="2026-02-28T08:00:00+00:00",
            updated_at="2026-02-28T08:01:00+00:00",
        )
    )

    assert feedback_store.count() == 1
