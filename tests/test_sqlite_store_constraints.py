from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from berlin_insider.digest import DigestKind
from berlin_insider.feedback.models import DeliveredItem, FeedbackEvent, MessageDeliveryRecord
from berlin_insider.feedback.store import SqliteFeedbackStore, SqliteMessageDeliveryStore
from berlin_insider.parser.models import ParsedCategory
from berlin_insider.storage.sqlite import ensure_schema, sqlite_connection


def _insert_source_and_item(db_path: Path, *, url: str = "https://example.com/a") -> DeliveredItem:
    ensure_schema(db_path)
    with sqlite_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO sources (source_id, source_url, adapter_kind, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(source_id) DO NOTHING
            """,
            ("test_source", "https://example.com", "test", "2026-02-28T08:00:00+00:00"),
        )
        conn.execute(
            """
            INSERT INTO items (
                canonical_url,
                source_id,
                original_url,
                title,
                description,
                summary,
                event_start_at,
                event_end_at,
                location,
                category,
                category_confidence,
                weekend_relevance,
                weekend_confidence,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                url,
                "test_source",
                url,
                "Title",
                None,
                "Summary",
                None,
                None,
                "Berlin",
                ParsedCategory.EVENT.value,
                0.9,
                "likely_this_weekend",
                0.9,
                "2026-02-28T08:00:00+00:00",
                "2026-02-28T08:00:00+00:00",
            ),
        )
        item_id = int(conn.execute("SELECT item_id FROM items WHERE canonical_url = ?", (url,)).fetchone()[0])
        conn.commit()
    return DeliveredItem(
        item_id=item_id,
        canonical_url=url,
        title="Title",
        summary="Summary",
        location="Berlin",
        category=ParsedCategory.EVENT,
        event_start_at=None,
        event_end_at=None,
    )


def test_feedback_events_foreign_key_requires_message_delivery(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    feedback_store = SqliteFeedbackStore(db_path)

    with pytest.raises(sqlite3.IntegrityError):
        feedback_store.upsert(
            FeedbackEvent(
                message_key="missing-message",
                vote="up",
                telegram_user_id=123,
                voted_at="2026-02-28T08:00:00+00:00",
                updated_at="2026-02-28T08:00:00+00:00",
            )
        )


def test_feedback_events_upsert_deduplicates_by_message_and_user(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    primary_item = _insert_source_and_item(db_path)
    message_store = SqliteMessageDeliveryStore(db_path)
    feedback_store = SqliteFeedbackStore(db_path)
    message_store.upsert(
        MessageDeliveryRecord(
            message_key="daily-2026-02-28-abc",
            digest_kind=DigestKind.DAILY,
            local_date="2026-02-28",
            sent_at="2026-02-28T08:00:00+00:00",
            telegram_message_id="42",
            primary_item=primary_item,
        )
    )

    feedback_store.upsert(
        FeedbackEvent(
            message_key="daily-2026-02-28-abc",
            vote="up",
            telegram_user_id=123,
            voted_at="2026-02-28T08:00:00+00:00",
            updated_at="2026-02-28T08:00:00+00:00",
        )
    )
    feedback_store.upsert(
        FeedbackEvent(
            message_key="daily-2026-02-28-abc",
            vote="down",
            telegram_user_id=123,
            voted_at="2026-02-28T08:00:00+00:00",
            updated_at="2026-02-28T08:01:00+00:00",
        )
    )

    assert feedback_store.count() == 1


def test_message_delivery_store_round_trips_alternative_item(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    primary_item = _insert_source_and_item(db_path, url="https://example.com/a")
    alternative_item = _insert_source_and_item(db_path, url="https://example.com/b")
    store = SqliteMessageDeliveryStore(db_path)
    store.upsert(
        MessageDeliveryRecord(
            message_key="daily-2026-02-28-abc",
            digest_kind=DigestKind.DAILY,
            local_date="2026-02-28",
            sent_at="2026-02-28T08:00:00+00:00",
            telegram_message_id="42",
            primary_item=primary_item,
            alternative_item=alternative_item,
        )
    )

    reloaded = store.get("daily-2026-02-28-abc")

    assert reloaded is not None
    assert reloaded.alternative_item is not None
    assert reloaded.alternative_item.canonical_url == "https://example.com/b"


def test_ensure_schema_upgrades_known_placeholder_sources(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    ensure_schema(db_path)
    with sqlite_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO sources (source_id, source_url, adapter_kind, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(source_id) DO UPDATE SET
                source_url = excluded.source_url,
                adapter_kind = excluded.adapter_kind,
                updated_at = excluded.updated_at
            """,
            (
                "visit_berlin_blog",
                "https://visit_berlin_blog",
                "derived",
                "2026-03-12T08:00:00+00:00",
            ),
        )
        conn.commit()

    ensure_schema(db_path)

    with sqlite_connection(db_path) as conn:
        row = conn.execute(
            "SELECT source_url, adapter_kind FROM sources WHERE source_id = ?",
            ("visit_berlin_blog",),
        ).fetchone()
    assert row is not None
    assert row[0] == "https://www.visitberlin.de/de/blog"
    assert row[1] == "RssAdapter"


def test_ensure_schema_removes_unused_placeholder_sources(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    ensure_schema(db_path)
    with sqlite_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO sources (source_id, source_url, adapter_kind, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            ("legacy_unknown", "https://legacy_unknown", "derived", "2026-03-12T08:00:00+00:00"),
        )
        conn.commit()

    ensure_schema(db_path)

    with sqlite_connection(db_path) as conn:
        row = conn.execute(
            "SELECT source_id FROM sources WHERE source_id = ?",
            ("legacy_unknown",),
        ).fetchone()
    assert row is None
