from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient

from berlin_insider.digest import DigestKind
from berlin_insider.feedback.models import DeliveredItem, MessageDeliveryRecord
from berlin_insider.feedback.store import SqliteFeedbackStore, SqliteMessageDeliveryStore
from berlin_insider.feedback.webhook import WebhookDependencies, create_webhook_app
from berlin_insider.parser.models import ParsedCategory
from berlin_insider.storage.detail_cache import SqliteDetailCacheStore
from berlin_insider.storage.sqlite import ensure_schema, sqlite_connection


class _FakeMessenger:
    def answer_callback_query(self, *, callback_query_id: str) -> None:  # noqa: ARG002
        return

    def edit_message_reply_markup(self, *, chat_id: object, message_id: int) -> None:  # noqa: ARG002
        return

    def edit_message_text(self, *, chat_id: object, message_id: int, text: str) -> None:  # noqa: ARG002
        return


def test_public_site_handles_empty_database(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    client = _client(db_path)

    response = client.get("/ui/")

    assert response.status_code == 200
    assert "Berlin Insider public ledger" in response.text
    assert "Items" in response.text
    assert "Delivery timeline" in response.text
    assert "Feedback analytics" in response.text
    assert "Operations" in response.text
    assert '/ui/static/site.css' in response.text
    assert "basePath" in response.text
    assert "/ui" in response.text
    assert client.get("/ui/static/site.css").status_code == 200
    assert client.get("/ui/api/overview").json()["counts"]["items"] == 0


def test_public_site_returns_sanitized_populated_payloads(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    _seed_public_data(db_path)
    client = _client(db_path)

    overview = client.get("/ui/api/overview")
    items = client.get(
        "/ui/api/items",
        params={"source": "mitvergnuegen", "has_summary": "true", "search": "gallery"},
    )
    deliveries = client.get("/ui/api/deliveries")
    feedback = client.get("/ui/api/feedback")
    ops = client.get("/ui/api/ops")

    assert overview.status_code == 200
    assert overview.json()["counts"]["items"] == 2
    assert overview.json()["worker"]["last_status"] == "success"

    assert items.status_code == 200
    assert items.json()["total"] == 1
    assert items.json()["items"][0]["title"] == "Gallery opening"
    assert items.json()["items"][0]["canonical_url"] == "https://example.com/gallery"
    assert items.json()["items"][0]["has_summary"] is True

    assert deliveries.status_code == 200
    assert deliveries.json()["deliveries"][0]["primary_item"]["title"] == "Gallery opening"
    assert deliveries.json()["deliveries"][0]["alternative_item"]["title"] == "Late-night ramen"

    assert feedback.status_code == 200
    assert feedback.json()["feedback"][0]["up_votes"] == 1
    assert feedback.json()["feedback"][0]["down_votes"] == 2
    assert feedback.json()["feedback"][0]["total_votes"] == 3
    assert "telegram_user_id" not in feedback.text

    assert ops.status_code == 200
    assert {row["adapter_kind"] for row in ops.json()["sources"]} == {"HtmlAdapter", "RssAdapter"}
    assert ops.json()["detail_cache"]["total_entries"] == 1
    assert ops.json()["detail_cache"]["recent_entries"][0]["detail_length"] == len("Cached detail text")
    assert ops.json()["telegram_updates_state"]["last_update_id"] == 777


def test_public_site_keeps_health_and_webhook_routes_working(tmp_path: Path) -> None:
    db_path = tmp_path / "berlin_insider.db"
    primary_item, alternative_item = _seed_public_data(db_path)
    delivery_store = SqliteMessageDeliveryStore(db_path)
    delivery_store.upsert(
        MessageDeliveryRecord(
            message_key="daily-2026-03-12-demo",
            digest_kind=DigestKind.DAILY,
            local_date="2026-03-12",
            sent_at="2026-03-12T08:00:00+00:00",
            telegram_message_id="81",
            primary_item=primary_item,
            alternative_item=alternative_item,
        )
    )
    app = create_webhook_app(
        deps=WebhookDependencies(
            messenger=_FakeMessenger(),  # type: ignore[arg-type]
            feedback_store=SqliteFeedbackStore(db_path),
            sent_message_store=delivery_store,
            secret="secret123",
        ),
        public_db_path=db_path,
    )
    client = TestClient(app)

    health = client.get("/healthz")
    valid = client.post("/telegram/webhook/secret123", json=_callback_update("daily-2026-03-12-demo"))
    invalid = client.post("/telegram/webhook/wrong", json={"update_id": 1})

    assert health.status_code == 200
    assert health.json() == {"status": "ok"}
    assert valid.status_code == 200
    assert valid.json() == {"status": "ok"}
    assert invalid.status_code == 404


def _callback_update(message_key: str) -> dict[str, object]:
    return {
        "update_id": 100,
        "callback_query": {
            "id": "cb-1",
            "data": f"fb:v1:daily:{message_key}:up",
            "from": {"id": 321},
            "message": {"message_id": 81, "chat": {"id": -1000}, "text": "Digest"},
        },
    }


def _client(db_path: Path) -> TestClient:
    app = create_webhook_app(
        deps=WebhookDependencies(
            messenger=_FakeMessenger(),  # type: ignore[arg-type]
            feedback_store=SqliteFeedbackStore(db_path),
            sent_message_store=SqliteMessageDeliveryStore(db_path),
            secret="secret123",
        ),
        public_db_path=db_path,
    )
    return TestClient(app)


def _seed_public_data(db_path: Path) -> tuple[DeliveredItem, DeliveredItem]:
    ensure_schema(db_path)
    created_at = datetime(2026, 3, 12, 7, 30, tzinfo=UTC).isoformat()
    with sqlite_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO sources (source_id, source_url, adapter_kind, updated_at)
            VALUES (?, ?, ?, ?), (?, ?, ?, ?)
            """,
            (
                "mitvergnuegen",
                "https://mitvergnuegen.com",
                "HtmlAdapter",
                created_at,
                "food_blog",
                "https://food.example.com",
                "RssAdapter",
                created_at,
            ),
        )
        conn.execute(
            """
            INSERT INTO items (
                canonical_url, source_id, original_url, title, description, summary,
                event_start_at, event_end_at, location, category, category_confidence,
                weekend_relevance, weekend_confidence, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "https://example.com/gallery",
                "mitvergnuegen",
                "https://example.com/gallery?ref=test",
                "Gallery opening",
                "Editorial note",
                "A bright opening with late cocktails.",
                "2026-03-14T19:00:00+01:00",
                None,
                "Mitte",
                ParsedCategory.EXHIBITION.value,
                0.95,
                "likely_this_weekend",
                0.91,
                created_at,
                created_at,
            ),
        )
        conn.execute(
            """
            INSERT INTO items (
                canonical_url, source_id, original_url, title, description, summary,
                event_start_at, event_end_at, location, category, category_confidence,
                weekend_relevance, weekend_confidence, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "https://example.com/ramen",
                "food_blog",
                "https://example.com/ramen",
                "Late-night ramen",
                None,
                None,
                None,
                None,
                "Neukoelln",
                ParsedCategory.FOOD.value,
                0.88,
                "possible",
                0.52,
                created_at,
                created_at,
            ),
        )
        conn.execute(
            """
            INSERT INTO worker_state (
                id, last_attempt_at, last_run_date_local, last_status, last_success_at,
                last_error_message, last_digest_length, last_curated_count,
                last_failed_sources_json, last_source_status_json, last_delivery_at,
                last_delivery_message_id, last_delivery_error, last_run_date_by_kind_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "2026-03-12T07:45:00+00:00",
                "2026-03-12",
                "success",
                "2026-03-12T07:46:00+00:00",
                None,
                420,
                2,
                "[]",
                '{"mitvergnuegen":"success","food_blog":"success"}',
                "2026-03-12T08:00:00+00:00",
                "81",
                None,
                '{"daily":"2026-03-12"}',
            ),
        )
        conn.execute(
            """
            INSERT INTO telegram_updates_state (id, last_update_id)
            VALUES (1, 777)
            """,
        )
        conn.commit()

    primary_item = _delivered_item(db_path, "https://example.com/gallery")
    alternative_item = _delivered_item(db_path, "https://example.com/ramen")
    delivery_store = SqliteMessageDeliveryStore(db_path)
    delivery_store.upsert(
        MessageDeliveryRecord(
            message_key="weekend-2026-03-12-public",
            digest_kind=DigestKind.WEEKEND,
            local_date="2026-03-12",
            sent_at="2026-03-12T08:00:00+00:00",
            telegram_message_id="81",
            primary_item=primary_item,
            alternative_item=alternative_item,
        )
    )
    feedback_store = SqliteFeedbackStore(db_path)
    feedback_store.upsert(_feedback_event("weekend-2026-03-12-public", 101, "up"))
    feedback_store.upsert(_feedback_event("weekend-2026-03-12-public", 202, "down"))
    feedback_store.upsert(_feedback_event("weekend-2026-03-12-public", 303, "down"))
    SqliteDetailCacheStore(db_path).upsert_detail(
        url="https://example.com/gallery",
        source_id="mitvergnuegen",
        detail_text="Cached detail text",
        detail_hash="hash-1",
        detail_metadata={"language": "en", "venue": "Mitte"},
        detail_status="ok",
    )
    return primary_item, alternative_item


def _delivered_item(db_path: Path, canonical_url: str) -> DeliveredItem:
    with sqlite_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT item_id, canonical_url, title, summary, location, category, event_start_at, event_end_at
            FROM items
            WHERE canonical_url = ?
            """,
            (canonical_url,),
        ).fetchone()
    assert row is not None
    return DeliveredItem(
        item_id=int(row[0]),
        canonical_url=str(row[1]),
        title=str(row[2]) if row[2] is not None else None,
        summary=str(row[3]) if row[3] is not None else None,
        location=str(row[4]) if row[4] is not None else None,
        category=ParsedCategory(str(row[5])) if row[5] is not None else None,
        event_start_at=str(row[6]) if row[6] is not None else None,
        event_end_at=str(row[7]) if row[7] is not None else None,
    )


def _feedback_event(message_key: str, telegram_user_id: int, vote: str):
    now = "2026-03-12T08:05:00+00:00"
    return type(
        "_FeedbackEvent",
        (),
        {
            "message_key": message_key,
            "telegram_user_id": telegram_user_id,
            "vote": vote,
            "voted_at": now,
            "updated_at": now,
        },
    )()
