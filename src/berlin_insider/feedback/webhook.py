from __future__ import annotations

import logging
from dataclasses import dataclass

from fastapi import FastAPI, HTTPException

from berlin_insider.feedback.ingest import ingest_feedback_update
from berlin_insider.feedback.store import SqliteFeedbackStore, SqliteMessageDeliveryStore
from berlin_insider.messenger.telegram import TelegramMessenger

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class WebhookDependencies:
    messenger: TelegramMessenger
    feedback_store: SqliteFeedbackStore
    sent_message_store: SqliteMessageDeliveryStore
    secret: str


def create_webhook_app(*, deps: WebhookDependencies) -> FastAPI:
    """Create FastAPI application that handles Telegram webhook feedback updates."""
    app = FastAPI()
    app.get("/healthz")(_build_healthz_handler())
    app.post("/telegram/webhook/{secret}")(_build_telegram_handler(deps=deps))
    return app


def _build_healthz_handler():
    async def _healthz() -> dict[str, str]:
        return {"status": "ok"}

    return _healthz


def _build_telegram_handler(*, deps: WebhookDependencies):
    async def _telegram_webhook(secret: str, update: dict[str, object]) -> dict[str, str]:
        if secret != deps.secret:
            raise HTTPException(status_code=404, detail="not found")
        result = ingest_feedback_update(
            update=update,
            messenger=deps.messenger,
            feedback_store=deps.feedback_store,
            sent_message_store=deps.sent_message_store,
        )
        logger.info(
            "Webhook feedback: processed=%s persisted=%s ignored=%s answered=%s",
            result.processed_callback,
            result.persisted_vote,
            result.ignored,
            result.answered_callback,
        )
        return {"status": "ok"}

    return _telegram_webhook
