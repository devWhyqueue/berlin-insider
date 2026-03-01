from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from threading import Lock

import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI

from berlin_insider.feedback.store import SqliteFeedbackStore, SqliteSentMessageStore
from berlin_insider.feedback.webhook import WebhookDependencies, create_webhook_app
from berlin_insider.messenger.telegram import TelegramMessenger
from berlin_insider.scheduler.models import ScheduleConfig
from berlin_insider.scheduler.orchestrator import Scheduler
from berlin_insider.scheduler.store import SqliteSchedulerStateStore

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class WorkerConfig:
    db_path: Path
    target_items: int
    schedule: ScheduleConfig
    host: str
    port: int
    webhook_public_base_url: str
    telegram_webhook_secret: str
    telegram_webhook_cert_path: Path | None = None


@dataclass(slots=True)
class _RuntimeState:
    state_store: SqliteSchedulerStateStore
    sent_message_store: SqliteSentMessageStore
    app: FastAPI
    scheduler: BackgroundScheduler


class Worker:
    def __init__(
        self,
        *,
        config: WorkerConfig,
        scheduler: Scheduler | None = None,
        messenger: TelegramMessenger | None = None,
    ) -> None:
        self._config = config
        self._scheduler = scheduler or Scheduler()
        self._messenger = messenger or TelegramMessenger.from_env()
        self._run_lock = Lock()

    def run(self) -> None:
        """Start the always-on worker with scheduler and webhook server."""
        runtime = self._prepare_runtime_state()
        runtime.scheduler.start()
        self._try_run_cycle(
            reason="startup catch-up",
            state_store=runtime.state_store,
            sent_message_store=runtime.sent_message_store,
        )
        try:
            uvicorn.run(runtime.app, host=self._config.host, port=self._config.port)
        finally:
            runtime.scheduler.shutdown(wait=False)

    def _prepare_runtime_state(self) -> _RuntimeState:
        state_store = SqliteSchedulerStateStore(self._config.db_path)
        sent_message_store = SqliteSentMessageStore(self._config.db_path)
        feedback_store = SqliteFeedbackStore(self._config.db_path)
        self._register_webhook()
        app = create_webhook_app(
            deps=WebhookDependencies(
                messenger=self._messenger,
                feedback_store=feedback_store,
                sent_message_store=sent_message_store,
                secret=self._config.telegram_webhook_secret,
            )
        )
        scheduler = _build_scheduler(
            worker=self, state_store=state_store, sent_message_store=sent_message_store
        )
        return _RuntimeState(
            state_store=state_store,
            sent_message_store=sent_message_store,
            app=app,
            scheduler=scheduler,
        )

    def _register_webhook(self) -> None:
        webhook_url = _build_webhook_url(
            base_url=self._config.webhook_public_base_url,
            secret=self._config.telegram_webhook_secret,
        )
        cert_path = _resolve_webhook_cert_path(self._config.telegram_webhook_cert_path)
        self._messenger.set_webhook(url=webhook_url, certificate_path=cert_path)
        if cert_path is None:
            logger.info("Registered Telegram webhook: %s", webhook_url)
            return
        logger.info("Registered Telegram webhook with certificate: %s (cert=%s)", webhook_url, cert_path)

    def _try_run_cycle(
        self,
        *,
        reason: str,
        state_store: SqliteSchedulerStateStore,
        sent_message_store: SqliteSentMessageStore,
    ) -> None:
        if not self._run_lock.acquire(blocking=False):
            logger.info("Worker skipped cycle (%s): another run is in progress", reason)
            return
        try:
            result = self._scheduler.run_once(
                state_store=state_store,
                config=self._config.schedule,
                db_path=self._config.db_path,
                target_items=self._config.target_items,
                force=False,
                messenger=self._messenger,
                sent_message_store=sent_message_store,
            )
            logger.info(
                "Worker cycle (%s): status=%s due=%s executed=%s delivered=%s reason=%s",
                reason,
                result.status,
                result.due,
                result.executed,
                result.delivered,
                result.reason,
            )
        finally:
            self._run_lock.release()


def _build_scheduler(
    *,
    worker: Worker,
    state_store: SqliteSchedulerStateStore,
    sent_message_store: SqliteSentMessageStore,
) -> BackgroundScheduler:
    cfg = worker._config.schedule
    scheduler = BackgroundScheduler(timezone=cfg.timezone, job_defaults={"coalesce": True})
    weekend_day = _weekday_to_cron_alias(cfg.weekend_weekday)
    if weekend_day is None:
        raise ValueError(f"invalid weekend weekday: {cfg.weekend_weekday}")
    daily_days = _daily_weekdays_excluding(weekend_day)
    scheduler.add_job(
        worker._try_run_cycle,
        trigger=CronTrigger(day_of_week=daily_days, hour=cfg.daily_hour, minute=cfg.daily_minute),
        kwargs={
            "reason": "daily trigger",
            "state_store": state_store,
            "sent_message_store": sent_message_store,
        },
        id="daily-digest",
        max_instances=1,
    )
    scheduler.add_job(
        worker._try_run_cycle,
        trigger=CronTrigger(
            day_of_week=weekend_day, hour=cfg.weekend_hour, minute=cfg.weekend_minute
        ),
        kwargs={
            "reason": "weekend trigger",
            "state_store": state_store,
            "sent_message_store": sent_message_store,
        },
        id="weekend-digest",
        max_instances=1,
    )
    return scheduler


def _daily_weekdays_excluding(weekend_weekday: str) -> str:
    ordered = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    weekend = _weekday_to_cron_alias(weekend_weekday)
    if weekend is None:
        return ",".join(ordered)
    return ",".join(day for day in ordered if day != weekend)


def _weekday_to_cron_alias(weekday: str) -> str | None:
    aliases = {
        "mon": "mon",
        "tue": "tue",
        "wed": "wed",
        "thu": "thu",
        "fri": "fri",
        "sat": "sat",
        "sun": "sun",
        "monday": "mon",
        "tuesday": "tue",
        "wednesday": "wed",
        "thursday": "thu",
        "friday": "fri",
        "saturday": "sat",
        "sunday": "sun",
    }
    return aliases.get(weekday.lower())


def _build_webhook_url(*, base_url: str, secret: str) -> str:
    return f"{base_url.rstrip('/')}/telegram/webhook/{secret}"


def _resolve_webhook_cert_path(configured_path: Path | None) -> Path | None:
    if configured_path is not None:
        if configured_path.exists():
            return configured_path
        logger.warning("Webhook cert path does not exist, skipping custom cert: %s", configured_path)
        return None
    default_path = Path("/etc/nginx/ssl/berlin-insider.crt")
    if default_path.exists():
        return default_path
    return None
