from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import UTC, datetime

import httpx

from berlin_insider.messenger.models import DeliveryResult, MessengerError

_DEFAULT_API_BASE = "https://api.telegram.org"


class TelegramMessenger:
    def __init__(
        self,
        *,
        bot_token: str,
        chat_id: str,
        api_base: str = _DEFAULT_API_BASE,
        timeout_seconds: float = 20.0,
    ) -> None:
        self._bot_token = bot_token
        self._chat_id = chat_id
        self._api_base = api_base.rstrip("/")
        self._timeout_seconds = timeout_seconds

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> TelegramMessenger:
        """Create an instance from TELEGRAM_* environment variables."""
        source = env if env is not None else os.environ
        bot_token = source.get("TELEGRAM_BOT_TOKEN")
        chat_id = source.get("TELEGRAM_CHAT_ID")
        api_base = source.get("TELEGRAM_API_BASE", _DEFAULT_API_BASE)
        missing = [
            key
            for key, value in (
                ("TELEGRAM_BOT_TOKEN", bot_token),
                ("TELEGRAM_CHAT_ID", chat_id),
            )
            if not value
        ]
        if missing:
            joined = ", ".join(missing)
            raise MessengerError(f"missing required Telegram environment variables: {joined}")
        if bot_token is None or chat_id is None:
            raise MessengerError("missing required Telegram environment variables")
        return cls(bot_token=bot_token, chat_id=chat_id, api_base=api_base)

    def send_digest(self, *, text: str) -> DeliveryResult:
        """Send digest text through Telegram Bot API."""
        response = self._post_send_message(text=text)
        message_id = _extract_message_id(response)
        return DeliveryResult(
            delivered_at=datetime.now(UTC),
            external_message_id=str(message_id),
        )

    def _post_send_message(self, *, text: str) -> httpx.Response:
        payload = _send_message_payload(chat_id=self._chat_id, text=text)
        url = f"{self._api_base}/bot{self._bot_token}/sendMessage"
        try:
            response = httpx.post(url, json=payload, timeout=self._timeout_seconds)
        except (
            httpx.HTTPError
        ) as exc:  # pragma: no cover - exercised via tests with subclass errors
            raise MessengerError(f"telegram request failed: {exc}") from exc
        _validate_http_status(response)
        return response


def _send_message_payload(*, chat_id: str, text: str) -> dict[str, object]:
    return {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }


def _validate_http_status(response: httpx.Response) -> None:
    if response.status_code < 400:
        return
    detail = response.text.strip() or f"HTTP {response.status_code}"
    raise MessengerError(f"telegram API returned {response.status_code}: {detail}")


def _extract_message_id(response: httpx.Response) -> int:
    payload_obj = _json_payload(response)
    if payload_obj.get("ok") is not True:
        description = payload_obj.get("description")
        message = description if isinstance(description, str) else "unknown telegram error"
        raise MessengerError(f"telegram API rejected message: {message}")
    result_obj = payload_obj.get("result")
    if not isinstance(result_obj, dict):
        raise MessengerError("telegram API payload missing result object")
    message_id = result_obj.get("message_id")
    if not isinstance(message_id, int):
        raise MessengerError("telegram API payload missing message_id")
    return message_id


def _json_payload(response: httpx.Response) -> dict[str, object]:
    try:
        payload_obj = response.json()
    except ValueError as exc:
        raise MessengerError("telegram API returned invalid JSON payload") from exc
    if not isinstance(payload_obj, dict):
        raise MessengerError("telegram API payload has unexpected shape")
    return payload_obj
