from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import httpx

from berlin_insider.messenger.models import DeliveryResult, FeedbackMetadata, MessengerError

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
        bot_token = _normalized_env_value(source.get("TELEGRAM_BOT_TOKEN"))
        chat_id = _normalized_env_value(source.get("TELEGRAM_CHAT_ID"))
        api_base = _normalized_env_value(source.get("TELEGRAM_API_BASE")) or _DEFAULT_API_BASE
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

    def send_digest(
        self, *, text: str, feedback_metadata: FeedbackMetadata | None = None
    ) -> DeliveryResult:
        """Send digest text through Telegram Bot API."""
        response = self._post_send_message(text=text, feedback_metadata=feedback_metadata)
        message_id = _extract_message_id(response)
        return DeliveryResult(
            delivered_at=datetime.now(UTC),
            external_message_id=str(message_id),
        )

    def get_updates(
        self, *, offset: int | None = None, timeout_seconds: int = 0
    ) -> list[dict[str, object]]:
        """Fetch Telegram updates from the Bot API."""
        payload: dict[str, object] = {"timeout": timeout_seconds}
        if offset is not None:
            payload["offset"] = offset
        response = self._post_api("getUpdates", payload=payload)
        payload_obj = _json_payload(response)
        if payload_obj.get("ok") is not True:
            raise MessengerError("telegram API rejected updates request")
        result = payload_obj.get("result")
        if not isinstance(result, list):
            raise MessengerError("telegram updates payload has unexpected shape")
        return [item for item in result if isinstance(item, dict)]

    def answer_callback_query(self, *, callback_query_id: str) -> None:
        """Acknowledge callback query events."""
        self._post_api("answerCallbackQuery", payload={"callback_query_id": callback_query_id})

    def edit_message_reply_markup(self, *, chat_id: int | str, message_id: int) -> None:
        """Remove inline keyboard after feedback is recorded."""
        self._post_api(
            "editMessageReplyMarkup",
            payload={"chat_id": str(chat_id), "message_id": message_id, "reply_markup": {}},
        )

    def set_webhook(
        self, *, url: str, certificate_path: Path | None = None, ip_address: str | None = None
    ) -> None:
        """Configure Telegram webhook endpoint for this bot."""
        payload: dict[str, object] = {"url": url}
        if ip_address:
            payload["ip_address"] = ip_address
        if certificate_path is None:
            response = self._post_api("setWebhook", payload=payload)
        else:
            response = self._post_webhook_with_cert(
                url=url, certificate_path=certificate_path, ip_address=ip_address
            )
        payload_obj = _json_payload(response)
        if payload_obj.get("ok") is not True:
            description = payload_obj.get("description")
            message = description if isinstance(description, str) else "unknown telegram error"
            raise MessengerError(f"telegram webhook registration failed: {message}")

    def _post_send_message(
        self, *, text: str, feedback_metadata: FeedbackMetadata | None = None
    ) -> httpx.Response:
        payload = _send_message_payload(
            chat_id=self._chat_id,
            text=text,
            feedback_metadata=feedback_metadata,
        )
        return self._post_api("sendMessage", payload=payload)

    def _post_api(self, method: str, *, payload: dict[str, object]) -> httpx.Response:
        url = self._api_url(method)
        try:
            response = httpx.post(url, json=payload, timeout=self._timeout_seconds)
        except (
            httpx.HTTPError
        ) as exc:  # pragma: no cover - exercised via tests with subclass errors
            raise MessengerError(f"telegram request failed: {exc}") from exc
        _validate_http_status(response)
        return response

    def _post_webhook_with_cert(
        self, *, url: str, certificate_path: Path, ip_address: str | None = None
    ) -> httpx.Response:
        endpoint = self._api_url("setWebhook")
        if not certificate_path.exists():
            raise MessengerError(f"telegram webhook certificate not found: {certificate_path}")
        data: dict[str, str] = {"url": url}
        if ip_address:
            data["ip_address"] = ip_address
        try:
            with certificate_path.open("rb") as cert_file:
                response = httpx.post(
                    endpoint,
                    data=data,
                    files={"certificate": (certificate_path.name, cert_file)},
                    timeout=self._timeout_seconds,
                )
        except OSError as exc:
            raise MessengerError(f"telegram webhook certificate read failed: {exc}") from exc
        except httpx.HTTPError as exc:
            raise MessengerError(f"telegram request failed: {exc}") from exc
        _validate_http_status(response)
        return response

    def _api_url(self, method: str) -> str:
        return f"{self._api_base}/bot{self._bot_token}/{method}"


def _send_message_payload(
    *, chat_id: str, text: str, feedback_metadata: FeedbackMetadata | None = None
) -> dict[str, object]:
    payload: dict[str, object] = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }
    if feedback_metadata is not None:
        kind = feedback_metadata.digest_kind.value
        key = feedback_metadata.message_key
        payload["reply_markup"] = {
            "inline_keyboard": [
                [
                    {"text": "👍", "callback_data": f"fb:v1:{kind}:{key}:up"},
                    {"text": "👎", "callback_data": f"fb:v1:{kind}:{key}:down"},
                ]
            ]
        }
    return payload


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


def _normalized_env_value(raw: str | None) -> str | None:
    if raw is None:
        return None
    cleaned = raw.strip()
    if not cleaned:
        return None
    return cleaned
