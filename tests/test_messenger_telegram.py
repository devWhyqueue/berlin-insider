from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from berlin_insider.digest import DigestKind
from berlin_insider.messenger.models import FeedbackMetadata
from berlin_insider.messenger.models import MessengerError
from berlin_insider.messenger.telegram import TelegramMessenger


def test_telegram_messenger_send_success(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_post(url, *, json, timeout):  # noqa: ANN001, ANN202
        captured["url"] = url
        captured["json"] = json
        captured["timeout"] = timeout
        return httpx.Response(status_code=200, json={"ok": True, "result": {"message_id": 123}})

    monkeypatch.setattr("berlin_insider.messenger.telegram.httpx.post", _fake_post)
    messenger = TelegramMessenger(bot_token="token", chat_id="-10001")

    result = messenger.send_digest(text="Berlin Insider")

    assert result.external_message_id == "123"
    assert captured["url"] == "https://api.telegram.org/bottoken/sendMessage"
    assert captured["json"] == {
        "chat_id": "-10001",
        "text": "Berlin Insider",
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }


def test_telegram_messenger_raises_on_http_error(monkeypatch) -> None:
    def _fake_post(url, *, json, timeout):  # noqa: ANN001, ANN202
        return httpx.Response(status_code=403, text="forbidden")

    monkeypatch.setattr("berlin_insider.messenger.telegram.httpx.post", _fake_post)
    messenger = TelegramMessenger(bot_token="token", chat_id="-10001")

    with pytest.raises(MessengerError, match="403"):
        messenger.send_digest(text="Berlin Insider")


def test_telegram_messenger_raises_on_rejected_payload(monkeypatch) -> None:
    def _fake_post(url, *, json, timeout):  # noqa: ANN001, ANN202
        return httpx.Response(status_code=200, json={"ok": False, "description": "Bad Request"})

    monkeypatch.setattr("berlin_insider.messenger.telegram.httpx.post", _fake_post)
    messenger = TelegramMessenger(bot_token="token", chat_id="-10001")

    with pytest.raises(MessengerError, match="Bad Request"):
        messenger.send_digest(text="Berlin Insider")


def test_telegram_messenger_raises_on_invalid_json_payload(monkeypatch) -> None:
    def _fake_post(url, *, json, timeout):  # noqa: ANN001, ANN202
        return httpx.Response(status_code=200, text="not-json")

    monkeypatch.setattr("berlin_insider.messenger.telegram.httpx.post", _fake_post)
    messenger = TelegramMessenger(bot_token="token", chat_id="-10001")

    with pytest.raises(MessengerError, match="invalid JSON"):
        messenger.send_digest(text="Berlin Insider")


def test_telegram_messenger_from_env_requires_keys() -> None:
    with pytest.raises(MessengerError, match="TELEGRAM_BOT_TOKEN"):
        TelegramMessenger.from_env(env={"TELEGRAM_CHAT_ID": "-10001"})


def test_telegram_messenger_sends_feedback_buttons(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_post(url, *, json, timeout):  # noqa: ANN001, ANN202
        captured["json"] = json
        return httpx.Response(status_code=200, json={"ok": True, "result": {"message_id": 123}})

    monkeypatch.setattr("berlin_insider.messenger.telegram.httpx.post", _fake_post)
    messenger = TelegramMessenger(bot_token="token", chat_id="-10001")
    messenger.send_digest(
        text="Berlin Insider",
        feedback_metadata=FeedbackMetadata(
            digest_kind=DigestKind.DAILY,
            message_key="daily-2026-02-23-abcdef",
        ),
    )
    payload = captured["json"]
    assert isinstance(payload, dict)
    markup = payload["reply_markup"]
    assert isinstance(markup, dict)
    keyboard = markup["inline_keyboard"]
    assert keyboard[0][0]["callback_data"] == "fb:v1:daily:daily-2026-02-23-abcdef:up"
    assert keyboard[0][1]["callback_data"] == "fb:v1:daily:daily-2026-02-23-abcdef:down"


def test_telegram_messenger_registers_webhook(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_post(url, *, json, timeout):  # noqa: ANN001, ANN202
        captured["url"] = url
        captured["json"] = json
        return httpx.Response(status_code=200, json={"ok": True, "result": True})

    monkeypatch.setattr("berlin_insider.messenger.telegram.httpx.post", _fake_post)
    messenger = TelegramMessenger(bot_token="token", chat_id="-10001")

    messenger.set_webhook(url="https://example.com/telegram/webhook/secret")

    assert captured["url"] == "https://api.telegram.org/bottoken/setWebhook"
    assert captured["json"] == {"url": "https://example.com/telegram/webhook/secret"}


def test_telegram_messenger_registers_webhook_with_explicit_ip(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_post(url, *, json, timeout):  # noqa: ANN001, ANN202
        captured["url"] = url
        captured["json"] = json
        return httpx.Response(status_code=200, json={"ok": True, "result": True})

    monkeypatch.setattr("berlin_insider.messenger.telegram.httpx.post", _fake_post)
    messenger = TelegramMessenger(bot_token="token", chat_id="-10001")

    messenger.set_webhook(
        url="https://example.com/telegram/webhook/secret",
        ip_address="203.0.113.10",
    )

    assert captured["url"] == "https://api.telegram.org/bottoken/setWebhook"
    assert captured["json"] == {
        "url": "https://example.com/telegram/webhook/secret",
        "ip_address": "203.0.113.10",
    }


def test_telegram_messenger_registers_webhook_with_certificate(monkeypatch, tmp_path: Path) -> None:
    cert_path = tmp_path / "webhook.crt"
    cert_path.write_text("dummy-cert", encoding="utf-8")
    captured: dict[str, object] = {}

    def _fake_post(url, *, json=None, data=None, files=None, timeout=None):  # noqa: ANN001, ANN202
        captured["url"] = url
        captured["json"] = json
        captured["data"] = data
        captured["has_files"] = files is not None
        return httpx.Response(status_code=200, json={"ok": True, "result": True})

    monkeypatch.setattr("berlin_insider.messenger.telegram.httpx.post", _fake_post)
    messenger = TelegramMessenger(bot_token="token", chat_id="-10001")

    messenger.set_webhook(
        url="https://example.com/telegram/webhook/secret", certificate_path=cert_path
    )

    assert captured["url"] == "https://api.telegram.org/bottoken/setWebhook"
    assert captured["json"] is None
    assert captured["data"] == {"url": "https://example.com/telegram/webhook/secret"}
    assert captured["has_files"] is True


def test_telegram_messenger_registers_webhook_with_certificate_and_ip(
    monkeypatch, tmp_path: Path
) -> None:
    cert_path = tmp_path / "webhook.crt"
    cert_path.write_text("dummy-cert", encoding="utf-8")
    captured: dict[str, object] = {}

    def _fake_post(url, *, json=None, data=None, files=None, timeout=None):  # noqa: ANN001, ANN202
        captured["url"] = url
        captured["json"] = json
        captured["data"] = data
        captured["has_files"] = files is not None
        return httpx.Response(status_code=200, json={"ok": True, "result": True})

    monkeypatch.setattr("berlin_insider.messenger.telegram.httpx.post", _fake_post)
    messenger = TelegramMessenger(bot_token="token", chat_id="-10001")

    messenger.set_webhook(
        url="https://example.com/telegram/webhook/secret",
        certificate_path=cert_path,
        ip_address="203.0.113.10",
    )

    assert captured["url"] == "https://api.telegram.org/bottoken/setWebhook"
    assert captured["json"] is None
    assert captured["data"] == {
        "url": "https://example.com/telegram/webhook/secret",
        "ip_address": "203.0.113.10",
    }
    assert captured["has_files"] is True


def test_telegram_messenger_edits_feedback_message(monkeypatch) -> None:
    payloads: list[dict[str, object]] = []

    def _fake_post(url, *, json=None, timeout=None):  # noqa: ANN001, ANN202
        payloads.append({"url": url, "json": json})
        return httpx.Response(status_code=200, json={"ok": True, "result": True})

    monkeypatch.setattr("berlin_insider.messenger.telegram.httpx.post", _fake_post)
    messenger = TelegramMessenger(bot_token="token", chat_id="-10001")

    messenger.edit_message_reply_markup(chat_id=-1000, message_id=42)
    messenger.edit_message_text(chat_id=-1000, message_id=42, text="Updated")

    assert payloads[0]["url"] == "https://api.telegram.org/bottoken/editMessageReplyMarkup"
    assert payloads[0]["json"] == {"chat_id": "-1000", "message_id": 42, "reply_markup": {}}
    assert payloads[1]["url"] == "https://api.telegram.org/bottoken/editMessageText"
    assert payloads[1]["json"] == {
        "chat_id": "-1000",
        "message_id": 42,
        "text": "Updated",
        "disable_web_page_preview": True,
    }
