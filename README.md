# Berlin Insider

Berlin event fetcher, curator, Telegram digest worker, and read-only public UI.

## Setup

```bash
uv sync --all-groups
uv run playwright install chromium
```

## Common Commands

```bash
uv run berlin-insider fetch
uv run berlin-insider fetch --json
uv run berlin-insider fetch --fetch-only
uv run berlin-insider fetch --parse-only
uv run berlin-insider fetch --digest --digest-kind weekend
uv run berlin-insider fetch --source eventbrite_berlin_weekend
uv run berlin-insider worker --run-once
uv run berlin-insider worker --webhook-public-base-url https://example.com --telegram-webhook-secret secret
uv run pytest
uv run ruff check .
uv run pyright
```

Default SQLite path: `.data/berlin_insider.db`.

## Runtime

`worker` is the production runtime. It:

- fetches, parses, curates, and sends Telegram digests;
- runs daily tips every non-weekend-digest day at 08:00 Europe/Berlin;
- runs weekend digests on Friday at 08:00 Europe/Berlin;
- receives Telegram feedback at `POST /telegram/webhook/{secret}`;
- serves the public UI at `GET /ui/`;
- exposes health at `GET /healthz`.

Required environment:

```bash
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
WEBHOOK_PUBLIC_BASE_URL=https://your-domain.example
TELEGRAM_WEBHOOK_SECRET=
```

Useful optional environment:

```bash
OPENAI_API_KEY=                 # enables GPT summaries
OPENAI_SUMMARY_MODEL=gpt-5-mini
OPENAI_SUMMARY_TIMEOUT_SECONDS=20
OPENAI_SUMMARY_MAX_OUTPUT_TOKENS=100
OPENAI_SUMMARY_RETRY_ATTEMPTS=1
TELEGRAM_API_BASE=https://api.telegram.org
WORKER_DB_PATH=.data/berlin_insider.db
WORKER_HOST=0.0.0.0
WORKER_PORT=8080
WORKER_TIMEZONE=Europe/Berlin
WORKER_TARGET_ITEMS=7
WORKER_WEEKEND_WEEKDAY=friday
WORKER_DAILY_HOUR=8
WORKER_WEEKEND_HOUR=8
TELEGRAM_WEBHOOK_CERT_PATH=/etc/nginx/ssl/berlin-insider.crt
TELEGRAM_WEBHOOK_IP=
```

`.env` is loaded automatically without overriding existing environment values.

## Public UI

Worker routes:

- `GET /ui/`
- `GET /ui/api/overview`
- `GET /ui/api/items`
- `GET /ui/api/deliveries`
- `GET /ui/api/feedback`
- `GET /ui/api/ops`

For the current deployment, the UI is expected at:

```text
https://berlin-insider.crabdance.com/ui/
```

## Deployment Notes

Production runs on Ubuntu with systemd from `~/berlin-insider`.

Minimal service shape:

```ini
[Unit]
Description=Berlin Insider worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/berlin-insider
ExecStart=/home/ubuntu/.local/bin/uv run berlin-insider worker --timezone Europe/Berlin --db-path .data/berlin_insider.db
Restart=always
RestartSec=5
```

Reverse proxy only these paths to `http://127.0.0.1:8080`:

- `/ui/`
- `/ui/api/`
- `/telegram/webhook/`
- `/healthz`

Do not proxy `/` wholesale if the vhost also serves Pi-hole or another root app.

## Operations

```bash
systemctl status berlin-insider-worker.service
journalctl -u berlin-insider-worker.service -n 200 --no-pager
journalctl -u berlin-insider-worker.service -f
systemctl cat berlin-insider-worker.service
```

Check Telegram webhook state:

```bash
TOKEN=$(grep '^TELEGRAM_BOT_TOKEN=' .env | cut -d= -f2- | tr -d '\r')
curl -s "https://api.telegram.org/bot${TOKEN}/getWebhookInfo"
```

If a digest does not send, inspect JSON/log fields such as `due`, `reason`, `status`, and `delivered`.
