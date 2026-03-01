# Berlin Insider

Fetcher component for collecting Berlin weekend content from curated sources.

## Setup

```powershell
uv sync --all-groups
uv run playwright install chromium
```

## Run

```powershell
uv run berlin-insider fetch
uv run berlin-insider fetch --json
uv run berlin-insider fetch --fetch-only
uv run berlin-insider fetch --parse-only
uv run berlin-insider fetch --target-items 10
uv run berlin-insider fetch --db-path .data/berlin_insider.db
uv run berlin-insider fetch --source eventbrite_berlin_weekend
uv run berlin-insider fetch --digest
uv run berlin-insider fetch --digest --digest-kind daily
uv run berlin-insider fetch --json --digest
uv run berlin-insider worker --webhook-public-base-url https://example.com --telegram-webhook-secret my-secret
```

## Worker Runtime

`worker` is the primary runtime and stays alive as a background service.
It runs cron-style digest jobs in-process and receives Telegram feedback through webhook callbacks.
By default it runs daily tips on every day except Friday at 08:00 and weekend digests on Friday 08:00
in `Europe/Berlin`, persisting state in `.data/berlin_insider.db`.

Required environment variables:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `WEBHOOK_PUBLIC_BASE_URL` (for webhook registration, for example `https://bot.example.com`)
- `TELEGRAM_WEBHOOK_SECRET`

Optional:

- `TELEGRAM_API_BASE` (default: `https://api.telegram.org`)
- `TELEGRAM_WEBHOOK_IP` (optional static IP passed to Telegram `setWebhook` to avoid stale DNS resolution)
- `OPENAI_API_KEY` (enables one-sentence `gpt-5-mini` summaries for parsed items/digests)
- `WORKER_HOST` (default: `0.0.0.0`)
- `WORKER_PORT` (default: `8080`)
- `WORKER_TIMEZONE` (default: `Europe/Berlin`)
- `WORKER_DAILY_HOUR` / `WORKER_DAILY_MINUTE`
- `WORKER_WEEKEND_WEEKDAY` / `WORKER_WEEKEND_HOUR` / `WORKER_WEEKEND_MINUTE`
- `WORKER_DB_PATH` (default: `.data/berlin_insider.db`)
- `WORKER_TARGET_ITEMS` (default: `7`)
- `TELEGRAM_WEBHOOK_CERT_PATH` (default: `/etc/nginx/ssl/berlin-insider.crt`)

When `OPENAI_API_KEY` is missing or summary generation fails for an item, the pipeline still runs
and sends digests; that item is shown without a summary line.

Run locally:

```powershell
uv run berlin-insider worker --webhook-public-base-url https://example.com --telegram-webhook-secret my-secret
```

## Deploy on Ubuntu (systemd)

This setup assumes the repo is cloned at `~/berlin-insider` and `.env` exists there.

1. Install runtime dependencies:

```bash
sudo apt update
sudo apt install -y git curl ca-certificates
curl -LsSf https://astral.sh/uv/install.sh | sh
source "$HOME/.local/bin/env"
```

2. Install project dependencies:

```bash
cd ~/berlin-insider
uv sync --all-groups
uv run playwright install --with-deps chromium
```

3. Webhook/TLS checklist (required for Telegram callbacks):

- Use a public DNS name (for example `bot.example.com`) that points to the VM.
- Open inbound TCP `80` and `443` in cloud/network firewall.
- Ensure a reverse proxy serves `https://<your-domain>` on `443` and forwards to `http://127.0.0.1:8080`.
- If another service already owns `80/443` (for example Pi-hole), move that service to different web ports first.
- Set `WEBHOOK_PUBLIC_BASE_URL=https://<your-domain>` in `.env`.
- If you use a self-signed certificate, set `TELEGRAM_WEBHOOK_CERT_PATH` to that cert file (default: `/etc/nginx/ssl/berlin-insider.crt`).

Minimal nginx site example (HTTP redirect + HTTPS reverse proxy):

```nginx
server {
    listen 80;
    server_name <your-domain>;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl;
    server_name <your-domain>;
    ssl_certificate /etc/nginx/ssl/berlin-insider.crt;
    ssl_certificate_key /etc/nginx/ssl/berlin-insider.key;
    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

4. Create systemd service (`/etc/systemd/system/berlin-insider-worker.service`):

```ini
[Unit]
Description=Berlin Insider worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=<your-linux-username>
WorkingDirectory=/home/<your-linux-username>/berlin-insider
ExecStart=/home/<your-linux-username>/.local/bin/uv run berlin-insider worker --timezone Europe/Berlin --db-path .data/berlin_insider.db
Restart=always
RestartSec=5
```

5. Enable service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now berlin-insider-worker.service
```

6. Verify:

```bash
systemctl status berlin-insider-worker.service
journalctl -u berlin-insider-worker.service -n 100 --no-pager
```

Also verify Telegram webhook registration:

```bash
cd ~/berlin-insider
TOKEN=$(grep '^TELEGRAM_BOT_TOKEN=' .env | cut -d= -f2- | tr -d '\r')
curl -s "https://api.telegram.org/bot${TOKEN}/getWebhookInfo"
```

## Troubleshooting

Check service health:

```bash
systemctl status berlin-insider-worker.service
```

Read recent worker logs:

```bash
journalctl -u berlin-insider-worker.service -n 200 --no-pager
journalctl -u berlin-insider-worker.service -f
```

Inspect unit configuration:

```bash
systemctl cat berlin-insider-worker.service
```

Default schedule behavior:

- `WEEKEND` digest: Friday at 08:00 (`Europe/Berlin`)
- `DAILY` digest: every other day at 08:00 (`Europe/Berlin`)

If you miss a message, the JSON output and logs usually explain why in fields like `due`, `reason`, `status`, and `delivered`.
