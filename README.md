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
- `OPENAI_API_KEY` (enables one-sentence `gpt-5-mini` summaries for parsed items/digests)
- `WORKER_HOST` (default: `0.0.0.0`)
- `WORKER_PORT` (default: `8080`)
- `WORKER_TIMEZONE` (default: `Europe/Berlin`)
- `WORKER_DAILY_HOUR` / `WORKER_DAILY_MINUTE`
- `WORKER_WEEKEND_WEEKDAY` / `WORKER_WEEKEND_HOUR` / `WORKER_WEEKEND_MINUTE`
- `WORKER_DB_PATH` (default: `.data/berlin_insider.db`)
- `WORKER_TARGET_ITEMS` (default: `7`)

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

3. Create systemd service (`/etc/systemd/system/berlin-insider-worker.service`):

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

4. Enable service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now berlin-insider-worker.service
```

5. Verify:

```bash
systemctl status berlin-insider-worker.service
journalctl -u berlin-insider-worker.service -n 100 --no-pager
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

If OpenAI summaries fail with `401`:

1. Confirm the key is loaded in the same shell/process:

```bash
cd ~/berlin-insider
~/.local/bin/uv run python -c "import berlin_insider.cli as c, os; c._load_dotenv_defaults(); k=os.getenv('OPENAI_API_KEY',''); print('loaded=', bool(k), 'prefix=', k[:12])"
```

2. Run an opt-in live OpenAI smoke test:

```bash
cd ~/berlin-insider
RUN_LIVE_OPENAI_TESTS=1 ~/.local/bin/uv run pytest tests/test_parser_summarizer.py -k live_openai_summary_smoke -q
```

3. If it still returns `401`, rotate the key in OpenAI dashboard and update `.env`.

## Feedback webhook

The worker registers Telegram webhook delivery on startup and accepts callback updates at:

`POST /telegram/webhook/{TELEGRAM_WEBHOOK_SECRET}`

## Persisted Data

SQLite persistence in `.data/berlin_insider.db` includes:

- operational scheduler/delivery state,
- sent-link dedupe history,
- sent message metadata and feedback votes,
- source website registry (`source_websites`),
- parsed run snapshots (`parse_runs`, `parsed_items`).
