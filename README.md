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
uv run berlin-insider schedule
uv run berlin-insider schedule --force
uv run berlin-insider schedule --json
uv run berlin-insider feedback
```

## Scheduler

`schedule` is a one-shot command meant to be called by an external scheduler.
By default it runs daily tips on every day except Friday at 08:00 and weekend digests on Friday 08:00 in
`Europe/Berlin`, and persists operational state in `.data/berlin_insider.db`.
When a run executes, it also sends the digest through Telegram.

Required environment variables:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Optional:

- `TELEGRAM_API_BASE` (default: `https://api.telegram.org`)
- `OPENAI_API_KEY` (enables one-sentence `gpt-5-mini` summaries for parsed items/digests)

When `OPENAI_API_KEY` is missing or summary generation fails for an item, the pipeline still runs
and sends digests; that item is shown without a summary line.

Example cron (Linux, host timezone set to Europe/Berlin):

```cron
0 8 * * 5 cd /path/to/berlin-insider && uv run berlin-insider schedule
```

Example Task Scheduler action (Windows):

```powershell
uv run berlin-insider schedule
```

## Deploy on Ubuntu (systemd)

The scheduler command is one-shot and should be triggered by a timer.
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

3. Create systemd service (`/etc/systemd/system/berlin-insider-schedule.service`):

```ini
[Unit]
Description=Berlin Insider scheduler one-shot
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=<your-linux-username>
WorkingDirectory=/home/<your-linux-username>/berlin-insider
ExecStart=/home/<your-linux-username>/.local/bin/uv run berlin-insider schedule --timezone Europe/Berlin --db-path .data/berlin_insider.db
```

4. Create systemd timer (`/etc/systemd/system/berlin-insider-schedule.timer`):

```ini
[Unit]
Description=Run Berlin Insider scheduler every 15 minutes

[Timer]
OnBootSec=2m
OnUnitActiveSec=15m
Persistent=true

[Install]
WantedBy=timers.target
```

5. Enable timer:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now berlin-insider-schedule.timer
```

6. Verify:

```bash
systemctl status berlin-insider-schedule.timer
journalctl -u berlin-insider-schedule.service -n 100 --no-pager
```

## Troubleshooting

Check timer/service health:

```bash
systemctl list-timers --all | grep berlin-insider
systemctl status berlin-insider-schedule.timer
systemctl status berlin-insider-schedule.service
```

Read recent scheduler logs:

```bash
journalctl -u berlin-insider-schedule.service -n 200 --no-pager
journalctl -u berlin-insider-schedule.service -f
```

Run a manual non-forced diagnostic:

```bash
cd ~/berlin-insider
~/.local/bin/uv run berlin-insider schedule --json
```

Run a forced send test:

```bash
cd ~/berlin-insider
~/.local/bin/uv run berlin-insider schedule --force --json
```

Inspect unit configuration:

```bash
systemctl cat berlin-insider-schedule.service
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

## Feedback polling

`feedback` is a one-shot command that polls Telegram callback updates (`getUpdates`) and persists
thumbs-up/down votes to `.data/berlin_insider.db` for ranking training data.

## Persisted Data

SQLite persistence in `.data/berlin_insider.db` includes:

- operational scheduler/delivery state,
- sent-link dedupe history,
- sent message metadata and feedback votes,
- source website registry (`source_websites`),
- parsed run snapshots (`parse_runs`, `parsed_items`).
