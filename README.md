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
By default it runs daily tips on Monday-Thursday 08:00 and weekend digests on Friday 08:00 in
`Europe/Berlin`, and persists operational state in `.data/berlin_insider.db`.
When a run executes, it also sends the digest through Telegram.

Required environment variables:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Optional:

- `TELEGRAM_API_BASE` (default: `https://api.telegram.org`)

Example cron (Linux, host timezone set to Europe/Berlin):

```cron
0 8 * * 5 cd /path/to/berlin-insider && uv run berlin-insider schedule
```

Example Task Scheduler action (Windows):

```powershell
uv run berlin-insider schedule
```

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
