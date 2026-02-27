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
uv run berlin-insider fetch --source eventbrite_berlin_weekend
```
