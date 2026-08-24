# Agent Guide

Keep changes small, boring, and aligned with the existing Python package layout.

## Project

- Python 3.13 project managed with `uv`.
- CLI entry point: `uv run berlin-insider`.
- Main runtime: `berlin_insider.app.runtime.worker`.
- SQLite default: `.data/berlin_insider.db`.
- Public UI lives under `src/berlin_insider/app/web/` and is served from `/ui/`.

## Commands

```bash
uv sync --all-groups
uv run playwright install chromium
uv run berlin-insider fetch
uv run berlin-insider worker --run-once
uv run pytest
uv run ruff check .
uv run pyright
```

After every code change, run the clean-code skill from the repo root and get a passing result.

## Working Rules

- Read the nearby code before editing.
- Do not overwrite unrelated dirty work.
- Prefer existing helpers and patterns over new abstractions.
- Keep docs concise and command-oriented.
- Do not commit secrets or `.env`.

## Deployment & Docker

Build & run container:

```bash
docker build -t berlin-insider:latest .
docker compose up -d
```

Check logs and health:

```bash
docker compose logs -f
curl http://localhost:8080/healthz
```

Production runs containerized via Docker on an OCI instance. Secrets and configs are managed via `.env` (see `.env.example`).

