# Agent Guide

Keep changes small, boring, and aligned with the existing Python package layout.

## Project

- Python 3.13 project managed with `uv`.
- CLI entry point: `uv run berlin-insider`.
- Main runtime: `berlin_insider.runtime.worker`.
- SQLite default: `.data/berlin_insider.db`.
- Public UI lives under `src/berlin_insider/web/` and is served from `/ui/`.

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

## Deployment Access

WSL:

```bash
ssh ubuntu@89.168.90.195
```

Windows:

```powershell
ssh ubuntu@89.168.90.195 -i C:\Users\yanni\.ssh\ssh-key-2023-09-20.key
```

Production is expected to run as `berlin-insider-worker.service` from `~/berlin-insider`.
