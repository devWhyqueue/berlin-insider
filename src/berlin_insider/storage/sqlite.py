from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS scheduler_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  last_attempt_at TEXT,
  last_run_date_local TEXT,
  last_status TEXT,
  last_success_at TEXT,
  last_error_message TEXT,
  last_digest_length INTEGER,
  last_curated_count INTEGER,
  last_failed_sources_json TEXT NOT NULL DEFAULT '[]',
  last_source_status_json TEXT NOT NULL DEFAULT '{}',
  last_delivery_at TEXT,
  last_delivery_message_id TEXT,
  last_delivery_error TEXT,
  last_run_date_by_kind_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS sent_links (
  digest_kind TEXT NOT NULL,
  canonical_url TEXT NOT NULL,
  first_sent_at TEXT NOT NULL,
  PRIMARY KEY (digest_kind, canonical_url)
);

CREATE TABLE IF NOT EXISTS sent_messages (
  message_key TEXT PRIMARY KEY,
  digest_kind TEXT NOT NULL,
  local_date TEXT NOT NULL,
  sent_at TEXT NOT NULL,
  telegram_message_id TEXT NOT NULL,
  selected_urls_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS telegram_updates_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  last_update_id INTEGER
);

CREATE TABLE IF NOT EXISTS feedback_events (
  message_key TEXT NOT NULL,
  digest_kind TEXT NOT NULL,
  vote TEXT NOT NULL CHECK (vote IN ('up', 'down')),
  telegram_user_id INTEGER NOT NULL,
  chat_id TEXT NOT NULL,
  message_id TEXT NOT NULL,
  voted_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (message_key, telegram_user_id),
  FOREIGN KEY (message_key) REFERENCES sent_messages (message_key) ON DELETE CASCADE
);
"""


def ensure_schema(path: Path) -> None:
    """Create the SQLite schema if it does not already exist."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite_connection(path) as conn:
        conn.executescript(SCHEMA)


@contextmanager
def sqlite_connection(path: Path) -> Iterator[sqlite3.Connection]:
    """Open a SQLite connection with foreign-key enforcement enabled."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        yield conn
    finally:
        conn.close()


def now_utc_iso() -> str:
    """Return current UTC timestamp as ISO-8601 string."""
    return datetime.now(UTC).isoformat()
