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

CREATE TABLE IF NOT EXISTS source_websites (
  source_id TEXT PRIMARY KEY,
  source_url TEXT NOT NULL,
  adapter_kind TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS parse_runs (
  run_id TEXT PRIMARY KEY,
  started_at TEXT NOT NULL,
  finished_at TEXT NOT NULL,
  total_items INTEGER NOT NULL,
  failed_sources_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS parsed_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  source_id TEXT NOT NULL,
  item_url TEXT NOT NULL,
  title TEXT,
  description TEXT,
  detail_text TEXT,
  summary TEXT,
  event_start_at TEXT,
  event_end_at TEXT,
  location TEXT,
  category TEXT NOT NULL,
  category_confidence REAL NOT NULL,
  weekend_relevance TEXT NOT NULL,
  weekend_confidence REAL NOT NULL,
  parse_notes_json TEXT NOT NULL,
  raw_json TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES parse_runs (run_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_parsed_items_run_id ON parsed_items (run_id);
CREATE INDEX IF NOT EXISTS idx_parsed_items_source_id ON parsed_items (source_id);
"""


def ensure_schema(path: Path) -> None:
    """Create the SQLite schema if it does not already exist."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite_connection(path) as conn:
        conn.executescript(SCHEMA)
        _ensure_parsed_items_column(conn, column_name="detail_text")
        _ensure_parsed_items_column(conn, column_name="summary")


def _ensure_parsed_items_column(conn: sqlite3.Connection, *, column_name: str) -> None:
    columns = conn.execute("PRAGMA table_info(parsed_items)").fetchall()
    existing_names = {str(column[1]) for column in columns}
    if column_name in existing_names:
        return
    conn.execute(f"ALTER TABLE parsed_items ADD COLUMN {column_name} TEXT")


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
