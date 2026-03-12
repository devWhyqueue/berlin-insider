from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS worker_state (
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

CREATE TABLE IF NOT EXISTS sources (
  source_id TEXT PRIMARY KEY,
  source_url TEXT NOT NULL,
  adapter_kind TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS items (
  item_id INTEGER PRIMARY KEY AUTOINCREMENT,
  canonical_url TEXT NOT NULL UNIQUE,
  source_id TEXT NOT NULL,
  original_url TEXT,
  title TEXT,
  description TEXT,
  summary TEXT,
  event_start_at TEXT,
  event_end_at TEXT,
  location TEXT,
  category TEXT,
  category_confidence REAL,
  weekend_relevance TEXT,
  weekend_confidence REAL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (source_id) REFERENCES sources (source_id)
);

CREATE TABLE IF NOT EXISTS message_deliveries (
  message_key TEXT PRIMARY KEY,
  digest_kind TEXT NOT NULL,
  local_date TEXT NOT NULL,
  sent_at TEXT NOT NULL,
  telegram_message_id TEXT NOT NULL,
  primary_item_id INTEGER NOT NULL,
  alternative_item_id INTEGER,
  FOREIGN KEY (primary_item_id) REFERENCES items (item_id),
  FOREIGN KEY (alternative_item_id) REFERENCES items (item_id)
);

CREATE TABLE IF NOT EXISTS telegram_updates_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  last_update_id INTEGER
);

CREATE TABLE IF NOT EXISTS feedback_events (
  message_key TEXT NOT NULL,
  telegram_user_id INTEGER NOT NULL,
  vote TEXT NOT NULL CHECK (vote IN ('up', 'down')),
  voted_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (message_key, telegram_user_id),
  FOREIGN KEY (message_key) REFERENCES message_deliveries (message_key) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS detail_cache (
  canonical_url TEXT PRIMARY KEY,
  source_id TEXT,
  detail_text TEXT NOT NULL,
  detail_hash TEXT NOT NULL,
  summary TEXT,
  detail_metadata_json TEXT NOT NULL DEFAULT '{}',
  first_fetched_at TEXT NOT NULL,
  last_fetched_at TEXT NOT NULL,
  last_used_at TEXT NOT NULL,
  detail_status TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_items_source_id ON items (source_id);
CREATE INDEX IF NOT EXISTS idx_message_deliveries_primary_item_id ON message_deliveries (primary_item_id);
CREATE INDEX IF NOT EXISTS idx_message_deliveries_alternative_item_id ON message_deliveries (alternative_item_id);
CREATE INDEX IF NOT EXISTS idx_detail_cache_last_used_at ON detail_cache (last_used_at);
CREATE INDEX IF NOT EXISTS idx_detail_cache_source_id ON detail_cache (source_id);
"""


def ensure_schema(path: Path) -> None:
    """Create the target schema."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite_connection(path) as conn:
        conn.executescript(SCHEMA)
        _ensure_detail_cache_column(
            conn,
            column_name="detail_metadata_json",
            definition="TEXT NOT NULL DEFAULT '{}'",
        )
        conn.commit()


def _ensure_detail_cache_column(
    conn: sqlite3.Connection, *, column_name: str, definition: str
) -> None:
    columns = conn.execute("PRAGMA table_info(detail_cache)").fetchall()
    existing_names = {str(column[1]) for column in columns}
    if column_name in existing_names:
        return
    conn.execute(f"ALTER TABLE detail_cache ADD COLUMN {column_name} {definition}")


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
