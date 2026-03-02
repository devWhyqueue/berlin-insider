from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from berlin_insider.storage.sqlite import ensure_schema, now_utc_iso, sqlite_connection
from berlin_insider.storage.url_normalize import canonicalize_url


@dataclass(slots=True)
class DetailCacheEntry:
    canonical_url: str
    source_id: str | None
    detail_text: str
    detail_hash: str
    summary: str | None
    first_fetched_at: str
    detail_status: str
    updated_at: str


class SqliteDetailCacheStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        ensure_schema(self._db_path)

    def get(self, url: str) -> DetailCacheEntry | None:
        """Return one cached detail entry for a URL, if available."""
        canonical_url = canonicalize_url(url)
        row = _fetch_cache_row(self._db_path, canonical_url=canonical_url)
        return _to_entry(row)

    def upsert_detail(
        self,
        *,
        url: str,
        source_id: str | None,
        detail_text: str,
        detail_hash: str,
        detail_status: str,
    ) -> None:
        """Insert or update detail cache while preserving summary for identical hashes."""
        canonical_url = canonicalize_url(url)
        now = now_utc_iso()
        summary, first_fetched_at = _existing_summary(
            self._db_path,
            canonical_url=canonical_url,
            detail_hash=detail_hash,
            fallback_first_fetched_at=now,
        )
        _upsert_detail_row(
            self._db_path,
            canonical_url=canonical_url,
            source_id=source_id,
            detail_text=detail_text,
            detail_hash=detail_hash,
            summary=summary,
            first_fetched_at=first_fetched_at,
            detail_status=detail_status,
            now=now,
        )

    def upsert_summary(self, *, url: str, detail_hash: str, summary: str) -> None:
        """Persist one summary when URL and detail hash still match."""
        canonical_url = canonicalize_url(url)
        now = now_utc_iso()
        with sqlite_connection(self._db_path) as conn:
            row = conn.execute(
                """
                UPDATE detail_cache
                SET summary = ?, last_used_at = ?, updated_at = ?
                WHERE canonical_url = ? AND detail_hash = ?
                """,
                (summary, now, now, canonical_url, detail_hash),
            )
            conn.commit()

    def touch_used(self, url: str) -> None:
        """Update last-used timestamp for one cache entry."""
        canonical_url = canonicalize_url(url)
        now = now_utc_iso()
        with sqlite_connection(self._db_path) as conn:
            conn.execute(
                """
                UPDATE detail_cache
                SET last_used_at = ?, updated_at = ?
                WHERE canonical_url = ?
                """,
                (now, now, canonical_url),
            )
            conn.commit()


def _fetch_cache_row(db_path: Path, *, canonical_url: str) -> tuple[object, ...] | None:
    with sqlite_connection(db_path) as conn:
        return conn.execute(
            """
            SELECT canonical_url, source_id, detail_text, detail_hash, summary,
                   first_fetched_at, detail_status, updated_at
            FROM detail_cache
            WHERE canonical_url = ?
            LIMIT 1
            """,
            (canonical_url,),
        ).fetchone()


def _to_entry(row: tuple[object, ...] | None) -> DetailCacheEntry | None:
    if row is None:
        return None
    return DetailCacheEntry(
        canonical_url=str(row[0]),
        source_id=str(row[1]) if row[1] is not None else None,
        detail_text=str(row[2]),
        detail_hash=str(row[3]),
        summary=str(row[4]) if row[4] is not None else None,
        first_fetched_at=str(row[5]),
        detail_status=str(row[6]),
        updated_at=str(row[7]),
    )


def _existing_summary(
    db_path: Path,
    *,
    canonical_url: str,
    detail_hash: str,
    fallback_first_fetched_at: str,
) -> tuple[str | None, str]:
    with sqlite_connection(db_path) as conn:
        existing = conn.execute(
            """
            SELECT detail_hash, summary, first_fetched_at
            FROM detail_cache
            WHERE canonical_url = ?
            LIMIT 1
            """,
            (canonical_url,),
        ).fetchone()
    if existing is None:
        return None, fallback_first_fetched_at
    existing_hash = str(existing[0])
    existing_summary = str(existing[1]) if existing[1] is not None else None
    first_fetched_at = str(existing[2])
    if existing_hash != detail_hash:
        return None, first_fetched_at
    return existing_summary, first_fetched_at


def _upsert_detail_row(
    db_path: Path,
    *,
    canonical_url: str,
    source_id: str | None,
    detail_text: str,
    detail_hash: str,
    summary: str | None,
    first_fetched_at: str,
    detail_status: str,
    now: str,
) -> None:
    with sqlite_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO detail_cache (
                canonical_url, source_id, detail_text, detail_hash, summary,
                first_fetched_at, last_fetched_at, last_used_at, detail_status, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(canonical_url) DO UPDATE SET
                source_id = excluded.source_id,
                detail_text = excluded.detail_text,
                detail_hash = excluded.detail_hash,
                summary = excluded.summary,
                first_fetched_at = excluded.first_fetched_at,
                last_fetched_at = excluded.last_fetched_at,
                last_used_at = excluded.last_used_at,
                detail_status = excluded.detail_status,
                updated_at = excluded.updated_at
            """,
            (
                canonical_url,
                source_id,
                detail_text,
                detail_hash,
                summary,
                first_fetched_at,
                now,
                now,
                detail_status,
                now,
            ),
        )
        conn.commit()
