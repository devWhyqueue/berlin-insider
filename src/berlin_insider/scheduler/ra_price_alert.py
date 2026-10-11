from __future__ import annotations

import logging
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from berlin_insider.app.runtime.pipeline import DEFAULT_USER_AGENT
from berlin_insider.feedback.messenger.formatter.telegram import escape_text, escape_url
from berlin_insider.storage.sqlite import ensure_schema, sqlite_connection

logger = logging.getLogger(__name__)

_RA_GRAPHQL_URL = "https://ra.co/graphql"
_RA_BERLIN_AREA_ID = 34
_MAX_PAGES = 10
_PAGE_SIZE = 100
_MAX_MESSAGE_CHARS = 4000

_RA_QUERY = """
query GetEvents($filters: FilterInputDtoInput, $pageSize: Int, $page: Int) {
  eventListings(filters: $filters, pageSize: $pageSize, page: $page) {
    totalResults
    data {
      event {
        id
        title
        date
        startTime
        contentUrl
        cost
        venue {
          name
        }
      }
    }
  }
}
"""


def lowest_price(cost: str) -> float | None:
    """Return lowest price in cost string, or 0.0 for free keywords."""
    # ponytail: lowest number in free text, upgrade if RA exposes ticket tiers
    # shortcut: lowest number in free text, upgrade if RA exposes ticket tiers
    cleaned = re.sub(r"\d{1,2}:\d{2}", "", cost)
    numbers = [
        float(match.replace(",", "."))
        for match in re.findall(r"\d+(?:[.,]\d+)?", cleaned)
    ]
    if numbers:
        return min(numbers)
    if re.search(r"(?i)\b(free|frei|kostenlos|gratis)\b", cleaned):
        return 0.0
    return None


def _fetch_ra_page(
    page: int, gte: str, lte: str, headers: dict[str, str]
) -> tuple[list[dict], int]:
    filters: dict[str, object] = {
        "areas": {"eq": _RA_BERLIN_AREA_ID},
        "listingDate": {"gte": gte, "lte": lte},
    }
    payload = {
        "query": _RA_QUERY,
        "variables": {"filters": filters, "pageSize": _PAGE_SIZE, "page": page},
    }
    res = httpx.post(_RA_GRAPHQL_URL, json=payload, headers=headers, timeout=15.0)
    res.raise_for_status()
    body = res.json()
    if "errors" in body and not body.get("data"):
        raise RuntimeError(f"RA GraphQL error: {body['errors']}")
    listings = (body.get("data") or {}).get("eventListings") or {}
    items = [
        item["event"]
        for item in listings.get("data") or []
        if isinstance(item.get("event"), dict)
    ]
    return items, int(listings.get("totalResults", 0))


def fetch_ra_events(*, now_utc: datetime, days: int = 7) -> list[dict]:
    """Fetch upcoming Berlin events from RA GraphQL API."""
    gte = now_utc.strftime("%Y-%m-%d")
    lte = (now_utc + timedelta(days=days)).strftime("%Y-%m-%d")
    headers = {
        "Referer": "https://ra.co/events/de/berlin",
        "User-Agent": DEFAULT_USER_AGENT,
    }
    events: list[dict] = []
    for page in range(1, _MAX_PAGES + 1):
        items, total = _fetch_ra_page(page, gte, lte, headers)
        events.extend(items)
        if not items or page * _PAGE_SIZE >= total:
            break
    return events


def _format_event_date(raw_start: str | None, raw_date: str | None) -> str:
    raw = raw_start or raw_date or ""
    if not raw:
        return ""
    try:
        dt = datetime.fromisoformat(raw)
        return dt.strftime("%a %d.%m.")
    except (ValueError, TypeError):
        return raw[:10]


def _render_event_line(event: dict) -> str:
    title = escape_text(str(event.get("title") or "Untitled"))
    content_url = str(event.get("contentUrl") or "")
    full_url = (
        content_url if content_url.startswith("http") else f"https://ra.co{content_url}"
    )
    link = f"[{title}]({escape_url(full_url)})"
    venue = event.get("venue")
    venue_name = str(venue.get("name") if isinstance(venue, dict) else "")
    dt_str = _format_event_date(event.get("startTime"), event.get("date"))
    cost_raw = str(event.get("cost") or "").strip()
    cost_label = "free" if cost_raw in ("", "0", "0€") else cost_raw
    parts = [
        link,
        escape_text(venue_name),
        escape_text(dt_str),
        escape_text(cost_label),
    ]
    return " \\| ".join(parts)


def _chunk_events(event_lines: list[tuple[str, str]]) -> list[tuple[str, list[str]]]:
    chunks: list[tuple[str, list[str]]] = []
    current_lines: list[str] = []
    current_ids: list[str] = []
    current_len = 0
    for line, event_id in event_lines:
        line_len = len(line) + (1 if current_lines else 0)
        if current_lines and current_len + line_len > _MAX_MESSAGE_CHARS:
            chunks.append(("\n".join(current_lines), current_ids))
            current_lines = [line]
            current_ids = [event_id]
            current_len = len(line)
        else:
            current_lines.append(line)
            current_ids.append(event_id)
            current_len += line_len
    if current_lines:
        chunks.append(("\n".join(current_lines), current_ids))
    return chunks


def _load_sent_alert_ids(db_path: Path) -> set[str]:
    ensure_schema(db_path)
    with sqlite_connection(db_path) as conn:
        rows = conn.execute("SELECT event_id FROM ra_price_alerts").fetchall()
        return {str(row[0]) for row in rows}


def _record_sent_alerts(db_path: Path, event_ids: list[str]) -> None:
    sent_at = datetime.now(UTC).isoformat()
    with sqlite_connection(db_path) as conn:
        conn.executemany(
            "INSERT OR IGNORE INTO ra_price_alerts (event_id, sent_at) VALUES (?, ?)",
            [(eid, sent_at) for eid in event_ids],
        )
        conn.commit()


def send_ra_price_alerts(
    *,
    db_path: Path,
    messenger: Any,
    now_utc: datetime | None = None,
    max_price: float = 10.0,
) -> int:
    """Send Telegram alerts for upcoming RA events below max_price."""
    now = now_utc if now_utc is not None else datetime.now(UTC)
    sent_ids = _load_sent_alert_ids(db_path)
    events = fetch_ra_events(now_utc=now, days=7)
    matching = [
        event
        for event in events
        if str(event.get("id", "")) not in sent_ids
        and (price := lowest_price(str(event.get("cost") or ""))) is not None
        and price < max_price
    ]
    if not matching:
        return 0
    matching.sort(key=lambda e: str(e.get("startTime") or e.get("date") or ""))
    event_lines = [(_render_event_line(event), str(event["id"])) for event in matching]
    chunks = _chunk_events(event_lines)
    total_sent = 0
    for text, chunk_ids in chunks:
        messenger.send_digest(text=text)
        _record_sent_alerts(db_path, chunk_ids)
        total_sent += len(chunk_ids)
    return total_sent
