from __future__ import annotations

import json
from pathlib import Path
from typing import Protocol
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_TRACKING_KEYS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "utm_id",
    "gclid",
    "fbclid",
}


class SentItemStore(Protocol):
    def is_sent(self, url: str) -> bool:
        """Return true when this canonical URL was already sent previously."""
        ...

    def mark_sent(self, urls: list[str]) -> None:
        """Persist canonical URLs that were selected in this run."""
        ...


class NoOpSentItemStore:
    def is_sent(self, url: str) -> bool:  # noqa: ARG002
        """Always report unsent for tests and stateless runs."""
        return False

    def mark_sent(self, urls: list[str]) -> None:  # noqa: ARG002
        """Intentionally ignore persisted links."""
        return


class JsonSentItemStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._sent = self._load()

    def is_sent(self, url: str) -> bool:
        """Return true when the canonical URL already exists in local store."""
        return canonicalize_url(url) in self._sent

    def mark_sent(self, urls: list[str]) -> None:
        """Persist selected canonical URLs atomically to JSON."""
        for url in urls:
            self._sent.add(canonicalize_url(url))
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = sorted(self._sent)
        temp_path = self._path.with_suffix(f"{self._path.suffix}.tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(self._path)

    def _load(self) -> set[str]:
        if not self._path.exists():
            return set()
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return set()
        if not isinstance(data, list):
            return set()
        return {canonicalize_url(value) for value in data if isinstance(value, str)}


def canonicalize_url(url: str) -> str:
    """Normalize URL for dedupe and sent-history comparisons."""
    parts = urlsplit(url.strip())
    hostname = (parts.hostname or "").lower()
    netloc = hostname
    if parts.port:
        is_default = (parts.scheme == "http" and parts.port == 80) or (
            parts.scheme == "https" and parts.port == 443
        )
        if not is_default:
            netloc = f"{hostname}:{parts.port}"
    path = parts.path or "/"
    if path != "/":
        path = path.rstrip("/") or "/"
    clean_query = _normalize_query(parts.query)
    return urlunsplit((parts.scheme.lower(), netloc, path, clean_query, ""))


def _normalize_query(query: str) -> str:
    if not query:
        return ""
    pairs = parse_qsl(query, keep_blank_values=True)
    kept = [(key, value) for key, value in pairs if key.lower() not in _TRACKING_KEYS]
    kept.sort()
    return urlencode(kept)
