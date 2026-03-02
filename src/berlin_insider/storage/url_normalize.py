from __future__ import annotations

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


def canonicalize_url(url: str) -> str:
    """Normalize URL for dedupe and cache key comparisons."""
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
