from __future__ import annotations

import time
from collections.abc import Callable

import httpx

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def get_text_with_retries(
    url: str,
    *,
    user_agent: str,
    timeout_seconds: float,
    retries: int = 2,
    on_retry: Callable[[str], None] | None = None,
) -> str:
    """Fetch a URL with retry-on-transient-failure behavior."""
    headers = {"User-Agent": user_agent}
    for attempt in range(retries + 1):
        try:
            with httpx.Client(timeout=timeout_seconds, follow_redirects=True) as client:
                response = client.get(url, headers=headers)
            if response.status_code in RETRYABLE_STATUS_CODES and attempt < retries:
                _emit_retry(on_retry, url, response.status_code, attempt + 1)
                time.sleep(2**attempt)
                continue
            response.raise_for_status()
            return response.text
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            if attempt >= retries:
                raise
            _emit_retry(on_retry, url, str(exc), attempt + 1)
            time.sleep(2**attempt)
    raise RuntimeError(f"Unexpected retry termination for {url}")


def _emit_retry(
    callback: Callable[[str], None] | None, url: str, reason: object, attempt: int
) -> None:
    if callback is None:
        return
    callback(f"Retry {attempt} for {url}: {reason}")
