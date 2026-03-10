from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from openai import APIConnectionError, APIError, APITimeoutError, OpenAI, RateLimitError

from berlin_insider.parser.models import ParsedItem

_DEFAULT_MODEL = "gpt-5-mini"
_DEFAULT_TIMEOUT_SECONDS = 20.0
_DEFAULT_MAX_OUTPUT_TOKENS = 320
_DEFAULT_RETRY_ATTEMPTS = 2


class SummaryGenerationError(RuntimeError):
    """Raised when external summary generation fails."""


class SummaryGenerator(Protocol):
    def summarize(self, item: ParsedItem) -> str | None:
        """Return one-sentence summary text for the parsed item."""


class NoOpSummaryGenerator:
    def summarize(self, item: ParsedItem) -> str | None:  # noqa: ARG002
        """Disable summary generation when no API credentials are configured."""
        return None


@dataclass(slots=True)
class OpenAISummaryGenerator:
    client: OpenAI
    model: str = _DEFAULT_MODEL
    max_output_tokens: int = _DEFAULT_MAX_OUTPUT_TOKENS
    retry_attempts: int = _DEFAULT_RETRY_ATTEMPTS

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> SummaryGenerator:
        """Build a summary generator from OPENAI_* environment variables."""
        source = env if env is not None else os.environ
        api_key = (source.get("OPENAI_API_KEY") or "").strip().strip("'\"")
        if not api_key:
            return NoOpSummaryGenerator()
        timeout = float(source.get("OPENAI_SUMMARY_TIMEOUT_SECONDS", _DEFAULT_TIMEOUT_SECONDS))
        model = source.get("OPENAI_SUMMARY_MODEL", _DEFAULT_MODEL)
        max_output_tokens = int(
            source.get("OPENAI_SUMMARY_MAX_OUTPUT_TOKENS", _DEFAULT_MAX_OUTPUT_TOKENS)
        )
        retry_attempts = int(source.get("OPENAI_SUMMARY_RETRY_ATTEMPTS", _DEFAULT_RETRY_ATTEMPTS))
        return cls(
            client=OpenAI(api_key=api_key, timeout=timeout),
            model=model,
            max_output_tokens=max_output_tokens,
            retry_attempts=retry_attempts,
        )

    def summarize(self, item: ParsedItem) -> str | None:
        """Generate one brief summary for one parsed item."""
        content = _summary_input_text(item)
        if content is None:
            return None
        for attempt in range(self.retry_attempts + 1):
            response = self._create_summary_response(content=content, attempt=attempt)
            if _should_retry_incomplete_response(
                response=response,
                attempt=attempt,
                retry_attempts=self.retry_attempts,
            ):
                continue
            return _summary_from_response(response)
        return None

    def _create_summary_response(self, *, content: str, attempt: int):
        budget = _max_output_tokens_for_attempt(base_tokens=self.max_output_tokens, attempt=attempt)
        try:
            return self.client.responses.create(
                model=self.model,
                instructions=_summary_instructions(),
                input=content,
                max_output_tokens=budget,
                reasoning={"effort": "low"},
                text={"verbosity": "low"},
            )
        except (APITimeoutError, RateLimitError, APIConnectionError, APIError) as exc:
            raise SummaryGenerationError(str(exc)) from exc


def _summary_input_text(item: ParsedItem) -> str | None:
    body = (item.detail_text or item.description or "").strip()
    if not body:
        return None
    title = (item.title or "Untitled").strip()
    return f"Title: {title}\n\nDetail:\n{body}"


def _normalize_summary_text(value: str) -> str | None:
    collapsed = " ".join(value.split())
    if not collapsed:
        return None
    return collapsed


def _incomplete_reason(response: object) -> str:
    details = getattr(response, "incomplete_details", None)
    reason = getattr(details, "reason", None)
    if isinstance(reason, str) and reason:
        return reason
    status = getattr(response, "status", None)
    if isinstance(status, str) and status:
        return status
    return "unknown"


def _max_output_tokens_for_attempt(*, base_tokens: int, attempt: int) -> int:
    growth_factors = [1.0, 1.5, 2.0]
    factor = growth_factors[min(attempt, len(growth_factors) - 1)]
    return max(int(base_tokens * factor), base_tokens)


def _summary_instructions() -> str:
    return (
        "Write a brief, compact English summary for Berlin event readers using only the "
        "most important details from the source. Keep it concise, avoid filler, "
        "repetition, long subordinate clauses, and unnecessary background. Output only "
        "the summary text."
    )


def _should_retry_incomplete_response(
    *,
    response: object,
    attempt: int,
    retry_attempts: int,
) -> bool:
    if getattr(response, "status", None) != "incomplete":
        return False
    reason = _incomplete_reason(response)
    if reason == "max_output_tokens" and attempt < retry_attempts:
        return True
    raise SummaryGenerationError(f"summary response incomplete: {reason}")


def _summary_from_response(response: object) -> str:
    summary = _normalize_summary_text(getattr(response, "output_text", ""))
    if summary is None:
        raise SummaryGenerationError("summary response was empty")
    return summary
