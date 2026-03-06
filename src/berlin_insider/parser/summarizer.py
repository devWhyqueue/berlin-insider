from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from openai import APIConnectionError, APIError, APITimeoutError, OpenAI, RateLimitError

from berlin_insider.parser.models import ParsedItem

_DEFAULT_MODEL = "gpt-5-mini"
_DEFAULT_TIMEOUT_SECONDS = 20.0
_DEFAULT_MAX_OUTPUT_TOKENS = 200
_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+(?=[A-Z])")


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
        return cls(
            client=OpenAI(api_key=api_key, timeout=timeout),
            model=model,
            max_output_tokens=max_output_tokens,
        )

    def summarize(self, item: ParsedItem) -> str | None:
        """Generate a one-sentence summary for one parsed item."""
        content = _summary_input_text(item)
        if content is None:
            return None
        try:
            response = self.client.responses.create(
                model=self.model,
                instructions=(
                    "Summarize article detail into exactly one neutral sentence in English "
                    "with key context for Berlin events readers. Output only the sentence."
                ),
                input=content,
                max_output_tokens=self.max_output_tokens,
                reasoning={"effort": "low"},
                text={"verbosity": "low"},
            )
        except (APITimeoutError, RateLimitError, APIConnectionError, APIError) as exc:
            raise SummaryGenerationError(str(exc)) from exc
        return _normalize_single_sentence(response.output_text)


def _summary_input_text(item: ParsedItem) -> str | None:
    body = (item.detail_text or item.description or "").strip()
    if not body:
        return None
    title = (item.title or "Untitled").strip()
    return f"Title: {title}\n\nDetail:\n{body}"


def _normalize_single_sentence(value: str) -> str | None:
    collapsed = " ".join(value.split())
    if not collapsed:
        return None
    first = _SENTENCE_SPLIT.split(collapsed, maxsplit=1)[0].strip()
    if not first:
        return None
    return first
