from __future__ import annotations

import os
from types import SimpleNamespace
from typing import Any, cast

import pytest

from berlin_insider.fetcher.models import SourceId
from berlin_insider.parser.models import ParsedCategory, ParsedItem, WeekendRelevance
from berlin_insider.parser.summarizer import (
    NoOpSummaryGenerator,
    OpenAISummaryGenerator,
    SummaryGenerationError,
    _max_output_tokens_for_attempt,
    _normalize_summary_text,
)


def _sample_item() -> ParsedItem:
    return ParsedItem(
        source_id=SourceId.MITVERGNUEGEN,
        item_url="https://example.com/event",
        title="Open-air concert",
        description="Live music with local artists.",
        event_start_at=None,
        event_end_at=None,
        location="Berlin",
        category=ParsedCategory.CULTURE,
        category_confidence=0.8,
        weekend_relevance=WeekendRelevance.POSSIBLE,
        weekend_confidence=0.7,
    )


def test_openai_summary_from_env_returns_noop_when_missing_key() -> None:
    generator = OpenAISummaryGenerator.from_env(env={})
    assert isinstance(generator, NoOpSummaryGenerator)


def test_openai_summary_from_env_trims_quotes_and_whitespace() -> None:
    generator = OpenAISummaryGenerator.from_env(
        env={
            "OPENAI_API_KEY": "  'sk-test-123'  ",
            "OPENAI_SUMMARY_MODEL": "gpt-5-mini",
            "OPENAI_SUMMARY_MAX_OUTPUT_TOKENS": "123",
            "OPENAI_SUMMARY_RETRY_ATTEMPTS": "4",
        }
    )
    assert isinstance(generator, OpenAISummaryGenerator)
    assert generator.model == "gpt-5-mini"
    assert generator.max_output_tokens == 123
    assert generator.retry_attempts == 4


def test_normalize_summary_text_collapses_whitespace() -> None:
    value = "The Astral Nachtmarkt runs\n\nat Astral Junction with DJs."
    assert _normalize_summary_text(value) == "The Astral Nachtmarkt runs at Astral Junction with DJs."


def test_normalize_summary_text_preserves_two_short_sentences() -> None:
    value = "Doors open at 20:00. Tickets are available online."
    assert _normalize_summary_text(value) == value


def test_summary_generator_retries_when_response_is_incomplete() -> None:
    calls: list[int] = []

    class _FakeResponses:
        def create(self, **kwargs):  # noqa: ANN003, ANN202
            calls.append(kwargs["max_output_tokens"])
            if len(calls) == 1:
                return SimpleNamespace(
                    status="incomplete",
                    incomplete_details=SimpleNamespace(reason="max_output_tokens"),
                    output_text="On Tuesday the Bettina von Arnim Library hosts",
                )
            return SimpleNamespace(
                status="completed",
                incomplete_details=None,
                output_text=(
                    "On Tuesday the Bettina von Arnim Library hosts a brief reading by Monika "
                    "Groth with free entry in Pankow."
                ),
            )

    generator = OpenAISummaryGenerator(
        client=cast(Any, SimpleNamespace(responses=_FakeResponses())),
        max_output_tokens=200,
        retry_attempts=2,
    )

    summary = generator.summarize(_sample_item())

    assert summary is not None
    assert summary.startswith("On Tuesday")
    assert calls == [200, 300]


def test_summary_generator_raises_when_retries_exhausted() -> None:
    class _FakeResponses:
        def create(self, **kwargs):  # noqa: ANN003, ANN202
            return SimpleNamespace(
                status="incomplete",
                incomplete_details=SimpleNamespace(reason="max_output_tokens"),
                output_text="Clipped text",
            )

    generator = OpenAISummaryGenerator(
        client=cast(Any, SimpleNamespace(responses=_FakeResponses())),
        max_output_tokens=200,
        retry_attempts=1,
    )

    with pytest.raises(SummaryGenerationError, match="max_output_tokens"):
        generator.summarize(_sample_item())


def test_max_output_tokens_grows_by_attempt() -> None:
    assert _max_output_tokens_for_attempt(base_tokens=200, attempt=0) == 200
    assert _max_output_tokens_for_attempt(base_tokens=200, attempt=1) == 300
    assert _max_output_tokens_for_attempt(base_tokens=200, attempt=2) == 400


@pytest.mark.skipif(
    os.getenv("RUN_LIVE_OPENAI_TESTS") != "1",
    reason="Set RUN_LIVE_OPENAI_TESTS=1 to run live OpenAI smoke test.",
)
def test_live_openai_summary_smoke() -> None:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        pytest.skip("OPENAI_API_KEY missing for live smoke test.")
    generator = OpenAISummaryGenerator.from_env(env=os.environ)
    assert isinstance(generator, OpenAISummaryGenerator)
    summary = generator.summarize(_sample_item())
    assert summary is not None
    assert len(summary.strip()) > 0
