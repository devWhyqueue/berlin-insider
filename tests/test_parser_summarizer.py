from __future__ import annotations

import os

import pytest

from berlin_insider.fetcher.models import SourceId
from berlin_insider.parser.models import ParsedCategory, ParsedItem, WeekendRelevance
from berlin_insider.parser.summarizer import (
    NoOpSummaryGenerator,
    OpenAISummaryGenerator,
    _normalize_single_sentence,
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
        }
    )
    assert isinstance(generator, OpenAISummaryGenerator)
    assert generator.model == "gpt-5-mini"
    assert generator.max_output_tokens == 123


def test_normalize_single_sentence_preserves_common_abbreviations() -> None:
    value = (
        "The Astral Nachtmarkt runs at Astral Junction (Rigaer Str. 86, 10247 Berlin) "
        "with DJs and curated fashion."
    )
    assert _normalize_single_sentence(value) == value


def test_normalize_single_sentence_keeps_first_sentence_when_multiple_present() -> None:
    value = "Doors open at 20:00. Tickets are available online."
    assert _normalize_single_sentence(value) == "Doors open at 20:00."


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
