from __future__ import annotations

import pytest
from berlin_insider.parser.summarizer import NoOpSummaryGenerator, OpenAISummaryGenerator


@pytest.fixture(autouse=True)
def mock_summary_generator_from_env(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> None:
    # Do not mock for tests that explicitly test the from_env method or live integration
    if (
        "test_openai_summary_from_env" in request.node.name
        or "test_live_openai_summary_smoke" in request.node.name
    ):
        return

    monkeypatch.setattr(
        OpenAISummaryGenerator,
        "from_env",
        lambda *args, **kwargs: NoOpSummaryGenerator(),
    )
