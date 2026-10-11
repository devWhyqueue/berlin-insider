from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from berlin_insider.scheduler.ra_price_alert import (
    lowest_price,
    send_ra_price_alerts,
)


@pytest.mark.parametrize(
    ("cost", "expected"),
    [
        ("", None),
        ("0", 0.0),
        ("10", 10.0),
        ("15€ ", 15.0),
        ("0-5-10", 0.0),
        ("8,50", 8.5),
        ("Free", 0.0),
        ("from 23:00", None),
    ],
)
def test_lowest_price(cost: str, expected: float | None) -> None:
    assert lowest_price(cost) == expected


class _FakeMessenger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def send_digest(self, *, text: str, feedback_metadata: Any = None) -> None:  # noqa: ARG002
        self.messages.append(text)


def test_send_ra_price_alerts_filters_under_ten_and_deduplicates(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "test.db"
    messenger = _FakeMessenger()

    fake_events = [
        {
            "id": "e1",
            "title": "Cheap Event",
            "startTime": "2026-10-15T22:00:00.000",
            "contentUrl": "/events/e1",
            "cost": "5€",
            "venue": {"name": "Club A"},
        },
        {
            "id": "e2",
            "title": "Free Party",
            "startTime": "2026-10-16T23:00:00.000",
            "contentUrl": "/events/e2",
            "cost": "Free",
            "venue": {"name": "Club B"},
        },
        {
            "id": "e3",
            "title": "Expensive Night",
            "startTime": "2026-10-17T20:00:00.000",
            "contentUrl": "/events/e3",
            "cost": "15€",
            "venue": {"name": "Club C"},
        },
        {
            "id": "e4",
            "title": "Exactly Ten",
            "startTime": "2026-10-18T20:00:00.000",
            "contentUrl": "/events/e4",
            "cost": "10€",
            "venue": {"name": "Club D"},
        },
        {
            "id": "e5",
            "title": "No Price",
            "startTime": "2026-10-19T20:00:00.000",
            "contentUrl": "/events/e5",
            "cost": "",
            "venue": {"name": "Club E"},
        },
    ]

    import berlin_insider.scheduler.ra_price_alert as ra_module

    monkeypatch.setattr(ra_module, "fetch_ra_events", lambda *, now_utc, days=7: fake_events)

    # First call sends matches (e1, e2)
    sent_count = send_ra_price_alerts(
        db_path=db_path,
        messenger=messenger,
        now_utc=datetime(2026, 10, 11, 12, 0, tzinfo=UTC),
    )

    assert sent_count == 2
    assert len(messenger.messages) == 1
    msg = messenger.messages[0]
    assert "[Cheap Event](https://ra.co/events/e1)" in msg
    assert "[Free Party](https://ra.co/events/e2)" in msg
    assert "Expensive Night" not in msg
    assert "Exactly Ten" not in msg

    # Second call sends nothing because e1 and e2 are already in DB
    sent_count_again = send_ra_price_alerts(
        db_path=db_path,
        messenger=messenger,
        now_utc=datetime(2026, 10, 11, 12, 0, tzinfo=UTC),
    )

    assert sent_count_again == 0
    assert len(messenger.messages) == 1


def test_send_ra_price_alerts_splits_large_messages(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    db_path = tmp_path / "test.db"
    messenger = _FakeMessenger()

    fake_events = [
        {
            "id": f"evt-{i}",
            "title": "Very Long Event Title " * 5 + f"#{i}",
            "startTime": "2026-10-15T22:00:00.000",
            "contentUrl": f"/events/{i}",
            "cost": "5€",
            "venue": {"name": "A Very Long Venue Name " * 3},
        }
        for i in range(40)
    ]

    import berlin_insider.scheduler.ra_price_alert as ra_module

    monkeypatch.setattr(ra_module, "fetch_ra_events", lambda *, now_utc, days=7: fake_events)

    sent_count = send_ra_price_alerts(
        db_path=db_path,
        messenger=messenger,
        now_utc=datetime(2026, 10, 11, 12, 0, tzinfo=UTC),
    )

    assert sent_count == 40
    assert len(messenger.messages) > 1
    for msg in messenger.messages:
        assert len(msg) <= 4000

