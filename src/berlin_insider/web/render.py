from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import asdict
from html import escape

from berlin_insider.web.models import (
    DeliveriesResponse,
    DeliveryRow,
    FeedbackAggregateRow,
    FeedbackResponse,
    ItemCard,
    ItemsResponse,
    OpsResponse,
    OverviewResponse,
)
from berlin_insider.web.render_helpers import (
    _delivery_row,
    _display,
    _empty_state,
    _feedback_row,
    _item_card,
    _ops_cache,
    _ops_sources,
    _ops_state,
    _panel,
    _select,
)

BASE_PATH = "/ui"
STATIC_PATH = f"{BASE_PATH}/static"


def _render_dashboard_html(
    *,
    overview: OverviewResponse,
    items: ItemsResponse,
    deliveries: DeliveriesResponse,
    feedback: FeedbackResponse,
    ops: OpsResponse,
) -> str:
    """Render the public dashboard HTML with base-path-aware assets."""
    initial_state = json.dumps(_initial_state(overview, items, deliveries, feedback, ops))
    return (
        _document_head()
        + _hero(overview)
        + _items_panel(items)
        + _deliveries_panel(deliveries)
        + _feedback_panel(feedback)
        + _ops_panel(ops)
        + _document_tail(initial_state)
    )


def _deliveries_panel(deliveries: DeliveriesResponse) -> str:
    body = _delivery_rows(deliveries.deliveries)
    return _panel(
        "Distribution record",
        "Delivery timeline",
        body,
        "deliveries-heading",
        "deliveries-timeline",
    )


def _document_head() -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Berlin Insider</title>
  <meta name="description" content="Public overview of Berlin Insider items, deliveries, feedback, and worker operations.">
  <link rel="stylesheet" href="{STATIC_PATH}/site.css">
</head>
<body>
  <div class="page-shell">
"""


def _document_tail(initial_state: str) -> str:
    return f"""
  </main>
  </div>
  <script id="initial-state" type="application/json">{escape(initial_state)}</script>
  <script src="{STATIC_PATH}/site.js" defer></script>
</body>
</html>
"""


def _feedback_panel(feedback: FeedbackResponse) -> str:
    body = _feedback_rows(feedback.feedback)
    return _panel(
        "Aggregated votes",
        "Feedback analytics",
        body,
        "feedback-heading",
        "feedback-strip",
        "feedback-strip",
    )


def _hero(overview: OverviewResponse) -> str:
    return f"""
    <header class="hero">
      <p class="eyebrow">Berlin Insider public ledger</p>
      <div class="hero-grid">
        <div class="hero-copy">
          <h1>Berlin signals, rendered with restraint.</h1>
          <p class="lede">A public read-only view into recent curation, delivery history, audience response, and worker health.</p>
        </div>
        <div class="hero-status">
          <span class="status-label">Last run status</span>
          <strong class="status-pill">{escape(_display(overview.worker.last_status))}</strong>
          <p>Last attempt <span>{escape(_display(overview.worker.last_attempt_at))}</span></p>
          <p>Last delivery <span>{escape(_display(overview.worker.last_delivery_at))}</span></p>
          <p>Snapshot <span>{escape(overview.generated_at)}</span></p>
        </div>
      </div>
      <div class="metrics" id="overview-metrics">{_metrics(overview)}</div>
    </header>
    <main>
"""


def _initial_state(
    overview: OverviewResponse,
    items: ItemsResponse,
    deliveries: DeliveriesResponse,
    feedback: FeedbackResponse,
    ops: OpsResponse,
) -> dict[str, object]:
    return {
        "overview": asdict(overview),
        "items": asdict(items),
        "deliveries": asdict(deliveries),
        "feedback": asdict(feedback),
        "ops": asdict(ops),
        "basePath": BASE_PATH,
    }


def _items_panel(items: ItemsResponse) -> str:
    body = (
        _filters(items)
        + f'<div class="items-grid" id="items-grid">{_item_cards(items.items)}</div>'
    )
    return _panel(
        "Filtered archive",
        "Items",
        body,
        "items-heading",
        "items-panel",
        note="Title, summary, source, category, time, location, and canonical link only.",
    )


def _item_cards(items: list[ItemCard]) -> str:
    if not items:
        return _empty_state("No items stored yet.")
    return "".join(_item_card(item) for item in items)


def _metrics(overview: OverviewResponse) -> str:
    cards = [
        ("Items", overview.counts.items),
        ("Deliveries", overview.counts.message_deliveries),
        ("Feedback", overview.counts.feedback_events),
        ("Sources", overview.counts.sources),
        ("Cache", overview.counts.detail_cache_entries),
    ]
    return "".join(
        f'<article class="metric-card"><span>{escape(label)}</span><strong>{value}</strong></article>'
        for label, value in cards
    )


def _delivery_rows(rows: Sequence[DeliveryRow]) -> str:
    if not rows:
        return _empty_state("No delivery history stored yet.")
    return "".join(_delivery_row(row) for row in rows)


def _feedback_rows(rows: list[FeedbackAggregateRow]) -> str:
    if not rows:
        return _empty_state("No feedback aggregated yet.")
    return "".join(_feedback_row(row) for row in rows)


def _ops_panel(ops: OpsResponse) -> str:
    body = _ops_sources(ops.sources) + _ops_cache(ops) + _ops_state(ops)
    return _panel(
        "Runtime diagnostics", "Operations", body, "ops-heading", "ops-layout", "ops-layout"
    )


def _filters(items: ItemsResponse) -> str:
    return f"""
        <form class="filters" id="items-filters">
          {_select("Source", "source", "filter-source", "All sources", items.available_sources)}
          {_select("Category", "category", "filter-category", "All categories", items.available_categories)}
          {_select("Summary", "has_summary", "filter-summary", "Any", ["true", "false"], ["With summary", "Without summary"])}
          {_select("Timing", "timing", "filter-timing", "All", ["upcoming", "undated"], ["Upcoming", "Undated"])}
          <label class="search-field"><span>Search</span><input type="search" name="search" id="filter-search" placeholder="Title, summary, or location"></label>
        </form>
"""
