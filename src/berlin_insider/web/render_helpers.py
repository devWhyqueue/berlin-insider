from __future__ import annotations

from html import escape

from berlin_insider.web.models import (
    DeliveryRow,
    DetailCacheEntryView,
    FeedbackAggregateRow,
    ItemCard,
    OpsResponse,
    SourceStatus,
)


def _cache_row(entry: DetailCacheEntryView) -> str:
    metadata = ", ".join(entry.metadata_keys) or "none"
    return """
        <article class="cache-row">
          <a href="{canonical_url}" target="_blank" rel="noreferrer">{canonical_url}</a>
          <p>{detail_status} · {detail_length} chars · metadata {metadata}</p>
          <p>Fetched {last_fetched_at} · used {last_used_at}</p>
          <p>{summary}</p>
        </article>
    """.format(
        canonical_url=escape(entry.canonical_url),
        detail_status=escape(entry.detail_status),
        detail_length=entry.detail_length,
        metadata=escape(metadata),
        last_fetched_at=escape(entry.last_fetched_at),
        last_used_at=escape(entry.last_used_at),
        summary=escape(entry.summary or "No cached summary."),
    )


def _delivery_row(row: DeliveryRow) -> str:
    alternative = row.alternative_item.title if row.alternative_item is not None else None
    return """
        <article class="timeline-row">
          <div class="timeline-mark"></div>
          <div class="timeline-body">
            <p class="timeline-meta">{digest_kind} · {local_date} · message {telegram_message_id}</p>
            <h3>{primary_title}</h3>
            <p>Sent at {sent_at}</p>
            <p>{alternative_text}</p>
          </div>
        </article>
    """.format(
        digest_kind=escape(row.digest_kind),
        local_date=escape(row.local_date),
        telegram_message_id=escape(row.telegram_message_id),
        primary_title=escape(row.primary_item.title or "Untitled item"),
        sent_at=escape(row.sent_at),
        alternative_text=escape(
            f"Alternative: {alternative}" if alternative else "Alternative: none"
        ),
    )


def _display(value: object) -> str:
    if value in (None, ""):
        return "Not available"
    return str(value)


def _empty_state(text: str) -> str:
    return f'<p class="empty-state">{escape(text)}</p>'


def _feedback_row(row: FeedbackAggregateRow) -> str:
    return f"""
        <article class="feedback-card">
          <p class="feedback-meta">{escape(row.digest_kind)} · {escape(row.local_date)}</p>
          <h3>{escape(row.message_key)}</h3>
          <p>{row.up_votes} up · {row.down_votes} down · {row.total_votes} total</p>
        </article>
    """


def _item_card(item: ItemCard) -> str:
    return """
        <article class="item-card">
          <p class="item-meta">{source} <span>{category}</span></p>
          <h3>{title}</h3>
          <p class="item-summary">{summary}</p>
          <dl class="item-details">
            <div><dt>When</dt><dd>{event_start_at}</dd></div>
            <div><dt>Where</dt><dd>{location}</dd></div>
          </dl>
          <a href="{canonical_url}" target="_blank" rel="noreferrer">Open source</a>
        </article>
    """.format(
        source=escape(item.source_id),
        category=escape(item.category or "uncategorized"),
        title=escape(item.title or "Untitled item"),
        summary=escape(item.summary or "No summary available."),
        event_start_at=escape(_display(item.event_start_at)),
        location=escape(_display(item.location)),
        canonical_url=escape(item.canonical_url),
    )


def _key_values(rows: list[tuple[str, object]]) -> str:
    body = "".join(
        f"<div><dt>{escape(label)}</dt><dd>{escape(_display(value))}</dd></div>"
        for label, value in rows
    )
    return f'<dl class="key-values">{body}</dl>'


def _ops_cache(ops: OpsResponse) -> str:
    entries = ops.detail_cache.recent_entries
    body = (
        "".join(_cache_row(entry) for entry in entries)
        if entries
        else _empty_state("No cache entries stored yet.")
    )
    return (
        f'<section class="ops-card"><h3>Detail cache</h3><p class="ops-lead">'
        f"{ops.detail_cache.total_entries} cached entries</p>{body}</section>"
    )


def _ops_sources(rows: list[SourceStatus]) -> str:
    content = (
        "".join(_source_row(row) for row in rows)
        if rows
        else _empty_state("No sources registered yet.")
    )
    return f'<section class="ops-card"><h3>Sources</h3>{content}</section>'


def _ops_state(ops: OpsResponse) -> str:
    worker = _key_values(
        [
            ("Status", ops.worker_state.last_status),
            ("Last attempt", ops.worker_state.last_attempt_at),
            ("Last success", ops.worker_state.last_success_at),
            ("Last delivery", ops.worker_state.last_delivery_at),
            ("Curated count", ops.worker_state.last_curated_count),
            ("Last error", ops.worker_state.last_error_message),
        ]
    )
    telegram = _key_values([("Last update id", ops.telegram_updates_state.last_update_id)])
    return (
        f'<section class="ops-card"><h3>Worker state</h3>{worker}</section>'
        f'<section class="ops-card"><h3>Telegram updates</h3>{telegram}</section>'
    )


def _panel(
    kicker: str,
    heading: str,
    body: str,
    panel_id: str,
    container_id: str,
    container_class: str = "",
    note: str = "",
) -> str:
    note_html = f'<p class="section-note">{escape(note)}</p>' if note else ""
    return (
        f'<section class="panel" aria-labelledby="{escape(panel_id)}">'
        '<div class="section-heading"><div>'
        f'<p class="kicker">{escape(kicker)}</p><h2 id="{escape(panel_id)}">{escape(heading)}</h2>'
        f'</div>{note_html}</div><div class="{escape(container_class)}" id="{escape(container_id)}">{body}</div></section>'
    )


def _select(
    label: str,
    name: str,
    element_id: str,
    placeholder: str,
    values: list[str],
    labels: list[str] | None = None,
) -> str:
    option_labels = labels or values
    options = "".join(
        f'<option value="{escape(value)}">{escape(text)}</option>'
        for value, text in zip(values, option_labels, strict=True)
    )
    return (
        f'<label><span>{escape(label)}</span><select name="{escape(name)}" id="{escape(element_id)}">'
        f'<option value="">{escape(placeholder)}</option>{options}</select></label>'
    )


def _source_row(row: SourceStatus) -> str:
    return """
        <article class="source-row">
          <p>{source_id}</p>
          <a href="{source_url}" target="_blank" rel="noreferrer">{source_url}</a>
          <span>{adapter_kind}</span>
        </article>
    """.format(
        source_id=escape(row.source_id),
        source_url=escape(row.source_url),
        adapter_kind=escape(row.adapter_kind),
    )
