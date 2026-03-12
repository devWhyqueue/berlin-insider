(function () {
  const initialNode = document.getElementById("initial-state");
  if (!initialNode) {
    return;
  }

  const state = JSON.parse(initialNode.textContent || "{}");
  const basePath = state.basePath || "/ui";
  const filtersForm = document.getElementById("items-filters");
  const itemsGrid = document.getElementById("items-grid");
  const metricsRoot = document.getElementById("overview-metrics");
  const deliveriesRoot = document.getElementById("deliveries-timeline");
  const feedbackRoot = document.getElementById("feedback-strip");
  const opsRoot = document.getElementById("ops-layout");

  const htmlEscape = (value) =>
    String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");

  const display = (value) => (value === null || value === undefined || value === "" ? "Not available" : String(value));

  const renderMetrics = (overview) => {
    const metrics = [
      ["Items", overview.counts.items],
      ["Deliveries", overview.counts.message_deliveries],
      ["Feedback", overview.counts.feedback_events],
      ["Sources", overview.counts.sources],
      ["Cache", overview.counts.detail_cache_entries],
    ];
    metricsRoot.innerHTML = metrics
      .map(([label, value]) => `<article class="metric-card"><span>${htmlEscape(label)}</span><strong>${value}</strong></article>`)
      .join("");
  };

  const renderItems = (payload) => {
    if (!payload.items.length) {
      itemsGrid.innerHTML = '<p class="empty-state">No items match the current filters.</p>';
      return;
    }
    itemsGrid.innerHTML = payload.items
      .map(
        (item) => `
          <article class="item-card">
            <p class="item-meta">${htmlEscape(item.source_id)} <span>${htmlEscape(item.category || "uncategorized")}</span></p>
            <h3>${htmlEscape(item.title || "Untitled item")}</h3>
            <p class="item-summary">${htmlEscape(item.summary || "No summary available.")}</p>
            <dl class="item-details">
              <div><dt>When</dt><dd>${htmlEscape(display(item.event_start_at))}</dd></div>
              <div><dt>Where</dt><dd>${htmlEscape(display(item.location))}</dd></div>
            </dl>
            <a href="${htmlEscape(item.canonical_url)}" target="_blank" rel="noreferrer">Open source</a>
          </article>
        `,
      )
      .join("");
  };

  const renderDeliveries = (payload) => {
    if (!payload.deliveries.length) {
      deliveriesRoot.innerHTML = '<p class="empty-state">No delivery history stored yet.</p>';
      return;
    }
    deliveriesRoot.innerHTML = payload.deliveries
      .map(
        (delivery) => `
          <article class="timeline-row">
            <div class="timeline-mark"></div>
            <div class="timeline-body">
              <p class="timeline-meta">${htmlEscape(delivery.digest_kind)} · ${htmlEscape(delivery.local_date)} · message ${htmlEscape(delivery.telegram_message_id)}</p>
              <h3>${htmlEscape(delivery.primary_item.title || "Untitled item")}</h3>
              <p>Sent at ${htmlEscape(delivery.sent_at)}</p>
              <p>${htmlEscape(delivery.alternative_item?.title ? `Alternative: ${delivery.alternative_item.title}` : "Alternative: none")}</p>
            </div>
          </article>
        `,
      )
      .join("");
  };

  const renderFeedback = (payload) => {
    if (!payload.feedback.length) {
      feedbackRoot.innerHTML = '<p class="empty-state">No feedback aggregated yet.</p>';
      return;
    }
    feedbackRoot.innerHTML = payload.feedback
      .map(
        (row) => `
          <article class="feedback-card">
            <p class="feedback-meta">${htmlEscape(row.digest_kind)} · ${htmlEscape(row.local_date)}</p>
            <h3>${htmlEscape(row.message_key)}</h3>
            <p>${row.up_votes} up · ${row.down_votes} down · ${row.total_votes} total</p>
          </article>
        `,
      )
      .join("");
  };

  const renderOps = (payload) => {
    const sourceMarkup = payload.sources.length
      ? payload.sources
          .map(
            (source) => `
              <article class="source-row">
                <p>${htmlEscape(source.source_id)}</p>
                <a href="${htmlEscape(source.source_url)}" target="_blank" rel="noreferrer">${htmlEscape(source.source_url)}</a>
                <span>${htmlEscape(source.adapter_kind)}</span>
              </article>
            `,
          )
          .join("")
      : '<p class="empty-state">No sources registered yet.</p>';
    const cacheMarkup = payload.detail_cache.recent_entries.length
      ? payload.detail_cache.recent_entries
          .map(
            (entry) => `
              <article class="cache-row">
                <a href="${htmlEscape(entry.canonical_url)}" target="_blank" rel="noreferrer">${htmlEscape(entry.canonical_url)}</a>
                <p>${htmlEscape(entry.detail_status)} · ${entry.detail_length} chars · metadata ${htmlEscape(entry.metadata_keys.join(", ") || "none")}</p>
                <p>${htmlEscape(entry.summary || "No cached summary.")}</p>
              </article>
            `,
          )
          .join("")
      : '<p class="empty-state">No cache entries stored yet.</p>';

    opsRoot.innerHTML = `
      <section class="ops-card">
        <h3>Sources</h3>
        ${sourceMarkup}
      </section>
      <section class="ops-card">
        <h3>Detail cache</h3>
        <p class="ops-lead">${payload.detail_cache.total_entries} cached entries</p>
        ${cacheMarkup}
      </section>
      <section class="ops-card">
        <h3>Worker state</h3>
        <dl class="key-values">
          <div><dt>Status</dt><dd>${htmlEscape(display(payload.worker_state.last_status))}</dd></div>
          <div><dt>Last attempt</dt><dd>${htmlEscape(display(payload.worker_state.last_attempt_at))}</dd></div>
          <div><dt>Last success</dt><dd>${htmlEscape(display(payload.worker_state.last_success_at))}</dd></div>
          <div><dt>Last delivery</dt><dd>${htmlEscape(display(payload.worker_state.last_delivery_at))}</dd></div>
          <div><dt>Curated count</dt><dd>${htmlEscape(display(payload.worker_state.last_curated_count))}</dd></div>
          <div><dt>Last error</dt><dd>${htmlEscape(display(payload.worker_state.last_error_message))}</dd></div>
        </dl>
      </section>
      <section class="ops-card">
        <h3>Telegram updates</h3>
        <dl class="key-values">
          <div><dt>Last update id</dt><dd>${htmlEscape(display(payload.telegram_updates_state.last_update_id))}</dd></div>
        </dl>
      </section>
    `;
  };

  const fetchJson = async (path, target) => {
    target?.classList.add("is-loading");
    try {
      const response = await fetch(path, { headers: { Accept: "application/json" } });
      if (!response.ok) {
        throw new Error(`Request failed: ${response.status}`);
      }
      return await response.json();
    } finally {
      target?.classList.remove("is-loading");
    }
  };

  const refreshItems = async () => {
    const params = new URLSearchParams(new FormData(filtersForm));
    [...params.keys()].forEach((key) => {
      if (!params.get(key)) {
        params.delete(key);
      }
    });
    const query = params.toString();
    const payload = await fetchJson(`${basePath}/api/items${query ? `?${query}` : ""}`, itemsGrid);
    renderItems(payload);
  };

  filtersForm?.addEventListener("input", () => {
    void refreshItems();
  });
  filtersForm?.addEventListener("change", () => {
    void refreshItems();
  });

  void Promise.all([
    fetchJson(`${basePath}/api/overview`, metricsRoot),
    fetchJson(`${basePath}/api/deliveries`, deliveriesRoot),
    fetchJson(`${basePath}/api/feedback`, feedbackRoot),
    fetchJson(`${basePath}/api/ops`, opsRoot),
  ]).then(([overview, deliveries, feedback, ops]) => {
    renderMetrics(overview);
    renderDeliveries(deliveries);
    renderFeedback(feedback);
    renderOps(ops);
  }).catch(() => {
    return;
  });
})();
