async function refresh() {
  const statusEl = document.getElementById("status");
  try {
    const [metricsRes, feedRes] = await Promise.all([
      fetch("/api/metrics"),
      fetch("/api/feed?limit=40"),
    ]);
    if (!metricsRes.ok || !feedRes.ok) throw new Error("API error");

    const metrics = await metricsRes.json();
    const feed = await feedRes.json();

    document.getElementById("txTotal").textContent = metrics.tx_total.toLocaleString();
    document.getElementById("anomTotal").textContent = metrics.anomaly_total.toLocaleString();
    document.getElementById("anomRate").textContent = `${metrics.anomaly_rate_pct}%`;
    document.getElementById("p50").textContent = `${metrics.latency_ms.p50} ms`;
    document.getElementById("p95").textContent = `${metrics.latency_ms.p95} ms`;
    document.getElementById("avg").textContent = `${metrics.latency_ms.avg} ms`;

    const userIcon = `<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="5.2" r="2.4" fill="none" stroke="currentColor" stroke-width="1.4"/><path d="M3.4 13c.6-2.4 2.3-3.6 4.6-3.6s4 1.2 4.6 3.6" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"/></svg>`;
    const alertIcon = `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3 3.8 18.2A1.2 1.2 0 0 0 4.9 20h14.2a1.2 1.2 0 0 0 1.1-1.8L12 3z" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linejoin="round"/><path d="M12 9v5" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/><circle cx="12" cy="16.6" r="1" fill="currentColor"/></svg>`;

    const txBody = document.getElementById("txBody");
    txBody.innerHTML = feed.transactions
      .map((t) => {
        const cls = t.is_anomaly ? "anomaly" : "";
        return `<tr class="${cls}">
          <td><span class="cell-user">${userIcon}${t.user_id}</span></td>
          <td>${Number(t.amount).toFixed(2)} ${t.currency || ""}</td>
          <td>${t.merchant}</td>
          <td>${t.country}</td>
          <td class="score">${Number(t.anomaly_score).toFixed(2)}</td>
          <td>${Number(t.latency_ms).toFixed(1)} ms</td>
        </tr>`;
      })
      .join("");

    const anomList = document.getElementById("anomList");
    anomList.innerHTML = feed.anomalies.length
      ? feed.anomalies
          .map((a) => {
            const reasons = (a.reasons || []).join(", ") || "rule hit";
            return `<li>
              <span class="anom-icon">${alertIcon}</span>
              <div>
                <div class="title">${a.user_id} · ${Number(a.amount).toFixed(2)} @ ${a.merchant}</div>
                <div class="meta">score ${Number(a.anomaly_score).toFixed(2)} · ${reasons}<br/>latency ${Number(a.latency_ms).toFixed(1)} ms · ${a.country} / ${a.channel}</div>
              </div>
            </li>`;
          })
          .join("")
      : `<li><span class="anom-icon">${alertIcon}</span><div class="meta">Waiting for anomalies…</div></li>`;

    const statusText = document.getElementById("statusText");
    if (statusText) statusText.textContent = "live";
    else statusEl.textContent = "live";
    statusEl.classList.add("ok");
  } catch (err) {
    const statusText = document.getElementById("statusText");
    if (statusText) statusText.textContent = "waiting for data…";
    else statusEl.textContent = "waiting for data…";
    statusEl.classList.remove("ok");
  }
}

refresh();
setInterval(refresh, 1500);
