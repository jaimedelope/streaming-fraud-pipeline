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

    const txBody = document.getElementById("txBody");
    txBody.innerHTML = feed.transactions
      .map((t) => {
        const cls = t.is_anomaly ? "anomaly" : "";
        return `<tr class="${cls}">
          <td>${t.user_id}</td>
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
              <div class="title">${a.user_id} · ${Number(a.amount).toFixed(2)} @ ${a.merchant}</div>
              <div class="meta">score ${Number(a.anomaly_score).toFixed(2)} · ${reasons}<br/>latency ${Number(a.latency_ms).toFixed(1)} ms · ${a.country} / ${a.channel}</div>
            </li>`;
          })
          .join("")
      : `<li><div class="meta">Waiting for anomalies…</div></li>`;

    statusEl.textContent = "live";
    statusEl.classList.add("ok");
  } catch (err) {
    statusEl.textContent = "waiting for data…";
    statusEl.classList.remove("ok");
  }
}

refresh();
setInterval(refresh, 1500);
