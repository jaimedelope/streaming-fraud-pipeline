const RATE_PER_SEC = 25;
const FRAUD_RATE = 0.1;
const WINDOW = 50;
const Z_THRESHOLD = 3.0;
const VELOCITY_WINDOW_SEC = 30;
const VELOCITY_LIMIT = 30;
const SCORE_THRESHOLD = 2.5;
const FEED_TX = 200;
const FEED_ANOM = 100;
const LATENCY_SAMPLES = 200;

const MERCHANTS = [
  "Amazon", "Uber", "Starbucks", "Netflix", "Shell Gas",
  "Apple Store", "Walmart", "Delta Airlines", "Airbnb", "Steam",
];
const COUNTRIES = ["US", "ES", "DE", "FR", "GB", "BR", "MX", "JP"];
const COUNTRY_WEIGHTS = [40, 15, 10, 8, 8, 7, 6, 6];
const CHANNELS = ["card", "mobile", "web", "atm"];

function weightedChoice(items, weights) {
  const total = weights.reduce((a, b) => a + b, 0);
  let r = Math.random() * total;
  for (let i = 0; i < items.length; i += 1) {
    r -= weights[i];
    if (r <= 0) return items[i];
  }
  return items[items.length - 1];
}

function gaussian(mean, std) {
  let u = 0;
  let v = 0;
  while (u === 0) u = Math.random();
  while (v === 0) v = Math.random();
  return mean + std * Math.sqrt(-2.0 * Math.log(u)) * Math.cos(2.0 * Math.PI * v);
}

function mean(arr) {
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

function stdev(arr) {
  const m = mean(arr);
  const v = arr.reduce((s, x) => s + (x - m) ** 2, 0) / arr.length;
  return Math.sqrt(v) || 1.0;
}

function percentile(sorted, p) {
  if (!sorted.length) return 0;
  return sorted[Math.floor(p * (sorted.length - 1))];
}

const USERS = Array.from({ length: 40 }, (_, i) => ({
  user_id: `user_${String(i + 1).padStart(3, "0")}`,
  avg: 20 + Math.random() * 100,
  std: 8 + Math.random() * 27,
  home_country: weightedChoice(COUNTRIES, COUNTRY_WEIGHTS),
}));

const userState = new Map();
const transactions = [];
const anomalies = [];
const latencies = [];
let txTotal = 0;
let anomalyTotal = 0;

function getState(userId) {
  let state = userState.get(userId);
  if (!state) {
    state = { amounts: [], recentTs: [], lastCountry: null };
    userState.set(userId, state);
  }
  return state;
}

function makeTransaction() {
  const user = USERS[Math.floor(Math.random() * USERS.length)];
  const inject = Math.random() < FRAUD_RATE;
  let amount;
  let country;
  let channel;
  if (inject) {
    amount = Number((user.avg * (8 + Math.random() * 17)).toFixed(2));
    const away = COUNTRIES.filter((c) => c !== user.home_country);
    country = away[Math.floor(Math.random() * away.length)] || user.home_country;
    channel = "web";
  } else {
    amount = Math.max(1.0, Number(gaussian(user.avg, user.std).toFixed(2)));
    if (Math.random() < 0.03) {
      const away = COUNTRIES.filter((c) => c !== user.home_country);
      country = away[Math.floor(Math.random() * away.length)] || user.home_country;
    } else {
      country = user.home_country;
    }
    channel = CHANNELS[Math.floor(Math.random() * CHANNELS.length)];
  }
  return {
    txn_id: crypto.randomUUID ? crypto.randomUUID() : String(Math.random()),
    user_id: user.user_id,
    amount,
    currency: "EUR",
    merchant: MERCHANTS[Math.floor(Math.random() * MERCHANTS.length)],
    country,
    channel,
    produced_at_ms: Date.now(),
  };
}

function scoreTransaction(txn, state, nowMs) {
  const reasons = [];
  let score = 0;
  const amount = txn.amount;

  if (state.amounts.length >= 10) {
    const z = Math.abs(amount - mean(state.amounts)) / stdev(state.amounts);
    if (z >= Z_THRESHOLD) {
      score += Math.min(z / 2.0, 4.0);
      reasons.push(`amount_zscore=${z.toFixed(2)}`);
    }
  }

  const cutoff = nowMs - VELOCITY_WINDOW_SEC * 1000;
  while (state.recentTs.length && state.recentTs[0] < cutoff) state.recentTs.shift();
  if (state.recentTs.length >= VELOCITY_LIMIT) {
    score += 2.5;
    reasons.push(`velocity=${state.recentTs.length}/${VELOCITY_WINDOW_SEC}s`);
  }

  if (state.lastCountry && txn.country && txn.country !== state.lastCountry && amount > 80) {
    score += 1.5;
    reasons.push(`country_hop=${state.lastCountry}->${txn.country}`);
  }

  if (amount >= 800) {
    score += 2.0;
    reasons.push("amount_abs_high");
  }

  return { score, reasons };
}

function ingestOne(nowMs = Date.now()) {
  const txn = makeTransaction();
  txn.produced_at_ms = nowMs;
  const state = getState(txn.user_id);
  const { score, reasons } = scoreTransaction(txn, state, nowMs);

  state.amounts.push(txn.amount);
  if (state.amounts.length > WINDOW) state.amounts.shift();
  state.recentTs.push(nowMs);
  state.lastCountry = txn.country;

  const latencyMs = 18 + Math.random() * 8;
  const isAnomaly = score >= SCORE_THRESHOLD;
  const result = {
    ...txn,
    anomaly_score: Number(score.toFixed(3)),
    is_anomaly: isAnomaly,
    reasons,
    latency_ms: Number(latencyMs.toFixed(2)),
  };

  transactions.unshift(result);
  if (transactions.length > FEED_TX) transactions.pop();
  if (isAnomaly) {
    anomalies.unshift(result);
    if (anomalies.length > FEED_ANOM) anomalies.pop();
    anomalyTotal += 1;
  }
  latencies.unshift(result.latency_ms);
  if (latencies.length > LATENCY_SAMPLES) latencies.pop();
  txTotal += 1;
}

function metrics() {
  const ordered = [...latencies].sort((a, b) => a - b);
  const avg = ordered.length ? ordered.reduce((s, x) => s + x, 0) / ordered.length : 0;
  return {
    tx_total: txTotal,
    anomaly_total: anomalyTotal,
    anomaly_rate_pct: txTotal ? Number(((anomalyTotal / txTotal) * 100).toFixed(2)) : 0,
    latency_ms: {
      avg: Number(avg.toFixed(2)),
      p50: Number(percentile(ordered, 0.5).toFixed(2)),
      p95: Number(percentile(ordered, 0.95).toFixed(2)),
    },
  };
}

const userIcon = `<svg viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="5.2" r="2.4" fill="none" stroke="currentColor" stroke-width="1.4"/><path d="M3.4 13c.6-2.4 2.3-3.6 4.6-3.6s4 1.2 4.6 3.6" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"/></svg>`;
const alertIcon = `<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3 3.8 18.2A1.2 1.2 0 0 0 4.9 20h14.2a1.2 1.2 0 0 0 1.1-1.8L12 3z" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linejoin="round"/><path d="M12 9v5" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/><circle cx="12" cy="16.6" r="1" fill="currentColor"/></svg>`;

function render() {
  const statusEl = document.getElementById("status");
  const m = metrics();
  document.getElementById("txTotal").textContent = m.tx_total.toLocaleString();
  document.getElementById("anomTotal").textContent = m.anomaly_total.toLocaleString();
  document.getElementById("anomRate").textContent = `${m.anomaly_rate_pct}%`;
  document.getElementById("p50").textContent = `${m.latency_ms.p50} ms`;
  document.getElementById("p95").textContent = `${m.latency_ms.p95} ms`;
  document.getElementById("avg").textContent = `${m.latency_ms.avg} ms`;

  document.getElementById("txBody").innerHTML = transactions.slice(0, 40).map((t) => {
    const cls = t.is_anomaly ? "anomaly" : "";
    return `<tr class="${cls}">
      <td><span class="cell-user">${userIcon}${t.user_id}</span></td>
      <td>${Number(t.amount).toFixed(2)} ${t.currency}</td>
      <td>${t.merchant}</td>
      <td>${t.country}</td>
      <td class="score">${Number(t.anomaly_score).toFixed(2)}</td>
      <td>${Number(t.latency_ms).toFixed(1)} ms</td>
    </tr>`;
  }).join("");

  const anomList = document.getElementById("anomList");
  anomList.innerHTML = anomalies.length
    ? anomalies.slice(0, 20).map((a) => {
      const reasons = (a.reasons || []).join(", ") || "rule hit";
      return `<li>
        <span class="anom-icon">${alertIcon}</span>
        <div>
          <div class="title">${a.user_id} · ${Number(a.amount).toFixed(2)} @ ${a.merchant}</div>
          <div class="meta">score ${Number(a.anomaly_score).toFixed(2)} · ${reasons}<br/>latency ${Number(a.latency_ms).toFixed(1)} ms · ${a.country} / ${a.channel}</div>
        </div>
      </li>`;
    }).join("")
    : `<li><span class="anom-icon">${alertIcon}</span><div class="meta">Waiting for anomalies…</div></li>`;

  const statusText = document.getElementById("statusText");
  if (statusText) statusText.textContent = "live";
  statusEl.classList.add("ok");
}

const warmupOrigin = Date.now() - 120000;
for (let i = 0; i < 400; i += 1) ingestOne(warmupOrigin + i * 200);
render();
setInterval(ingestOne, Math.round(1000 / RATE_PER_SEC));
setInterval(render, 1500);
