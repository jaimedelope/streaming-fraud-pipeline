# Streaming Fraud Detection Pipeline

End-to-end **real-time demo** for a data engineering portfolio:

**fake transactions → Kafka (Redpanda) → anomaly detector → Redis → live dashboard**

Designed to be easy to run locally with Docker and easy to explain in a case study.

## Architecture

```text
┌────────────┐   transactions   ┌──────────────┐   anomalies    ┌──────────┐
│  Producer  │ ───────────────► │   Redpanda   │ ◄───────────── │ Detector │
│ (Python)   │                  │ (Kafka API)  │ ─────────────► │ (Python) │
└────────────┘                  └──────────────┘   consume tx   └────┬─────┘
                                                                     │
                                                              metrics + feeds
                                                                     ▼
                                                              ┌─────────────┐
                                                              │    Redis    │
                                                              └──────┬──────┘
                                                                     │
                                                                     ▼
                                                              ┌─────────────┐
                                                              │  Dashboard  │
                                                              │  :8088      │
                                                              └─────────────┘
```

### What each piece does

| Component | Role |
|-----------|------|
| **Producer** | Emits ~25 fake card transactions/sec across 40 users (merchant, channel, home country). ~10% are intentional fraud injections (high amount + country hop). |
| **Redpanda** | Kafka-compatible broker (lighter than full Kafka for demos). Topics: `transactions`, `anomalies`. |
| **Detector** | Rolling per-user stats, z-score on amount, velocity, country hops. Publishes anomalies + latency. Does **not** read the producer’s `is_injected_fraud` label. |
| **Redis** | Shared live feeds + latency samples for the UI (not a system of record). |
| **Dashboard** | FastAPI UI on `:8088` with counters, anomaly rate, p50/p95/avg latency, live tables. |

## Quick start

Prerequisites: **Docker Desktop** running.

```bash
docker compose up --build
```

Open the dashboard: [http://localhost:8088](http://localhost:8088)

You should see transactions streaming within ~30–60 seconds (broker + services warming up).

Stop everything:

```bash
docker compose down -v
```

## Anomaly logic (intentionally simple)

The detector scores each event with explainable rules (great for interviews):

1. **Amount z-score** vs the user’s last 50 amounts (needs ≥10 samples; flag if ≥ 3σ)
2. **Velocity** — ≥ 30 transactions in 30 seconds (above the ~19 tx/user expected at 25/s)
3. **Country hop** with amount > 80 (legit traffic stays in the user’s home country; injected fraud jumps)
4. **Absolute amount** spike (≥ 800)

Events with score ≥ 2.5 are marked as anomalies. Injected fraud plus those rules land around a **~10%** anomaly rate on the dashboard.

Latency is `consumed_at_ms - produced_at_ms` (producer timestamp embedded in each event). The UI shows p50 / p95 / average of the recent samples.

## Project layout

```text
producer/     # transaction generator
detector/     # streaming consumer + scoring
dashboard/    # FastAPI UI
docs/         # case-study notes
docker-compose.yml
```

## Next ideas (for v2)

- Spark Structured Streaming job instead of (or next to) the Python detector
- Dead-letter topic + replay
- Prometheus / Grafana for latency histograms
- Feature store-style aggregations windowed in Flink

## Portfolio note

This repo is the **implementation**. The portfolio site will link here as the GitHub “Code” URL and host a case study page with screenshots + architecture narrative.
