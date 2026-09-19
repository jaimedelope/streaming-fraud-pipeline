# Case study notes — Streaming Fraud Pipeline

Use this as the outline for the portfolio case-study page.

## Problem

Payment platforms need to flag suspicious activity in near real time without waiting for overnight batch jobs.

## Solution

A streaming pipeline that:

1. Ingests high-volume synthetic transactions into Kafka
2. Scores each event with online statistical + rule features
3. Surfaces anomalies and latency SLIs on a live dashboard

## Design choices

- **Redpanda** instead of full Kafka: same API, faster local startup for demos
- **Python detector** first: clear rules, easy to explain; Spark/Flink can replace later
- **Redis** as a lightweight serving layer for the UI (not a system of record)

## Metrics to highlight

- Throughput (tx/sec)
- Anomaly rate
- End-to-end latency p50 / p95

## Screenshots to capture

1. Dashboard overview with metrics
2. Live transactions table
3. Anomaly list with reasons
4. `docker compose` services running
5. Architecture diagram
