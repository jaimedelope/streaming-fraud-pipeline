"""Consume transactions, score anomalies, publish results and metrics."""

from __future__ import annotations

import json
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field

import numpy as np
import redis
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
TX_TOPIC = os.getenv("TRANSACTIONS_TOPIC", "transactions")
ANOM_TOPIC = os.getenv("ANOMALIES_TOPIC", "anomalies")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

WINDOW = 50  # per-user rolling window for amount stats
Z_THRESHOLD = 3.0
VELOCITY_WINDOW_SEC = 30
# ~25 tx/s across 40 users ≈ 19 tx/user/30s. Flag only a clear burst above that.
VELOCITY_LIMIT = 30


@dataclass
class UserState:
    amounts: deque = field(default_factory=lambda: deque(maxlen=WINDOW))
    recent_ts: deque = field(default_factory=lambda: deque(maxlen=100))
    last_country: str | None = None


def wait_redis(url: str, retries: int = 60) -> redis.Redis:
    client = redis.Redis.from_url(url, decode_responses=True)
    for attempt in range(1, retries + 1):
        try:
            client.ping()
            print(f"[detector] redis connected")
            return client
        except Exception as exc:  # noqa: BLE001
            print(f"[detector] waiting for redis ({attempt}/{retries}): {exc}")
            time.sleep(2)
    raise RuntimeError("Redis not available")


def wait_consumer(bootstrap: str) -> KafkaConsumer:
    last_err: Exception | None = None
    for attempt in range(1, 61):
        try:
            consumer = KafkaConsumer(
                TX_TOPIC,
                bootstrap_servers=bootstrap,
                group_id="fraud-detector",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                key_deserializer=lambda b: b.decode("utf-8") if b else None,
                consumer_timeout_ms=1000,
            )
            # Touch cluster
            consumer.topics()
            print(f"[detector] consumer connected to {bootstrap}")
            return consumer
        except (NoBrokersAvailable, Exception) as exc:  # noqa: BLE001
            last_err = exc
            print(f"[detector] waiting for kafka consumer ({attempt}/60): {exc}")
            time.sleep(2)
    raise RuntimeError(f"Kafka consumer failed: {last_err}")


def wait_producer(bootstrap: str) -> KafkaProducer:
    last_err: Exception | None = None
    for attempt in range(1, 61):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda v: v.encode("utf-8"),
                acks="all",
            )
            producer.partitions_for(ANOM_TOPIC)
            print(f"[detector] producer connected")
            return producer
        except (NoBrokersAvailable, Exception) as exc:  # noqa: BLE001
            last_err = exc
            print(f"[detector] waiting for kafka producer ({attempt}/60): {exc}")
            time.sleep(2)
    raise RuntimeError(f"Kafka producer failed: {last_err}")


def score_transaction(txn: dict, state: UserState) -> tuple[float, list[str]]:
    reasons: list[str] = []
    score = 0.0
    amount = float(txn["amount"])
    now_ms = int(time.time() * 1000)

    # Z-score on amount once we have enough history
    if len(state.amounts) >= 10:
        arr = np.array(state.amounts, dtype=float)
        mean = float(arr.mean())
        std = float(arr.std()) or 1.0
        z = abs(amount - mean) / std
        if z >= Z_THRESHOLD:
            score += min(z / 2.0, 4.0)
            reasons.append(f"amount_zscore={z:.2f}")

    # Velocity: too many txns in a short window
    cutoff = now_ms - VELOCITY_WINDOW_SEC * 1000
    while state.recent_ts and state.recent_ts[0] < cutoff:
        state.recent_ts.popleft()
    if len(state.recent_ts) >= VELOCITY_LIMIT:
        score += 2.5
        reasons.append(f"velocity={len(state.recent_ts)}/{VELOCITY_WINDOW_SEC}s")

    # Country hop after established history
    country = txn.get("country")
    if state.last_country and country and country != state.last_country and amount > 80:
        score += 1.5
        reasons.append(f"country_hop={state.last_country}->{country}")

    # Hard rule for very large absolute amounts
    if amount >= 800:
        score += 2.0
        reasons.append("amount_abs_high")

    return score, reasons


def update_state(state: UserState, txn: dict) -> None:
    state.amounts.append(float(txn["amount"]))
    state.recent_ts.append(int(time.time() * 1000))
    state.last_country = txn.get("country")


def push_metrics(r: redis.Redis, latency_ms: float, is_anomaly: bool) -> None:
    pipe = r.pipeline()
    pipe.incr("metrics:tx_total")
    if is_anomaly:
        pipe.incr("metrics:anomaly_total")
    pipe.lpush("metrics:latency_ms", f"{latency_ms:.2f}")
    pipe.ltrim("metrics:latency_ms", 0, 999)
    pipe.execute()


def main() -> None:
    r = wait_redis(REDIS_URL)
    consumer = wait_consumer(BOOTSTRAP)
    producer = wait_producer(BOOTSTRAP)
    users: dict[str, UserState] = defaultdict(UserState)
    r.delete("feed:transactions", "feed:anomalies", "metrics:latency_ms")
    r.set("metrics:tx_total", 0)
    r.set("metrics:anomaly_total", 0)

    print(f"[detector] listening on {TX_TOPIC} → {ANOM_TOPIC}")
    processed = 0

    while True:
        polled = consumer.poll(timeout_ms=1000, max_records=100)
        if not polled:
            continue

        for _tp, records in polled.items():
            for record in records:
                txn = record.value
                user_id = txn.get("user_id", "unknown")
                state = users[user_id]

                score, reasons = score_transaction(txn, state)
                update_state(state, txn)

                consumed_at_ms = int(time.time() * 1000)
                produced_at_ms = int(txn.get("produced_at_ms") or consumed_at_ms)
                latency_ms = max(0.0, float(consumed_at_ms - produced_at_ms))

                is_anomaly = score >= 2.5
                result = {
                    **txn,
                    "anomaly_score": round(score, 3),
                    "is_anomaly": is_anomaly,
                    "reasons": reasons,
                    "consumed_at_ms": consumed_at_ms,
                    "latency_ms": round(latency_ms, 2),
                }

                # Keep a live feed of all txns (trimmed)
                r.lpush("feed:transactions", json.dumps(result))
                r.ltrim("feed:transactions", 0, 199)

                if is_anomaly:
                    producer.send(ANOM_TOPIC, key=user_id, value=result)
                    r.lpush("feed:anomalies", json.dumps(result))
                    r.ltrim("feed:anomalies", 0, 99)

                push_metrics(r, latency_ms, is_anomaly)
                processed += 1
                if processed % 100 == 0:
                    print(
                        f"[detector] processed={processed} "
                        f"last_score={score:.2f} anomaly={is_anomaly} latency_ms={latency_ms:.1f}"
                    )

        producer.flush()


if __name__ == "__main__":
    main()
