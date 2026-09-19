"""Generate fake payment transactions and publish them to Kafka."""

from __future__ import annotations

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
TOPIC = os.getenv("TOPIC", "transactions")
RATE = float(os.getenv("RATE_PER_SEC", "25"))

MERCHANTS = [
    "Amazon",
    "Uber",
    "Starbucks",
    "Netflix",
    "Shell Gas",
    "Apple Store",
    "Walmart",
    "Delta Airlines",
    "Airbnb",
    "Steam",
]
COUNTRIES = ["US", "ES", "DE", "FR", "GB", "BR", "MX", "JP"]
CHANNELS = ["card", "mobile", "web", "atm"]

# Simulated user profiles with typical spend ranges and a home country.
# Legitimate traffic stays local so country_hop only fires on real jumps.
USERS = [
    {
        "user_id": f"user_{i:03d}",
        "avg": random.uniform(20, 120),
        "std": random.uniform(8, 35),
        "home_country": random.choices(COUNTRIES, weights=[40, 15, 10, 8, 8, 7, 6, 6])[0],
    }
    for i in range(1, 41)
]


def wait_for_kafka(bootstrap: str, retries: int = 60) -> KafkaProducer:
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda v: v.encode("utf-8"),
                acks="all",
                linger_ms=20,
            )
            # Force metadata fetch
            producer.partitions_for(TOPIC)
            print(f"[producer] connected to {bootstrap}")
            return producer
        except (NoBrokersAvailable, Exception) as exc:  # noqa: BLE001
            last_err = exc
            print(f"[producer] waiting for kafka ({attempt}/{retries}): {exc}")
            time.sleep(2)
    raise RuntimeError(f"Kafka not available: {last_err}")


def make_transaction(inject_fraud: bool = False) -> dict:
    user = random.choice(USERS)
    now = datetime.now(timezone.utc)

    if inject_fraud:
        # Extreme amount + country hop away from the user's home
        amount = round(random.uniform(user["avg"] * 8, user["avg"] * 25), 2)
        away = [c for c in COUNTRIES if c != user["home_country"]] or COUNTRIES
        country = random.choice(away)
        channel = "web"
        fraud_label = True
    else:
        amount = max(1.0, round(random.gauss(user["avg"], user["std"]), 2))
        # ~3% of legit txns travel; the rest stay in the home country
        if random.random() < 0.03:
            away = [c for c in COUNTRIES if c != user["home_country"]] or COUNTRIES
            country = random.choice(away)
        else:
            country = user["home_country"]
        channel = random.choice(CHANNELS)
        fraud_label = False

    produced_at_ms = int(time.time() * 1000)
    return {
        "txn_id": str(uuid.uuid4()),
        "user_id": user["user_id"],
        "amount": amount,
        "currency": "EUR",
        "merchant": random.choice(MERCHANTS),
        "country": country,
        "channel": channel,
        "ts": now.isoformat(),
        "produced_at_ms": produced_at_ms,
        # Ground truth only for demo evaluation — detector does not use this field
        "is_injected_fraud": fraud_label,
    }


def main() -> None:
    producer = wait_for_kafka(BOOTSTRAP)
    interval = 1.0 / max(RATE, 0.1)
    sent = 0
    print(f"[producer] publishing to topic={TOPIC} rate={RATE}/s")

    while True:
        # ~10% of events are intentional fraud injections (keeps the live feed readable)
        inject = random.random() < 0.10
        txn = make_transaction(inject_fraud=inject)
        producer.send(TOPIC, key=txn["user_id"], value=txn)
        sent += 1
        if sent % 100 == 0:
            producer.flush()
            print(f"[producer] sent={sent} last_amount={txn['amount']} fraud={txn['is_injected_fraud']}")
        time.sleep(interval)


if __name__ == "__main__":
    main()
