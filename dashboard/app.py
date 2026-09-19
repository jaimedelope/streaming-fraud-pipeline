"""Live dashboard for the streaming fraud detection pipeline."""

from __future__ import annotations

import json
import os
import statistics
from pathlib import Path

import redis
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

app = FastAPI(title="Streaming Fraud Pipeline Dashboard")
static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=static_dir), name="static")

_redis: redis.Redis | None = None


def get_redis() -> redis.Redis:
    global _redis
    if _redis is None:
        _redis = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    return _redis


@app.get("/")
def index() -> FileResponse:
    return FileResponse(static_dir / "index.html")


@app.get("/api/health")
def health() -> dict:
    try:
        get_redis().ping()
        return {"status": "ok", "redis": True}
    except Exception as exc:  # noqa: BLE001
        return JSONResponse({"status": "degraded", "redis": False, "error": str(exc)}, status_code=503)


@app.get("/api/metrics")
def metrics() -> dict:
    r = get_redis()
    tx_total = int(r.get("metrics:tx_total") or 0)
    anomaly_total = int(r.get("metrics:anomaly_total") or 0)
    latencies = [float(x) for x in r.lrange("metrics:latency_ms", 0, 199)]

    p50 = p95 = avg = 0.0
    if latencies:
        ordered = sorted(latencies)
        avg = statistics.fmean(ordered)
        p50 = ordered[int(0.50 * (len(ordered) - 1))]
        p95 = ordered[int(0.95 * (len(ordered) - 1))]

    return {
        "tx_total": tx_total,
        "anomaly_total": anomaly_total,
        "anomaly_rate_pct": round((anomaly_total / tx_total) * 100, 2) if tx_total else 0.0,
        "latency_ms": {
            "avg": round(avg, 2),
            "p50": round(p50, 2),
            "p95": round(p95, 2),
            "samples": len(latencies),
        },
    }


@app.get("/api/feed")
def feed(limit: int = 40) -> dict:
    r = get_redis()
    limit = max(1, min(limit, 100))
    txns = [json.loads(x) for x in r.lrange("feed:transactions", 0, limit - 1)]
    anomalies = [json.loads(x) for x in r.lrange("feed:anomalies", 0, min(20, limit) - 1)]
    return {"transactions": txns, "anomalies": anomalies}
