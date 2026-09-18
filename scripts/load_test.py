"""Finite Kafka -> ClickHouse -> REST volume-reconciliation and rate test.

Uses a unique benchmark symbol; creates real persisted synthetic data. Does NOT
benchmark Binance network latency or exactly-once processing during failures.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import httpx
from confluent_kafka import Producer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from services.ingestion.main import IngestionConfig, make_synthetic_tick, tick_json  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=int, default=1500)
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--min-rate", type=int, default=1000)
    args = parser.parse_args()
    if min(args.rate, args.seconds, args.min_rate) <= 0 or args.rate * args.seconds > 10_000_000:
        parser.error("positive values required; maximum 10 million ticks per run")
    run_id = uuid.uuid4().hex
    symbol = "BENCH" + run_id[:16].upper()
    config = IngestionConfig(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))
    producer = Producer(config.producer_config())
    errors = []
    acknowledged = 0

    def on_delivery(error, _):
        nonlocal acknowledged
        if error is not None:
            errors.append(str(error))
        else:
            acknowledged += 1

    start_window = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    started = time.monotonic()
    expected = Decimal(0)
    count = args.rate * args.seconds
    for index in range(count):
        # Schedule batches of 100 to avoid timer granularity capping generator rate.
        if index % 100 == 0:
            time.sleep(max(0, started + index / args.rate - time.monotonic()))
            producer.poll(0)
        tick = make_synthetic_tick(run_id, index, symbol)
        expected += Decimal(tick.quantity)
        while True:
            try:
                producer.produce(config.topic, key=symbol.encode(), value=tick_json(tick), on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.01)
    last_sent = time.monotonic()
    remaining = producer.flush(30)
    elapsed = time.monotonic() - started
    rate = acknowledged / elapsed
    if remaining or errors or acknowledged != count:
        print(json.dumps({"status": "failed", "unflushed": remaining, "delivery_errors": errors[:5], "acknowledged": acknowledged}))
        return 1
    end_window = datetime.now(timezone.utc).replace(second=0, microsecond=0) + timedelta(minutes=1)
    url = os.getenv("ORDERFLOW_API_URL", "http://localhost:8000") + f"/api/v1/footprint/{symbol}"
    observed = Decimal(0)
    with httpx.Client(timeout=10) as client:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            response = client.get(url, params={"start": start_window.isoformat(), "end": end_window.isoformat()},
                                  headers={"X-API-Key": os.getenv("API_KEY", "")})
            response.raise_for_status()
            observed = sum((Decimal(c["total_volume"]) for c in response.json()["candles"]), Decimal(0))
            if observed >= expected:
                break
            time.sleep(0.25)
    passed = observed == expected and rate >= args.min_rate
    print(json.dumps({"status": "passed" if passed else "failed", "symbol": symbol, "acknowledged": acknowledged,
                      "acknowledged_ticks_per_second": round(rate, 1), "minimum_rate": args.min_rate,
                      "expected_base_volume": str(expected), "observed_base_volume": str(observed),
                      "tail_visible_seconds": round(time.monotonic() - last_sent, 3)}, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
