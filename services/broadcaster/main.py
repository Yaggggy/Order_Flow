from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
import uuid
from datetime import datetime, timedelta

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from orderflow.analytics import UTC, fetch_footprint, minute_floor
from orderflow.config import Settings
from orderflow.runtime import clickhouse_client, configure_logging, log, redis_client

BROADCASTS = Counter("broadcaster_snapshots_total", "Snapshots published", ["symbol"])
ERRORS = Counter("broadcaster_errors_total", "Broadcast polling failures", ["symbol"])
DURATION = Histogram("broadcaster_cycle_seconds", "One full polling cycle")
LAST_SUCCESS = Gauge("broadcaster_last_success_timestamp_seconds", "Last successfully published snapshot", ["symbol"])
LEADER_KEY = "orderflow:broadcaster:lease"

PUBLISH_LUA = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
local seq = redis.call('INCR', KEYS[2])
local encoded = string.sub(ARGV[2], 1, -2) .. ',"sequence":' .. tostring(seq) .. '}'
redis.call('SET', KEYS[3], encoded, 'EX', ARGV[3])
redis.call('PUBLISH', KEYS[4], encoded)
return seq
"""
RENEW_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
 return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"""
RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
"""


async def run(settings: Settings) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    redis = redis_client(settings)
    ch = None
    token = uuid.uuid4().hex
    try:
        while not stop.is_set():
            started = time.monotonic()
            try:
                owned = await redis.eval(RENEW_LUA, 1, LEADER_KEY, token, 30_000)
                if not owned:
                    owned = await redis.set(LEADER_KEY, token, nx=True, px=30_000)
                if owned:
                    if ch is None:
                        ch = await clickhouse_client(settings)
                    
                    now = datetime.now(UTC)
                    end = minute_floor(now) + timedelta(minutes=1)
                    start = end - timedelta(minutes=settings.broadcast_lookback_minutes)
                    for symbol in settings.symbols:
                        if stop.is_set():
                            break
                        try:
                            payload = await fetch_footprint(ch, symbol, start, end,
                                                           max_price_levels=settings.max_price_levels)
                            payload.update(type="footprint_snapshot", semantics="replace_window",
                                           snapshot_id=uuid.uuid4().hex)
                            sequence = await redis.eval(
                                PUBLISH_LUA, 4, LEADER_KEY, f"orderflow:seq:{symbol}",
                                f"orderflow:last:{symbol}", f"orderflow:{symbol}",
                                token, json.dumps(payload, separators=(",", ":")), settings.snapshot_ttl_seconds,
                            )
                            if sequence:
                                BROADCASTS.labels(symbol).inc()
                                LAST_SUCCESS.labels(symbol).set(time.time())
                            else:
                                log("broadcaster_lease_lost", level=logging.WARNING)
                                break
                        except Exception as exc:
                            ERRORS.labels(symbol).inc()
                            log("broadcast_failed", level=logging.ERROR, symbol=symbol, error_type=type(exc).__name__)
            except Exception as exc:
                ERRORS.labels("dependency").inc()
                log("broadcaster_dependency_failed", level=logging.ERROR, error_type=type(exc).__name__)
            elapsed = time.monotonic() - started
            DURATION.observe(elapsed)
            try:
                await asyncio.wait_for(stop.wait(), timeout=max(0.05, settings.broadcast_interval - elapsed))
            except TimeoutError:
                pass
    finally:
        try:
            await redis.eval(RELEASE_LUA, 1, LEADER_KEY, token)
        except Exception:
            pass
        await redis.aclose()
        if ch is not None:
            await ch.close()


def main() -> None:
    configure_logging()
    settings = Settings.from_env()
    start_http_server(settings.broadcaster_metrics_port)
    log("broadcaster_starting", symbols=settings.symbols, interval=settings.broadcast_interval)
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
