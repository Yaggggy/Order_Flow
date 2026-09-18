from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable

import pytest

pytestmark = pytest.mark.integration


# This test deliberately uses a symbol that the default live/synthetic ingestion
# configuration does not publish.  A unique event-id prefix also makes reruns safe
# without deleting any shared Kafka or ClickHouse data.
SYMBOL = "TEST" + uuid.uuid4().hex[:16].upper()


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        pytest.skip(f"integration environment variable {name} is not configured")
    return value


def _admin_auth() -> tuple[str, str]:
    password = os.getenv("CLICKHOUSE_ADMIN_PASSWORD") or os.getenv("CLICKHOUSE_PASSWORD")
    if not password:
        pytest.skip("CLICKHOUSE_ADMIN_PASSWORD (or CLICKHOUSE_PASSWORD) is not configured")
    return os.getenv("CLICKHOUSE_ADMIN_USER", "default"), password


def _ch_rows(client: Any, query: str, auth: tuple[str, str]) -> list[dict[str, Any]]:
    response = client.post(
        _env("CLICKHOUSE_HTTP_URL", "http://localhost:8123/"),
        params={"query": query.rstrip(";\n") + " FORMAT JSONEachRow"},
        auth=auth,
        timeout=10.0,
    )
    response.raise_for_status()
    return [json.loads(line) for line in response.text.splitlines() if line.strip()]


def _wait_for(
    fn: Callable[[], Any], *, timeout: float = 45.0, interval: float = 0.5
) -> Any:
    deadline = time.monotonic() + timeout
    last: Any = None
    while time.monotonic() < deadline:
        last = fn()
        if last:
            return last
        time.sleep(interval)
    return last


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _fixed_decimal(value: Any) -> Decimal:
    assert isinstance(value, str), f"API decimals must be strings, got {value!r}"
    assert "e" not in value.lower(), f"API decimal used exponent notation: {value!r}"
    return Decimal(value)


def _decimal_field(obj: dict[str, Any], *names: str) -> Decimal:
    for name in names:
        if name in obj:
            return _fixed_decimal(obj[name])
    raise AssertionError(f"none of {names!r} found in {obj!r}")


def _candles(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        candidates = payload.get("candles")
        if isinstance(candidates, list):
            return candidates
        data = payload.get("data")
        if isinstance(data, dict) and isinstance(data.get("candles"), list):
            return data["candles"]
        if isinstance(data, list):
            return data
    if isinstance(payload, list):
        return payload
    raise AssertionError(f"unexpected footprint response shape: {payload!r}")


def _price_levels(candle: dict[str, Any]) -> list[dict[str, Any]]:
    levels = candle.get("levels", candle.get("price_levels"))
    if not isinstance(levels, list):
        raise AssertionError(f"candle has no levels list: {candle!r}")
    return levels


def _assert_poc(candle: dict[str, Any], expected: Decimal) -> None:
    if "poc_price" in candle:
        assert _fixed_decimal(candle["poc_price"]) == expected
        return
    poc = candle.get("poc", candle.get("point_of_control"))
    if isinstance(poc, dict) and "price" in poc:
        assert _fixed_decimal(poc["price"]) == expected
        return
    if poc is not None:
        assert _fixed_decimal(poc) == expected
        return
    raise AssertionError(f"candle does not expose deterministic POC: {candle!r}")


def _produce_ticks(ticks: list[dict[str, Any]]) -> None:
    kafka = pytest.importorskip("confluent_kafka")
    producer_config: dict[str, str] = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
        "enable.idempotence": "true",
        "acks": "all",
        "message.timeout.ms": "30000",
    }
    for source_name, config_name in (
        ("KAFKA_SECURITY_PROTOCOL", "security.protocol"),
        ("KAFKA_SASL_MECHANISMS", "sasl.mechanisms"),
        ("KAFKA_SASL_USERNAME", "sasl.username"),
        ("KAFKA_SASL_PASSWORD", "sasl.password"),
    ):
        if os.getenv(source_name):
            producer_config[config_name] = os.environ[source_name]
    producer = kafka.Producer(producer_config)
    errors: list[str] = []

    def delivered(error: Any, _message: Any) -> None:
        if error is not None:
            errors.append(str(error))

    topic = os.getenv("KAFKA_TOPIC", "market.ticks")
    for tick in ticks:
        producer.produce(
            topic,
            key=tick["symbol"].encode("utf-8"),
            value=json.dumps(tick, separators=(",", ":")).encode("utf-8"),
            on_delivery=delivered,
        )
        producer.poll(0)
    remaining = producer.flush(30)
    assert remaining == 0, f"Kafka producer left {remaining} messages unflushed"
    assert not errors, f"Kafka delivery errors: {errors}"


def test_kafka_clickhouse_rest_and_websocket_pipeline() -> None:
    httpx = pytest.importorskip("httpx")
    run_id = uuid.uuid4().hex
    minute = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=3)
    timestamp_ms = int(minute.timestamp() * 1000)
    event_ids = [f"integration:{run_id}:{index}" for index in range(3)]
    ticks = [
        {
            "event_id": event_ids[0],
            "trade_id": f"{run_id}:0",
            "source": "synthetic",
            "symbol": SYMBOL,
            "timestamp_ms": timestamp_ms + 1000,
            "price": "100.00000000",
            "quantity": "2.00000000",
            "is_buyer_maker": False,
        },
        {
            "event_id": event_ids[1],
            "trade_id": f"{run_id}:1",
            "source": "synthetic",
            "symbol": SYMBOL,
            "timestamp_ms": timestamp_ms + 2000,
            "price": "100.00000000",
            "quantity": "1.00000000",
            "is_buyer_maker": True,
        },
        {
            "event_id": event_ids[2],
            "trade_id": f"{run_id}:2",
            "source": "synthetic",
            "symbol": SYMBOL,
            "timestamp_ms": timestamp_ms + 3000,
            "price": "101.00000000",
            "quantity": "3.00000000",
            "is_buyer_maker": False,
        },
    ]
    _produce_ticks(ticks)

    auth = _admin_auth()
    raw_filter = ",".join("'" + event_id + "'" for event_id in event_ids)
    raw_query = f"""
        SELECT count() AS n,
               toString(sum(quantity)) AS quantity,
               toString(sum(ask_volume)) AS ask_volume,
               toString(sum(bid_volume)) AS bid_volume
        FROM order_flow.trades_raw
        WHERE event_id IN ({raw_filter})
    """
    with httpx.Client() as client:
        raw_row: dict[str, Any] | None = None

        def raw_ready() -> bool:
            nonlocal raw_row
            rows = _ch_rows(client, raw_query, auth)
            raw_row = rows[0] if rows else None
            # Do not wait forever if a duplicate was produced: this test is also
            # intended to expose at-least-once duplicates rather than hide them.
            return raw_row is not None and int(raw_row["n"]) >= len(ticks)

        assert _wait_for(raw_ready), "Kafka-to-raw materialized view did not catch up"
        assert raw_row is not None
        assert int(raw_row["n"]) == 3, "unexpected duplicate rows for unique integration events"
        assert Decimal(raw_row["quantity"]) == Decimal("6.00000000")
        assert Decimal(raw_row["ask_volume"]) == Decimal("5.00000000")
        assert Decimal(raw_row["bid_volume"]) == Decimal("1.00000000")

        api_url = _env("ORDERFLOW_API_URL", "http://localhost:8000").rstrip("/")
        headers: dict[str, str] = {}
        api_key = os.getenv("API_KEY", "")
        if api_key:
            headers["X-API-Key"] = api_key
        params = {"start": _iso(minute), "end": _iso(minute + timedelta(minutes=1))}
        footprint: Any = None

        def api_ready() -> bool:
            nonlocal footprint
            response = client.get(
                f"{api_url}/api/v1/footprint/{SYMBOL}",
                params=params,
                headers=headers,
                timeout=10.0,
            )
            if response.status_code in {404, 422}:
                return False
            response.raise_for_status()
            footprint = response.json()
            return bool(_candles(footprint))

        assert _wait_for(api_ready), "raw-to-footprint materialized view/API did not catch up"
        candle_list = _candles(footprint)
        candle = next(
            (
                item
                for item in candle_list
                if str(item.get("minute", "")).startswith(minute.strftime("%Y-%m-%dT%H:%M:%S"))
            ),
            None,
        )
        assert candle is not None, f"missing candle for {minute.isoformat()}: {footprint!r}"
        levels = _price_levels(candle)
        assert [_decimal_field(level, "price") for level in levels] == [
            Decimal("100.00000000"),
            Decimal("101.00000000"),
        ], "API price levels must be ascending"
        lower, upper = levels
        assert _decimal_field(lower, "ask", "ask_volume") == Decimal("2.00000000")
        assert _decimal_field(lower, "bid", "bid_volume") == Decimal("1.00000000")
        assert _decimal_field(lower, "total", "total_volume") == Decimal("3.00000000")
        assert _decimal_field(lower, "delta") == Decimal("1.00000000")
        assert _decimal_field(upper, "ask", "ask_volume") == Decimal("3.00000000")
        assert _decimal_field(upper, "bid", "bid_volume") == Decimal("0.00000000")
        assert _decimal_field(upper, "total", "total_volume") == Decimal("3.00000000")
        assert _decimal_field(upper, "delta") == Decimal("3.00000000")
        assert _decimal_field(candle, "total", "total_volume") == Decimal("6.00000000")
        assert _decimal_field(candle, "delta") == Decimal("4.00000000")
        _assert_poc(candle, Decimal("100.00000000"))

    # complete replacement-window snapshot directly to the API's Redis channel.
    redis = pytest.importorskip("redis")
    websockets = pytest.importorskip("websockets.sync.client")
    redis_client = redis.Redis.from_url(
        os.getenv("REDIS_URL", "redis://:password@localhost:6379/0"), decode_responses=True
    )
    ws_url = os.getenv("ORDERFLOW_WS_URL", "ws://localhost:8000/ws/orderbook").rstrip("/") + f"/{SYMBOL}"
    snapshot = {
        "snapshot_id": f"integration-{run_id}",
        "generated_at": _iso(datetime.now(timezone.utc)),
        "window_start": _iso(minute),
        "window_end": _iso(minute + timedelta(minutes=1)),
        "type": "footprint_snapshot",
        "symbol": SYMBOL,
        "candles": [],
        "semantics": "replace_window",
    }
    with websockets.connect(ws_url, open_timeout=10, close_timeout=5) as websocket:
        websocket.send(json.dumps({"type": "auth", "api_key": os.getenv("API_KEY", "")}))
        ack = json.loads(websocket.recv(timeout=10))
        assert ack["type"] == "subscribed", ack
        redis_client.publish(f"orderflow:{SYMBOL}", json.dumps(snapshot, separators=(",", ":")))
        received: dict[str, Any] | None = None
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                message = websocket.recv(timeout=min(2.0, max(0.1, deadline - time.monotonic())))
            except TimeoutError:
                continue
            decoded = json.loads(message)
            if decoded.get("type") == "footprint_snapshot" and decoded.get("snapshot_id") == snapshot["snapshot_id"]:
                received = decoded
                break
        assert received == snapshot
