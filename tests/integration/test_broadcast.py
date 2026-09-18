import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from websockets.sync.client import connect

pytestmark = pytest.mark.integration


def test_broadcaster_delivers_recent_clickhouse_candles():
    symbol = os.getenv("SYMBOLS", "BTCUSDT").split(",")[0].strip().upper()
    url = os.getenv("ORDERFLOW_WS_URL", "ws://localhost:8000/ws/orderbook").rstrip("/") + f"/{symbol}"
    with connect(url, max_size=16 * 1024 * 1024, open_timeout=10) as ws:
        ws.send(json.dumps({"type": "auth", "api_key": os.getenv("API_KEY", "")}))
        deadline = time.monotonic() + 45
        snapshot = None
        while time.monotonic() < deadline:
            try:
                payload = json.loads(ws.recv(timeout=5))
            except TimeoutError:
                continue
            if payload.get("type") == "footprint_snapshot" and payload.get("candles"):
                snapshot = payload
                break
        assert snapshot is not None, "no populated snapshot; check ingestion, Kafka consumer and broadcaster"
        assert snapshot["semantics"] == "replace_window"
        assert isinstance(snapshot["sequence"], int) and snapshot["sequence"] > 0
        generated = datetime.fromisoformat(snapshot["generated_at"].replace("Z", "+00:00"))
        assert (datetime.now(timezone.utc) - generated).total_seconds() < 15
        for candle in snapshot["candles"]:
            ask = Decimal(candle["ask_volume"])
            bid = Decimal(candle["bid_volume"])
            assert ask + bid == Decimal(candle["total_volume"])
            assert ask - bid == Decimal(candle["delta"])
            assert any(level["price"] == candle["poc_price"] for level in candle["levels"])
