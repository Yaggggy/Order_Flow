import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from orderflow.config import Settings
from services.api import main

KEY = "test-api-key"


@pytest.fixture
def client(monkeypatch):
    ch = SimpleNamespace(query=AsyncMock(return_value=SimpleNamespace(result_rows=[])), close=AsyncMock())
    redis = SimpleNamespace(ping=AsyncMock(return_value=True), aclose=AsyncMock())
    monkeypatch.setattr(main, "clickhouse_client", AsyncMock(return_value=ch))
    monkeypatch.setattr(main, "redis_client", lambda _: redis)
    with TestClient(main.create_app(Settings(api_key=KEY))) as client:
        yield client, ch, redis


def test_auth_and_health(client):
    http, _, _ = client
    assert http.get("/health/live").status_code == 200
    assert http.get("/health/ready").status_code == 200
    assert http.get("/api/v1/footprint/BTCUSDT").status_code == 401
    assert http.get("/api/v1/footprint/BTCUSDT", headers={"X-API-Key": "wrong"}).status_code == 401
    response = http.get("/api/v1/footprint/btcusdt", headers={"X-API-Key": KEY})
    assert response.status_code == 200
    assert response.json()["symbol"] == "BTCUSDT"
    assert response.json()["candles"] == []


def test_bad_window_and_symbol(client):
    http, _, _ = client
    headers = {"X-API-Key": KEY}
    assert http.get("/api/v1/footprint/BTCUSDT?start=2026-01-01T12:00:00", headers=headers).status_code == 422
    assert http.get("/api/v1/footprint/BTC%27USDT", headers=headers).status_code == 422


def test_dependency_failures_do_not_leak_details(client):
    http, ch, redis = client
    ch.query.side_effect = RuntimeError("secret-password")
    result = http.get("/api/v1/footprint/BTCUSDT", headers={"X-API-Key": KEY})
    assert result.status_code == 503 and "secret-password" not in result.text
    assert http.get("/health/ready").status_code == 503
    ch.query.side_effect = None
    redis.ping.side_effect = OSError("offline")
    assert http.get("/health/ready").status_code == 503


def test_query_capacity(client):
    http, _, _ = client
    http.app.state.query_slots = asyncio.Semaphore(0)
    result = http.get("/api/v1/footprint/BTCUSDT", headers={"X-API-Key": KEY})
    assert result.status_code == 503
    assert result.headers["Retry-After"] == "1"


def test_websocket_rejects_bad_auth_and_origin(client):
    http, _, _ = client
    with http.websocket_connect("/ws/orderbook/BTCUSDT") as ws:
        ws.send_json({"type": "auth", "api_key": "wrong"})
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_json()
        assert exc.value.code == 1008
    with pytest.raises(WebSocketDisconnect):
        with http.websocket_connect("/ws/orderbook/BTCUSDT", headers={"Origin": "https://untrusted.example"}):
            pass


async def test_websocket_cache_race_deduplication():
    def snapshot(seq):
        return json.dumps({"type": "footprint_snapshot", "symbol": "BTCUSDT", "sequence": seq,
                           "snapshot_id": f"snapshot-{seq}", "candles": []})

    class FakePubsub:
        def __init__(self):
            self.events = [{"type": "subscribe"}, {"type": "message", "data": snapshot(1)},
                           {"type": "message", "data": snapshot(2)}, {"type": "message", "data": snapshot(3)}]
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            return False
        async def subscribe(self, channel):
            assert channel == "orderflow:BTCUSDT"
        async def get_message(self, **kwargs):
            if self.events:
                return self.events.pop(0)
            await asyncio.sleep(100)

    event = asyncio.Event()
    sent = []
    async def send_json(payload):
        sent.append(payload)
        if payload.get("sequence") == 3:
            event.set()
    ws = SimpleNamespace(send_json=send_json)
    redis = SimpleNamespace(pubsub=lambda: FakePubsub(), get=AsyncMock(return_value=snapshot(2)))
    task = asyncio.create_task(main.relay_snapshots(ws, redis, "BTCUSDT", Settings()))
    await asyncio.wait_for(event.wait(), timeout=1)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert [x["sequence"] for x in sent if x["type"] == "footprint_snapshot"] == [2, 3]


async def test_slow_websocket_has_bounded_send():
    async def slow_send(_):
        await asyncio.sleep(10)
    with pytest.raises(TimeoutError):
        await main.send_json_bounded(SimpleNamespace(send_json=slow_send), {}, 0.01)
