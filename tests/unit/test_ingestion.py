from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from services.ingestion.main import (
    IngestionConfig,
    KafkaPublisher,
    PublisherError,
    make_synthetic_tick,
    parse_binance_trade,
    parse_tick,
    should_auto_fallback,
)


def test_binance_trade_maps_maker_side_and_decimal_strings() -> None:
    tick = parse_binance_trade(
        {
            "e": "trade",
            "s": "BTCUSDT",
            "t": 12345,
            "T": 1_700_000_000_123,
            "p": "60000.10",
            "q": "0.01000000",
            "m": False,
        },
        expected_symbol="BTCUSDT",
    )
    assert tick.event_id == "binance:BTCUSDT:12345"
    assert tick.trade_id == "12345"
    assert tick.source == "binance"
    assert tick.price == "60000.10"
    assert tick.quantity == "0.01000000"
    assert tick.is_buyer_maker is False

    maker = parse_binance_trade(
        {
            "e": "trade",
            "s": "ETHUSDT",
            "t": "7",
            "T": 1,
            "p": "1.00",
            "q": "2.00000000",
            "m": True,
        }
    )
    assert maker.is_buyer_maker is True


def test_malformed_boolean_and_unsafe_decimal_are_rejected() -> None:
    payload: dict[str, Any] = {
        "event_id": "synthetic:x:1",
        "trade_id": "x:1",
        "source": "synthetic",
        "symbol": "BTCUSDT",
        "timestamp_ms": 1,
        "price": "1.00",
        "quantity": "1.00000000",
        "is_buyer_maker": 0,
    }
    with pytest.raises(ValueError, match="boolean"):
        parse_tick(payload)
    payload["is_buyer_maker"] = False
    payload["quantity"] = "0.000000001"
    with pytest.raises(ValueError, match="8 fractional"):
        parse_tick(payload)


def test_synthetic_config_defaults_and_wire_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGESTION_MODE", "synthetic")
    monkeypatch.setenv("SYMBOLS", "BTCUSDT,ethusdt")
    monkeypatch.setenv("SYNTHETIC_TICKS_PER_SECOND", "1500")
    config = IngestionConfig.from_env()
    assert config.symbols == ("BTCUSDT", "ETHUSDT")
    assert config.synthetic_ticks_per_second == 1500
    assert config.producer_config() == {
        "bootstrap.servers": "redpanda:9092",
        "enable.idempotence": "true",
        "acks": "all",
        "max.in.flight.requests.per.connection": "5",
        "linger.ms": "5",
        "compression.type": "lz4",
        "message.timeout.ms": "120000",
        "queue.buffering.max.messages": "100000",
        "queue.buffering.max.kbytes": "65536",
    }
    tick = make_synthetic_tick("run-uuid", 3, "BTCUSDT", now_ms=123)
    encoded = json.loads(json.dumps(tick.wire()))
    assert encoded["event_id"] == "synthetic:run-uuid:3"
    assert encoded["timestamp_ms"] == 123
    assert isinstance(encoded["price"], str)
    assert isinstance(encoded["quantity"], str)
    assert "e" not in encoded["price"].lower()


def test_auto_fallback_never_happens_after_a_live_tick() -> None:
    assert should_auto_fallback(live_seen=False, startup_timed_out=True)
    assert not should_auto_fallback(live_seen=True, startup_timed_out=True)
    assert not should_auto_fallback(live_seen=False, startup_timed_out=False)


class FakeProducer:
    def __init__(self, *, buffer_errors: int = 0, delivery_error: str | None = None) -> None:
        self.buffer_errors = buffer_errors
        self.delivery_error = delivery_error
        self.attempts = 0
        self.messages: list[bytes] = []
        self.flushed = False

    def produce(self, _topic: str, *, key: bytes, value: bytes, on_delivery: Any) -> None:
        assert key == b"BTCUSDT", "Kafka messages must be keyed by symbol"
        self.attempts += 1
        if self.attempts <= self.buffer_errors:
            raise BufferError("local producer queue full")
        self.messages.append(value)
        on_delivery(self.delivery_error, _Message(value))

    def poll(self, _timeout: float = 0) -> None:
        return None

    def flush(self, _timeout: float) -> int:
        self.flushed = True
        return 0


class _Message:
    def __init__(self, value: bytes) -> None:
        self._value = value

    def value(self) -> bytes:
        return self._value


@pytest.mark.asyncio
async def test_publisher_retries_buffer_error_without_dropping_tick() -> None:
    fake = FakeProducer(buffer_errors=2)
    config = IngestionConfig(mode="synthetic", queue_maxsize=1, flush_timeout_seconds=1)
    publisher = KafkaPublisher(config, producer_factory=lambda _cfg: fake)
    await publisher.start()
    await publisher.submit(make_synthetic_tick("run", 0, "BTCUSDT", now_ms=1))
    await asyncio.wait_for(publisher.queue.join(), timeout=1)
    await publisher.stop()
    assert fake.attempts == 3
    assert len(fake.messages) == 1
    assert fake.flushed


@pytest.mark.asyncio
async def test_delivery_error_is_fatal() -> None:
    fake = FakeProducer(delivery_error="broker unavailable")
    config = IngestionConfig(mode="synthetic", queue_maxsize=1, flush_timeout_seconds=1)
    publisher = KafkaPublisher(config, producer_factory=lambda _cfg: fake)
    await publisher.start()
    await publisher.submit(make_synthetic_tick("run", 0, "BTCUSDT", now_ms=1))
    await asyncio.wait_for(publisher.queue.join(), timeout=1)
    with pytest.raises(PublisherError, match="broker unavailable"):
        await publisher.stop()
