"""Kafka ingestion for Binance trades and a deterministic synthetic source."""


from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import re
import signal
import sys
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping, Protocol

try:  # confluent-kafka is installed in the application image.
    from confluent_kafka import Producer
except ImportError:  # Keeps validation/unit tests importable without Kafka installed.
    Producer = None  # type: ignore[assignment,misc]

try:
    from prometheus_client import Counter, Gauge, start_http_server
except ImportError:  # pragma: no cover - only useful for static/unit import environments.
    class _MetricStub:
        def labels(self, **_: Any) -> "_MetricStub": return self
        def inc(self, *_: Any, **__: Any) -> None: return None
        def set(self, *_: Any, **__: Any) -> None: return None

    Counter = Gauge = lambda *_args, **_kwargs: _MetricStub()  # type: ignore[assignment]
    start_http_server = None  # type: ignore[assignment]

try:
    from websockets.asyncio.client import connect as websocket_connect
except ImportError:  # websockets < 14 compatibility for local tooling.
    from websockets import connect as websocket_connect  # type: ignore[no-redef]

LOGGER = logging.getLogger("orderflow.ingestion")
_SYMBOL_RE = re.compile(r"^[A-Z0-9._-]{1,32}$")
_SOURCE_VALUES = frozenset({"binance", "synthetic"})
_PRICE_QUANTUM = Decimal("0.10")
_QUANTITY_QUANTUM = Decimal("0.00000001")

INVALID_TICKS = Counter(
    "ingestion_invalid_ticks_total", "Ticks rejected before Kafka", ["reason", "source"]
)
PRODUCED_TICKS = Counter(
    "ingestion_produced_ticks_total", "Ticks handed to the Kafka producer", ["source"]
)
DELIVERED_TICKS = Counter("ingestion_delivered_ticks_total", "Kafka-acknowledged ticks")
LAST_DELIVERY = Gauge("ingestion_last_delivery_timestamp_seconds", "Last Kafka delivery acknowledgement")
DELIVERY_ERRORS = Counter(
    "ingestion_delivery_errors_total", "Kafka delivery errors (fatal)", ["source"]
)
BUFFER_RETRIES = Counter(
    "ingestion_buffer_retries_total", "Kafka local queue-full retries", ["source"]
)
QUEUE_DEPTH = Gauge("ingestion_queue_depth", "Number of ticks waiting for Kafka")


class ConfigError(ValueError):
    """An invalid process configuration."""


class PublisherError(RuntimeError):
    """A fatal producer or bounded-shutdown error."""


def _json_log(level: int, event: str, **fields: Any) -> None:
    """Emit one parseable structured log record without logging payload secrets."""
    record = {"event": event, "ts": time.time(), **fields}
    LOGGER.log(level, json.dumps(record, separators=(",", ":"), default=str))


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number") from exc
    if not math.isfinite(value) or value < minimum:
        raise ConfigError(f"{name} must be finite and >= {minimum}")
    return value


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc
    if value < minimum:
        raise ConfigError(f"{name} must be >= {minimum}")
    return value


def _symbols(raw: str) -> tuple[str, ...]:
    values = tuple(s.strip().upper() for s in raw.split(",") if s.strip())
    if not values:
        raise ConfigError("SYMBOLS must contain at least one symbol")
    if len(set(values)) != len(values):
        raise ConfigError("SYMBOLS must not contain duplicates")
    if any(not _SYMBOL_RE.fullmatch(s) for s in values):
        raise ConfigError("SYMBOLS contains an invalid symbol")
    return values


@dataclass(frozen=True, slots=True)
class IngestionConfig:
    bootstrap_servers: str = "redpanda:9092"
    topic: str = "market.ticks"
    mode: str = "auto"
    symbols: tuple[str, ...] = ("BTCUSDT",)
    synthetic_ticks_per_second: int = 1500
    binance_ws_base: str = "wss://stream.binance.com:9443/ws"
    metrics_port: int = 9101
    queue_maxsize: int = 10_000
    startup_fallback_seconds: float = 10.0
    reconnect_base_seconds: float = 1.0
    reconnect_max_seconds: float = 30.0
    flush_timeout_seconds: float = 10.0
    message_timeout_ms: int = 120_000

    @classmethod
    def from_env(cls) -> "IngestionConfig":
        mode = os.getenv("INGESTION_MODE", "auto").strip().lower()
        if mode not in {"auto", "binance", "synthetic"}:
            raise ConfigError("INGESTION_MODE must be auto, binance, or synthetic")
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092").strip()
        topic = os.getenv("KAFKA_TOPIC", "market.ticks").strip()
        if not bootstrap or not topic:
            raise ConfigError("KAFKA_BOOTSTRAP_SERVERS and KAFKA_TOPIC are required")
        return cls(
            bootstrap_servers=bootstrap,
            topic=topic,
            mode=mode,
            symbols=_symbols(os.getenv("SYMBOLS", "BTCUSDT")),
            synthetic_ticks_per_second=_env_int(
                "SYNTHETIC_TICKS_PER_SECOND", 1500, minimum=1
            ),
            binance_ws_base=os.getenv(
                "BINANCE_WS_BASE", "wss://stream.binance.com:9443/ws"
            ).rstrip("/"),
            metrics_port=_env_int("INGESTION_METRICS_PORT", 9101, minimum=1),
            queue_maxsize=_env_int("INGESTION_QUEUE_MAXSIZE", 10_000, minimum=1),
            startup_fallback_seconds=_env_float(
                "BINANCE_STARTUP_FALLBACK_SECONDS", 10.0, minimum=0.0
            ),
            reconnect_base_seconds=_env_float(
                "BINANCE_RECONNECT_BASE_SECONDS", 1.0, minimum=0.01
            ),
            reconnect_max_seconds=_env_float(
                "BINANCE_RECONNECT_MAX_SECONDS", 30.0, minimum=0.01
            ),
            flush_timeout_seconds=_env_float(
                "KAFKA_FLUSH_TIMEOUT_SECONDS", 10.0, minimum=0.1
            ),
            message_timeout_ms=_env_int("KAFKA_MESSAGE_TIMEOUT_MS", 120_000, minimum=1),
        )

    def __post_init__(self) -> None:
        if self.mode not in {"auto", "binance", "synthetic"}:
            raise ConfigError("invalid ingestion mode")
        if not self.symbols or any(not _SYMBOL_RE.fullmatch(s) for s in self.symbols):
            raise ConfigError("invalid symbols")
        if self.reconnect_max_seconds < self.reconnect_base_seconds:
            raise ConfigError("reconnect max must be >= reconnect base")

    def producer_config(self) -> dict[str, str]:
        """Kafka settings; idempotence does not imply end-to-end deduplication.

        Optional SASL/TLS settings are copied only when supplied, so the development
        Redpanda configuration remains password-free.
        """
        result = {
            "bootstrap.servers": self.bootstrap_servers,
            "enable.idempotence": "true",
            "acks": "all",
            "max.in.flight.requests.per.connection": "5",
            "linger.ms": "5",
            "compression.type": "lz4",
            "message.timeout.ms": str(self.message_timeout_ms),
            "queue.buffering.max.messages": "100000",
            "queue.buffering.max.kbytes": "65536",
        }
        optional = {
            "KAFKA_SECURITY_PROTOCOL": "security.protocol",
            "KAFKA_SASL_MECHANISMS": "sasl.mechanisms",
            "KAFKA_SASL_USERNAME": "sasl.username",
            "KAFKA_SASL_PASSWORD": "sasl.password",
            "KAFKA_SSL_CA_LOCATION": "ssl.ca.location",
            "KAFKA_SSL_CERTIFICATE_LOCATION": "ssl.certificate.location",
            "KAFKA_SSL_KEY_LOCATION": "ssl.key.location",
            "KAFKA_SSL_KEY_PASSWORD": "ssl.key.password",
        }
        for env_name, kafka_name in optional.items():
            value = os.getenv(env_name)
            if value:
                result[kafka_name] = value
        return result


@dataclass(frozen=True, slots=True)
class Tick:
    event_id: str
    trade_id: str
    source: str
    symbol: str
    timestamp_ms: int
    price: str
    quantity: str
    is_buyer_maker: bool

    def wire(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "trade_id": self.trade_id,
            "source": self.source,
            "symbol": self.symbol,
            "timestamp_ms": self.timestamp_ms,
            "price": self.price,
            "quantity": self.quantity,
            "is_buyer_maker": self.is_buyer_maker,
        }


def _decimal_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a decimal string")
    try:
        decimal = Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{name} is not a decimal") from exc
    if not decimal.is_finite() or decimal <= 0:
        raise ValueError(f"{name} must be finite and > 0")
    exponent = decimal.as_tuple().exponent
    if isinstance(exponent, int) and exponent < -8:
        raise ValueError(f"{name} has more than 8 fractional places")
    # Decimal(18,8): at most ten digits to the left of the decimal point.
    if decimal.adjusted() >= 10:
        raise ValueError(f"{name} exceeds Decimal(18,8)")
    return format(decimal, "f")


def parse_tick(value: Mapping[str, Any]) -> Tick:
    """Validate an already decoded wire object and return canonical fixed strings."""
    required = {
        "event_id", "trade_id", "source", "symbol", "timestamp_ms",
        "price", "quantity", "is_buyer_maker",
    }
    missing = required.difference(value)
    if missing:
        raise ValueError(f"missing fields: {','.join(sorted(missing))}")
    event_id, trade_id, source, symbol = (
        value["event_id"], value["trade_id"], value["source"], value["symbol"]
    )
    if not isinstance(event_id, str) or not event_id:
        raise ValueError("event_id must be a non-empty string")
    if not isinstance(trade_id, str) or not trade_id:
        raise ValueError("trade_id must be a non-empty string")
    if not isinstance(source, str) or source not in _SOURCE_VALUES:
        raise ValueError("unknown source")
    if not isinstance(symbol, str) or not _SYMBOL_RE.fullmatch(symbol):
        raise ValueError("invalid symbol")
    timestamp = value["timestamp_ms"]
    if isinstance(timestamp, bool) or not isinstance(timestamp, int) or not 0 < timestamp < 4_294_967_296_000:
        raise ValueError("timestamp_ms must fit the positive UTC DateTime minute range")
    if not isinstance(value["is_buyer_maker"], bool):
        raise ValueError("is_buyer_maker must be boolean")
    return Tick(
        event_id=event_id,
        trade_id=trade_id,
        source=source,
        symbol=symbol,
        timestamp_ms=timestamp,
        price=_decimal_string(value["price"], "price"),
        quantity=_decimal_string(value["quantity"], "quantity"),
        is_buyer_maker=value["is_buyer_maker"],
    )


def parse_binance_trade(payload: Mapping[str, Any], expected_symbol: str | None = None) -> Tick:
    """Map one Binance ``@trade`` payload to the repository wire contract."""
    if not isinstance(payload, Mapping):
        raise ValueError("Binance payload must be an object")
    if payload.get("e") != "trade":
        raise ValueError("not a Binance trade event")
    symbol = payload.get("s")
    if not isinstance(symbol, str):
        raise ValueError("Binance trade has no symbol")
    symbol = symbol.upper()
    if expected_symbol is not None and symbol != expected_symbol:
        raise ValueError("Binance symbol does not match stream")
    trade_id = payload.get("t")
    if isinstance(trade_id, bool) or not isinstance(trade_id, (int, str)):
        raise ValueError("invalid Binance trade id")
    # Binance's t is numeric, while the repository deliberately uses a string.
    trade_id_str = str(trade_id)
    return parse_tick({
        "event_id": f"binance:{symbol}:{trade_id_str}",
        "trade_id": trade_id_str,
        "source": "binance",
        "symbol": symbol,
        "timestamp_ms": payload.get("T"),
        "price": payload.get("p"),
        "quantity": payload.get("q"),
        "is_buyer_maker": payload.get("m"),
    })


def make_synthetic_tick(run_id: str, counter: int, symbol: str, now_ms: int | None = None) -> Tick:
    """Create a deterministic, decimal-only synthetic tick for load testing."""
    if counter < 0:
        raise ValueError("counter must be non-negative")
    # The price is always an exact 0.10 tick; quantity has exactly eight places.
    price = (Decimal("50000.00") + Decimal((counter % 2001) - 1000) * _PRICE_QUANTUM).quantize(_PRICE_QUANTUM)
    quantity = (Decimal(1 + ((counter * 17) % 50_000_000)) * _QUANTITY_QUANTUM).quantize(_QUANTITY_QUANTUM)
    trade_id = f"{run_id}:{counter}"
    return Tick(
        event_id=f"synthetic:{trade_id}",
        trade_id=trade_id,
        source="synthetic",
        symbol=symbol,
        timestamp_ms=now_ms if now_ms is not None else time.time_ns() // 1_000_000,
        price=format(price, "f"),
        quantity=format(quantity, "f"),
        is_buyer_maker=(counter % 2 == 1),
    )


def tick_json(tick: Tick) -> bytes:
    """Encode only validated wire values; no binary float conversion is involved."""
    return json.dumps(
        tick.wire(), ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")


class ProducerLike(Protocol):
    def produce(self, *args: Any, **kwargs: Any) -> Any: ...
    def poll(self, timeout: float = 0) -> Any: ...
    def flush(self, timeout: float) -> int: ...


class KafkaPublisher:
    """One async queue and one producer worker, preserving a tick on BufferError."""

    def __init__(
        self,
        config: IngestionConfig,
        producer_factory: Callable[[dict[str, str]], ProducerLike] | None = None,
    ) -> None:
        self.config = config
        self.queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=config.queue_maxsize)
        self._producer_factory = producer_factory
        self.producer: ProducerLike | None = None
        self._poll_task: asyncio.Task[None] | None = None
        self._worker_task: asyncio.Task[None] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stopping = False
        self._started = False
        self.fatal_event = asyncio.Event()
        self.fatal_error: str | None = None

    async def start(self) -> None:
        if self._started:
            return
        if self._producer_factory is not None:
            self.producer = self._producer_factory(self.config.producer_config())
        else:
            if Producer is None:
                raise PublisherError("confluent-kafka is not installed")
            self.producer = Producer(self.config.producer_config())  # type: ignore[operator]
        self._loop = asyncio.get_running_loop()
        self._started = True
        self._poll_task = asyncio.create_task(self._poll_loop(), name="kafka-poll")
        self._worker_task = asyncio.create_task(self._produce_loop(), name="kafka-produce")

    def _set_fatal(self, message: str, *, delivery: bool = False, source: str = "unknown") -> None:
        if self.fatal_error is None:
            self.fatal_error = message
        # Delivery callbacks can run in the thread used by bounded flush().
        if self._loop is not None and self._loop.is_running():
            try:
                self._loop.call_soon_threadsafe(self.fatal_event.set)
            except RuntimeError:
                self.fatal_event.set()
        else:
            self.fatal_event.set()
        if delivery:
            DELIVERY_ERRORS.labels(source=source).inc()
        _json_log(logging.ERROR, "kafka_delivery_error" if delivery else "kafka_publisher_error", error=message)

    def _delivery_report(self, error: Any, message: Any) -> None:
        if error is not None:
            source = "unknown"
            try:
                source = json.loads(message.value().decode("utf-8")).get("source", source)
            except Exception:
                pass
            self._set_fatal(str(error), delivery=True, source=source)
        else:
            DELIVERED_TICKS.inc()
            LAST_DELIVERY.set(time.time())

    async def _poll_loop(self) -> None:
        assert self.producer is not None
        while not self._stopping:
            try:
                self.producer.poll(0)
            except Exception as exc:  # poll failure is not recoverable for this process.
                self._set_fatal(f"producer poll failed: {exc}")
                return
            await asyncio.sleep(0.05)

    async def _produce_loop(self) -> None:
        assert self.producer is not None
        while not self._stopping or not self.queue.empty():
            try:
                tick = await asyncio.wait_for(self.queue.get(), timeout=0.1)
            except asyncio.TimeoutError:
                continue
            try:
                while True:
                    if self.fatal_event.is_set():
                        raise PublisherError(self.fatal_error or "Kafka producer is fatal")
                    try:
                        self.producer.produce(
                            self.config.topic,
                            key=tick.symbol.encode("utf-8"),
                            value=tick_json(tick),
                            on_delivery=self._delivery_report,
                        )
                        PRODUCED_TICKS.labels(source=tick.source).inc()
                        break
                    except BufferError:
                        BUFFER_RETRIES.labels(source=tick.source).inc()
                        # poll_loop drains delivery callbacks; this poll is a bounded
                        # retry hint and is safe because librdkafka's producer is thread-safe.
                        self.producer.poll(0)
                        await asyncio.sleep(0.01)
            except PublisherError as exc:
                self._set_fatal(str(exc))
                return
            except Exception as exc:
                self._set_fatal(f"producer produce failed: {exc}")
                return
            finally:
                self.queue.task_done()
                QUEUE_DEPTH.set(self.queue.qsize())

    async def submit(self, tick: Tick) -> None:
        """Wait for bounded capacity; never drop the tick currently being submitted."""
        if not self._started or self._stopping:
            raise PublisherError("publisher is not accepting ticks")
        if self.fatal_event.is_set():
            raise PublisherError(self.fatal_error or "Kafka producer is fatal")
        put_task = asyncio.create_task(self.queue.put(tick))
        fatal_task = asyncio.create_task(self.fatal_event.wait())
        try:
            done, _ = await asyncio.wait(
                {put_task, fatal_task}, return_when=asyncio.FIRST_COMPLETED
            )
            if put_task in done:
                QUEUE_DEPTH.set(self.queue.qsize())
                return
            raise PublisherError(self.fatal_error or "Kafka producer is fatal")
        finally:
            # A cancelled websocket/source must not leave an orphaned queue.put
            # that enqueues after shutdown believes all accepted work is drained.
            for task in (put_task, fatal_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(put_task, fatal_task, return_exceptions=True)

    async def stop(self) -> None:
        """Drain and flush within a bound; a nonzero flush count is fatal."""
        if not self._started or self._stopping:
            return
        self._stopping = True
        shutdown_error: Exception | None = None
        try:
            try:
                await asyncio.wait_for(
                    self.queue.join(), timeout=self.config.flush_timeout_seconds
                )
            except asyncio.TimeoutError:
                shutdown_error = PublisherError("timed out draining Kafka input queue")
                _json_log(logging.ERROR, "kafka_queue_drain_timeout")
            if self._poll_task is not None:
                await self._poll_task
            assert self.producer is not None
            try:
                remaining = await asyncio.wait_for(
                    asyncio.to_thread(self.producer.flush, self.config.flush_timeout_seconds),
                    timeout=self.config.flush_timeout_seconds + 0.5,
                )
                if remaining:
                    shutdown_error = PublisherError(
                        f"Kafka flush left {remaining} message(s) unflushed"
                    )
            except asyncio.TimeoutError:
                shutdown_error = PublisherError("Kafka flush timed out")
            except Exception as exc:
                shutdown_error = PublisherError(f"Kafka flush failed: {exc}")
            if self.fatal_error:
                shutdown_error = PublisherError(self.fatal_error)
        finally:
            if self._worker_task is not None and not self._worker_task.done():
                self._worker_task.cancel()
                await asyncio.gather(self._worker_task, return_exceptions=True)
            if shutdown_error is not None:
                raise shutdown_error


async def _wait_or_stop(stop_event: asyncio.Event, seconds: float) -> bool:
    """Return true when stop was requested, including during reconnect sleep."""
    if seconds <= 0:
        return stop_event.is_set()
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
        return True
    except asyncio.TimeoutError:
        return False


async def binance_worker(
    symbol: str,
    config: IngestionConfig,
    publisher: KafkaPublisher,
    stop_event: asyncio.Event,
    live_valid_event: asyncio.Event | None = None,
) -> None:
    """Consume one Binance ``@trade`` stream and reconnect with jitter.

    Once a valid tick is observed, callers must not replace this source with synthetic
    data.  Auto fallback is decided only during initial startup by ``run_sources``.
    """
    url = f"{config.binance_ws_base}/{symbol.lower()}@trade"
    backoff = config.reconnect_base_seconds
    while not stop_event.is_set():
        try:
            async with websocket_connect(
                url, ping_interval=20, ping_timeout=20, close_timeout=5, max_queue=1024
            ) as websocket:
                _json_log(logging.INFO, "binance_connected", symbol=symbol, url=url)
                backoff = config.reconnect_base_seconds
                async for raw in websocket:
                    if stop_event.is_set():
                        return
                    try:
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8")
                        payload = json.loads(raw)
                        tick = parse_binance_trade(payload, expected_symbol=symbol)
                    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
                        INVALID_TICKS.labels(reason="binance_validation", source="binance").inc()
                        _json_log(logging.WARNING, "invalid_binance_tick", symbol=symbol, error=str(exc))
                        continue
                    if live_valid_event is not None:
                        live_valid_event.set()
                    await publisher.submit(tick)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if stop_event.is_set():
                return
            _json_log(logging.WARNING, "binance_disconnected", symbol=symbol, error=str(exc))
            jitter = random.uniform(0.0, min(backoff * 0.25, 5.0))
            if await _wait_or_stop(stop_event, min(config.reconnect_max_seconds, backoff + jitter)):
                return
            backoff = min(config.reconnect_max_seconds, backoff * 2.0)


async def synthetic_source(
    config: IngestionConfig, publisher: KafkaPublisher, stop_event: asyncio.Event
) -> None:
    run_id = str(uuid.uuid4())
    _json_log(
        logging.WARNING,
        "synthetic_source_enabled",
        warning="synthetic data is not market data",
        run_id=run_id,
        ticks_per_second=config.synthetic_ticks_per_second,
        symbols=list(config.symbols),
    )
    counter = 0
    next_deadline = time.monotonic()
    interval = 1.0 / config.synthetic_ticks_per_second
    while not stop_event.is_set():
        if await _wait_or_stop(stop_event, max(0.0, next_deadline - time.monotonic())):
            return
        symbol = config.symbols[counter % len(config.symbols)]
        await publisher.submit(make_synthetic_tick(run_id, counter, symbol))
        counter += 1
        next_deadline += interval
        # If backpressure or scheduling stalled us, reset rather than burst forever.
        if next_deadline < time.monotonic() - 1.0:
            next_deadline = time.monotonic()


def should_auto_fallback(*, live_seen: bool, startup_timed_out: bool) -> bool:
    """Policy helper: fallback only before any valid live tick was seen."""
    return startup_timed_out and not live_seen


async def run_sources(
    config: IngestionConfig, publisher: KafkaPublisher, stop_event: asyncio.Event
) -> None:
    if config.mode == "synthetic":
        await synthetic_source(config, publisher, stop_event)
        return

    live_valid_event = asyncio.Event()
    tasks = [
        asyncio.create_task(
            binance_worker(symbol, config, publisher, stop_event, live_valid_event),
            name=f"binance-{symbol}",
        )
        for symbol in config.symbols
    ]
    try:
        if config.mode == "auto":
            live_wait = asyncio.create_task(live_valid_event.wait(), name="live-startup-watch")
            try:
                await asyncio.wait_for(
                    asyncio.shield(live_wait),
                    timeout=config.startup_fallback_seconds,
                )
            except asyncio.TimeoutError:
                if should_auto_fallback(
                    live_seen=live_valid_event.is_set(), startup_timed_out=True
                ):
                    _json_log(
                        logging.WARNING,
                        "binance_startup_fallback",
                        warning="no valid live tick before startup deadline; using synthetic permanently",
                    )
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    await synthetic_source(config, publisher, stop_event)
                    return
            finally:
                if not live_wait.done():
                    live_wait.cancel()
                await asyncio.gather(live_wait, return_exceptions=True)
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def run_service(config: IngestionConfig, *, producer_factory: Callable[[dict[str, str]], ProducerLike] | None = None) -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(signum, stop_event.set)
        except (NotImplementedError, RuntimeError):
            pass
    publisher = KafkaPublisher(config, producer_factory=producer_factory)
    await publisher.start()
    source_task = asyncio.create_task(run_sources(config, publisher, stop_event), name="ingestion-source")
    fatal_task = asyncio.create_task(publisher.fatal_event.wait(), name="kafka-fatal-watch")
    stop_task = asyncio.create_task(stop_event.wait(), name="shutdown-watch")
    try:
        done, _ = await asyncio.wait(
            {source_task, fatal_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if fatal_task in done and publisher.fatal_error:
            raise PublisherError(publisher.fatal_error)
        if source_task in done:
            source_error = source_task.exception()
            if source_error:
                raise source_error
    finally:
        stop_event.set()
        if not source_task.done():
            source_task.cancel()
            await asyncio.gather(source_task, return_exceptions=True)
        fatal_task.cancel()
        stop_task.cancel()
        await asyncio.gather(fatal_task, stop_task, return_exceptions=True)
        await publisher.stop()


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(message)s",
    )
    try:
        config = IngestionConfig.from_env()
        if start_http_server is not None:
            start_http_server(config.metrics_port)
        asyncio.run(run_service(config))
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        _json_log(logging.ERROR, "ingestion_exit_failure", error=str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
