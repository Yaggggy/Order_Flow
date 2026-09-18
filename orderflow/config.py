from __future__ import annotations

import os
import re
from dataclasses import dataclass

SYMBOL_PATTERN = re.compile(r"^[A-Z0-9._-]{1,32}$")


def symbol_name(value: str) -> str:
    symbol = value.strip().upper()
    if not SYMBOL_PATTERN.fullmatch(symbol):
        raise ValueError("symbol must be 1–32 ASCII letters, digits, '.', '_' or '-'")
    return symbol


@dataclass(frozen=True)
class Settings:
    clickhouse_host: str = "clickhouse"
    clickhouse_port: int = 8123
    clickhouse_database: str = "order_flow"
    clickhouse_user: str = "orderflow"
    clickhouse_password: str = ""
    clickhouse_secure: bool = False
    redis_url: str = "redis://redis:6379/0"
    api_key: str = ""
    allowed_origins: tuple[str, ...] = ()
    symbols: tuple[str, ...] = ("BTCUSDT",)
    max_price_levels: int = 50_000
    query_timeout_seconds: int = 5
    query_concurrency: int = 8
    ws_max_clients: int = 500
    ws_send_timeout: float = 5.0
    broadcast_interval: float = 1.0
    broadcast_lookback_minutes: int = 3
    snapshot_ttl_seconds: int = 15
    broadcaster_metrics_port: int = 9102

    @classmethod
    def from_env(cls) -> "Settings":
        result = cls(
            clickhouse_host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            clickhouse_port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            clickhouse_database=os.getenv("CLICKHOUSE_DATABASE", "order_flow"),
            clickhouse_user=os.getenv("CLICKHOUSE_USER", "orderflow"),
            clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            clickhouse_secure=os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true",
            redis_url=os.getenv("REDIS_URL", "redis://redis:6379/0"),
            api_key=os.getenv("API_KEY", ""),
            allowed_origins=tuple(x.strip() for x in os.getenv("ALLOWED_ORIGINS", "").split(",") if x.strip()),
            symbols=tuple(dict.fromkeys(symbol_name(x) for x in os.getenv("SYMBOLS", "BTCUSDT").split(",") if x.strip())),
            max_price_levels=int(os.getenv("MAX_PRICE_LEVELS", "50000")),
            query_timeout_seconds=int(os.getenv("QUERY_TIMEOUT_SECONDS", "5")),
            query_concurrency=int(os.getenv("QUERY_CONCURRENCY", "8")),
            ws_max_clients=int(os.getenv("WS_MAX_CLIENTS", "500")),
            ws_send_timeout=float(os.getenv("WS_SEND_TIMEOUT_SECONDS", "5")),
            broadcast_interval=float(os.getenv("BROADCAST_INTERVAL_SECONDS", "1")),
            broadcast_lookback_minutes=int(os.getenv("BROADCAST_LOOKBACK_MINUTES", "3")),
            snapshot_ttl_seconds=int(os.getenv("SNAPSHOT_TTL_SECONDS", "15")),
            broadcaster_metrics_port=int(os.getenv("BROADCASTER_METRICS_PORT", "9102")),
        )
        if not result.symbols:
            raise ValueError("SYMBOLS cannot be empty")
        for name in ("max_price_levels", "query_timeout_seconds", "query_concurrency", "ws_max_clients", "snapshot_ttl_seconds"):
            if getattr(result, name) < 1:
                raise ValueError(f"{name} must be positive")
        if not 1 <= result.broadcast_lookback_minutes <= 60:
            raise ValueError("BROADCAST_LOOKBACK_MINUTES must be 1–60")
        if not 0.1 <= result.broadcast_interval <= 60 or not 0 < result.ws_send_timeout <= 60:
            raise ValueError("invalid broadcast interval or websocket send timeout")
        return result
