from __future__ import annotations

import json
import logging
import time
from typing import Any

import clickhouse_connect
from redis.asyncio import Redis

from orderflow.config import Settings


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")


def log(event: str, *, level: int = logging.INFO, **fields: Any) -> None:
    logging.getLogger("orderflow").log(level, json.dumps({"event": event, "ts": time.time(), **fields}, default=str))


async def clickhouse_client(settings: Settings):
    return await clickhouse_connect.get_async_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
        secure=settings.clickhouse_secure,
        autogenerate_session_id=False,
        connect_timeout=3,
        send_receive_timeout=settings.query_timeout_seconds + 3,
        settings={"max_execution_time": settings.query_timeout_seconds, "max_threads": 2},
    )


def redis_client(settings: Settings) -> Redis:
    return Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=5,
        health_check_interval=15,
        max_connections=settings.ws_max_clients + 32,
    )
