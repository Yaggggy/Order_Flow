from __future__ import annotations

import asyncio
import hmac
import json
import logging
import time
from contextlib import asynccontextmanager, suppress
from datetime import datetime
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from fastapi.security import APIKeyHeader
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from redis.exceptions import RedisError

from orderflow.analytics import TooManyLevels, WindowError, fetch_footprint, query_window
from orderflow.config import Settings, symbol_name
from orderflow.runtime import clickhouse_client, configure_logging, log, redis_client

REQUESTS = Counter("api_requests_total", "Completed HTTP requests", ["method", "route", "status"])
LATENCY = Histogram("api_request_seconds", "HTTP request duration", ["route"])
WS_CLIENTS = Gauge("api_websocket_clients", "Active websocket clients in this process")
WS_ERRORS = Counter("api_websocket_errors_total", "Websocket dependency or transport failures")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def authenticated(candidate: str | None, expected: str) -> bool:
    return not expected or (isinstance(candidate, str) and hmac.compare_digest(candidate.encode(), expected.encode()))


async def require_key(request: Request, key: Annotated[str | None, Depends(api_key_header)]) -> None:
    if not authenticated(key, request.app.state.settings.api_key):
        raise HTTPException(status_code=401, detail="invalid API key")


async def require_capacity(request: Request):
    semaphore = request.app.state.query_slots
    try:
        await asyncio.wait_for(semaphore.acquire(), timeout=0.1)
    except TimeoutError:
        raise HTTPException(status_code=503, detail="query capacity reached", headers={"Retry-After": "1"}) from None
    try:
        yield
    finally:
        semaphore.release()


async def send_json_bounded(ws: WebSocket, value: Any, timeout: float) -> None:
    await asyncio.wait_for(ws.send_json(value), timeout=timeout)


async def relay_snapshots(ws: WebSocket, redis: Any, symbol: str, settings: Settings) -> None:
    """Subscribe-before-cache closes the initial snapshot race. Sequences suppress
    older queued updates after the cache read. Redis Pub/Sub is not a durable log.
    """
    last_sequence = -1
    last_id: str | None = None
    last_sent = time.monotonic()

    async def forward(raw: str) -> None:
        nonlocal last_sequence, last_id, last_sent
        payload = json.loads(raw)
        if payload.get("type") != "footprint_snapshot" or payload.get("symbol") != symbol:
            return
        snapshot_id = payload.get("snapshot_id")
        if not snapshot_id or snapshot_id == last_id:
            return
        sequence = payload.get("sequence")
        if isinstance(sequence, int) and sequence <= last_sequence:
            return
        await send_json_bounded(ws, payload, settings.ws_send_timeout)
        if isinstance(sequence, int):
            last_sequence = sequence
        last_id = snapshot_id
        last_sent = time.monotonic()

    async with redis.pubsub() as pubsub:
        await pubsub.subscribe(f"orderflow:{symbol}")
        # Await the actual subscription acknowledgement, not merely the write.
        acknowledgement = await pubsub.get_message(ignore_subscribe_messages=False, timeout=5)
        if not acknowledgement or acknowledgement["type"] != "subscribe":
            raise TimeoutError("Redis subscription acknowledgement timed out")
        await send_json_bounded(ws, {"type": "subscribed", "symbol": symbol,
                                    "semantics": "replace_window", "streaming": symbol in settings.symbols}, settings.ws_send_timeout)
        cached = await redis.get(f"orderflow:last:{symbol}")
        if cached:
            await forward(cached)
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
            if message and message["type"] == "message":
                await forward(message["data"])
            if time.monotonic() - last_sent >= 15:
                await send_json_bounded(ws, {"type": "heartbeat", "symbol": symbol}, settings.ws_send_timeout)
                last_sent = time.monotonic()


async def watch_disconnect(ws: WebSocket) -> None:
    while True:
        message = await ws.receive()
        if message["type"] == "websocket.disconnect":
            return
        # No client commands after auth. Transport ping/pong is handled by Uvicorn.


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        configure_logging()
        app.state.settings = settings
        app.state.query_slots = asyncio.Semaphore(settings.query_concurrency)
        app.state.ws_count = 0
        app.state.redis = redis_client(settings)
        app.state.ch = None
        if not settings.api_key:
            log("api_auth_disabled", level=logging.WARNING, warning="local development only")
        try:
            # Compose waits on bootstrap. Bounded retry also supports independent deployment.
            for attempt in range(10):
                try:
                    if app.state.ch is None:
                        app.state.ch = await clickhouse_client(settings)
                    await app.state.redis.ping()
                    break
                except Exception:
                    if attempt == 9:
                        raise
                    log("api_startup_retry", attempt=attempt + 1)
                    await asyncio.sleep(min(0.5 * 2 ** attempt, 5))
            yield
        finally:
            await app.state.redis.aclose()
            if app.state.ch is not None:
                await app.state.ch.close()

    app = FastAPI(title="Order Flow & Footprint API", version="1.0.0", lifespan=lifespan,
                  description="Executed-trade footprints, not limit-order-book depth. Volumes are base-asset decimal strings.")
    if settings.allowed_origins:
        app.add_middleware(CORSMiddleware, allow_origins=list(settings.allowed_origins),
                           allow_methods=["GET"], allow_headers=["X-API-Key"], allow_credentials=False)

    @app.middleware("http")
    async def metrics(request: Request, call_next):
        started = time.monotonic()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            route = request.scope.get("route")
            path = route.path if route else "unmatched"
            REQUESTS.labels(request.method, path, status).inc()
            LATENCY.labels(path).observe(time.monotonic() - started)

    @app.get("/health/live", include_in_schema=False)
    async def live():
        return {"status": "ok"}

    @app.get("/health/ready", include_in_schema=False)
    async def ready(request: Request):
        try:
            async with asyncio.timeout(settings.query_timeout_seconds + 2):
                await request.app.state.ch.query("SELECT 1")
                await request.app.state.redis.ping()
            return {"status": "ready"}
        except Exception:
            return JSONResponse(status_code=503, content={"status": "not_ready"})

    @app.get("/metrics", include_in_schema=False)
    async def metrics_endpoint():
        return Response(generate_latest(), headers={"Content-Type": CONTENT_TYPE_LATEST})

    @app.get("/api/v1/footprint/{symbol}", dependencies=[Depends(require_key), Depends(require_capacity)])
    async def footprint(request: Request, symbol: str, start: datetime | None = None, end: datetime | None = None):
        try:
            normalized = symbol_name(symbol)
            begin, finish = query_window(start, end)
            result = await fetch_footprint(request.app.state.ch, normalized, begin, finish,
                                          max_price_levels=settings.max_price_levels)
            return result
        except (ValueError, WindowError, TooManyLevels) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None
        except Exception as exc:
            log("footprint_query_failed", level=logging.ERROR, error_type=type(exc).__name__)
            raise HTTPException(status_code=503, detail="analytics temporarily unavailable", headers={"Retry-After": "1"}) from None

    @app.websocket("/ws/orderbook/{symbol}")
    async def orderbook(ws: WebSocket, symbol: str):
        try:
            normalized = symbol_name(symbol)
        except ValueError:
            await ws.close(code=1008)
            return
        origin = ws.headers.get("origin")
        if origin and origin not in settings.allowed_origins:
            await ws.close(code=1008)
            return
        if ws.app.state.ws_count >= settings.ws_max_clients:
            await ws.close(code=1013)
            return
        ws.app.state.ws_count += 1
        WS_CLIENTS.inc()
        tasks: list[asyncio.Task] = []
        close_code = 1000
        try:
            await ws.accept()
            try:
                auth = await asyncio.wait_for(ws.receive_json(), timeout=5)
                if not isinstance(auth, dict) or auth.get("type") != "auth" or not authenticated(auth.get("api_key"), settings.api_key):
                    close_code = 1008
                    return
            except (ValueError, TimeoutError):
                close_code = 1008
                return
            tasks = [asyncio.create_task(relay_snapshots(ws, ws.app.state.redis, normalized, settings)),
                     asyncio.create_task(watch_disconnect(ws))]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        except WebSocketDisconnect:
            pass
        except (RedisError, TimeoutError, OSError, ValueError, RuntimeError) as exc:
            WS_ERRORS.inc()
            log("websocket_closed", error_type=type(exc).__name__, symbol=normalized)
            close_code = 1013
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            with suppress(Exception):
                await asyncio.wait_for(ws.close(code=close_code), timeout=1)
            ws.app.state.ws_count -= 1
            WS_CLIENTS.dec()

    return app


app = create_app()
