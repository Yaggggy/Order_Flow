from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
from typing import Any, Iterable

UTC = timezone.utc

# Identifiers are static. User values use ClickHouse typed query parameters.
FOOTPRINT_SQL = """
SELECT minute, price,
       sumMerge(ask_volume_state) AS ask_volume,
       sumMerge(bid_volume_state) AS bid_volume,
       sumMerge(total_volume_state) AS total_volume
FROM order_flow.footprint_bars_1m
WHERE symbol = {symbol:String}
  AND minute >= {start:DateTime('UTC')}
  AND minute < {end:DateTime('UTC')}
GROUP BY minute, price
ORDER BY minute ASC, price ASC
LIMIT {row_limit:UInt32}
"""


class WindowError(ValueError):
    pass


class TooManyLevels(ValueError):
    pass


def iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def minute_floor(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(second=0, microsecond=0)


def query_window(start: datetime | None, end: datetime | None, *, now: datetime | None = None) -> tuple[datetime, datetime]:
    """Minute-aligned half-open range; never suggest subminute accuracy from bars."""
    now = now or datetime.now(UTC)
    for value in (start, end):
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise WindowError("start and end must include a timezone, e.g. Z")
        if value is not None and (value.second != 0 or value.microsecond != 0):
            raise WindowError("start and end must be aligned to a minute")
    end = end.astimezone(UTC) if end else minute_floor(now) + timedelta(minutes=1)
    start = start.astimezone(UTC) if start else end - timedelta(minutes=60)
    if start >= end:
        raise WindowError("start must be before end")
    if end - start > timedelta(hours=24):
        raise WindowError("maximum query window is 24 hours")
    if start.year < 1970 or end.year > 2105:
        raise WindowError("window must be within 1970–2105")
    return start, end


def fixed(value: Decimal) -> str:
    return format(value, ".8f")


def candles_from_rows(rows: Iterable[tuple[Any, ...]], *, now: datetime | None = None) -> list[dict[str, Any]]:
    """Rows must be unique merged minute/price levels; do not read raw states."""
    now = now or datetime.now(UTC)
    groups: dict[datetime, list[tuple[Decimal, Decimal, Decimal, Decimal]]] = {}
    for minute, price, ask, bid, total in rows:
        if minute.tzinfo is None:
            minute = minute.replace(tzinfo=UTC)
        groups.setdefault(minute, []).append(tuple(Decimal(x) for x in (price, ask, bid, total)))
    candles: list[dict[str, Any]] = []
    # CH sum(Decimal64) returns Decimal128. Python's default precision 28 is
    # insufficient for large merged totals, so leave ample precision here.
    with localcontext() as ctx:
        ctx.prec = 60
        for minute, values in sorted(groups.items()):
            values.sort(key=lambda x: x[0])
            levels = [{"price": fixed(p), "ask_volume": fixed(a), "bid_volume": fixed(b),
                       "total_volume": fixed(t), "delta": fixed(a - b)} for p, a, b, t in values]
            ask = sum((x[1] for x in values), Decimal(0))
            bid = sum((x[2] for x in values), Decimal(0))
            total = sum((x[3] for x in values), Decimal(0))
            # values ascending => first max breaks equal-volume ties at lowest price.
            poc = max(values, key=lambda x: x[3])[0]
            candles.append({"minute": iso(minute), "levels": levels,
                            "ask_volume": fixed(ask), "bid_volume": fixed(bid),
                            "total_volume": fixed(total), "delta": fixed(ask - bid),
                            "poc_price": fixed(poc),
                            "is_closed": minute + timedelta(minutes=1) <= now})
    return candles


async def fetch_footprint(client: Any, symbol: str, start: datetime, end: datetime,
                          *, max_price_levels: int, now: datetime | None = None) -> dict[str, Any]:
    result = await client.query(FOOTPRINT_SQL, parameters={
        "symbol": symbol, "start": start, "end": end, "row_limit": max_price_levels + 1,
    })
    if len(result.result_rows) > max_price_levels:
        raise TooManyLevels("too many price levels; request a smaller time window")
    now = now or datetime.now(UTC)
    return {"symbol": symbol, "interval": "1m", "volume_unit": "base_asset", "price_level": "exact_trade_price",
            "window_start": iso(start), "window_end": iso(end), "generated_at": iso(now),
            "candles": candles_from_rows(result.result_rows, now=now)}
