from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orderflow.analytics import TooManyLevels, WindowError, candles_from_rows, fetch_footprint, query_window
from orderflow.config import Settings, symbol_name

NOW = datetime(2026, 1, 1, 12, 0, 30, tzinfo=timezone.utc)
MINUTE = NOW.replace(second=0)


def test_decimal_aggregation_poc_tie_and_delta():
    candles = candles_from_rows([
        (MINUTE, Decimal("101"), Decimal("3"), Decimal("0"), Decimal("3")),
        (MINUTE, Decimal("100"), Decimal("2"), Decimal("1"), Decimal("3")),
    ], now=NOW)
    candle = candles[0]
    assert candle["poc_price"] == "100.00000000"
    assert candle["ask_volume"] == "5.00000000"
    assert candle["bid_volume"] == "1.00000000"
    assert candle["total_volume"] == "6.00000000"
    assert candle["delta"] == "4.00000000"
    assert candle["levels"][0]["delta"] == "1.00000000"
    assert not candle["is_closed"]


def test_large_decimal_totals_are_not_rounded_by_python_context():
    huge = Decimal("12345678901234567890123456789.12345678")
    candle = candles_from_rows([(MINUTE, Decimal(1), huge, Decimal("0.00000001"), huge)], now=NOW)[0]
    assert candle["delta"] == "12345678901234567890123456789.12345677"


def test_empty_and_closed_candles():
    assert candles_from_rows([]) == []
    candle = candles_from_rows([(MINUTE - timedelta(minutes=1), 1, 0, 2, 2)], now=NOW)[0]
    assert candle["is_closed"]
    assert candle["delta"] == "-2.00000000"


def test_default_window_includes_current_minute():
    start, end = query_window(None, None, now=NOW)
    assert end == MINUTE + timedelta(minutes=1)
    assert end - start == timedelta(hours=1)


@pytest.mark.parametrize("start,end", [
    (MINUTE.replace(tzinfo=None), MINUTE),
    (MINUTE, MINUTE),
    (MINUTE, MINUTE + timedelta(hours=25)),
    (MINUTE, MINUTE + timedelta(seconds=65)),
])
def test_invalid_windows(start, end):
    with pytest.raises(WindowError):
        query_window(start, end)


def test_utc_conversion_and_symbol_validation():
    offset = timezone(timedelta(hours=5, minutes=30))
    start, end = query_window(MINUTE.astimezone(offset), (MINUTE + timedelta(minutes=1)).astimezone(offset))
    assert start == MINUTE and end.tzinfo == timezone.utc
    assert symbol_name("btcusdt") == "BTCUSDT"
    with pytest.raises(ValueError):
        symbol_name("BTC'; DROP TABLE x")


async def test_parameterized_query_and_limit():
    client = SimpleNamespace(query=AsyncMock(return_value=SimpleNamespace(result_rows=[])))
    response = await fetch_footprint(client, "BTCUSDT", MINUTE, MINUTE + timedelta(minutes=1), max_price_levels=20)
    args, kwargs = client.query.call_args
    assert "sumMerge(ask_volume_state)" in args[0]
    assert "BTCUSDT" not in args[0]
    assert kwargs["parameters"]["row_limit"] == 21
    assert response["candles"] == []
    client.query.return_value.result_rows = [()] * 21
    with pytest.raises(TooManyLevels):
        await fetch_footprint(client, "BTCUSDT", MINUTE, MINUTE + timedelta(minutes=1), max_price_levels=20)


def test_invalid_config(monkeypatch):
    monkeypatch.setenv("BROADCAST_LOOKBACK_MINUTES", "0")
    with pytest.raises(ValueError):
        Settings.from_env()
