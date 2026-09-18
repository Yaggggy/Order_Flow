"""Reconnectable native WebSocket example. Run with API_KEY in the environment."""
import asyncio
import json
import os
import random

from websockets.asyncio.client import connect


async def main():
    url = os.getenv("WS_URL", "ws://localhost:8000/ws/orderbook/BTCUSDT")
    delay = 1.0
    while True:
        try:
            async with connect(url, max_size=16 * 1024 * 1024) as ws:
                await ws.send(json.dumps({"type": "auth", "api_key": os.getenv("API_KEY", "")}))
                async for raw in ws:
                    payload = json.loads(raw)
                    delay = 1
                    if payload["type"] == "footprint_snapshot":
                        print(json.dumps({"sequence": payload.get("sequence"), "symbol": payload["symbol"],
                                          "candles": len(payload["candles"]), "generated_at": payload["generated_at"]}))
                    else:
                        print(payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"Connection lost ({type(exc).__name__}); retrying in {delay:.1f}s")
            await asyncio.sleep(delay + random.uniform(0, 0.5))
            delay = min(delay * 2, 30)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
