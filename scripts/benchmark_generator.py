"""Local scheduling/serialization benchmark with an IN-MEMORY Kafka stand-in.
Not a broker, ClickHouse, network or end-to-end throughput benchmark.
"""
import asyncio
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from services.ingestion.main import IngestionConfig, KafkaPublisher, synthetic_source  # noqa: E402


class MemoryProducer:
    count = 0
    def produce(self, topic, *, key, value, on_delivery):
        assert topic == "market.ticks" and key == b"BTCUSDT"
        assert isinstance(value, bytes)
        self.count += 1
        on_delivery(None, None)
    def poll(self, timeout=0):
        pass
    def flush(self, timeout):
        return 0


async def main():
    config = IngestionConfig(mode="synthetic", synthetic_ticks_per_second=1500)
    producer = MemoryProducer()
    publisher = KafkaPublisher(config, producer_factory=lambda _: producer)
    await publisher.start()
    stop = asyncio.Event()
    started = time.monotonic()
    task = asyncio.create_task(synthetic_source(config, publisher, stop))
    await asyncio.sleep(3)
    stop.set()
    await task
    await publisher.stop()
    elapsed = time.monotonic() - started
    print(json.dumps({"scope": "in-memory producer only", "ticks": producer.count, "seconds": round(elapsed, 3),
                      "ticks_per_second": round(producer.count / elapsed, 1)}, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
