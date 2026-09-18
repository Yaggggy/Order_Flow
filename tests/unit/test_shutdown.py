import asyncio

import pytest

from services.ingestion.main import IngestionConfig, KafkaPublisher, PublisherError, make_synthetic_tick


class UnflushedProducer:
    def produce(self, *args, **kwargs):
        pass
    def poll(self, timeout=0):
        pass
    def flush(self, timeout):
        return 1


async def test_unflushed_records_fail_shutdown():
    publisher = KafkaPublisher(IngestionConfig(mode="synthetic"), producer_factory=lambda _: UnflushedProducer())
    await publisher.start()
    with pytest.raises(PublisherError, match="unflushed"):
        await publisher.stop()


async def test_cancelled_submit_does_not_leave_an_orphaned_put():
    # Start only the acceptance side to keep the queue deliberately full.
    publisher = KafkaPublisher(IngestionConfig(mode="synthetic", queue_maxsize=1))
    publisher._started = True
    await publisher.queue.put(make_synthetic_tick("run", 0, "BTCUSDT"))
    task = asyncio.create_task(publisher.submit(make_synthetic_tick("run", 1, "BTCUSDT")))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    publisher.queue.get_nowait()
    publisher.queue.task_done()
    await asyncio.sleep(0.01)
    assert publisher.queue.empty()
