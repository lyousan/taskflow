"""Repeatable v0.5 lifecycle benchmark; no machine-dependent pass/fail gate."""
from __future__ import annotations

import argparse
import asyncio
import tempfile
import time
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

from taskqx import ConsumerOptions, RedisBroker, SQLiteBroker, SubmitRequest


async def measure(broker: SQLiteBroker | RedisBroker, count: int, concurrency: int) -> dict[str, float]:
    async with broker:
        started = time.perf_counter()
        await broker.submit_many([SubmitRequest(queue="batch", payload={"index": index}) for index in range(count)])
        batch_submit = time.perf_counter() - started
        started = time.perf_counter()
        for index in range(count):
            await broker.submit(queue="single", payload={"index": index})
        single_submit = time.perf_counter() - started
        started = time.perf_counter()
        for _ in range(count):
            delivery = await broker.consumer("single").__anext__()
            await delivery.ack()
        claim_ack = time.perf_counter() - started
        started = time.perf_counter()
        delivery = await broker.consumer("batch").__anext__()
        await delivery.retry(reason="benchmark")
        retried = await broker.consumer("batch").__anext__()
        await retried.ack()
        retry = time.perf_counter() - started
        started = time.perf_counter()
        await broker.submit(queue="large", payload={"body": "x" * 1_000_000})
        large_payload = time.perf_counter() - started
        started = time.perf_counter()
        for _ in range(count):
            await broker.submit(queue="dedup", payload={"same": True}, dedup_scope="benchmark", dedup_key="one", dedup_ttl=timedelta(minutes=1))
        dedup_conflicts = time.perf_counter() - started
        await broker.submit(queue="reclaim", payload={"id": 1})
        abandoned = await broker.consumer("reclaim", options=ConsumerOptions(lease_seconds=0.01)).__anext__()
        assert abandoned.message.payload == {"id": 1}
        started = time.perf_counter()
        await asyncio.sleep(0.02)
        reclaimed = await broker.consumer("reclaim", options=ConsumerOptions(lease_seconds=1)).__anext__()
        await reclaimed.ack()
        reclaim = time.perf_counter() - started
        await broker.submit_many([SubmitRequest(queue="concurrent", payload={"index": index}) for index in range(count)])
        started = time.perf_counter()
        work: asyncio.Queue[int] = asyncio.Queue()
        for index in range(count):
            work.put_nowait(index)
        async def consume_worker() -> None:
            consumer = broker.consumer("concurrent")
            while True:
                try:
                    work.get_nowait()
                except asyncio.QueueEmpty:
                    return
                delivery = await consumer.__anext__()
                await delivery.ack()
                work.task_done()
        await asyncio.gather(*(consume_worker() for _ in range(min(concurrency, count))))
        concurrent_consumers = time.perf_counter() - started
    return {"single_submit_seconds": single_submit, "batch_submit_seconds": batch_submit,
            "claim_ack_seconds": claim_ack, "retry_seconds": retry, "reclaim_seconds": reclaim,
            "large_payload_seconds": large_payload, "dedup_conflicts_seconds": dedup_conflicts,
            "concurrent_consumers_seconds": concurrent_consumers}


async def main_async(count: int, redis_url: str | None, concurrency: int) -> None:
    with tempfile.TemporaryDirectory(prefix="taskqx-v05-") as directory:
        results = {"sqlite": await measure(SQLiteBroker(Path(directory) / "tasks.db"), count, concurrency)}
    if redis_url:
        namespace = f"taskqx-benchmark-{uuid4()}"
        broker = RedisBroker.from_url(redis_url, namespace=namespace)
        try:
            results["redis"] = await measure(broker, count, concurrency)
        finally:
            cleaner = RedisBroker.from_url(redis_url, namespace=namespace)
            await cleaner.start()
            keys = [key async for key in cleaner._redis.scan_iter(match=f"{namespace}:*")]
            if keys:
                await cleaner._redis.unlink(*keys)
            await cleaner.close()
    for backend, values in results.items():
        print(backend, " ".join(f"{name}={value:.6f}" for name, value in values.items()))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=1_000)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--redis-url")
    args = parser.parse_args()
    if args.count < 1 or args.concurrency < 1:
        raise SystemExit("--count and --concurrency must be positive")
    asyncio.run(main_async(args.count, args.redis_url, args.concurrency))


if __name__ == "__main__":
    main()
