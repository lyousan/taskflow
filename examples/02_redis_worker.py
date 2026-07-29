"""Redis 多消费者 Worker 示例；需要本机 Redis 和 taskflow[redis]。"""
from __future__ import annotations

import asyncio

from taskflow import RedisBroker


async def main() -> None:
    completed = asyncio.Event()

    async def resize_image(message) -> None:  # type: ignore[no-untyped-def]
        print(f"resize {message.payload['image_id']} to {message.payload['width']}px")
        completed.set()

    async with RedisBroker.from_url("redis://127.0.0.1:6379/2", namespace="taskflow-example") as broker:
        await broker.submit(queue="images", payload={"image_id": "img-1", "width": 800})
        worker = broker.worker("images", resize_image, concurrency=8)
        await worker.start()
        await asyncio.wait_for(completed.wait(), timeout=5)
        await worker.close()


if __name__ == "__main__":
    asyncio.run(main())
