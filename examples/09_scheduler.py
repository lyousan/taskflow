"""在没有 Worker 的空闲队列上运行独立 lifecycle scheduler。"""
from __future__ import annotations

import asyncio
from datetime import timedelta

from taskqx import SQLiteBroker


async def main() -> None:
    async with SQLiteBroker() as broker:
        submitted = await broker.submit(
            queue="webhooks",
            payload={"url": "https://example.com/hook"},
            delay=timedelta(milliseconds=10),
        )
        scheduler = broker.scheduler(interval=timedelta(milliseconds=5))

        # 生产环境在独立进程/服务任务中持续 await scheduler.run()。
        await asyncio.sleep(0.02)
        await scheduler.tick()

        delivery = await broker.consumer("webhooks").__anext__()
        assert delivery.message.id == submitted.message_id
        await delivery.ack()
        print("scheduled:", delivery.message.payload["url"])


if __name__ == "__main__":
    asyncio.run(main())
