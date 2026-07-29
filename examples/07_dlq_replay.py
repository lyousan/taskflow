"""将不可恢复消息送入 DLQ、检查后重放。"""
from __future__ import annotations

import asyncio

from taskflow import SQLiteBroker


async def main() -> None:
    async with SQLiteBroker() as broker:
        submitted = await broker.submit(queue="images", payload={"image_id": "broken"})
        delivery = await broker.consumer("images").__anext__()
        await delivery.reject(reason="source image is corrupt")

        dead_letters = await broker.admin.list_dead_letters("images")
        print("DLQ:", [(item.message.id, item.reason) for item in dead_letters])

        # 先检查消息与 dedup 策略；这里明确移除旧 dedup 记录后重放。
        await broker.admin.replay_dead_letter("images", submitted.message_id, dedup_mode="remove")
        replayed = await broker.consumer("images").__anext__()
        print("replayed:", replayed.message.payload)
        await replayed.ack()


if __name__ == "__main__":
    asyncio.run(main())
