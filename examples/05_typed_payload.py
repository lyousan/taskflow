"""使用 dataclass 约束 payload 编解码边界。"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from taskflow import SQLiteBroker


@dataclass(frozen=True)
class ResizeImage:
    image_id: str
    width: int
    height: int


async def main() -> None:
    completed = asyncio.Event()

    async def resize(message) -> None:  # type: ignore[no-untyped-def]
        payload: ResizeImage = message.payload
        print(f"resize {payload.image_id} -> {payload.width}x{payload.height}")
        completed.set()

    async with SQLiteBroker() as broker:
        await broker.submit(queue="images", payload=ResizeImage("img-1", 800, 600), payload_type=ResizeImage)
        worker = broker.worker("images", resize, payload_type=ResizeImage)
        await worker.start()
        await asyncio.wait_for(completed.wait(), timeout=5)
        await worker.close()


if __name__ == "__main__":
    asyncio.run(main())
