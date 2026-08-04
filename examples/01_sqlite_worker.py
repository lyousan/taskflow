"""最小 SQLite submit + Worker 示例。"""
from __future__ import annotations

import asyncio

from taskqx import SQLiteBroker


async def main() -> None:
    completed = asyncio.Event()

    async def handle_email(message) -> None:  # type: ignore[no-untyped-def]
        print(f"发送邮件给 {message.payload['to']}")
        completed.set()

    async with SQLiteBroker("taskqx-example.db") as broker:
        await broker.submit(queue="emails", payload={"to": "user@example.com"})
        worker = broker.worker("emails", handle_email, concurrency=4)
        await worker.start()
        await asyncio.wait_for(completed.wait(), timeout=5)
        await worker.close()


if __name__ == "__main__":
    asyncio.run(main())
