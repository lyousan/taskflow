"""延迟提交与 RetryPolicy 示例。"""
from __future__ import annotations

import asyncio
from datetime import timedelta

from taskflow import FixedBackoff, RetryableError, RetryPolicy, SQLiteBroker


async def main() -> None:
    attempts = 0
    completed = asyncio.Event()

    async def call_webhook(message) -> None:  # type: ignore[no-untyped-def]
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RetryableError("upstream temporarily unavailable")
        print(f"webhook delivered on attempt {message.payload['url']}")
        completed.set()

    async with SQLiteBroker() as broker:
        await broker.submit(queue="webhooks", payload={"url": "https://example.com/hook"}, delay=timedelta(milliseconds=10))
        worker = broker.worker(
            "webhooks", call_webhook,
            retry_policy=RetryPolicy(max_attempts=3, backoff=FixedBackoff(delay=0.01)),
        )
        await worker.start()
        await asyncio.wait_for(completed.wait(), timeout=5)
        await worker.close()


if __name__ == "__main__":
    asyncio.run(main())
