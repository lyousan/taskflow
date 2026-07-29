"""原子/非原子批量提交与 dedup 示例。"""
from __future__ import annotations

import asyncio
from datetime import timedelta

from taskflow import SQLiteBroker, SubmitRequest


async def main() -> None:
    async with SQLiteBroker() as broker:
        atomic = await broker.submit_many([
            SubmitRequest(queue="imports", payload={"row": 1}),
            SubmitRequest(queue="imports", payload={"row": 2}),
        ])
        print("atomic accepted:", [result.accepted for result in atomic])

        first = await broker.submit(
            queue="imports", payload={"row": "deduplicated"},
            dedup_scope="daily-import", dedup_key="source-a:42", dedup_ttl=timedelta(hours=1),
        )
        duplicate = await broker.submit(
            queue="imports", payload={"row": "deduplicated"},
            dedup_scope="daily-import", dedup_key="source-a:42", dedup_ttl=timedelta(hours=1),
        )
        print("dedup:", first.accepted, duplicate.accepted, duplicate.existing_message_id)

        partial = await broker.submit_many([
            SubmitRequest(queue="imports", payload={"row": 3}),
            SubmitRequest(queue="invalid queue!", payload={"row": 4}),
        ], atomic=False)
        print("non-atomic errors:", [type(item.error).__name__ if item.error else None for item in partial])


if __name__ == "__main__":
    asyncio.run(main())
