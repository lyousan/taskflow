"""低层 consumer()/Delivery API：业务决定 ACK、retry 或 reject。"""
from __future__ import annotations

import asyncio

from taskflow import SQLiteBroker


async def main() -> None:
    async with SQLiteBroker() as broker:
        await broker.submit(queue="payments", payload={"payment_id": "pay-1"})
        delivery = await broker.consumer("payments").__anext__()
        try:
            # 在此执行幂等的业务副作用，例如调用支付网关。
            print("charge", delivery.message.payload["payment_id"])
        except TimeoutError:
            await delivery.retry(reason="gateway timeout")
            raise
        except ValueError as error:
            await delivery.reject(reason="invalid payment", error=error)
            raise
        else:
            await delivery.ack()


if __name__ == "__main__":
    asyncio.run(main())
