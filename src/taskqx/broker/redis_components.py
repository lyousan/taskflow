"""Redis backend 的 Delivery 与 Consumer 运行时组件。"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from typing_extensions import Self

from ..errors import ValidationError
from ..types import ConsumerOptions, FinishOutcome, TaskMessage

if TYPE_CHECKING:
    from .redis import RedisBroker


class RedisDelivery:
    """Redis lease token 保护的一次投递上下文。"""

    def __init__(
        self,
        broker: RedisBroker,
        message: TaskMessage,
        delivery_id: str,
        token: str,
        consumer_id: str,
        attempt: int,
        claimed_at: datetime,
        lease_until: datetime,
    ) -> None:
        self._broker, self._lease_token = broker, token
        self._lease_seconds = (lease_until - claimed_at).total_seconds()
        self.message, self.delivery_id, self.consumer_id, self.attempt = (
            message,
            delivery_id,
            consumer_id,
            attempt,
        )
        self.claimed_at, self.lease_until = claimed_at, lease_until

    async def ack(self) -> FinishOutcome:
        return await self._broker._finish(self, "ack")

    async def retry(
        self,
        *,
        reason: str | None = None,
        delay: timedelta | None = None,
        max_attempts: int | None = None,
    ) -> FinishOutcome:
        return await self._broker._finish(
            self, "retry", reason, delay=delay, max_attempts=max_attempts
        )

    async def reject(
        self, *, reason: str, error: BaseException | None = None
    ) -> FinishOutcome:
        if not reason:
            raise ValidationError("reject 必须提供非空 reason")
        return await self._broker._finish(self, "reject", reason, error)

    async def extend_lease(self, *, seconds: float | None = None) -> datetime:
        self.lease_until = await self._broker._extend(self, seconds)
        return self.lease_until


class RedisConsumer:
    """Redis Stream Consumer Group 的异步投递迭代器。"""

    def __init__(
        self,
        broker: RedisBroker,
        queue: str,
        consumer_id: str,
        options: ConsumerOptions,
    ) -> None:
        self._broker, self.queue, self.consumer_id, self.options = (
            broker,
            queue,
            consumer_id,
            options,
        )
        self._closed = False

    async def start(self) -> None:
        await self._broker.start()

    async def close(self) -> None:
        self._closed = True

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    def __aiter__(self) -> AsyncIterator[RedisDelivery]:
        return self

    async def __anext__(self) -> RedisDelivery:
        while not self._closed:
            delivery = await self._broker._claim(
                self.queue, self.consumer_id, self.options.lease_seconds
            )
            if delivery is not None:
                return delivery
            await asyncio.sleep(self.options.poll_interval)
        raise StopAsyncIteration
