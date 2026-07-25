"""SQLite backend 的 Delivery 与 Consumer 运行时组件。

它们只依赖 Broker 的私有生命周期方法；状态迁移仍集中在 ``sqlite.py``，组件本身
只负责把后端状态包装成公共 Protocol。
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from typing_extensions import Self

from ..errors import ValidationError
from ..types import ConsumerOptions, FinishOutcome, TaskMessage

if TYPE_CHECKING:
    from .sqlite import SQLiteBroker

class SQLiteDelivery:
    """持有一次租约及其不可公开的防陈旧 token。"""

    def __init__(self, broker: SQLiteBroker, message: TaskMessage, delivery_id: str,
                 lease_token: str, consumer_id: str, attempt: int,
                 claimed_at: datetime, lease_until: datetime) -> None:
        self._broker, self._lease_token = broker, lease_token
        self._lease_seconds = (lease_until - claimed_at).total_seconds()
        self.message, self.delivery_id, self.consumer_id = message, delivery_id, consumer_id
        self.attempt, self.claimed_at, self.lease_until = attempt, claimed_at, lease_until

    async def ack(self) -> FinishOutcome:
        """确认业务处理成功；同一投递重复确认是幂等的。"""
        return await self._broker._finish(self, "ack")

    async def retry(self, *, reason: str | None = None,
                    delay: timedelta | None = None,
                    max_attempts: int | None = None) -> FinishOutcome:
        """重新投递；指定正数 ``delay`` 时先持久化到 DELAYED。"""
        return await self._broker._finish(self, "retry", reason, delay=delay,
                                          max_attempts=max_attempts)

    async def reject(self, *, reason: str, error: BaseException | None = None) -> FinishOutcome:
        """拒绝消息并写入 DLQ。"""
        if not reason:
            raise ValidationError("reject 必须提供非空 reason")
        return await self._broker._finish(self, "reject", reason, error)

    async def extend_lease(self, *, seconds: float | None = None) -> datetime:
        """延长当前 lease，且永不超过消息的 expires_at。"""
        until = await self._broker._extend(self, seconds)
        self.lease_until = until
        return until


class SQLiteConsumer(AbstractAsyncContextManager["SQLiteConsumer"]):
    """通过轮询 SQLite 领取消息的异步迭代器。"""

    def __init__(self, broker: SQLiteBroker, queue: str, consumer_id: str,
                 options: ConsumerOptions) -> None:
        self._broker, self.queue, self.consumer_id, self.options = broker, queue, consumer_id, options
        self._closed = False

    async def start(self) -> None:
        self._broker._ensure_open()

    async def close(self) -> None:
        self._closed = True

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    def __aiter__(self) -> AsyncIterator[SQLiteDelivery]:
        return self

    async def __anext__(self) -> SQLiteDelivery:
        while not self._closed:
            delivery = await self._broker._claim(self.queue, self.consumer_id,
                                                 self.options.lease_seconds)
            if delivery is not None:
                return delivery
            await asyncio.sleep(self.options.poll_interval)
        raise StopAsyncIteration
