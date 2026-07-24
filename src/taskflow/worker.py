"""受控并发的 handler 执行器。"""
from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

from typing_extensions import Self

from .errors import ValidationError
from .observability import metric
from .types import ConsumerOptions, TaskMessage

Handler = Callable[[TaskMessage], Awaitable[None] | None]


class TaskWorker:
    """以固定 in-flight 上限领取、处理并终结消息的 Worker。"""

    def __init__(self, broker: Any, queue: str, handler: Handler, *, concurrency: int,
                 options: ConsumerOptions | None = None) -> None:
        if concurrency < 1:
            raise ValidationError("concurrency 必须大于等于 1")
        self._broker, self.queue, self._handler = broker, queue, handler
        base = options or ConsumerOptions()
        self._options = ConsumerOptions(concurrency=1, lease_seconds=base.lease_seconds,
                                        poll_interval=base.poll_interval)
        self.concurrency, self._closed = concurrency, False
        self._consumers: list[Any] = []
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        """启动固定数量的领取循环；每个循环最多处理一条 in-flight 消息。"""
        if self._tasks:
            return
        for _ in range(self.concurrency):
            consumer = self._broker.consumer(self.queue, options=self._options)
            await consumer.start()
            self._consumers.append(consumer)
            self._tasks.append(asyncio.create_task(self._serve(consumer)))

    async def run(self) -> None:
        """启动并等待 graceful shutdown。"""
        await self.start()
        if self._tasks:
            await asyncio.gather(*self._tasks)

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def close(self) -> None:
        """停止新领取并等待当前 handler 完成与 ACK/Retry。"""
        self._closed = True
        for consumer in self._consumers:
            await consumer.close()
        current = asyncio.current_task()
        pending = [task for task in self._tasks if task is not current]
        if pending:
            await asyncio.gather(*pending)

    async def _serve(self, consumer: Any) -> None:
        while not self._closed:
            try:
                delivery = await consumer.__anext__()
            except StopAsyncIteration:
                return
            try:
                started_at = time.perf_counter()
                result = self._handler(delivery.message)
                if result is not None:
                    await result
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - handler errors must be retried
                await delivery.retry(reason=f"{type(exc).__name__}: {exc}")
            else:
                await delivery.ack()
                await metric(getattr(self._broker, "metrics", None), "processing_duration",
                             time.perf_counter() - started_at, queue=self.queue)
