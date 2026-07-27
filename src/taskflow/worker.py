"""高层 Worker：受控并发、异常分类、退避重试与 lease heartbeat。"""
from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from typing_extensions import Self

from .errors import (
    LeaseLostError,
    PayloadDecodingError,
    RejectMessage,
    RetryableError,
    ValidationError,
)
from .observability import metric
from .payloads import decode_payload
from .retry import RetryPolicy
from .types import ConsumerOptions, TaskMessage

if TYPE_CHECKING:
    from .protocols import TaskBroker, TaskConsumer, TaskDelivery

Handler = Callable[[TaskMessage], Awaitable[None] | None]


class TaskWorker:
    """安全封装底层 Consumer/Delivery 的应用层执行器。

    每个领取循环在完成 handler 的 ACK、Retry 或 Reject 前不会领取下一条，
    因而总 in-flight 数严格不超过 ``concurrency``。handler 被取消时不执行
    终结操作，lease 会依照后端正常回收。
    """

    def __init__(self, broker: TaskBroker, queue: str, handler: Handler, *, concurrency: int,
                 consumer_id: str | None = None, options: ConsumerOptions | None = None,
                 retry_policy: RetryPolicy | None = None,
                 heartbeat_seconds: float | None = None,
                 payload_type: type[Any] | None = None) -> None:
        if concurrency < 1:
            raise ValidationError("concurrency 必须大于等于 1")
        base = options or ConsumerOptions()
        if heartbeat_seconds is not None and heartbeat_seconds <= 0:
            raise ValidationError("heartbeat_seconds 必须为正数")
        self._broker, self.queue, self._handler = broker, queue, handler
        self._options = ConsumerOptions(concurrency=1, lease_seconds=base.lease_seconds,
                                        poll_interval=base.poll_interval)
        self._consumer_id = consumer_id
        # None 保持 v0.1 Worker 的消息级 max_attempts 语义；显式策略才增加
        # 更严格的 Worker 上限和退避/异常分类。
        self._retry_policy = retry_policy
        self._payload_type = payload_type
        self._heartbeat_seconds = heartbeat_seconds if heartbeat_seconds is not None else base.lease_seconds / 3
        self.concurrency, self._closed = concurrency, False
        self._consumers: list[TaskConsumer] = []
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        """启动固定数量的领取循环。重复调用是幂等的。"""
        if self._closed:
            raise ValidationError("已关闭的 Worker 不能重新启动")
        if self._tasks:
            return
        for index in range(self.concurrency):
            consumer_id = None if self._consumer_id is None else f"{self._consumer_id}-{index + 1}"
            consumer = self._broker.consumer(self.queue, consumer_id=consumer_id, options=self._options)
            await consumer.start()
            self._consumers.append(consumer)
            self._tasks.append(asyncio.create_task(self._serve(consumer), name=f"taskflow:{self.queue}:{index}"))

    async def run(self) -> None:
        """运行直到调用方取消或调用 :meth:`close`。"""
        await self.start()
        if self._tasks:
            await asyncio.gather(*self._tasks)

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def close(self) -> None:
        """停止新领取并等待正在执行的 handler 正常终结。"""
        if self._closed:
            return
        self._closed = True
        for consumer in self._consumers:
            await consumer.close()
        current = asyncio.current_task()
        pending = [task for task in self._tasks if task is not current]
        if pending:
            await asyncio.gather(*pending)

    async def _serve(self, consumer: TaskConsumer) -> None:
        while not self._closed:
            try:
                delivery = await consumer.__anext__()
            except StopAsyncIteration:
                return
            await self._handle(delivery)

    async def _handle(self, delivery: TaskDelivery) -> None:
        heartbeat = asyncio.create_task(self._heartbeat(delivery))
        try:
            started_at = time.perf_counter()
            message = delivery.message
            if self._payload_type is not None:
                message = replace(message, payload=decode_payload(
                    message.payload, self._payload_type,
                    schema_name=message.payload_schema_name,
                    schema_version=message.payload_schema_version,
                ))
            result = self._handler(message)
            if result is not None:
                await result
        except asyncio.CancelledError:
            # 不在取消路径中伪造 ACK/Retry；后端会在 lease 到期后恢复消息。
            raise
        except RejectMessage as exc:
            await delivery.reject(reason=_reason(exc), error=exc)
        except PayloadDecodingError as exc:
            await delivery.reject(reason="poison_payload", error=exc)
        except RetryableError as exc:
            await self._retry(delivery, exc)
        except Exception as exc:  # noqa: BLE001 - handler 异常必须有确定处理结果
            policy = self._retry_policy or RetryPolicy()
            if policy.should_retry(exc):
                await self._retry(delivery, exc)
            else:
                await delivery.reject(reason=_reason(exc), error=exc)
        else:
            await delivery.ack()
            await metric(getattr(self._broker, "metrics", None), "processing_duration",
                         time.perf_counter() - started_at, queue=self.queue)
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat

    async def _retry(self, delivery: TaskDelivery, error: BaseException) -> None:
        policy = self._retry_policy
        if policy is None:
            # 保留 v0.1 第三方 Delivery 的 retry(reason=...) 调用契约。
            await delivery.retry(reason=_reason(error))
            return
        delay = policy.delay_for(delivery.attempt)
        # 后端以 min(message.max_attempts, policy.max_attempts) 为有效上限，
        # 因而显式消息上限永远不会被 Worker 放宽。
        limit = policy.max_attempts
        await delivery.retry(reason=_reason(error), delay=delay,
                             max_attempts=limit)

    async def _heartbeat(self, delivery: TaskDelivery) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_seconds)
            try:
                await delivery.extend_lease()
            except LeaseLostError:
                # Handler 结束后 ACK/Retry 会再次得到明确的 LeaseLostError；
                # heartbeat 自身不应在 finally 阶段覆盖业务终结结果。
                return


def _reason(error: BaseException) -> str:
    return f"{type(error).__name__}: {error}"
