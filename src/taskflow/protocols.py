"""面向扩展者的异步公共协议。"""
from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal, Protocol, overload

from .capabilities import BackendCapabilities, SubmissionCapabilities
from .submission.base import PreparedSubmission
from .types import (
    BatchSubmitItemResult,
    ConsumerOptions,
    FinishOutcome,
    QueueStats,
    SubmitRequest,
    SubmitResult,
    TaskMessage,
)

if TYPE_CHECKING:
    from .retry import RetryPolicy
    from .worker import Handler, TaskWorker


class TaskDelivery(Protocol):
    """后端无关的 Delivery 最小契约。"""

    message: TaskMessage
    delivery_id: str
    consumer_id: str
    attempt: int
    claimed_at: datetime
    lease_until: datetime

    async def ack(self) -> FinishOutcome: ...
    async def retry(self, *, reason: str | None = None, delay: timedelta | None = None,
                    max_attempts: int | None = None) -> FinishOutcome: ...
    async def reject(self, *, reason: str, error: BaseException | None = None) -> FinishOutcome: ...
    async def extend_lease(self, *, seconds: float | None = None) -> datetime: ...


class TaskConsumer(Protocol):
    """异步投递迭代器的协议。"""

    async def start(self) -> None: ...
    async def close(self) -> None: ...
    def __aiter__(self) -> AsyncIterator[TaskDelivery]: ...
    async def __anext__(self) -> TaskDelivery: ...


class TaskBroker(Protocol):
    """上层业务依赖的稳定 broker 协议。"""

    capabilities: BackendCapabilities

    async def start(self) -> None: ...
    async def close(self) -> None: ...
    async def submit(
        self, *, queue: str, payload: Any, metadata: Mapping[str, Any] | None = None,
        dedup_key: str | None = None, dedup_scope: str | None = None,
        dedup_ttl: timedelta | None = None, delay: timedelta | None = None, expires_at: datetime | None = None,
        max_attempts: int | None = None, workflow_id: str | None = None,
        parent_id: str | None = None, payload_type: type[Any] | None = None,
    ) -> SubmitResult: ...
    @overload
    async def submit_many(self, messages: list[SubmitRequest], *, atomic: Literal[True] = True) -> list[SubmitResult]: ...
    @overload
    async def submit_many(self, messages: list[SubmitRequest], *, atomic: Literal[False]) -> list[BatchSubmitItemResult]: ...
    async def submit_many(self, messages: list[SubmitRequest], *, atomic: bool = True) -> list[SubmitResult] | list[BatchSubmitItemResult]: ...
    def consumer(self, queue: str, *, consumer_id: str | None = None, options: ConsumerOptions | None = None) -> TaskConsumer: ...
    def worker(
        self, queue: str, handler: Handler, *, concurrency: int | None = None,
        consumer_id: str | None = None, options: ConsumerOptions | None = None,
        retry_policy: RetryPolicy | None = None, heartbeat_seconds: float | None = None,
        payload_type: type[Any] | None = None,
    ) -> TaskWorker: ...
    async def run(
        self, queue: str, handler: Handler, *, concurrency: int | None = None,
        consumer_id: str | None = None, options: ConsumerOptions | None = None,
        retry_policy: RetryPolicy | None = None, heartbeat_seconds: float | None = None,
        payload_type: type[Any] | None = None,
    ) -> None: ...
    async def inspect(self, queue: str) -> QueueStats: ...
    async def inspect_message(self, message_id: str) -> TaskMessage | None: ...


class SubmissionStore(Protocol):
    """提交准入扩展点；实现必须保证其声明的原子性。"""

    capabilities: SubmissionCapabilities

    async def submit(self, submission: PreparedSubmission) -> SubmitResult:
        """在 backend 的原子边界内完成单条准入和入队。"""
        ...

    async def submit_many(self, submissions: list[PreparedSubmission]) -> list[SubmitResult]:
        """在实现声明的批量语义下完成一组准入和入队。"""
        ...
