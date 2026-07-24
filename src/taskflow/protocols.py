"""面向扩展者的异步公共协议。"""
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol

from .capabilities import BackendCapabilities, SubmissionCapabilities
from .submission.base import PreparedSubmission
from .types import ConsumerOptions, QueueStats, SubmitRequest, SubmitResult


class TaskConsumer(Protocol):
    """异步投递迭代器的协议。"""

    async def start(self) -> None: ...
    async def close(self) -> None: ...
    def __aiter__(self) -> AsyncIterator[object]: ...


class TaskBroker(Protocol):
    """上层业务依赖的稳定 broker 协议。"""

    capabilities: BackendCapabilities

    async def start(self) -> None: ...
    async def close(self) -> None: ...
    async def submit(self, **kwargs: object) -> SubmitResult: ...
    async def submit_many(self, messages: list[SubmitRequest]) -> list[SubmitResult]: ...
    def consumer(self, queue: str, *, consumer_id: str | None = None, options: ConsumerOptions | None = None) -> TaskConsumer: ...
    async def inspect(self, queue: str) -> QueueStats: ...


class SubmissionStore(Protocol):
    """提交准入扩展点；实现必须保证其声明的原子性。"""

    capabilities: SubmissionCapabilities

    async def submit(self, submission: PreparedSubmission) -> SubmitResult:
        """在 backend 的原子边界内完成单条准入和入队。"""

    async def submit_many(self, submissions: list[PreparedSubmission]) -> list[SubmitResult]:
        """在实现声明的批量语义下完成一组准入和入队。"""
