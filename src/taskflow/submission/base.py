"""提交准入扩展点使用的已准备提交对象。"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime

from ..capabilities import DedupGuarantee, SubmissionCapabilities
from ..types import SubmitResult


@dataclass(frozen=True, slots=True)
class PreparedSubmission:
    """Broker 已完成校验和序列化、可由任意 Store 原子提交的载荷。"""

    message_id: str
    queue: str
    envelope: bytes
    status: str
    created_at: datetime
    expires_at_ms: int | None
    dedup_scope: str | None
    dedup_key: str | None
    dedup_ttl_ms: int | None
    max_attempts: int
    serializer_name: str
    serializer_version: str
    available_at_ms: int | None = None


class CallbackSubmissionStore:
    """兼容旧式 callable 的轻量适配器。

    内置 broker 使用专用 Store；此类仅保留给需要将外部提交函数接入协议的用户。
    """

    capabilities = SubmissionCapabilities(
        dedup_guarantee=DedupGuarantee.EXACT,
        per_key_dedup_ttl=True,
        stores_original_message_id=True,
        atomic_submit=True,
        batch_submit=False,
    )

    def __init__(self, submitter: Callable[[PreparedSubmission], Awaitable[SubmitResult]]) -> None:
        self._submitter = submitter

    async def submit(self, submission: PreparedSubmission) -> SubmitResult:
        """将完整的已准备对象交给回调。"""
        return await self._submitter(submission)

    async def submit_many(self, submissions: list[PreparedSubmission]) -> list[SubmitResult]:
        """逐项委托；能力声明明确该适配器不提供批量原子性。"""

        return [await self.submit(item) for item in submissions]
