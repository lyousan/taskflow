"""Taskflow 的不可变数据模型与请求对象。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Generic, TypeVar

_PageItem = TypeVar("_PageItem")


def utc_now() -> datetime:
    """返回带 UTC 时区的当前时间。"""

    return datetime.now(timezone.utc)


class MessageStatus(str, Enum):
    """消息在 Taskflow 中的投递状态。"""

    READY = "ready"
    DELAYED = "delayed"
    LEASED = "leased"
    ACKED = "acked"
    DEAD_LETTERED = "dead_lettered"
    EXPIRED = "expired"


class SubmitDecision(str, Enum):
    """提交准入的结果。"""

    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    PROBABLE_DUPLICATE = "probable_duplicate"


class FinishOutcome(str, Enum):
    """一次终结请求实际触发的状态迁移结果。"""

    ACKED = "acked"
    RETRIED = "retried"
    DEAD_LETTERED = "dead_lettered"
    EXPIRED = "expired"
    IDEMPOTENT = "idempotent"


@dataclass(frozen=True, slots=True)
class TaskMessage:
    """不可变且 JSON 兼容的业务消息。"""

    id: str
    queue: str
    payload: Any
    metadata: Mapping[str, Any] = field(default_factory=dict)
    dedup_key: str | None = None
    dedup_scope: str | None = None
    workflow_id: str | None = None
    parent_id: str | None = None
    created_at: datetime = field(default_factory=utc_now)
    expires_at: datetime | None = None
    max_attempts: int = 3
    available_at: datetime | None = None
    payload_schema_name: str | None = None
    payload_schema_version: str | None = None


@dataclass(frozen=True, slots=True)
class MessageState:
    """Read-only delivery state attached to an immutable task message."""

    message: TaskMessage
    status: MessageStatus
    attempt: int
    last_action: str | None = None
    last_reason: str | None = None
    consumer_id: str | None = None
    delivery_id: str | None = None
    claimed_at: datetime | None = None
    lease_until: datetime | None = None


@dataclass(frozen=True, slots=True)
class MessageSummary:
    """Message-list metadata that never reads or decodes the business payload."""

    message_id: str
    queue: str
    status: MessageStatus
    attempt: int
    created_at: datetime
    serializer_name: str
    serializer_version: str
    last_action: str | None = None
    last_reason: str | None = None
    consumer_id: str | None = None
    delivery_id: str | None = None
    claimed_at: datetime | None = None
    lease_until: datetime | None = None


@dataclass(frozen=True, slots=True)
class Page(Generic[_PageItem]):
    """A bounded read-only page; ``total`` is ``None`` when unavailable cheaply."""

    items: tuple[_PageItem, ...]
    next_cursor: str | None
    total: int | None


@dataclass(frozen=True, slots=True)
class SubmitRequest:
    """批量提交使用的请求对象。"""

    queue: str
    payload: Any
    metadata: Mapping[str, Any] | None = None
    dedup_key: str | None = None
    dedup_scope: str | None = None
    dedup_ttl: timedelta | None = None
    expires_at: datetime | None = None
    max_attempts: int | None = None
    workflow_id: str | None = None
    parent_id: str | None = None
    delay: timedelta | None = None
    payload_type: type[Any] | None = None


@dataclass(frozen=True, slots=True)
class SubmitResult:
    """一次提交的确定性结果。"""

    message_id: str
    accepted: bool
    decision: SubmitDecision
    existing_message_id: str | None = None
    stream_entry_id: str | None = None
    dedup_expires_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class BatchSubmitItemResult:
    """一项 non-atomic 批量提交的确定结果。

    ``atomic=False`` 从不因为相邻项失败而中断。``error`` 保留该项在准备、
    序列化或持久化阶段产生的原始异常，方便调用方按错误类型决定是否重试；
    成功项的 ``result`` 不为 ``None``。
    """

    index: int
    result: SubmitResult | None = None
    error: Exception | None = None

    @property
    def accepted(self) -> bool:
        """该项是否已被 broker 接受（重复提交返回 ``False``）。"""

        return self.result is not None and self.result.accepted


@dataclass(frozen=True, slots=True)
class ConsumerOptions:
    """消费者拉取和租约参数；``concurrency`` 由 ``broker.worker/run`` 使用。"""

    concurrency: int = 1
    lease_seconds: float = 300.0
    poll_interval: float = 0.05


@dataclass(frozen=True, slots=True)
class QueueStats:
    """队列快照与累计生命周期统计。"""

    queue: str
    ready: int
    leased: int
    dead_letters: int
    expired: int
    earliest_ready_at: datetime | None
    submitted_total: int
    acked_total: int
    retried_total: int
    reclaimed_total: int
    dead_lettered_total: int
    delayed: int = 0


@dataclass(frozen=True, slots=True)
class DeadLetter:
    """死信审计记录。"""

    message: TaskMessage
    attempt: int
    reason: str | None
    source: str
    failed_at: datetime
    error_type: str | None = None
    traceback: str | None = None


@dataclass(frozen=True, slots=True)
class ExpiredMessage:
    """过期消息审计记录。"""

    message: TaskMessage
    status_at_expiry: MessageStatus
    expired_at: datetime
    attempt: int


# Kept as a compatibility import for callers that group public models under types.
from .config import QueueConfig  # noqa: F401
