"""统一的 queue 级运行配置。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from .errors import ValidationError
from .retry import RetryPolicy


@dataclass(frozen=True, slots=True)
class QueueConfig:
    """一个队列的默认策略。

    单次 ``submit``/``worker`` 参数优先于此配置，此配置再优先于 broker 默认值。
    """

    max_attempts: int = 3
    lease: timedelta = timedelta(minutes=5)
    # ``None`` preserves the message-level max_attempts limit.  An explicit
    # policy adds worker-side exception classification/backoff and may impose
    # a stricter limit, exactly like the v0.2 worker argument.
    retry_policy: RetryPolicy | None = None
    default_dedup_ttl: timedelta | None = None
    max_payload_bytes: int | None = None

    def __post_init__(self) -> None:
        if isinstance(self.max_attempts, bool) or not isinstance(self.max_attempts, int) or self.max_attempts < 1:
            raise ValidationError("QueueConfig.max_attempts 必须大于等于 1")
        if not isinstance(self.lease, timedelta) or self.lease.total_seconds() <= 0:
            raise ValidationError("QueueConfig.lease 必须为正数 timedelta")
        if self.retry_policy is not None and not isinstance(self.retry_policy, RetryPolicy):
            raise ValidationError("QueueConfig.retry_policy 必须是 RetryPolicy 或 None")
        if self.default_dedup_ttl is not None and (
            not isinstance(self.default_dedup_ttl, timedelta)
            or self.default_dedup_ttl.total_seconds() <= 0
        ):
            raise ValidationError("QueueConfig.default_dedup_ttl 必须为正数")
        if self.max_payload_bytes is not None and (
            isinstance(self.max_payload_bytes, bool)
            or not isinstance(self.max_payload_bytes, int)
            or self.max_payload_bytes <= 0
        ):
            raise ValidationError("QueueConfig.max_payload_bytes 必须为正整数")
