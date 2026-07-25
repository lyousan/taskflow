"""Worker 使用的重试策略与退避算法。"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol

from .errors import ValidationError


class Backoff(Protocol):
    """根据当前 Delivery attempt（从 1 开始）计算下一次延迟。"""

    def delay_for(self, attempt: int) -> timedelta: ...


@dataclass(frozen=True, slots=True)
class ImmediateBackoff:
    """不等待，立即重新投递。"""

    def delay_for(self, attempt: int) -> timedelta:
        _validate_attempt(attempt)
        return timedelta()


@dataclass(frozen=True, slots=True)
class FixedBackoff:
    """每次重试采用固定等待时间。"""

    delay: float | timedelta
    jitter: bool = False

    def __post_init__(self) -> None:
        if _seconds(self.delay) < 0:
            raise ValidationError("FixedBackoff.delay 不能为负数")

    def delay_for(self, attempt: int) -> timedelta:
        _validate_attempt(attempt)
        seconds = _seconds(self.delay)
        return timedelta(seconds=random.uniform(0, seconds) if self.jitter else seconds)


@dataclass(frozen=True, slots=True)
class ExponentialBackoff:
    """指数退避；第一次失败后的延迟为 ``initial``。"""

    initial: float | timedelta = 1
    maximum: float | timedelta = 60
    factor: float = 2
    jitter: bool = False

    def __post_init__(self) -> None:
        if _seconds(self.initial) < 0 or _seconds(self.maximum) < 0 or not math.isfinite(self.factor) or self.factor < 1:
            raise ValidationError("指数退避参数必须有效")

    def delay_for(self, attempt: int) -> timedelta:
        _validate_attempt(attempt)
        seconds = min(_seconds(self.maximum), _seconds(self.initial) * self.factor ** (attempt - 1))
        return timedelta(seconds=random.uniform(0, seconds) if self.jitter else seconds)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Worker 的异常分类、重试上限和退避策略。

    ``max_attempts`` 包含当前投递，因而值为 3 时 handler 最多运行三次。
    """

    max_attempts: int = 3
    backoff: Backoff = ImmediateBackoff()
    retry_on: tuple[type[BaseException], ...] = (Exception,)
    reject_on: tuple[type[BaseException], ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.max_attempts, int) or isinstance(self.max_attempts, bool) or self.max_attempts < 1:
            raise ValidationError("RetryPolicy.max_attempts 必须大于等于 1")

    @classmethod
    def fixed(cls, *, delay: float | timedelta, max_attempts: int = 3,
              jitter: bool = False) -> RetryPolicy:
        return cls(max_attempts=max_attempts, backoff=FixedBackoff(delay, jitter=jitter))

    @classmethod
    def exponential(cls, *, initial_delay: float | timedelta = 1,
                    max_delay: float | timedelta = 60, factor: float = 2,
                    max_attempts: int = 3, jitter: bool = False) -> RetryPolicy:
        return cls(max_attempts=max_attempts,
                   backoff=ExponentialBackoff(initial_delay, max_delay, factor, jitter))

    def should_retry(self, error: BaseException) -> bool:
        return not isinstance(error, self.reject_on) and isinstance(error, self.retry_on)

    def delay_for(self, attempt: int) -> timedelta:
        return self.backoff.delay_for(attempt)


def _seconds(value: float | timedelta) -> float:
    try:
        seconds = value.total_seconds() if isinstance(value, timedelta) else float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValidationError("退避时间必须是有限的非负数") from exc
    if not math.isfinite(seconds) or seconds < 0:
        raise ValidationError("退避时间必须是有限的非负数")
    return seconds


def _validate_attempt(attempt: int) -> None:
    if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt < 1:
        raise ValidationError("attempt 从 1 开始")
