"""Backends 共享的 UTC 时间与 ID 辅助函数。"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from ..errors import ValidationError


def timestamp(value: datetime) -> float:
    """将带时区的时间转换为 UTC epoch 秒。"""
    if value.tzinfo is None:
        raise ValidationError("时间必须带有时区，且建议使用 UTC")
    return value.astimezone(timezone.utc).timestamp()


def datetime_from_timestamp(value: float | None) -> datetime | None:
    """将 epoch 秒恢复为 UTC datetime。"""
    return None if value is None else datetime.fromtimestamp(value, timezone.utc)


def new_id() -> str:
    """生成无外部依赖且适合任务标识的 UUID。"""
    return str(uuid.uuid4())
