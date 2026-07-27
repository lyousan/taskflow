"""统一的持久化标识命名规则。"""
from __future__ import annotations

import re

from .errors import ValidationError

_VALID_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
MAX_NAME_LENGTH = 128
_LEGACY_NAME = re.compile(r"^[A-Za-z0-9._-]+$")
LEGACY_MAX_NAME_LENGTH = 255


def validate_persistent_name(value: str, *, label: str, allow_legacy: bool = False) -> None:
    """校验会直接进入 SQLite/Redis 键空间的稳定名称。

    ``allow_legacy`` 只用于读取 v0.2 已持久化名称；新部署应始终使用默认规则。
    """

    pattern = _LEGACY_NAME if allow_legacy else _VALID_NAME
    maximum = LEGACY_MAX_NAME_LENGTH if allow_legacy else MAX_NAME_LENGTH
    if not isinstance(value, str) or not value or len(value) > maximum or pattern.fullmatch(value) is None or set(value) <= {".", "-"}:
        expression = "[A-Za-z0-9._-]+" if allow_legacy else "[A-Za-z0-9][A-Za-z0-9._-]{0,127}"
        raise ValidationError(f"{label} 必须匹配 {expression} 且长度不超过 {maximum}")
