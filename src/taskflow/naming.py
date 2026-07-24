"""统一的持久化标识命名规则。"""
from __future__ import annotations

import re

from .errors import ValidationError

_VALID_NAME = re.compile(r"^[A-Za-z0-9._-]+$")
MAX_NAME_LENGTH = 255


def validate_persistent_name(value: str, *, label: str) -> None:
    """校验会直接进入 SQLite/Redis 键空间的稳定名称。"""

    if not value or len(value) > MAX_NAME_LENGTH or _VALID_NAME.fullmatch(value) is None:
        raise ValidationError(f"{label} 必须匹配 [A-Za-z0-9._-]+ 且长度不超过 {MAX_NAME_LENGTH}")
