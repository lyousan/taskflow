"""Bounded cursor helpers for public read-only operation pages."""

from __future__ import annotations

import base64
import json
from typing import Any

from .errors import ValidationError

_MAX_PAGE_SIZE = 100


def validate_page_limit(limit: int) -> int:
    """Validate a deliberately conservative page size."""
    if (
        isinstance(limit, bool)
        or not isinstance(limit, int)
        or not 1 <= limit <= _MAX_PAGE_SIZE
    ):
        raise ValidationError(f"limit 必须是 1 到 {_MAX_PAGE_SIZE} 的整数")
    return limit


def encode_cursor(*parts: Any) -> str:
    """Encode cursor coordinates without exposing their storage representation."""
    return base64.urlsafe_b64encode(
        json.dumps(parts, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")


def decode_cursor(cursor: str | None, *, size: int) -> tuple[Any, ...] | None:
    """Decode an opaque cursor and reject one for another page type."""
    if cursor is None:
        return None
    try:
        padding = "=" * (-len(cursor) % 4)
        values = json.loads(base64.urlsafe_b64decode(cursor + padding))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValidationError("cursor 无效或已损坏") from exc
    if not isinstance(values, list) or len(values) != size:
        raise ValidationError("cursor 不属于此列表")
    return tuple(values)
