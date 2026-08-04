"""Shared safe rendering for optional interactive operations clients."""

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any


def safe_value(value: Any, *, include_payload: bool = False) -> Any:
    """Convert public API values for display without leaking payload by default."""

    if is_dataclass(value):
        value = asdict(value)  # type: ignore[arg-type]  # Runtime display adapter boundary.
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list | tuple):
        return [safe_value(item, include_payload=include_payload) for item in value]
    if isinstance(value, dict):
        return {
            key: item if key != "payload" or include_payload else "<redacted>"
            for key, item in (
                (key, safe_value(item, include_payload=include_payload))
                for key, item in value.items()
            )
        }
    return value


def render_json(value: Any, *, include_payload: bool = False) -> str:
    """Return stable, copyable JSON suitable for a terminal view."""

    return json.dumps(
        safe_value(value, include_payload=include_payload),
        ensure_ascii=False,
        default=str,
        indent=2,
        sort_keys=True,
    )
