"""后端共用的管理 API 规则。"""
from __future__ import annotations

from .errors import ValidationError


def resolve_replay_dedup_mode(*, dedup_mode: str | None, reuse_dedup: bool | None,
                              has_replacement: bool) -> str:
    """解析互斥的 keep/remove/replace 策略，并兼容旧 ``reuse_dedup``。"""

    if dedup_mode is None:
        return "keep" if reuse_dedup is None or reuse_dedup else "replace" if has_replacement else "remove"
    if dedup_mode not in {"keep", "remove", "replace"}:
        raise ValidationError("dedup_mode 必须是 keep、remove 或 replace")
    if reuse_dedup is not None and reuse_dedup != (dedup_mode == "keep"):
        raise ValidationError("dedup_mode 与 reuse_dedup 不能表达冲突的策略")
    if dedup_mode in {"keep", "remove"} and has_replacement:
        raise ValidationError(f"dedup_mode={dedup_mode!r} 时不能指定新的 dedup 参数")
    if dedup_mode == "replace" and not has_replacement:
        raise ValidationError("dedup_mode='replace' 必须提供新的 dedup 参数")
    return dedup_mode
