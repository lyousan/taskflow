"""默认且安全的 JSON 序列化实现。"""
from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any, Protocol

from .errors import ValidationError


class Serializer(Protocol):
    """可替换的消息序列化协议。"""

    name: str
    version: str

    def dumps(self, value: Any) -> bytes: ...
    def loads(self, payload: bytes) -> Any: ...


class JsonSerializer:
    """仅接收 JSON 兼容值的默认序列化器。"""

    name = "json"
    version = "1"

    def dumps(self, value: Any) -> bytes:
        """编码值，并将不可编码值转成清晰的领域异常。"""

        try:
            return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
        except (TypeError, ValueError) as exc:
            raise ValidationError("payload 与 metadata 必须是 JSON 兼容数据") from exc

    def loads(self, payload: bytes) -> Any:
        """解码 JSON 字节串。"""

        return json.loads(payload.decode())


class SerializerRegistry:
    """按持久化 name/version 选择 decoder 的显式注册表。"""

    def __init__(self, serializers: Iterable[Serializer] = ()) -> None:
        self._serializers: dict[tuple[str, str], Serializer] = {}
        for serializer in serializers:
            self.register(serializer)

    def register(self, serializer: Serializer) -> None:
        self._serializers[(serializer.name, serializer.version)] = serializer

    def resolve(self, name: str, version: str) -> Serializer:
        try:
            return self._serializers[(name, version)]
        except KeyError as exc:
            raise ValidationError(f"未注册 serializer {name!r} v{version!r}") from exc
