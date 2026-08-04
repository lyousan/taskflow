"""跨 backend 测试共享的测试替身。"""

from __future__ import annotations

import json


class BinaryJsonSerializer:
    """产生非 UTF-8 bytes，验证 backend 不假定 serializer 输出文本。"""

    name = "binary-json"
    version = "7"

    def dumps(self, value: object) -> bytes:
        return b"\xff" + json.dumps(value, separators=(",", ":")).encode()

    def loads(self, payload: bytes) -> object:
        assert payload.startswith(b"\xff")
        return json.loads(payload[1:].decode())
