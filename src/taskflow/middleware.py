"""不改变状态机语义的异步生命周期钩子。"""
from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any

Hook = Callable[..., Awaitable[None] | None]


class Middleware:
    """轻量钩子容器，适合接入日志、指标和链路追踪。"""

    def __init__(self) -> None:
        self._hooks: dict[str, list[Hook]] = {}

    def add(self, event: str, hook: Hook) -> None:
        """注册一个事件钩子。钩子不得修改传入的不可变消息。"""

        self._hooks.setdefault(event, []).append(hook)

    async def emit(self, event: str, *args: Any) -> None:
        """按注册顺序执行一个事件的所有钩子。"""

        for hook in self._hooks.get(event, []):
            result = hook(*args)
            if inspect.isawaitable(result):
                await result
