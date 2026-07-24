"""可替换的指标与结构化生命周期事件。"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from .middleware import Middleware
from .types import TaskMessage, utc_now

logger = logging.getLogger(__name__)


class MetricsSink(Protocol):
    async def increment(self, name: str, value: int = 1, **labels: str) -> None: ...
    async def observe(self, name: str, value: float, **labels: str) -> None: ...


@dataclass(frozen=True, slots=True)
class BrokerEvent:
    name: str
    timestamp: datetime
    queue: str
    message_id: str
    status: str | None = None
    delivery_id: str | None = None
    attempt: int | None = None
    consumer_id: str | None = None
    reason: str | None = None
    serializer_name: str | None = None
    serializer_version: str | None = None


async def metric(metrics: MetricsSink | None, name: str, value: float = 1, **labels: str) -> None:
    if metrics is not None:
        try:
            if isinstance(value, int):
                await metrics.increment(name, value, **labels)
            else:
                await metrics.observe(name, value, **labels)
        except Exception:
            logger.exception("taskflow metrics sink failed", extra={"metric": name})


async def event(middleware: Middleware, name: str, message: TaskMessage, *, status: str | None = None,
                delivery: Any | None = None, reason: str | None = None,
                serializer_name: str | None = None, serializer_version: str | None = None) -> None:
    await middleware.emit("event", BrokerEvent(
        name, utc_now(), message.queue, message.id, status,
        getattr(delivery, "delivery_id", None), getattr(delivery, "attempt", None),
        getattr(delivery, "consumer_id", None), reason, serializer_name, serializer_version,
    ))
