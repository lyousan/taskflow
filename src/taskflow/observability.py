"""可替换的指标与结构化生命周期事件。"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from .middleware import Middleware
from .types import TaskMessage, utc_now

logger = logging.getLogger(__name__)


class EventSink(Protocol):
    async def emit(self, event: TaskflowEvent) -> None: ...


class MetricsSink(Protocol):
    """v0.2-compatible metrics contract."""

    async def increment(self, name: str, value: int = 1, **labels: str) -> None: ...
    async def observe(self, name: str, value: float, **labels: str) -> None: ...


class GaugeMetricsSink(MetricsSink, Protocol):
    """Optional v0.3 extension for instantaneous queue measurements."""

    async def gauge(self, name: str, value: float, **labels: str) -> None: ...


@dataclass(frozen=True, slots=True)
class TaskflowEvent:
    """Stable v0.3 event delivered to :class:`EventSink`."""

    event_name: str
    timestamp: datetime
    queue: str
    message_id: str
    delivery_id: str | None = None
    consumer_id: str | None = None
    attempt: int | None = None
    status: str | None = None
    reason: str | None = None
    error_type: str | None = None
    backend: str | None = None
    serializer_name: str | None = None
    serializer_version: str | None = None

    @property
    def name(self) -> str:
        """Convenience spelling for consumers shared with legacy middleware."""
        return self.event_name


@dataclass(frozen=True, slots=True)
class BrokerEvent:
    """v0.2 middleware event shape, kept source- and positional-compatible."""

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
    error_type: str | None = None
    backend: str | None = None

    @property
    def event_name(self) -> str:
        return self.name


async def metric(metrics: MetricsSink | None, name: str, value: float = 1, **labels: str) -> None:
    if metrics is None:
        return
    try:
        gauge = getattr(metrics, "gauge", None)
        if name in {"queue_ready", "queue_leased", "queue_delayed"} and callable(gauge):
            await gauge(name, value, **labels)
        elif isinstance(value, int):
            await metrics.increment(name, value, **labels)
        else:
            await metrics.observe(name, value, **labels)
    except Exception:
        logger.exception("taskflow metrics sink failed", extra={"metric": name})


async def event(
    middleware: Middleware,
    name: str,
    message: TaskMessage,
    *,
    status: str | None = None,
    delivery: Any | None = None,
    delivery_id: str | None = None,
    consumer_id: str | None = None,
    attempt: int | None = None,
    reason: str | None = None,
    error_type: str | None = None,
    backend: str | None = None,
    event_sink: EventSink | None = None,
    serializer_name: str | None = None,
    serializer_version: str | None = None,
) -> None:
    delivery_id = getattr(delivery, "delivery_id", None) if delivery is not None else delivery_id
    consumer_id = getattr(delivery, "consumer_id", None) if delivery is not None else consumer_id
    attempt = getattr(delivery, "attempt", None) if delivery is not None else attempt
    item = TaskflowEvent(
        event_name=name,
        timestamp=utc_now(),
        queue=message.queue,
        message_id=message.id,
        delivery_id=delivery_id,
        consumer_id=consumer_id,
        attempt=attempt,
        status=status,
        reason=reason,
        error_type=error_type,
        backend=backend,
        serializer_name=serializer_name,
        serializer_version=serializer_version,
    )
    await middleware.emit("event", BrokerEvent(
        name, item.timestamp, item.queue, item.message_id, item.status, item.delivery_id,
        item.attempt, item.consumer_id, item.reason, item.serializer_name,
        item.serializer_version, item.error_type, item.backend,
    ))
    if event_sink is not None:
        try:
            await event_sink.emit(item)
        except Exception:
            logger.exception("taskflow event sink failed", extra={"event": name})
