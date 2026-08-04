"""Observability emitted after a submission store has committed."""

from __future__ import annotations

from ..middleware import Middleware
from ..observability import EventSink, MetricsSink, event, metric
from ..serialization import Serializer
from ..types import MessageStatus, SubmitResult, TaskMessage
from .base import PreparedSubmission


class SubmissionObserver:
    """Publish the stable lifecycle contract for completed submissions."""

    def __init__(
        self,
        *,
        backend: str,
        middleware: Middleware,
        metrics: MetricsSink | None,
        event_sink: EventSink | None,
        serializer: Serializer,
    ) -> None:
        self._backend = backend
        self._middleware = middleware
        self._metrics = metrics
        self._event_sink = event_sink
        self._serializer = serializer

    async def record(
        self, prepared: PreparedSubmission, message: TaskMessage, result: SubmitResult
    ) -> None:
        """Emit exactly the outcome committed by a submission store."""
        if not result.accepted:
            await metric(self._metrics, "duplicate_total", queue=message.queue)
            await event(
                self._middleware,
                "duplicate",
                message,
                status=prepared.status,
                backend=self._backend,
                event_sink=self._event_sink,
                serializer_name=self._serializer.name,
                serializer_version=self._serializer.version,
            )
            return
        await self._middleware.emit("after_submit", message, result)
        await metric(self._metrics, "submitted_total", queue=message.queue)
        await event(
            self._middleware,
            "submitted",
            message,
            status=prepared.status,
            backend=self._backend,
            event_sink=self._event_sink,
            serializer_name=self._serializer.name,
            serializer_version=self._serializer.version,
        )
        if prepared.status == MessageStatus.EXPIRED.value:
            await metric(self._metrics, "expired_total", queue=message.queue)
            await event(
                self._middleware,
                "expired",
                message,
                status=MessageStatus.EXPIRED.value,
                reason="expired_on_submit",
                backend=self._backend,
                event_sink=self._event_sink,
                serializer_name=self._serializer.name,
                serializer_version=self._serializer.version,
            )
