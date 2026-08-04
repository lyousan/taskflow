"""Redis lifecycle observation, isolated from committed state transitions."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ..observability import event as emit_event
from ..observability import metric
from ..types import FinishOutcome, MessageStatus

if TYPE_CHECKING:
    from .redis import RedisBroker
    from .redis_components import RedisDelivery

logger = logging.getLogger(__name__)


class RedisObservability:
    """Emit lifecycle metrics/events only after the corresponding Lua commit."""

    def __init__(self, broker: RedisBroker) -> None:
        self._broker = broker

    async def claimed(self, delivery: RedisDelivery) -> None:
        broker = self._broker
        await broker.middleware.emit("after_claim", delivery)
        await metric(broker.metrics, "claimed_total", queue=delivery.message.queue)
        await emit_event(
            broker.middleware,
            "claimed",
            delivery.message,
            status=MessageStatus.LEASED.value,
            delivery=delivery,
            backend="redis",
            event_sink=broker.event_sink,
            serializer_name=broker._serializer.name,
            serializer_version=broker._serializer.version,
        )

    async def finished(
        self,
        delivery: RedisDelivery,
        outcome: FinishOutcome,
        *,
        reason: str | None,
        error_type: str | None,
    ) -> None:
        event_name, metric_name = {
            FinishOutcome.EXPIRED: ("expired", "expired_total"),
            FinishOutcome.ACKED: ("ack", "acked_total"),
            FinishOutcome.RETRIED: ("retry", "retried_total"),
            FinishOutcome.DEAD_LETTERED: ("dead_lettered", "dead_lettered_total"),
        }[outcome]
        broker = self._broker
        await broker.middleware.emit(f"after_{event_name}", delivery, reason)
        await metric(broker.metrics, metric_name, queue=delivery.message.queue)
        await emit_event(
            broker.middleware,
            event_name,
            delivery.message,
            status=outcome.value,
            delivery=delivery,
            reason=reason,
            error_type=error_type,
            backend="redis",
            event_sink=broker.event_sink,
            serializer_name=broker._serializer.name,
            serializer_version=broker._serializer.version,
        )

    async def maintenance(
        self,
        queue: str,
        message_id: str,
        name: str,
        status: str,
        *,
        reason: str | None = None,
        error_type: str | None = None,
        metric_name: str | None = None,
    ) -> None:
        """Best-effort sink isolation after a successful maintenance script."""

        broker = self._broker
        try:
            fields = await broker._redis.hgetall(broker._message_key(queue, message_id))
            if not fields:
                return
            message = broker._decode(
                fields["envelope"],
                fields.get("serializer_name"),
                fields.get("serializer_version"),
            )
            if metric_name is not None:
                await metric(broker.metrics, metric_name, queue=message.queue)
            await emit_event(
                broker.middleware,
                name,
                message,
                status=status,
                delivery_id=fields.get("last_delivery_id")
                or fields.get("delivery_id")
                or None,
                consumer_id=fields.get("last_consumer_id")
                or fields.get("consumer_id")
                or None,
                attempt=int(fields.get("attempt", 0)),
                reason=reason,
                error_type=error_type,
                backend="redis",
                event_sink=broker.event_sink,
                serializer_name=fields.get("serializer_name"),
                serializer_version=fields.get("serializer_version"),
            )
        except Exception:
            logger.exception(
                "taskqx failed to publish committed maintenance event",
                extra={"event": name},
            )
