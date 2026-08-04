"""Redis delivery state transitions and PEL crash-window recovery."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from ..errors import LeaseLostError, ValidationError
from ..observability import metric
from ..types import FinishOutcome, MessageStatus
from ._time import datetime_from_timestamp as _datetime
from ._time import timestamp as _timestamp
from .redis_calls import claim_call, extend_lease_call, finish_call, pel_recover_call
from .redis_components import RedisDelivery

if TYPE_CHECKING:
    from .redis import RedisBroker


class RedisStateMachine:
    """Own Redis Stream/Lua delivery transitions; broker exposes delegates only."""

    def __init__(self, broker: RedisBroker) -> None:
        self._broker = broker

    async def claim(
        self, queue: str, consumer_id: str, lease_seconds: float
    ) -> RedisDelivery | None:
        broker = self._broker
        await broker.maintain(queue)
        await broker._ensure_group(queue)
        now = await broker._now()
        received = await broker._redis.xreadgroup(
            broker._group_name(),
            consumer_id,
            {broker._queue_key(queue, "stream"): ">"},
            count=1,
            block=1,
        )
        if not received:
            return None
        _, entries = received[0]
        entry_id, fields = entries[0]
        message_id = fields["message_id"]
        delivery_id, token = broker._id_factory(), broker._id_factory()
        lease_until = now + timedelta(seconds=lease_seconds)
        status = int(
            await claim_call(
                broker,
                queue=queue,
                message_id=message_id,
                now=_timestamp(now),
                consumer_id=consumer_id,
                delivery_id=delivery_id,
                token=token,
                lease_until=_timestamp(lease_until),
                entry_id=entry_id,
            ).execute(broker._redis)
        )
        if status == -1:
            await broker._emit_maintenance_event(
                queue,
                message_id,
                "expired",
                MessageStatus.EXPIRED.value,
                reason="expired_during_claim",
                metric_name="expired_total",
            )
            return None
        if status <= 0:
            return None
        fields = await broker._redis.hgetall(broker._message_key(queue, message_id))
        delivery = RedisDelivery(
            broker,
            broker._decode(
                fields["envelope"],
                fields.get("serializer_name"),
                fields.get("serializer_version"),
            ),
            delivery_id,
            token,
            consumer_id,
            status,
            now,
            lease_until,
        )
        await broker._observability.claimed(delivery)
        return delivery

    async def finish(
        self,
        delivery: RedisDelivery,
        action: str,
        reason: str | None = None,
        error: BaseException | None = None,
        delay: timedelta | None = None,
        max_attempts: int | None = None,
    ) -> FinishOutcome:
        broker = self._broker
        now = await broker._now()
        if delay is not None and delay.total_seconds() < 0:
            raise ValidationError("delay 不能为负数")
        if max_attempts is not None and max_attempts < 1:
            raise ValidationError("max_attempts 必须大于等于 1")
        queue, message_id = delivery.message.queue, delivery.message.id
        error_type = type(error).__name__ if error else ""
        result = int(
            await finish_call(
                broker,
                queue=queue,
                message_id=message_id,
                action=action,
                delivery_id=delivery.delivery_id,
                token=delivery._lease_token,
                now=_timestamp(now),
                reason=reason or "",
                error_type=error_type,
                retry_available_at=_timestamp(now + delay)
                if delay and delay.total_seconds() > 0
                else None,
                max_attempts=max_attempts,
                ack_tombstone_ttl=broker._ack_tombstone_ttl(queue).total_seconds(),
            ).execute(broker._redis)
        )
        if result == 0:
            await metric(broker.metrics, "lease_lost_total", queue=queue)
            raise LeaseLostError("租约已经失效，不能终结当前投递")
        if result == 2:
            return FinishOutcome.IDEMPOTENT
        outcome = {
            3: FinishOutcome.EXPIRED,
            4: FinishOutcome.ACKED,
            5: FinishOutcome.RETRIED,
            6: FinishOutcome.DEAD_LETTERED,
        }[result]
        await broker._observability.finished(
            delivery, outcome, reason=reason, error_type=error_type or None
        )
        return outcome

    async def extend(self, delivery: RedisDelivery, seconds: float | None) -> datetime:
        broker = self._broker
        period = seconds if seconds is not None else delivery._lease_seconds
        if period <= 0:
            raise ValidationError("续租时长必须为正数")
        now = await broker._now()
        until = now + timedelta(seconds=period)
        fields = await broker._redis.hgetall(
            broker._message_key(delivery.message.queue, delivery.message.id)
        )
        if fields.get("expires_at") and float(fields["expires_at"]) > 0:
            until = min(until, _datetime(float(fields["expires_at"])) or until)
        result = int(
            await extend_lease_call(
                broker,
                queue=delivery.message.queue,
                message_id=delivery.message.id,
                delivery_id=delivery.delivery_id,
                token=delivery._lease_token,
                lease_until=_timestamp(until),
            ).execute(broker._redis)
        )
        if result == -1:
            await broker._emit_maintenance_event(
                delivery.message.queue,
                delivery.message.id,
                "expired",
                MessageStatus.EXPIRED.value,
                reason="expired_during_lease_extension",
                metric_name="expired_total",
            )
            raise LeaseLostError("消息已过期")
        if result != 1:
            raise LeaseLostError("租约已经失效，不能续租")
        return until

    async def recover_uncommitted_pel(self, queue: str) -> int:
        broker = self._broker
        stream = broker._queue_key(queue, "stream")
        try:
            _, entries, _ = await broker._redis.xautoclaim(
                stream,
                broker._group_name(),
                "taskqx-reclaimer",
                min_idle_time=broker._pending_recovery_ms,
                start_id="0-0",
                count=100,
            )
        except Exception as exc:
            if "NOGROUP" in str(exc):
                return 0
            raise
        restored = 0
        for entry_id, fields in entries:
            message_id = fields["message_id"]
            moved = int(
                await pel_recover_call(
                    broker,
                    queue=queue,
                    message_id=message_id,
                    entry_id=entry_id,
                ).execute(broker._redis)
            )
            restored += moved
            if moved:
                await broker._emit_maintenance_event(
                    queue,
                    message_id,
                    "pel_recovered",
                    MessageStatus.READY.value,
                    reason="pending entry recovery",
                    metric_name="reclaimed_total",
                )
        return restored
