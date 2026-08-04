"""Redis delayed, expiry, lease-reclaim and PEL-recovery maintenance."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..types import MessageStatus
from ._time import timestamp as _timestamp
from .redis_calls import (
    cleanup_acked_call,
    due_delayed_call,
    expire_call,
    reclaim_lease_call,
)

if TYPE_CHECKING:
    from .redis import RedisBroker


class RedisMaintenance:
    """Run committed Redis maintenance transitions and delegate their observation."""

    def __init__(self, broker: RedisBroker) -> None:
        self._broker = broker

    async def run(self, queue: str | None = None) -> int:
        if queue is None:
            return 0
        broker = self._broker
        await broker._ensure_group(queue)
        now = await broker._now()
        reclaimed = await broker._recover_uncommitted_pel(queue)
        for message_id in await broker._redis.zrangebyscore(
            broker._queue_key(queue, "delayed"), "-inf", _timestamp(now)
        ):
            moved = int(
                await due_delayed_call(
                    broker,
                    queue=queue,
                    message_id=message_id,
                    now=_timestamp(now),
                ).execute(broker._redis)
            )
            reclaimed += int(bool(moved))
            if moved == 1:
                await broker._emit_maintenance_event(
                    queue, message_id, "due", MessageStatus.READY.value
                )
            elif moved == 2:
                await broker._emit_maintenance_event(
                    queue,
                    message_id,
                    "expired",
                    MessageStatus.EXPIRED.value,
                    reason="expired_from:delayed",
                    metric_name="expired_total",
                )
        for message_id in await broker._redis.zrangebyscore(
            broker._queue_key(queue, "expiry"), "-inf", _timestamp(now)
        ):
            moved = int(
                await expire_call(
                    broker,
                    queue=queue,
                    message_id=message_id,
                    now=_timestamp(now),
                ).execute(broker._redis)
            )
            if moved:
                await broker._emit_maintenance_event(
                    queue,
                    message_id,
                    "expired",
                    MessageStatus.EXPIRED.value,
                    reason="expired_by_maintenance",
                    metric_name="expired_total",
                )
        for message_id in await broker._redis.zrangebyscore(
            broker._queue_key(queue, "leases"), "-inf", _timestamp(now)
        ):
            result = int(
                await reclaim_lease_call(
                    broker,
                    queue=queue,
                    message_id=message_id,
                    now=_timestamp(now),
                ).execute(broker._redis)
            )
            reclaimed += int(bool(result))
            transitions = {
                1: ("reclaimed", MessageStatus.READY.value, "reclaimed_total"),
                2: (
                    "dead_lettered",
                    MessageStatus.DEAD_LETTERED.value,
                    "dead_lettered_total",
                ),
                3: ("expired", MessageStatus.EXPIRED.value, "expired_total"),
            }
            if result in transitions:
                name, status, metric_name = transitions[result]
                reason = "expired_from:leased" if result == 3 else "lease_timeout"
                await broker._emit_maintenance_event(
                    queue, message_id, name, status, reason=reason, metric_name=metric_name
                )
        for message_id in await broker._redis.zrangebyscore(
            broker._queue_key(queue, "retention"), "-inf", _timestamp(now)
        ):
            cleaned = int(
                await cleanup_acked_call(
                    broker,
                    queue=queue,
                    message_id=message_id,
                    now=_timestamp(now),
                ).execute(broker._redis)
            )
            reclaimed += cleaned
        return reclaimed
