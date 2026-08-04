"""SQLite delivery state transitions.

The Broker owns connection lifecycle; this object owns delivery transitions.
"""

from __future__ import annotations

import traceback
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import aiosqlite

from ..errors import LeaseLostError, ValidationError
from ..observability import event, metric
from ..types import FinishOutcome, MessageStatus
from ._time import datetime_from_timestamp as _datetime
from ._time import timestamp as _timestamp
from .sqlite_components import SQLiteDelivery

if TYPE_CHECKING:
    from .sqlite import SQLiteBroker, _MaintenanceEvent


class SQLiteStateMachine:
    """Execute SQLite delivery transitions within the broker transaction boundary."""

    def __init__(self, broker: SQLiteBroker) -> None:
        self._broker = broker

    async def claim(
        self, queue: str, consumer_id: str, lease_seconds: float
    ) -> SQLiteDelivery | None:
        broker = self._broker
        await broker.start()
        now = broker._now()
        maintenance_events: list[_MaintenanceEvent] = []
        updated: aiosqlite.Row | None = None
        delivery_id = token = ""
        lease_until = now
        async with broker._lock:
            assert broker._connection is not None
            cursor = await broker._connection.cursor()
            await cursor.execute("BEGIN IMMEDIATE")
            try:
                _, maintenance_events = await broker._maintain(cursor, now, queue)
                row = await (
                    await cursor.execute(
                        "SELECT * FROM messages WHERE queue=? AND status=? ORDER BY created_at, id LIMIT 1",
                        (queue, MessageStatus.READY.value),
                    )
                ).fetchone()
                if row is None:
                    await cursor.execute("COMMIT")
                else:
                    delivery_id, token = broker._id_factory(), broker._id_factory()
                    lease_until = now + timedelta(seconds=lease_seconds)
                    await cursor.execute(
                        "UPDATE messages SET status=?, attempt=attempt+1, consumer_id=?, delivery_id=?, "
                        "lease_token=?, claimed_at=?, lease_until=?, last_action=NULL WHERE id=?",
                        (
                            MessageStatus.LEASED.value,
                            consumer_id,
                            delivery_id,
                            token,
                            _timestamp(now),
                            _timestamp(lease_until),
                            row["id"],
                        ),
                    )
                    updated = await (
                        await cursor.execute(
                            "SELECT * FROM messages WHERE id=?", (row["id"],)
                        )
                    ).fetchone()
                    await cursor.execute("COMMIT")
            except Exception:
                await cursor.execute("ROLLBACK")
                raise
        await broker._emit_maintenance_events(maintenance_events)
        if updated is None:
            return None
        message = broker._decode_message(
            updated["envelope"],
            updated["serializer_name"],
            updated["serializer_version"],
        )
        delivery = SQLiteDelivery(
            broker,
            message,
            delivery_id,
            token,
            consumer_id,
            updated["attempt"],
            now,
            lease_until,
        )
        await broker.middleware.emit("after_claim", delivery)
        await metric(broker.metrics, "claimed_total", queue=queue)
        await event(
            broker.middleware,
            "claimed",
            message,
            status=MessageStatus.LEASED.value,
            delivery=delivery,
            backend="sqlite",
            event_sink=broker.event_sink,
            serializer_name=broker._serializer.name,
            serializer_version=broker._serializer.version,
        )
        return delivery

    async def extend(self, delivery: SQLiteDelivery, seconds: float | None) -> datetime:
        broker = self._broker
        await broker.start()
        period = seconds if seconds is not None else delivery._lease_seconds
        if period <= 0:
            raise ValidationError("续租时长必须为正数")
        now = broker._now()
        expired_event: _MaintenanceEvent | None = None
        async with broker._lock:
            assert broker._connection is not None
            cursor = await broker._connection.cursor()
            await cursor.execute("BEGIN IMMEDIATE")
            try:
                row = await (
                    await cursor.execute(
                        "SELECT * FROM messages WHERE id=?", (delivery.message.id,)
                    )
                ).fetchone()
                if (
                    row is None
                    or row["status"] != MessageStatus.LEASED.value
                    or row["delivery_id"] != delivery.delivery_id
                    or row["lease_token"] != delivery._lease_token
                    or row["lease_until"] <= _timestamp(now)
                ):
                    raise LeaseLostError("租约已经失效，不能续租")
                until = now + timedelta(seconds=period)
                if row["expires_at"] is not None:
                    until = min(until, _datetime(row["expires_at"]) or until)
                if until <= now:
                    await broker._expire(
                        cursor, row["id"], now, MessageStatus.LEASED, row["attempt"]
                    )
                    expired_event = broker._maintenance_event(
                        row,
                        "expired",
                        MessageStatus.EXPIRED.value,
                        reason="expired_during_lease_extension",
                        metric_name="expired_total",
                    )
                    await cursor.execute("COMMIT")
                else:
                    await cursor.execute(
                        "UPDATE messages SET lease_until=? WHERE id=?",
                        (_timestamp(until), row["id"]),
                    )
                    await cursor.execute("COMMIT")
                    return until
            except Exception:
                await cursor.execute("ROLLBACK")
                raise
        assert expired_event is not None
        await broker._emit_maintenance_events([expired_event])
        raise LeaseLostError("消息已过期")

    async def finish(
        self,
        delivery: SQLiteDelivery,
        action: str,
        reason: str | None = None,
        error: BaseException | None = None,
        delay: timedelta | None = None,
        max_attempts: int | None = None,
    ) -> FinishOutcome:
        broker = self._broker
        await broker.start()
        now = broker._now()
        async with broker._lock:
            assert broker._connection is not None
            cursor = await broker._connection.cursor()
            await cursor.execute("BEGIN IMMEDIATE")
            try:
                row = await (
                    await cursor.execute(
                        "SELECT * FROM messages WHERE id=?", (delivery.message.id,)
                    )
                ).fetchone()
                if row is None:
                    raise LeaseLostError("消息不存在")
                if (
                    row["last_delivery_id"] == delivery.delivery_id
                    and row["last_action"] == action
                    and row["status"] != MessageStatus.LEASED.value
                ):
                    await cursor.execute("COMMIT")
                    return FinishOutcome.IDEMPOTENT
                if (
                    row["status"] != MessageStatus.LEASED.value
                    or row["delivery_id"] != delivery.delivery_id
                    or row["lease_token"] != delivery._lease_token
                    or row["lease_until"] <= _timestamp(now)
                ):
                    raise LeaseLostError("租约已经失效，不能终结当前投递")
                if row["expires_at"] is not None and row["expires_at"] <= _timestamp(
                    now
                ):
                    await broker._expire(
                        cursor, row["id"], now, MessageStatus.LEASED, row["attempt"]
                    )
                    outcome = FinishOutcome.EXPIRED
                elif action == "ack":
                    retention_until = now + broker._ack_tombstone_ttl(row["queue"])
                    await cursor.execute(
                        "UPDATE messages SET status=?, last_delivery_id=delivery_id, "
                        "last_consumer_id=consumer_id, consumer_id=NULL, delivery_id=NULL, "
                        "lease_token=NULL, claimed_at=NULL, lease_until=NULL, last_action=?, "
                        "last_reason=NULL, acked_at=?, retention_until=? WHERE id=?",
                        (
                            MessageStatus.ACKED.value,
                            action,
                            _timestamp(now),
                            _timestamp(retention_until),
                            row["id"],
                        ),
                    )
                    await broker._counter(cursor, row["queue"], "acked_total")
                    outcome = FinishOutcome.ACKED
                elif action == "retry":
                    if delay is not None and delay.total_seconds() < 0:
                        raise ValidationError("delay 不能为负数")
                    limit = (
                        min(row["max_attempts"], max_attempts)
                        if max_attempts is not None
                        else row["max_attempts"]
                    )
                    if limit < 1:
                        raise ValidationError("max_attempts 必须大于等于 1")
                    if row["attempt"] >= limit:
                        await broker._dead_letter(
                            cursor, row, now, "retry_limit", reason, last_action="retry"
                        )
                        outcome = FinishOutcome.DEAD_LETTERED
                    else:
                        available_at = (
                            now + delay if delay and delay.total_seconds() > 0 else None
                        )
                        await cursor.execute(
                            "UPDATE messages SET status=?, available_at=?, last_delivery_id=delivery_id, last_consumer_id=consumer_id, consumer_id=NULL, delivery_id=NULL, lease_token=NULL, claimed_at=NULL, lease_until=NULL, last_action=?, last_reason=? WHERE id=?",
                            (
                                (
                                    MessageStatus.DELAYED
                                    if available_at
                                    else MessageStatus.READY
                                ).value,
                                _timestamp(available_at) if available_at else None,
                                action,
                                reason,
                                row["id"],
                            ),
                        )
                        await broker._counter(cursor, row["queue"], "retried_total")
                        outcome = FinishOutcome.RETRIED
                else:
                    await broker._dead_letter(cursor, row, now, "reject", reason, error)
                    outcome = FinishOutcome.DEAD_LETTERED
                await cursor.execute("COMMIT")
            except Exception:
                await cursor.execute("ROLLBACK")
                raise
        event_names = {
            FinishOutcome.ACKED: ("ack", "acked_total"),
            FinishOutcome.RETRIED: ("retry", "retried_total"),
            FinishOutcome.DEAD_LETTERED: ("dead_lettered", "dead_lettered_total"),
            FinishOutcome.EXPIRED: ("expired", "expired_total"),
        }
        event_name, metric_name = event_names[outcome]
        await broker.middleware.emit(f"after_{event_name}", delivery, reason)
        await metric(broker.metrics, metric_name, queue=delivery.message.queue)
        await event(
            broker.middleware,
            event_name,
            delivery.message,
            status=outcome.value,
            delivery=delivery,
            reason=reason,
            error_type=type(error).__name__ if error else None,
            backend="sqlite",
            event_sink=broker.event_sink,
            serializer_name=broker._serializer.name,
            serializer_version=broker._serializer.version,
        )
        return outcome

    async def counter(self, cursor: aiosqlite.Cursor, queue: str, column: str) -> None:
        await cursor.execute(
            "INSERT INTO queue_counters(queue) VALUES (?) ON CONFLICT(queue) DO NOTHING",
            (queue,),
        )
        await cursor.execute(
            f"UPDATE queue_counters SET {column}={column}+1 WHERE queue=?", (queue,)
        )

    async def dead_letter(
        self,
        cursor: aiosqlite.Cursor,
        row: aiosqlite.Row,
        now: datetime,
        source: str,
        reason: str | None,
        error: BaseException | None = None,
        *,
        last_action: str | None = None,
    ) -> None:
        await cursor.execute(
            "INSERT OR REPLACE INTO dead_letters VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                row["id"],
                row["queue"],
                row["attempt"],
                reason,
                source,
                _timestamp(now),
                type(error).__name__ if error else None,
                "".join(traceback.format_exception(error)) if error else None,
            ),
        )
        await cursor.execute(
            "UPDATE messages SET status=?, last_delivery_id=delivery_id, last_consumer_id=consumer_id, consumer_id=NULL, delivery_id=NULL, lease_token=NULL, claimed_at=NULL, lease_until=NULL, last_action=?, last_reason=? WHERE id=?",
            (
                MessageStatus.DEAD_LETTERED.value,
                last_action or source,
                reason,
                row["id"],
            ),
        )
        await self.counter(cursor, row["queue"], "dead_lettered_total")

    async def expire(
        self,
        cursor: aiosqlite.Cursor,
        message_id: str,
        now: datetime,
        old_status: MessageStatus,
        attempt: int,
    ) -> None:
        row = await (
            await cursor.execute(
                "SELECT queue, delivery_id, consumer_id FROM messages WHERE id=?",
                (message_id,),
            )
        ).fetchone()
        if row is None:
            return
        await cursor.execute(
            "INSERT OR REPLACE INTO expired_messages VALUES (?, ?, ?, ?, ?)",
            (message_id, row["queue"], attempt, old_status.value, _timestamp(now)),
        )
        await cursor.execute(
            "UPDATE messages SET status=?, last_delivery_id=delivery_id, last_consumer_id=consumer_id, consumer_id=NULL, delivery_id=NULL, lease_token=NULL, claimed_at=NULL, lease_until=NULL, last_action='expired' WHERE id=?",
            (MessageStatus.EXPIRED.value, message_id),
        )
