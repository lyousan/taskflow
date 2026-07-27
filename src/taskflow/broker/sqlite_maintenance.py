"""SQLite maintenance transaction orchestration."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

import aiosqlite

from ..observability import event, metric
from ..types import MessageStatus
from ._time import timestamp as _timestamp

if TYPE_CHECKING:
    from .sqlite import SQLiteBroker

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MaintenanceEvent:
    name: str
    status: str
    envelope: bytes
    serializer_name: str
    serializer_version: str
    delivery_id: str | None
    consumer_id: str | None
    attempt: int
    reason: str | None = None
    error_type: str | None = None
    metric_name: str | None = None


class SQLiteMaintenance:
    """Commit maintenance transitions before publishing their observations."""

    def __init__(self, broker: SQLiteBroker) -> None:
        self._broker = broker

    async def run(self, queue: str | None = None) -> int:
        broker = self._broker
        broker._ensure_open()
        await broker.start()
        async with broker._lock:
            assert broker._connection is not None
            cursor = await broker._connection.cursor()
            await cursor.execute("BEGIN IMMEDIATE")
            try:
                count, events = await self.maintain(cursor, broker._now(), queue)
                await cursor.execute("COMMIT")
            except Exception:
                await cursor.execute("ROLLBACK")
                raise
        await broker._emit_maintenance_events(events)
        return count

    def event(self, row: aiosqlite.Row, name: str, status: str, *, reason: str | None = None,
              error_type: str | None = None, metric_name: str | None = None) -> MaintenanceEvent:
        return MaintenanceEvent(name, status, row["envelope"], row["serializer_name"], row["serializer_version"],
                                row["delivery_id"], row["consumer_id"], row["attempt"], reason, error_type, metric_name)

    async def emit(self, events: list[MaintenanceEvent]) -> None:
        broker = self._broker
        for item in events:
            try:
                message = broker._decode_message(item.envelope, item.serializer_name, item.serializer_version)
                if item.metric_name is not None:
                    await metric(broker.metrics, item.metric_name, queue=message.queue)
                await event(broker.middleware, item.name, message, status=item.status,
                    delivery_id=item.delivery_id, consumer_id=item.consumer_id, attempt=item.attempt,
                    reason=item.reason, error_type=item.error_type, backend="sqlite", event_sink=broker.event_sink,
                    serializer_name=item.serializer_name, serializer_version=item.serializer_version)
            except Exception:
                logger.exception("taskflow failed to publish committed maintenance event", extra={"event": item.name})

    async def maintain(self, cursor: aiosqlite.Cursor, now: datetime,
                       queue: str | None = None) -> tuple[int, list[MaintenanceEvent]]:
        """Apply due, expiry and lease-reclaim transitions in the active transaction."""
        broker = self._broker
        predicate, params = ("", []) if queue is None else (" AND queue=?", [queue])
        events: list[MaintenanceEvent] = []
        expired = await (await cursor.execute(
            "SELECT * FROM messages WHERE status IN (?, ?, ?) AND expires_at IS NOT NULL AND expires_at<=?" + predicate,
            [MessageStatus.READY.value, MessageStatus.DELAYED.value, MessageStatus.LEASED.value, _timestamp(now), *params])).fetchall()
        for row in expired:
            await broker._state_machine.expire(cursor, row["id"], now, MessageStatus(row["status"]), row["attempt"])
            events.append(self.event(row, "expired", MessageStatus.EXPIRED.value,
                reason=f"expired_from:{row['status']}", metric_name="expired_total"))
        due = await (await cursor.execute(
            "SELECT * FROM messages WHERE status=? AND available_at IS NOT NULL AND available_at<=?" + predicate,
            [MessageStatus.DELAYED.value, _timestamp(now), *params])).fetchall()
        for row in due:
            await cursor.execute("UPDATE messages SET status=?, available_at=NULL, last_action='due' WHERE id=?",
                (MessageStatus.READY.value, row["id"]))
            events.append(self.event(row, "due", MessageStatus.READY.value))
        leases = await (await cursor.execute(
            "SELECT * FROM messages WHERE status=? AND lease_until<=?" + predicate,
            [MessageStatus.LEASED.value, _timestamp(now), *params])).fetchall()
        for row in leases:
            if row["attempt"] >= row["max_attempts"]:
                await broker._state_machine.dead_letter(cursor, row, now, "lease_timeout", "租约超时且已达到最大尝试次数")
                events.append(self.event(row, "dead_lettered", MessageStatus.DEAD_LETTERED.value,
                    reason="租约超时且已达到最大尝试次数", metric_name="dead_lettered_total"))
            else:
                await cursor.execute("UPDATE messages SET status=?, last_delivery_id=delivery_id, last_consumer_id=consumer_id, consumer_id=NULL, delivery_id=NULL, lease_token=NULL, claimed_at=NULL, lease_until=NULL, last_action='reclaimed', last_reason=? WHERE id=?", (MessageStatus.READY.value, "租约超时", row["id"]))
                await broker._state_machine.counter(cursor, row["queue"], "reclaimed_total")
                events.append(self.event(row, "reclaimed", MessageStatus.READY.value,
                    reason="租约超时", metric_name="reclaimed_total"))
        return sum(1 for _ in expired) + sum(1 for _ in due) + sum(1 for _ in leases), events
