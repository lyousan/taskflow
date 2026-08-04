"""v0.7 backend-only scheduler contracts."""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

import pytest

from taskqx import ConsumerOptions, FinishOutcome, MessageStatus, SQLiteBroker
from taskqx.scheduler import BackendScheduler
from taskqx.types import utc_now


@pytest.mark.asyncio
async def test_scheduler_discovers_idle_queue_and_promotes_delayed_message() -> None:
    now = [utc_now()]
    async with SQLiteBroker(clock=lambda: now[0]) as broker:
        submitted = await broker.submit(
            queue="idle", payload={"id": 1}, delay=timedelta(seconds=1)
        )
        scheduler = broker.scheduler()

        assert await scheduler.tick() == 0
        now[0] += timedelta(seconds=1)
        assert await scheduler.tick() == 1

        delivery = await broker.consumer("idle").__anext__()
        assert delivery.message.id == submitted.message_id
        assert await delivery.ack() is FinishOutcome.ACKED


@pytest.mark.asyncio
async def test_scheduler_advances_expiry_lease_and_ack_tombstones() -> None:
    now = [utc_now()]
    async with SQLiteBroker(
        clock=lambda: now[0], default_ack_tombstone_ttl=timedelta(seconds=1)
    ) as broker:
        leased = await broker.submit(queue="jobs", payload={"leased": True})
        delivery = await broker.consumer(
            "jobs", options=ConsumerOptions(lease_seconds=1)
        ).__anext__()
        assert delivery.message.id == leased.message_id
        retained = await broker.submit(
            queue="jobs",
            payload={"acked": True},
            workflow_id="billing",
            parent_id="origin",
        )
        assert await (await broker.consumer("jobs").__anext__()).ack() is FinishOutcome.ACKED
        expired = await broker.submit(
            queue="jobs", payload={"expired": True}, expires_at=now[0] + timedelta(seconds=1)
        )
        now[0] += timedelta(seconds=1)
        scheduler = broker.scheduler(queues=["jobs"])
        assert await scheduler.tick() == 3
        assert await broker.inspect_message(retained.message_id) is None
        tombstone = (
            await broker.list_message_summaries("jobs", status=MessageStatus.ACKED)
        ).items[0]
        assert tombstone.message_id == retained.message_id
        assert tombstone.payload_pruned and tombstone.acked_at is not None
        assert (tombstone.workflow_id, tombstone.parent_id) == ("billing", "origin")
        assert [item.message.id for item in await broker.admin.list_expired("jobs")] == [
            expired.message_id
        ]
        reclaimed = await broker.consumer("jobs").__anext__()
        assert reclaimed.message.id == leased.message_id


@pytest.mark.asyncio
async def test_concurrent_scheduler_ticks_are_idempotent() -> None:
    now = [utc_now()]
    async with SQLiteBroker(clock=lambda: now[0]) as broker:
        submitted = await broker.submit(
            queue="jobs", payload={}, delay=timedelta(seconds=1)
        )
        now[0] += timedelta(seconds=1)
        first, second = await asyncio.gather(
            broker.scheduler().tick(), broker.scheduler().tick()
        )
        assert sorted((first, second)) == [0, 1]
        assert (await broker.consumer("jobs").__anext__()).message.id == submitted.message_id


@pytest.mark.asyncio
async def test_scheduler_logs_tick_failure_then_continues(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class Backend:
        calls = 0

        async def _scheduler_queues(self) -> tuple[str, ...]:
            return ("jobs",)

        async def maintain(self, queue: str | None = None) -> int:
            assert queue == "jobs"
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("maintenance failed")
            return 1

    backend = Backend()
    caplog.set_level(logging.ERROR, logger="taskqx.scheduler")
    scheduler = BackendScheduler(backend, interval=timedelta(milliseconds=1))
    await scheduler.start()
    await asyncio.sleep(0.05)
    await scheduler.close()

    record = next(
        record
        for record in caplog.records
        if record.name == "taskqx.scheduler"
    )
    assert backend.calls > 1
    assert record.exc_info is not None
    assert record.__dict__["action"] == "maintain"
    assert record.__dict__["outcome"] == "failed"
    assert record.__dict__["error_type"] == "RuntimeError"
