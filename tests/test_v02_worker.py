"""v0.2 Worker、退避和延迟投递验收测试。"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

import pytest

from taskqx import (
    ConsumerOptions,
    ExponentialBackoff,
    FixedBackoff,
    RejectMessage,
    RetryableError,
    RetryPolicy,
    SQLiteBroker,
    ValidationError,
)
from taskqx.types import MessageStatus, utc_now


async def receive(broker: SQLiteBroker):
    return await broker.consumer("jobs").__anext__()


@pytest.mark.asyncio
async def test_delayed_submit_and_retry_are_not_claimed_early() -> None:
    now = [utc_now()]
    async with SQLiteBroker(clock=lambda: now[0]) as broker:
        submitted = await broker.submit(
            queue="jobs", payload={"kind": "submit"}, delay=timedelta(seconds=10)
        )
        assert (await broker.inspect("jobs")).delayed == 1
        assert broker._connection is not None
        row = await (
            await broker._connection.execute(
                "SELECT status FROM messages WHERE id=?", (submitted.message_id,)
            )
        ).fetchone()  # type: ignore[union-attr]
        assert row["status"] == MessageStatus.DELAYED.value  # type: ignore[index]

        now[0] += timedelta(seconds=10)
        delivery = await receive(broker)
        assert delivery.message.id == submitted.message_id and delivery.attempt == 1
        await delivery.retry(reason="temporary", delay=timedelta(seconds=10))
        assert (await broker.inspect("jobs")).delayed == 1

        now[0] += timedelta(seconds=10)
        retried = await receive(broker)
        assert retried.message.id == submitted.message_id and retried.attempt == 2
        await retried.ack()


@pytest.mark.asyncio
async def test_delayed_message_survives_sqlite_restart(tmp_path) -> None:
    database = tmp_path / "delayed.db"
    now = [utc_now()]
    async with SQLiteBroker(database, clock=lambda: now[0]) as first:
        submitted = await first.submit(
            queue="jobs", payload={}, delay=timedelta(seconds=5)
        )

    now[0] += timedelta(seconds=5)
    async with SQLiteBroker(database, clock=lambda: now[0]) as restarted:
        delivery = await receive(restarted)
        assert delivery.message.id == submitted.message_id
        await delivery.ack()


@pytest.mark.asyncio
async def test_delayed_message_expiring_before_due_goes_to_eq() -> None:
    now = [utc_now()]
    async with SQLiteBroker(clock=lambda: now[0]) as broker:
        await broker.submit(
            queue="jobs",
            payload={},
            delay=timedelta(seconds=10),
            expires_at=now[0] + timedelta(seconds=5),
        )
        now[0] += timedelta(seconds=10)
        stats = await broker.inspect("jobs")
        assert stats.ready == 0 and stats.delayed == 0 and stats.expired == 1


@pytest.mark.asyncio
async def test_worker_limits_concurrency_and_applies_policy() -> None:
    active = 0
    maximum = 0
    completed: list[int] = []
    release = asyncio.Event()

    async def handler(message) -> None:
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await release.wait()
        completed.append(message.payload["id"])
        active -= 1

    async with SQLiteBroker() as broker:
        worker = broker.worker(
            "jobs", handler, concurrency=2, retry_policy=RetryPolicy(max_attempts=4)
        )
        await worker.start()
        for value in range(4):
            await broker.submit(queue="jobs", payload={"id": value})
        while active < 2:
            await asyncio.sleep(0.001)
        assert maximum == 2
        release.set()
        while len(completed) < 4:
            await asyncio.sleep(0.001)
        await worker.close()
        assert maximum == 2 and sorted(completed) == [0, 1, 2, 3]
        assert (await broker.inspect("jobs")).acked_total == 4


@pytest.mark.asyncio
async def test_worker_classifies_retries_rejects_and_heartbeats() -> None:
    calls: dict[str, int] = {"retry": 0}

    async def handler(message) -> None:
        kind = message.payload["kind"]
        if kind == "retry" and calls["retry"] == 0:
            calls["retry"] += 1
            raise RetryableError("temporary")
        if kind == "reject":
            raise RejectMessage("invalid")
        if kind == "long":
            await asyncio.sleep(0.08)

    policy = RetryPolicy(max_attempts=3, backoff=FixedBackoff(0.01))
    async with SQLiteBroker() as broker:
        worker = broker.worker(
            "jobs",
            handler,
            retry_policy=policy,
            options=ConsumerOptions(lease_seconds=0.03),
            heartbeat_seconds=0.01,
        )
        await worker.start()
        for kind in ("retry", "reject", "long"):
            await broker.submit(queue="jobs", payload={"kind": kind})
        for _ in range(200):
            stats = await broker.inspect("jobs")
            if stats.acked_total == 2 and stats.dead_letters == 1:
                break
            await asyncio.sleep(0.005)
        await worker.close()
        letters = await broker.admin.list_dead_letters("jobs")
        assert calls["retry"] == 1
        assert stats.acked_total == 2 and len(letters) == 1
        assert (
            letters[0].source == "reject"
            and letters[0].reason == "RejectMessage: invalid"
        )


@pytest.mark.asyncio
async def test_worker_cancellation_leaves_delivery_for_lease_recovery() -> None:
    now = [utc_now()]
    started = asyncio.Event()

    async def handler(_message) -> None:
        started.set()
        await asyncio.Event().wait()

    async with SQLiteBroker(clock=lambda: now[0]) as broker:
        runner = asyncio.create_task(
            broker.run("jobs", handler, options=ConsumerOptions(lease_seconds=1))
        )
        await broker.submit(queue="jobs", payload={})
        await started.wait()
        runner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await runner
        now[0] += timedelta(seconds=2)
        recovered = await receive(broker)
        assert recovered.attempt == 2
        await recovered.ack()


@pytest.mark.asyncio
async def test_worker_policy_max_attempts_overrides_message_default() -> None:
    calls = 0

    async def handler(_message) -> None:
        nonlocal calls
        calls += 1
        raise RetryableError("always")

    async with SQLiteBroker(default_max_attempts=5) as broker:
        worker = broker.worker(
            "jobs", handler, retry_policy=RetryPolicy(max_attempts=2)
        )
        await worker.start()
        await broker.submit(queue="jobs", payload={})
        for _ in range(100):
            if (await broker.inspect("jobs")).dead_letters == 1:
                break
            await asyncio.sleep(0.001)
        await worker.close()
        assert calls == 2 and (await broker.inspect("jobs")).dead_letters == 1


@pytest.mark.asyncio
async def test_worker_policy_never_widens_message_max_attempts() -> None:
    calls = 0

    async def handler(_message) -> None:
        nonlocal calls
        calls += 1
        raise RetryableError("always")

    async with SQLiteBroker(default_max_attempts=5) as broker:
        worker = broker.worker(
            "jobs", handler, retry_policy=RetryPolicy(max_attempts=3)
        )
        await worker.start()
        await broker.submit(queue="jobs", payload={}, max_attempts=1)
        for _ in range(100):
            if (await broker.inspect("jobs")).dead_letters == 1:
                break
            await asyncio.sleep(0.001)
        await worker.close()
        assert calls == 1


@pytest.mark.asyncio
async def test_worker_without_policy_preserves_v01_message_limit() -> None:
    calls = 0

    async def handler(_message) -> None:
        nonlocal calls
        calls += 1
        raise RetryableError("always")

    async with SQLiteBroker(default_max_attempts=5) as broker:
        worker = broker.worker("jobs", handler)
        await worker.start()
        await broker.submit(queue="jobs", payload={}, max_attempts=4)
        for _ in range(100):
            if (await broker.inspect("jobs")).dead_letters == 1:
                break
            await asyncio.sleep(0.001)
        await worker.close()
        assert calls == 4


def test_backoff_is_attempt_based_and_bounded() -> None:
    policy = RetryPolicy.exponential(initial_delay=1, max_delay=3, factor=2)
    assert [policy.delay_for(attempt).total_seconds() for attempt in (1, 2, 3)] == [
        1,
        2,
        3,
    ]
    assert ExponentialBackoff(initial=0, maximum=0).delay_for(1) == timedelta()


@pytest.mark.parametrize(
    "backoff",
    [
        lambda: FixedBackoff(-1),
        lambda: ExponentialBackoff(initial=float("nan")),
        lambda: ExponentialBackoff(maximum=float("inf")),
        lambda: ExponentialBackoff(factor=0.5),
    ],
)
def test_backoff_rejects_invalid_numeric_parameters(backoff) -> None:
    with pytest.raises(ValidationError):
        backoff()


@pytest.mark.asyncio
async def test_worker_logs_retry_exhaustion_with_safe_correlatable_context(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def handler(_message) -> None:
        raise RetryableError("temporary backend fault")

    payload_marker = "payload-must-not-appear-in-log"
    caplog.set_level(logging.WARNING, logger="taskqx.worker")
    async with SQLiteBroker() as broker:
        worker = broker.worker(
            "jobs",
            handler,
            retry_policy=RetryPolicy(max_attempts=1),
            options=ConsumerOptions(poll_interval=0.001),
        )
        await worker.start()
        submitted = await broker.submit(
            queue="jobs", payload={"secret": payload_marker}
        )
        for _ in range(100):
            if (await broker.inspect("jobs")).dead_letters == 1:
                break
            await asyncio.sleep(0.001)
        await worker.close()

    records = [
        record
        for record in caplog.records
        if record.name == "taskqx.worker" and record.message.startswith("taskqx")
    ]
    retry, exhausted = records
    assert retry.levelno == logging.WARNING and retry.exc_info is not None
    assert exhausted.levelno == logging.ERROR and exhausted.exc_info is not None
    for record in records:
        assert {
            "backend",
            "namespace",
            "queue",
            "message_id",
            "delivery_id",
            "consumer_id",
            "attempt",
            "max_attempts",
            "action",
            "outcome",
            "retry_delay",
            "error_type",
        } <= record.__dict__.keys()
        assert record.__dict__["queue"] == "jobs"
        assert record.__dict__["message_id"] == submitted.message_id
    assert exhausted.__dict__["outcome"] == "dead_lettered"
    assert payload_marker not in caplog.text
