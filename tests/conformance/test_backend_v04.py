"""v0.4 的公开 Protocol 跨 backend 契约。

本套测试刻意不读取 backend 私有 client、namespace、连接或状态表；每个 Redis
用例使用随机 namespace 隔离，并由测试 fixture 在结束时删除该 namespace 的 keys。
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import TypedDict
from uuid import uuid4

import pytest

from taskflow import (
    ConsumerOptions,
    RedisBroker,
    RetryableError,
    RetryPolicy,
    SQLiteBroker,
    SubmitRequest,
    TaskMessage,
    ValidationError,
)
from taskflow.types import utc_now

Broker = SQLiteBroker | RedisBroker


@dataclass(frozen=True)
class Resize:
    image_id: str
    width: int


class ResizeDict(TypedDict):
    image_id: str
    width: int


@pytest.fixture(params=("sqlite", pytest.param("redis", marks=pytest.mark.redis)))
async def broker(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator[Broker]:
    if request.param == "sqlite":
        async with SQLiteBroker(tmp_path / "conformance-v04.db") as instance:
            yield instance
        return

    pytest.importorskip("redis.asyncio")
    namespace = f"taskflow-conformance-v04-{uuid4()}"
    redis_instance = RedisBroker.from_url(namespace=namespace)
    await redis_instance.start()
    try:
        yield redis_instance
    finally:
        await redis_instance.close()
        from redis.asyncio import Redis

        cleanup = Redis.from_url("redis://127.0.0.1:6379/2", decode_responses=True)
        try:
            keys = [key async for key in cleanup.scan_iter(match=f"{namespace}:*")]
            if keys:
                await cleanup.delete(*keys)
        finally:
            await cleanup.aclose()


async def _wait_for_stats(broker: Broker, *, acked: int, dead_letters: int) -> None:
    for _ in range(300):
        stats = await broker.inspect("jobs")
        if stats.acked_total == acked and stats.dead_letters == dead_letters:
            return
        await asyncio.sleep(0.005)
    raise AssertionError("backend did not reach expected terminal state")


@pytest.mark.asyncio
async def test_submit_delay_retry_heartbeat_and_stats(broker: Broker) -> None:
    attempts = 0

    async def handler(message: TaskMessage) -> None:
        nonlocal attempts
        if message.payload["kind"] == "retry" and attempts == 0:
            attempts += 1
            raise RetryableError("temporary")
        if message.payload["kind"] == "long":
            await asyncio.sleep(0.08)

    worker = broker.worker(
        "jobs", handler, concurrency=2,
        options=ConsumerOptions(lease_seconds=0.1, poll_interval=0.001),
        heartbeat_seconds=0.02,
        retry_policy=RetryPolicy.fixed(delay=0.01, max_attempts=2),
    )
    await worker.start()
    await broker.submit(queue="jobs", payload={"kind": "retry"})
    await broker.submit(queue="jobs", payload={"kind": "long"})
    delayed = await broker.submit(queue="jobs", payload={"kind": "delayed"},
                                  delay=timedelta(milliseconds=40))
    await _wait_for_stats(broker, acked=3, dead_letters=0)
    await worker.close()
    assert attempts == 1
    assert delayed.accepted


@pytest.mark.asyncio
async def test_typed_payload_batch_and_expiry_contract(broker: Broker) -> None:
    typed = await broker.submit(queue="jobs", payload={"image_id": "one", "width": 1},
                                payload_type=ResizeDict)
    inspected = await broker.inspect_message(typed.message_id)
    assert inspected is not None
    assert inspected.payload_schema_name == f"{ResizeDict.__module__}.{ResizeDict.__qualname__}"

    items = await broker.submit_many([
        SubmitRequest(queue="jobs", payload=Resize("two", 2)),
        SubmitRequest(queue="bad queue!", payload={}),
        SubmitRequest(queue="jobs", payload={"image_id": "three", "width": 3}, payload_type=ResizeDict),
    ], atomic=False)
    assert [item.index for item in items] == [0, 1, 2]
    assert items[0].result is not None and items[0].result.accepted
    assert isinstance(items[1].error, ValidationError)
    assert items[2].result is not None and items[2].result.accepted

    with pytest.raises(ValidationError):
        await broker.submit(queue="jobs", payload={"image_id": "broken", "width": "1"},
                            payload_type=ResizeDict)
    expired = await broker.submit(queue="jobs", payload={},
                                 expires_at=utc_now() - timedelta(seconds=1))
    assert expired.accepted
    assert (await broker.inspect("jobs")).expired >= 1


@pytest.mark.asyncio
async def test_worker_cancellation_does_not_ack_inflight_delivery(broker: Broker) -> None:
    started = asyncio.Event()

    async def handler(_message: TaskMessage) -> None:
        started.set()
        await asyncio.Event().wait()

    runner = asyncio.create_task(
        broker.run("jobs", handler, options=ConsumerOptions(lease_seconds=0.01, poll_interval=0.001)),
    )
    await broker.submit(queue="jobs", payload={})
    await asyncio.wait_for(started.wait(), timeout=1)
    runner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await runner
    await asyncio.sleep(0.03)
    reclaimed = await broker.consumer("jobs").__anext__()
    assert reclaimed.attempt == 2
    await reclaimed.ack()


@pytest.mark.asyncio
async def test_dlq_replay_dedup_and_target_queue_contract(broker: Broker) -> None:
    submitted = await broker.submit(
        queue="jobs", payload={"kind": "repair"}, dedup_scope="original", dedup_key="item",
        dedup_ttl=timedelta(minutes=1),
    )
    delivery = await broker.consumer("jobs").__anext__()
    await delivery.reject(reason="manual repair")
    assert [item.message.id for item in await broker.admin.list_dead_letters("jobs")] == [submitted.message_id]

    await broker.admin.replay_dead_letter(
        "jobs", submitted.message_id, target_queue="repairs", dedup_mode="replace",
        dedup_scope="repair", dedup_key="item", dedup_ttl=timedelta(minutes=1),
    )
    replayed = await broker.consumer("repairs").__anext__()
    assert replayed.message.id == submitted.message_id
    await replayed.ack()
    assert not await broker.admin.list_dead_letters("jobs")


@pytest.mark.asyncio
async def test_eq_replay_and_expiry_contract(broker: Broker) -> None:
    submitted = await broker.submit(
        queue="jobs", payload={"kind": "expired"}, expires_at=utc_now() - timedelta(seconds=1),
    )
    assert (await broker.inspect("jobs")).expired == 1
    assert [item.message.id for item in await broker.admin.list_expired("jobs")] == [submitted.message_id]

    await broker.admin.replay_expired("jobs", submitted.message_id, expires_at=None, target_queue="repairs",
                                      dedup_mode="remove")
    replayed = await broker.consumer("repairs").__anext__()
    assert replayed.message.id == submitted.message_id
    await replayed.ack()
    assert not await broker.admin.list_expired("jobs")
