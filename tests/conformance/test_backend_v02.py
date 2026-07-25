"""SQLite / Redis 共享的 v0.2 行为契约。

Backend-specific 测试仍保留在各自文件中；本文件只放两个 backend 都必须满足的
高层语义，避免新增 Worker 功能只在一个 backend 上被验证。
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from taskflow import (
    ConsumerOptions,
    RedisBroker,
    RejectMessage,
    RetryableError,
    RetryPolicy,
    SQLiteBroker,
)


@pytest.fixture(params=("sqlite", "redis"))
async def broker(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator[object]:
    instance: Any
    if request.param == "sqlite":
        async with SQLiteBroker(tmp_path / "conformance.db") as instance:
            yield instance
        return

    pytest.importorskip("redis.asyncio")
    instance = RedisBroker.from_url(namespace=f"taskflow-conformance-{uuid4()}")
    await instance.start()
    try:
        yield instance
    finally:
        keys = [key async for key in instance._redis.scan_iter(match=f"{instance._namespace}:*")]
        if keys:
            await instance._redis.unlink(*keys)
        await instance.close()


async def _wait_for_terminal(broker: object, *, acked: int, dead_letters: int) -> None:
    for _ in range(300):
        stats = await broker.inspect("jobs")  # type: ignore[attr-defined]
        if stats.acked_total == acked and stats.dead_letters == dead_letters:
            return
        await asyncio.sleep(0.005)
    raise AssertionError(f"backend did not reach terminal counts: {stats!r}")


@pytest.mark.asyncio
async def test_worker_success_retry_reject_and_policy_limit(broker: object) -> None:
    attempts = 0

    async def handler(message) -> None:
        nonlocal attempts
        kind = message.payload["kind"]
        if kind == "retry" and attempts == 0:
            attempts += 1
            raise RetryableError("temporary")
        if kind == "reject":
            raise RejectMessage("invalid")

    worker = broker.worker(  # type: ignore[attr-defined]
        "jobs", handler, concurrency=2,
        options=ConsumerOptions(lease_seconds=0.2),
        retry_policy=RetryPolicy.fixed(delay=0.01, max_attempts=2),
    )
    await worker.start()
    await broker.submit(queue="jobs", payload={"kind": "ok"})  # type: ignore[attr-defined]
    await broker.submit(queue="jobs", payload={"kind": "retry"})  # type: ignore[attr-defined]
    await broker.submit(queue="jobs", payload={"kind": "reject"})  # type: ignore[attr-defined]
    await _wait_for_terminal(broker, acked=2, dead_letters=1)
    await worker.close()
    assert attempts == 1


@pytest.mark.asyncio
async def test_delayed_retry_is_persistent_and_not_claimed_early(broker: object) -> None:
    submitted = await broker.submit(  # type: ignore[attr-defined]
        queue="jobs", payload={}, delay=timedelta(milliseconds=40), max_attempts=2)
    assert (await broker.inspect("jobs")).delayed == 1  # type: ignore[attr-defined]
    await asyncio.sleep(0.01)
    assert (await broker.inspect("jobs")).delayed == 1  # type: ignore[attr-defined]
    delivery = await broker.consumer("jobs").__anext__()  # type: ignore[attr-defined]
    assert delivery.message.id == submitted.message_id
    await delivery.retry(reason="temporary", delay=timedelta(milliseconds=40))
    assert (await broker.inspect("jobs")).delayed == 1  # type: ignore[attr-defined]
    await asyncio.sleep(0.05)
    retry = await broker.consumer("jobs").__anext__()  # type: ignore[attr-defined]
    assert retry.attempt == 2
    await retry.ack()


@pytest.mark.asyncio
async def test_worker_heartbeat_protects_long_handler(broker: object) -> None:
    async def handler(_message) -> None:
        await asyncio.sleep(0.25)

    worker = broker.worker(  # type: ignore[attr-defined]
        "jobs", handler, options=ConsumerOptions(lease_seconds=0.1),
        heartbeat_seconds=0.02)
    await worker.start()
    await broker.submit(queue="jobs", payload={})  # type: ignore[attr-defined]
    await _wait_for_terminal(broker, acked=1, dead_letters=0)
    await worker.close()
