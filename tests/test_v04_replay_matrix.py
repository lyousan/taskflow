"""v0.4 DLQ/EQ replay dedup、target queue 与冲突回滚的跨 backend 矩阵。"""
from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from taskflow import RedisBroker, SQLiteBroker, ValidationError
from taskflow.types import utc_now

Broker = SQLiteBroker | RedisBroker


@pytest.fixture(params=("sqlite", "redis"))
async def broker(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator[Broker]:
    if request.param == "sqlite":
        async with SQLiteBroker(tmp_path / "replay-v04.db") as sqlite_broker:
            yield sqlite_broker
        return
    pytest.importorskip("redis.asyncio")
    namespace = f"taskflow-replay-v04-{uuid4()}"
    redis_broker = RedisBroker.from_url(namespace=namespace)
    await redis_broker.start()
    try:
        yield redis_broker
    finally:
        await redis_broker.close()
        from redis.asyncio import Redis

        cleanup = Redis.from_url("redis://127.0.0.1:6379/2", decode_responses=True)
        try:
            keys = [key async for key in cleanup.scan_iter(match=f"{namespace}:*")]
            if keys:
                await cleanup.delete(*keys)
        finally:
            await cleanup.aclose()


async def _replay_dead_letter(broker: Broker, message_id: str, mode: str, *, target_queue: str | None = None) -> None:
    if mode == "replace":
        await broker.admin.replay_dead_letter(
            "jobs", message_id, target_queue=target_queue, dedup_mode="replace",
            dedup_scope="replacement", dedup_key="new", dedup_ttl=timedelta(minutes=1),
        )
        return
    await broker.admin.replay_dead_letter("jobs", message_id, target_queue=target_queue, dedup_mode=mode)


async def _replay_expired(broker: Broker, message_id: str, mode: str, *, target_queue: str | None = None) -> None:
    if mode == "replace":
        await broker.admin.replay_expired(
            "jobs", message_id, expires_at=None, target_queue=target_queue, dedup_mode="replace",
            dedup_scope="replacement", dedup_key="new", dedup_ttl=timedelta(minutes=1),
        )
        return
    await broker.admin.replay_expired("jobs", message_id, expires_at=None, target_queue=target_queue, dedup_mode=mode)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ("keep", "remove", "replace"))
async def test_dlq_replay_dedup_modes_target_queue_and_repeat(broker: Broker, mode: str) -> None:
    first = await broker.submit(queue="jobs", payload={"id": 1}, dedup_scope="original", dedup_key="old",
                                dedup_ttl=timedelta(minutes=1))
    delivery = await broker.consumer("jobs").__anext__()
    await delivery.reject(reason="repair")

    await _replay_dead_letter(broker, first.message_id, mode, target_queue="repairs")
    replayed = await broker.inspect_message(first.message_id)
    assert replayed is not None and replayed.queue == "repairs"
    with pytest.raises(ValidationError):
        await _replay_dead_letter(broker, first.message_id, mode, target_queue="repairs")

    original = await broker.submit(queue="jobs", payload={"id": "old"}, dedup_scope="original", dedup_key="old",
                                   dedup_ttl=timedelta(minutes=1))
    replacement = await broker.submit(queue="jobs", payload={"id": "new"}, dedup_scope="replacement", dedup_key="new",
                                      dedup_ttl=timedelta(minutes=1))
    if mode == "keep":
        assert not original.accepted and original.existing_message_id == first.message_id
        assert replacement.accepted
    elif mode == "remove":
        assert original.accepted and replacement.accepted
    else:
        assert original.accepted
        assert not replacement.accepted and replacement.existing_message_id == first.message_id


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ("keep", "remove", "replace"))
async def test_eq_replay_dedup_modes_and_target_queue(broker: Broker, mode: str) -> None:
    first = await broker.submit(queue="jobs", payload={"id": 1}, expires_at=utc_now() - timedelta(seconds=1),
                                dedup_scope="original", dedup_key="old", dedup_ttl=timedelta(minutes=1))
    await _replay_expired(broker, first.message_id, mode, target_queue="repairs")
    replayed = await broker.inspect_message(first.message_id)
    assert replayed is not None and replayed.queue == "repairs" and replayed.expires_at is None
    with pytest.raises(ValidationError):
        await _replay_expired(broker, first.message_id, mode)

    original = await broker.submit(queue="jobs", payload={"id": "old"}, dedup_scope="original", dedup_key="old",
                                   dedup_ttl=timedelta(minutes=1))
    replacement = await broker.submit(queue="jobs", payload={"id": "new"}, dedup_scope="replacement", dedup_key="new",
                                      dedup_ttl=timedelta(minutes=1))
    if mode == "keep":
        assert not original.accepted and replacement.accepted
    elif mode == "remove":
        assert original.accepted and replacement.accepted
    else:
        assert original.accepted and not replacement.accepted


@pytest.mark.asyncio
@pytest.mark.parametrize("expired", (False, True))
async def test_replay_dedup_conflict_keeps_original_audit_record(broker: Broker, expired: bool) -> None:
    if expired:
        first = await broker.submit(queue="jobs", payload={"id": 1}, expires_at=utc_now() - timedelta(seconds=1))
    else:
        first = await broker.submit(queue="jobs", payload={"id": 1})
    if expired:
        blocker = await broker.submit(queue="jobs", payload={"id": 2}, dedup_scope="conflict", dedup_key="key",
                                      dedup_ttl=timedelta(minutes=1))
        with pytest.raises(ValidationError):
            await broker.admin.replay_expired("jobs", first.message_id, expires_at=None, dedup_mode="replace",
                                              dedup_scope="conflict", dedup_key="key", dedup_ttl=timedelta(minutes=1))
        assert blocker.accepted
        assert any(item.message.id == first.message_id for item in await broker.admin.list_expired("jobs"))
        return

    delivery = await broker.consumer("jobs").__anext__()
    await delivery.reject(reason="repair")
    blocker = await broker.submit(queue="jobs", payload={"id": 2}, dedup_scope="conflict", dedup_key="key",
                                  dedup_ttl=timedelta(minutes=1))
    with pytest.raises(ValidationError):
        await broker.admin.replay_dead_letter("jobs", first.message_id, dedup_mode="replace",
                                              dedup_scope="conflict", dedup_key="key", dedup_ttl=timedelta(minutes=1))
    assert blocker.accepted
    assert any(item.message.id == first.message_id for item in await broker.admin.list_dead_letters("jobs"))
