"""v0.5 crash-window and unavailable-backend fault exercises."""
from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path
from uuid import uuid4

import pytest
from typing_extensions import Self

from taskflow import ConsumerOptions, RedisBroker, SQLiteBroker


@pytest.mark.asyncio
async def test_sqlite_simulated_process_interruption_recovers_unacked_lease(tmp_path: Path) -> None:
    database = tmp_path / "interrupted.db"
    first = SQLiteBroker(database)
    await first.start()
    await first.submit(queue="jobs", payload={"id": 1})
    await first.consumer("jobs", options=ConsumerOptions(lease_seconds=0.01)).__anext__()
    await first.close()  # Equivalent persisted state to a process dying before ACK.
    await asyncio.sleep(0.02)
    async with SQLiteBroker(database) as restarted:
        recovered = await restarted.consumer("jobs", options=ConsumerOptions(lease_seconds=1)).__anext__()
        assert recovered.message.payload == {"id": 1}
        await recovered.ack()


class _UnavailableRedis:
    async def ping(self) -> bool:
        raise ConnectionError("connection refused")


@pytest.mark.asyncio
async def test_redis_health_reports_unavailable_backend() -> None:
    report = await RedisBroker(_UnavailableRedis(), namespace="unavailable").health_check()
    assert not report.healthy
    assert report.checks[0].name == "connection"
    assert "ConnectionError" in (report.checks[0].detail or "")


def test_cli_reports_unavailable_redis_without_traceback(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    from taskflow import cli

    class UnavailableBroker:
        async def __aenter__(self) -> Self:
            raise ConnectionError("connection refused")

        async def __aexit__(self, *_: object) -> None:
            pass

    monkeypatch.setattr(cli.RedisBroker, "from_url", lambda *_args, **_kwargs: UnavailableBroker())
    assert cli.main(["--redis-url", "redis://unavailable/2", "health"]) == 1
    assert "backend unavailable: ConnectionError: connection refused" in capsys.readouterr().err


@pytest.fixture
async def redis_broker() -> AsyncGenerator[RedisBroker, None]:
    pytest.importorskip("redis.asyncio")
    namespace = f"taskflow-fault-{uuid4()}"
    broker = RedisBroker.from_url(namespace=namespace, pending_recovery_seconds=0.0)
    await broker.start()
    try:
        yield broker
    finally:
        keys = [key async for key in broker._redis.scan_iter(match=f"{namespace}:*")]
        if keys:
            await broker._redis.unlink(*keys)
        await broker.close()


@pytest.mark.asyncio
@pytest.mark.redis
async def test_redis_lua_disconnect_before_and_after_ack_is_observable(redis_broker: RedisBroker, monkeypatch: pytest.MonkeyPatch) -> None:
    before = await redis_broker.submit(queue="jobs", payload={"case": "before"})
    delivery = await redis_broker.consumer("jobs").__anext__()
    original_eval = redis_broker._redis.eval

    async def disconnect_before(*_args: object) -> object:
        raise ConnectionError("disconnect before Lua")

    monkeypatch.setattr(redis_broker._redis, "eval", disconnect_before)
    with pytest.raises(ConnectionError, match="before Lua"):
        await delivery.ack()
    monkeypatch.setattr(redis_broker._redis, "eval", original_eval)
    assert (await redis_broker._redis.hget(redis_broker._message_key(before.message_id), "status")) == "leased"

    after = await redis_broker.submit(queue="jobs", payload={"case": "after"})
    second = await redis_broker.consumer("jobs").__anext__()

    async def disconnect_after(*args: object) -> object:
        await original_eval(*args)
        raise ConnectionError("disconnect after Lua")

    monkeypatch.setattr(redis_broker._redis, "eval", disconnect_after)
    with pytest.raises(ConnectionError, match="after Lua"):
        await second.ack()
    monkeypatch.setattr(redis_broker._redis, "eval", original_eval)
    assert (await redis_broker._redis.hget(redis_broker._message_key(after.message_id), "status")) == "acked"
