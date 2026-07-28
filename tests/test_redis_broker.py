"""使用本机 Redis 的 v0.1 生命周期集成测试。"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TypedDict
from uuid import uuid4

import pytest

from taskflow import (
    ConsumerOptions,
    FinishOutcome,
    JsonSerializer,
    LeaseLostError,
    QueueConfig,
    RedisBroker,
    RedisStringDedupSubmissionStore,
    RedisSubmissionStore,
    RejectMessage,
    RetryableError,
    RetryPolicy,
    SerializerRegistry,
    SubmitRequest,
    ValidationError,
)
from tests.support import BinaryJsonSerializer

pytest.importorskip("redis.asyncio")
pytestmark = pytest.mark.redis


@dataclass(frozen=True)
class TypedResize:
    image_id: str
    width: int


class TypedResizeDict(TypedDict):
    image_id: str
    width: int


async def clean(broker: RedisBroker) -> None:
    """只清理本测试随机命名空间下的键，绝不影响其他 Redis 数据。"""

    keys = [key async for key in broker._redis.scan_iter(match=f"{broker._namespace}:*")]
    if keys:
        await broker._redis.unlink(*keys)


@pytest.fixture
async def broker() -> AsyncGenerator[RedisBroker, None]:
    """为每个用例隔离 Redis namespace，并在结束后回收测试键。"""

    instance = RedisBroker.from_url(namespace=f"taskflow-test-{uuid4()}", pending_recovery_seconds=0.0)
    await instance.start()
    try:
        yield instance
    finally:
        await clean(instance)
        await instance.close()


async def receive(broker: RedisBroker):
    """领取一条 jobs 消息。"""

    return await broker.consumer("jobs").__anext__()


async def redis_now(broker: RedisBroker):
    """Create relative timestamps from the backend's authoritative clock."""

    return await broker._now()


@pytest.mark.asyncio
async def test_submit_dedup_retry_ack_and_stats(broker: RedisBroker) -> None:
    """验证精确去重、立即重试和确认统计。"""

    first = await broker.submit(queue="jobs", payload={"id": 1}, dedup_scope="test", dedup_key="one", dedup_ttl=timedelta(minutes=1))
    duplicate = await broker.submit(queue="jobs", payload={"id": 1}, dedup_scope="test", dedup_key="one", dedup_ttl=timedelta(minutes=1))
    assert first.accepted and not duplicate.accepted and duplicate.existing_message_id == first.message_id
    assert await broker._redis.type(broker._queue_key("jobs", "stream")) == "stream"
    delivery = await receive(broker)
    await delivery.retry(reason="temporary")
    assert await broker._redis.zcard(broker._queue_key("jobs", "ready")) == 1
    retried = await receive(broker)
    assert retried.attempt == 2
    await retried.ack()
    assert await broker._redis.zcard(broker._queue_key("jobs", "ready")) == 0
    assert (await broker.inspect("jobs")).acked_total == 1


@pytest.mark.asyncio
async def test_redis_terminal_operations_are_idempotent(broker: RedisBroker) -> None:
    await broker.submit(queue="jobs", payload={})
    acked = await receive(broker)
    assert await acked.ack() is FinishOutcome.ACKED
    assert await acked.ack() is FinishOutcome.IDEMPOTENT

    await broker.submit(queue="jobs", payload={})
    retried = await receive(broker)
    assert await retried.retry(reason="temporary") is FinishOutcome.RETRIED
    assert await retried.retry(reason="temporary") is FinishOutcome.IDEMPOTENT
    await (await receive(broker)).ack()

    await broker.submit(queue="jobs", payload={}, max_attempts=1)
    limited = await receive(broker)
    assert await limited.retry(reason="limit") is FinishOutcome.DEAD_LETTERED
    assert await limited.retry(reason="limit") is FinishOutcome.IDEMPOTENT

    await broker.submit(queue="jobs", payload={})
    current = await receive(broker)
    assert await current.reject(reason="bad") is FinishOutcome.DEAD_LETTERED
    assert await current.reject(reason="bad") is FinishOutcome.IDEMPOTENT


@pytest.mark.asyncio
async def test_v05_redis_health_reports_schema_and_consumer_group(broker: RedisBroker) -> None:
    await broker.submit(queue="jobs", payload={})
    delivery = await broker.consumer("jobs").__anext__()
    await delivery.ack()

    report = await broker.health_check()
    checks = {check.name: check for check in report.checks}
    assert report.healthy
    assert checks["schema_version"].status == "ok"
    assert checks["consumer_groups"].status == "ok"

    await broker._redis.set(broker._schema_key(), "999")
    incompatible = await broker.health_check()
    incompatible_checks = {check.name: check for check in incompatible.checks}
    assert not incompatible.healthy
    assert incompatible_checks["schema_version"].status == "error"


@pytest.mark.asyncio
async def test_v05_redis_health_is_read_only_and_reports_missing_configured_group(broker: RedisBroker, monkeypatch: pytest.MonkeyPatch) -> None:
    async def unexpected_write(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("health check must not initialize Redis")

    monkeypatch.setattr(broker._redis, "setnx", unexpected_write)
    monkeypatch.setattr(broker._redis, "xgroup_create", unexpected_write)
    readonly_report = await broker.health_check()
    assert readonly_report.healthy

    configured = RedisBroker(broker._redis, namespace=broker._namespace, queues={"configured": QueueConfig()})
    report = await configured.health_check()
    checks = {check.name: check for check in report.checks}
    assert not report.healthy
    assert checks["consumer_groups"].status == "error"
    assert "configured" in (checks["consumer_groups"].detail or "")


@pytest.mark.asyncio
async def test_v05_redis_consistency_dry_run_and_repair(broker: RedisBroker) -> None:
    submitted = await broker.submit(queue="jobs", payload={})
    await broker._redis.zrem(broker._queue_key("jobs", "ready"), submitted.message_id)

    report = await broker.check_consistency("jobs")
    assert ("missing_ready_index", submitted.message_id) in {(issue.name, issue.message_id) for issue in report.issues}
    proposed = await broker.repair_consistency("jobs")
    assert proposed.dry_run
    assert await broker._redis.zscore(broker._queue_key("jobs", "ready"), submitted.message_id) is None
    await broker.repair_consistency("jobs", dry_run=False)
    assert (await broker.check_consistency("jobs")).consistent

    fields = await broker._redis.hgetall(broker._message_key(submitted.message_id))
    await broker._redis.xdel(broker._queue_key("jobs", "stream"), fields["entry_id"])
    assert any(issue.name == "missing_stream_entry" for issue in (await broker.check_consistency("jobs")).issues)
    await broker.repair_consistency("jobs", dry_run=False)
    assert (await broker.check_consistency("jobs")).consistent

    repaired_fields = await broker._redis.hgetall(broker._message_key(submitted.message_id))
    await broker._redis.zrem(broker._queue_key("jobs", "ready"), submitted.message_id)
    await broker._redis.xdel(broker._queue_key("jobs", "stream"), repaired_fields["entry_id"])
    lost_all_indexes = await broker.check_consistency("jobs")
    assert {"missing_ready_index", "missing_stream_entry"} <= {issue.name for issue in lost_all_indexes.issues}
    await broker.repair_consistency("jobs", dry_run=False)
    assert (await broker.check_consistency("jobs")).consistent


@pytest.mark.asyncio
async def test_v05_redis_consistency_scans_pel_beyond_first_thousand_entries(broker: RedisBroker) -> None:
    queue = "pel-audit"
    stream = broker._queue_key(queue, "stream")
    await broker._redis.xgroup_create(stream, broker._group_name(), id="0", mkstream=True)
    total = 1_002
    for index in range(total):
        await broker._redis.xadd(stream, {"message_id": f"raw-{index}", "envelope": "unused"})
    received = await broker._redis.xreadgroup(
        broker._group_name(), "audit", {stream: ">"}, count=total,
    )
    entries = received[0][1]
    stale_entry_id = entries[-2][0]
    orphan_entry_id = entries[-1][0]
    stale_message_id = "raw-1000"
    await broker._redis.hset(broker._message_key(stale_message_id), mapping={
        "queue": queue, "status": "ready", "entry_id": stale_entry_id,
    })
    await broker._redis.xdel(stream, orphan_entry_id)

    report = await broker.check_consistency(queue)
    assert ("stale_pel", stale_message_id, stale_entry_id) in {
        (issue.name, issue.message_id, issue.detail) for issue in report.issues
    }
    assert ("orphan_pel", None, orphan_entry_id) in {
        (issue.name, issue.message_id, issue.detail) for issue in report.issues
    }



@pytest.mark.asyncio
async def test_v05_redis_cleanup_deprecated_keys_is_explicit(broker: RedisBroker) -> None:
    legacy = f"{broker._namespace}:legacy:old-message"
    await broker._redis.set(legacy, "obsolete")
    assert await broker.cleanup_deprecated_keys() == (legacy,)
    assert await broker._redis.exists(legacy)
    assert await broker.cleanup_deprecated_keys(dry_run=False) == (legacy,)
    assert not await broker._redis.exists(legacy)


@pytest.mark.asyncio
async def test_v05_redis_reads_legacy_message_without_serializer_identity(broker: RedisBroker) -> None:
    submitted = await broker.submit(queue="jobs", payload={"legacy": True})
    await broker._redis.hdel(broker._message_key(submitted.message_id), "serializer_name", "serializer_version")
    inspected = await broker.inspect_message(submitted.message_id)
    assert inspected is not None and inspected.payload == {"legacy": True}
    report = await broker.health_check()
    checks = {check.name: check for check in report.checks}
    assert report.healthy
    assert checks["serializer_registry"].status == "ok"
    assert checks["legacy_serializer_identity"].status == "warning"
    assert checks["unrecoverable_errors"].status == "ok"


@pytest.mark.asyncio
async def test_v04_redis_typed_payloads_worker_poison_and_replay(broker: RedisBroker) -> None:
    received: list[TypedResizeDict] = []
    completed = asyncio.Event()

    async def handler(message) -> None:  # type: ignore[no-untyped-def]
        received.append(message.payload)
        completed.set()

    worker = broker.worker("jobs", handler, payload_type=TypedResizeDict,
                           options=ConsumerOptions(lease_seconds=1, poll_interval=0.001))
    await worker.start()
    submitted = await broker.submit(queue="jobs", payload={"image_id": "ok", "width": 10},
                                    payload_type=TypedResizeDict)
    await asyncio.wait_for(completed.wait(), timeout=1)
    await worker.close()
    assert received == [{"image_id": "ok", "width": 10}]

    invalid = await broker.submit(queue="jobs", payload={"image_id": "bad", "width": "10"})
    poison_worker = broker.worker("jobs", handler, payload_type=TypedResizeDict,
                                  options=ConsumerOptions(lease_seconds=1, poll_interval=0.001))
    await poison_worker.start()
    for _ in range(200):
        if (await broker.inspect("jobs")).dead_letters == 1:
            break
        await asyncio.sleep(0.005)
    await poison_worker.close()
    assert (await broker.admin.list_dead_letters("jobs"))[0].message.id == invalid.message_id

    await broker.admin.replay_dead_letter(
        "jobs", invalid.message_id, payload=TypedResize("repaired", 20),
        dedup_mode="remove",
    )
    replayed = await broker.inspect_message(invalid.message_id)
    assert replayed is not None
    assert replayed.payload == {"image_id": "repaired", "width": 20}
    assert replayed.payload_schema_name == f"{TypedResize.__module__}.{TypedResize.__qualname__}"
    assert submitted.accepted


@pytest.mark.asyncio
async def test_v04_redis_batch_modes_are_atomic_or_per_item(broker: RedisBroker) -> None:
    atomic = await broker.submit_many([
        SubmitRequest(queue="jobs", payload={"n": 1}),
        SubmitRequest(queue="jobs", payload={"n": 2}),
    ])
    assert [item.accepted for item in atomic] == [True, True]

    items = await broker.submit_many([
        SubmitRequest(queue="jobs", payload={"n": 3}),
        SubmitRequest(queue="bad queue!", payload={"n": 4}),
        SubmitRequest(queue="jobs", payload={"n": 5}),
    ], atomic=False)
    assert [item.index for item in items] == [0, 1, 2]
    assert items[0].result is not None and items[0].result.accepted
    assert isinstance(items[1].error, ValidationError)
    assert items[2].result is not None and items[2].result.accepted


@pytest.mark.asyncio
async def test_v03_replay_none_compatibility_and_explicit_null_override(broker: RedisBroker) -> None:
    submitted = await broker.submit(queue="jobs", payload={"kept": True})
    delivery = await receive(broker)
    await delivery.reject(reason="repair")

    await broker.admin.replay_dead_letter("jobs", submitted.message_id, payload=None, dedup_mode="remove")
    preserved = await broker.inspect_message(submitted.message_id)
    assert preserved is not None and preserved.payload == {"kept": True}

    delivery = await receive(broker)
    await delivery.reject(reason="repair again")
    await broker.admin.replay_dead_letter(
        "jobs", submitted.message_id, payload=None, replace_payload=True, dedup_mode="remove",
    )
    replaced = await broker.inspect_message(submitted.message_id)
    assert replaced is not None and replaced.payload is None


@pytest.mark.asyncio
async def test_pydantic_typed_worker_success_and_poison_path(broker: RedisBroker) -> None:
    pydantic = pytest.importorskip("pydantic")
    Nested = pydantic.create_model("RedisNested", value=(int, ...))
    Model = pydantic.create_model(
        "RedisImageJob", image_id=(str, ...), created_at=(datetime, ...), nested=(Nested, ...), note=(str | None, None),
    )
    WrongVersion = pydantic.create_model(
        "RedisImageJob", image_id=(str, ...), created_at=(datetime, ...), nested=(Nested, ...), note=(str | None, None),
    )
    WrongVersion.__taskflow_schema_version__ = "2"
    handled = asyncio.Event()

    async def handler(message) -> None:  # type: ignore[no-untyped-def]
        assert isinstance(message.payload, Model)
        handled.set()

    worker = broker.worker("jobs", handler, payload_type=Model,
                           options=ConsumerOptions(lease_seconds=1, poll_interval=0.001))
    await worker.start()
    await broker.submit(queue="jobs", payload=Model(
        image_id="ok", created_at=datetime(2026, 1, 1, tzinfo=timezone.utc), nested={"value": 1}, note=None,
    ))
    await asyncio.wait_for(handled.wait(), timeout=1)
    await worker.close()

    poison_worker = broker.worker("jobs", handler, payload_type=WrongVersion,
                                  options=ConsumerOptions(lease_seconds=1, poll_interval=0.001))
    await poison_worker.start()
    await broker.submit(queue="jobs", payload=Model(
        image_id="bad", created_at=datetime(2026, 1, 1, tzinfo=timezone.utc), nested={"value": 2}, note=None,
    ))
    for _ in range(200):
        if (await broker.inspect("jobs")).dead_letters == 1:
            break
        await asyncio.sleep(0.005)
    await poison_worker.close()
    assert (await broker.admin.list_dead_letters("jobs"))[0].reason == "poison_payload"


@pytest.mark.asyncio
async def test_delayed_submit_and_retry_are_not_claimed_early(broker: RedisBroker) -> None:
    submitted = await broker.submit(queue="jobs", payload={}, delay=timedelta(milliseconds=40))
    assert (await broker.inspect("jobs")).delayed == 1
    assert await broker._redis.hget(broker._message_key(submitted.message_id), "status") == "delayed"
    await asyncio.sleep(0.05)
    delivery = await receive(broker)
    assert delivery.message.id == submitted.message_id
    await delivery.retry(reason="temporary", delay=timedelta(milliseconds=40))
    assert (await broker.inspect("jobs")).delayed == 1
    await asyncio.sleep(0.05)
    retried = await receive(broker)
    assert retried.attempt == 2
    await retried.ack()


@pytest.mark.asyncio
async def test_delayed_message_survives_redis_restart() -> None:
    namespace = f"taskflow-delayed-restart-{uuid4()}"
    first = RedisBroker.from_url(namespace=namespace)
    await first.start()
    try:
        submitted = await first.submit(queue="jobs", payload={}, delay=timedelta(milliseconds=40))
        await first.close()
        restarted = RedisBroker.from_url(namespace=namespace)
        await restarted.start()
        try:
            await asyncio.sleep(0.05)
            delivery = await receive(restarted)
            assert delivery.message.id == submitted.message_id
            await delivery.ack()
        finally:
            await clean(restarted)
            await restarted.close()
    finally:
        await first.close()


@pytest.mark.asyncio
async def test_redis_worker_concurrency_policy_and_heartbeat(broker: RedisBroker) -> None:
    active = 0
    maximum = 0
    retry_calls = 0

    async def handler(message) -> None:
        nonlocal active, maximum, retry_calls
        active += 1
        maximum = max(maximum, active)
        kind = message.payload["kind"]
        if kind == "retry" and retry_calls == 0:
            retry_calls += 1
            active -= 1
            raise RetryableError("temporary")
        if kind == "reject":
            active -= 1
            raise RejectMessage("invalid")
        if kind == "long":
            await asyncio.sleep(0.25)
        active -= 1

    worker = broker.worker(
        "jobs", handler, concurrency=2,
        options=ConsumerOptions(lease_seconds=0.1), heartbeat_seconds=0.02,
        retry_policy=RetryPolicy.fixed(delay=0.01, max_attempts=3),
    )
    await worker.start()
    for kind in ("retry", "reject", "long"):
        await broker.submit(queue="jobs", payload={"kind": kind})
    stats = await broker.inspect("jobs")
    for _ in range(200):
        stats = await broker.inspect("jobs")
        if stats.acked_total == 2 and stats.dead_letters == 1:
            break
        await asyncio.sleep(0.005)
    await worker.close()
    letters = await broker.admin.list_dead_letters("jobs")
    assert maximum <= 2 and retry_calls == 1
    assert stats.acked_total == 2 and len(letters) == 1
    assert letters[0].source == "reject"


@pytest.mark.asyncio
async def test_redis_worker_cancellation_reclaims_delivery(broker: RedisBroker) -> None:
    started = asyncio.Event()

    async def handler(_message) -> None:
        started.set()
        await asyncio.Event().wait()

    runner = asyncio.create_task(
        broker.run("jobs", handler, options=ConsumerOptions(lease_seconds=0.01)))
    await broker.submit(queue="jobs", payload={})
    await started.wait()
    runner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await runner
    await asyncio.sleep(0.02)
    recovered = await receive(broker)
    assert recovered.attempt == 2
    await recovered.ack()


@pytest.mark.asyncio
async def test_redis_delayed_message_expiring_before_due_goes_to_eq(broker: RedisBroker) -> None:
    now = await redis_now(broker)
    submitted = await broker.submit(queue="jobs", payload={}, delay=timedelta(milliseconds=40),
                                    expires_at=now + timedelta(milliseconds=10))
    await asyncio.sleep(0.05)
    stats = await broker.inspect("jobs")
    assert stats.ready == 0 and stats.delayed == 0 and stats.expired == 1
    assert any(item.message.id == submitted.message_id
               for item in await broker.admin.list_expired("jobs"))


@pytest.mark.asyncio
@pytest.mark.parametrize("ttl", [timedelta(), timedelta(milliseconds=-1)])
async def test_explicit_invalid_dedup_ttl_never_falls_back_to_default(broker: RedisBroker, ttl: timedelta) -> None:
    broker._default_dedup_ttl = timedelta(hours=1)
    with pytest.raises(ValidationError):
        await broker.submit(queue="jobs", payload={}, dedup_scope="s", dedup_key="x", dedup_ttl=ttl)


@pytest.mark.asyncio
async def test_binary_serializer_identity_and_ready_index(broker: RedisBroker) -> None:
    broker._serializer = BinaryJsonSerializer()
    submitted = await broker.submit(queue="jobs", payload={"binary": True})
    state = await broker._redis.hgetall(broker._message_key(submitted.message_id))
    assert (state["serializer_name"], state["serializer_version"]) == ("binary-json", "7")
    assert await broker._redis.zcard(broker._queue_key("jobs", "ready")) == 1
    stats = await broker.inspect("jobs")
    assert stats.ready == 1 and stats.earliest_ready_at is not None
    delivery = await receive(broker)
    assert delivery.message.payload == {"binary": True}
    await delivery.ack()


@pytest.mark.asyncio
async def test_redis_status_indexes_obey_lifecycle_invariants(broker: RedisBroker) -> None:
    submitted = await broker.submit(queue="jobs", payload={})
    message_key = broker._message_key(submitted.message_id)
    stream = broker._queue_key("jobs", "stream")
    ready = broker._queue_key("jobs", "ready")
    leases = broker._queue_key("jobs", "leases")
    assert (await broker._redis.hget(message_key, "status")) == "ready"
    assert await broker._redis.zscore(ready, submitted.message_id) is not None
    assert any(fields["message_id"] == submitted.message_id for _, fields in await broker._redis.xrange(stream))
    delivery = await receive(broker)
    assert (await broker._redis.hget(message_key, "status")) == "leased"
    assert await broker._redis.zscore(ready, submitted.message_id) is None
    assert await broker._redis.zscore(leases, submitted.message_id) is not None
    await delivery.reject(reason="bad")
    assert (await broker._redis.hget(message_key, "status")) == "dead_lettered"
    assert submitted.message_id in await broker._redis.lrange(broker._queue_key("jobs", "dlq"), 0, -1)


@pytest.mark.asyncio
async def test_reject_lease_reclaim_and_expiry(broker: RedisBroker) -> None:
    """验证 DLQ、租约回收和 EQ。"""

    rejected = await broker.submit(queue="jobs", payload={"kind": "reject"})
    await (await receive(broker)).reject(reason="invalid")
    assert (await broker.admin.list_dead_letters("jobs"))[0].message.id == rejected.message_id

    await broker.submit(queue="jobs", payload={"kind": "lease"}, max_attempts=1)
    leased = await broker.consumer("jobs", options=ConsumerOptions(lease_seconds=0.001)).__anext__()
    await asyncio.sleep(0.01)
    await broker.maintain("jobs")
    assert any(item.message.id == leased.message.id for item in await broker.admin.list_dead_letters("jobs"))

    expired = await broker.submit(queue="jobs", payload={"kind": "expired"},
                                  expires_at=await redis_now(broker) - timedelta(seconds=1))
    assert any(item.message.id == expired.message_id for item in await broker.admin.list_expired("jobs"))


@pytest.mark.asyncio
async def test_stale_lease_cannot_ack_and_expired_message_can_replay(broker: RedisBroker) -> None:
    """回收后的旧 delivery 无权终结消息，EQ 消息可按新策略重放。"""

    await broker.submit(queue="jobs", payload={"kind": "lease"})
    old = await broker.consumer("jobs", options=ConsumerOptions(lease_seconds=0.001)).__anext__()
    await asyncio.sleep(0.01)
    current = await receive(broker)
    with pytest.raises(LeaseLostError):
        await old.ack()
    await current.ack()

    result = await broker.submit(queue="jobs", payload={"kind": "expiry"},
                                 expires_at=await redis_now(broker) - timedelta(seconds=1))
    await broker.admin.replay_expired("jobs", result.message_id, expires_at=None)
    replayed = await receive(broker)
    assert replayed.message.id == result.message_id


@pytest.mark.asyncio
async def test_replay_can_remove_or_replace_dedup_without_losing_dlq(broker: RedisBroker) -> None:
    first = await broker.submit(queue="jobs", payload={}, dedup_scope="old", dedup_key="one", dedup_ttl=timedelta(minutes=1))
    await (await receive(broker)).reject(reason="repair")
    await broker.admin.replay_dead_letter("jobs", first.message_id, reuse_dedup=False)
    assert (await broker.submit(queue="jobs", payload={}, dedup_scope="old", dedup_key="one", dedup_ttl=timedelta(minutes=1))).accepted

    replayed = await receive(broker)
    await replayed.reject(reason="replace")
    taken = await broker.submit(queue="jobs", payload={}, dedup_scope="replace", dedup_key="taken", dedup_ttl=timedelta(minutes=1))
    with pytest.raises(ValidationError, match="其他消息"):
        await broker.admin.replay_dead_letter("jobs", first.message_id, reuse_dedup=False,
            dedup_scope="replace", dedup_key="taken", dedup_ttl=timedelta(minutes=1))
    assert any(letter.message.id == first.message_id for letter in await broker.admin.list_dead_letters("jobs"))
    assert taken.accepted


@pytest.mark.asyncio
async def test_pel_gap_is_recovered_after_consumer_crash(broker: RedisBroker) -> None:
    """XREADGROUP 后、状态领取 Lua 前崩溃的消息必须重新进入可消费 Stream。"""

    submitted = await broker.submit(queue="jobs", payload={"kind": "pel-gap"})
    await broker._ensure_group("jobs")
    stream = broker._queue_key("jobs", "stream")
    received = await broker._redis.xreadgroup(broker._group_name(), "crashed-worker", {stream: ">"}, count=1)
    assert received
    await broker.maintain("jobs")
    recovered = await receive(broker)
    assert recovered.message.id == submitted.message_id
    await recovered.ack()


@pytest.mark.asyncio
async def test_expired_lease_cannot_be_terminated_or_extended(broker: RedisBroker) -> None:
    """维护循环尚未来得及执行时，失效租约也不能终结或续租。"""

    await broker.submit(queue="jobs", payload={"kind": "expired-lease"})
    delivery = await broker.consumer("jobs", options=ConsumerOptions(lease_seconds=0.001)).__anext__()
    await asyncio.sleep(0.01)
    with pytest.raises(LeaseLostError):
        await delivery.ack()
    with pytest.raises(LeaseLostError):
        await delivery.extend_lease(seconds=1)


@pytest.mark.asyncio
async def test_extend_lease_is_capped_by_message_expiry(broker: RedisBroker) -> None:
    await broker.submit(queue="jobs", payload={}, expires_at=await redis_now(broker) + timedelta(seconds=30))
    delivery = await broker.consumer("jobs", options=ConsumerOptions(lease_seconds=1)).__anext__()
    extended = await delivery.extend_lease(seconds=300)
    assert delivery.message.expires_at is not None
    assert extended <= delivery.message.expires_at


@pytest.mark.asyncio
async def test_initial_expiry_and_expired_ack_keep_indexes_and_stats_consistent(broker: RedisBroker) -> None:
    initial = await broker.submit(queue="jobs", payload={}, expires_at=await redis_now(broker) - timedelta(seconds=1))
    assert await broker._redis.zscore(broker._queue_key("jobs", "expiry"), initial.message_id) is None

    active = await broker.submit(queue="jobs", payload={}, expires_at=await redis_now(broker) + timedelta(seconds=1))
    delivery = await broker.consumer("jobs", options=ConsumerOptions(lease_seconds=2)).__anext__()
    assert delivery.message.id == active.message_id
    await asyncio.sleep(1.05)
    await delivery.ack()
    state = await broker._redis.hgetall(broker._message_key(active.message_id))
    assert state["status"] == "expired"
    assert int((await broker.inspect("jobs")).acked_total) == 0
    assert all(field not in state for field in ("consumer_id", "delivery_id", "lease_token", "claimed_at", "lease_until"))


@pytest.mark.asyncio
async def test_claim_time_expiry_publishes_one_event_and_metric(broker: RedisBroker, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    class Metrics:
        def __init__(self) -> None:
            self.increments: list[str] = []

        async def increment(self, name, value=1, **labels):  # type: ignore[no-untyped-def]
            self.increments.append(name)

        async def observe(self, name, value, **labels):  # type: ignore[no-untyped-def]
            pass

    class Sink:
        def __init__(self) -> None:
            self.events: list[object] = []

        async def emit(self, event) -> None:  # type: ignore[no-untyped-def]
            self.events.append(event)

    async def skip_maintenance(_queue=None) -> int:  # type: ignore[no-untyped-def]
        return 0

    metrics, sink = Metrics(), Sink()
    broker.metrics, broker.event_sink = metrics, sink
    submitted = await broker.submit(queue="jobs", payload={}, expires_at=await redis_now(broker) + timedelta(milliseconds=50))
    await asyncio.sleep(0.06)
    monkeypatch.setattr(broker, "maintain", skip_maintenance)
    assert await broker._claim("jobs", "claim-expiry", 10) is None
    assert [item.message.id for item in await broker.admin.list_expired("jobs")] == [submitted.message_id]
    assert metrics.increments.count("expired_total") == 1
    assert [item.event_name for item in sink.events].count("expired") == 1  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_extend_lease_expiry_commits_eq_transition_and_observability(broker: RedisBroker) -> None:
    class Metrics:
        def __init__(self) -> None:
            self.increments: list[str] = []

        async def increment(self, name, value=1, **labels):  # type: ignore[no-untyped-def]
            self.increments.append(name)

        async def observe(self, name, value, **labels):  # type: ignore[no-untyped-def]
            pass

    class Sink:
        def __init__(self) -> None:
            self.events: list[object] = []

        async def emit(self, event) -> None:  # type: ignore[no-untyped-def]
            self.events.append(event)

    metrics, sink = Metrics(), Sink()
    broker.metrics, broker.event_sink = metrics, sink
    submitted = await broker.submit(queue="jobs", payload={}, expires_at=await redis_now(broker) + timedelta(milliseconds=50))
    delivery = await broker.consumer("jobs", options=ConsumerOptions(lease_seconds=10)).__anext__()
    await asyncio.sleep(0.06)
    with pytest.raises(LeaseLostError, match="过期"):
        await delivery.extend_lease(seconds=1)
    assert [item.message.id for item in await broker.admin.list_expired("jobs")] == [submitted.message_id]
    assert metrics.increments.count("expired_total") == 1
    assert [item.event_name for item in sink.events].count("expired") == 1  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_queue_profiles_route_submissions_and_reject_mixed_batches() -> None:
    namespace = f"taskflow-profile-{uuid4()}"
    instance = RedisBroker.from_url(
        namespace=namespace,
        submission_stores={
            "default": lambda broker: RedisSubmissionStore(broker),
            "exact": lambda broker: RedisStringDedupSubmissionStore(broker),
        },
        queue_submission_profiles={"exact-jobs": "exact"},
    )
    await instance.start()
    try:
        assert instance.submission_capabilities("plain-jobs").dedup_guarantee.value == "none"
        assert instance.submission_capabilities("exact-jobs").dedup_guarantee.value == "exact"
        assert instance.submission_capabilities("exact-jobs").batch_submit
        assert instance.submission_capabilities("exact-jobs").batch_atomic
        results = await instance.submit_many([
            SubmitRequest(queue="exact-jobs", payload={"n": 1}, dedup_scope="s", dedup_key="same", dedup_ttl=timedelta(minutes=1)),
            SubmitRequest(queue="exact-jobs", payload={"n": 3}, dedup_scope="s", dedup_key="same", dedup_ttl=timedelta(minutes=1)),
        ])
        assert [result.accepted for result in results] == [True, False]
        with pytest.raises(ValidationError, match="混合"):
            await instance.submit_many([
                SubmitRequest(queue="exact-jobs", payload={"n": 1}),
                SubmitRequest(queue="plain-jobs", payload={"n": 2}),
            ])
        assert (await instance.inspect("plain-jobs")).submitted_total == 0
    finally:
        await clean(instance)
        await instance.close()


@pytest.mark.asyncio
async def test_redis_maintenance_emits_expiry_reclaim_and_dlq_events(broker: RedisBroker) -> None:
    class Sink:
        def __init__(self) -> None:
            self.events: list[object] = []

        async def emit(self, event) -> None:  # type: ignore[no-untyped-def]
            self.events.append(event)

    sink = Sink()
    broker.event_sink = sink
    await broker.submit(queue="jobs", payload={}, delay=timedelta(milliseconds=40),
                        expires_at=await redis_now(broker) + timedelta(milliseconds=10))
    await asyncio.sleep(0.05)
    await broker.maintain("jobs")

    await broker.submit(queue="jobs", payload={})
    await broker.consumer("jobs", options=ConsumerOptions(lease_seconds=0.01)).__anext__()
    await asyncio.sleep(0.02)
    await broker.maintain("jobs")
    await (await broker.consumer("jobs").__anext__()).ack()

    await broker.submit(queue="jobs", payload={}, max_attempts=1)
    await broker.consumer("jobs", options=ConsumerOptions(lease_seconds=0.01)).__anext__()
    await asyncio.sleep(0.02)
    await broker.maintain("jobs")

    names = [event.event_name for event in sink.events]  # type: ignore[attr-defined]
    assert "expired" in names and "reclaimed" in names and "dead_lettered" in names
    assert all(event.backend == "redis" for event in sink.events)  # type: ignore[attr-defined]


def test_redis_namespace_must_be_a_safe_persistent_identifier() -> None:
    class UnusedRedis:
        pass

    for namespace in ("", "with:colon", "has{tag}", "中文", "x" * 256):
        with pytest.raises(ValidationError, match="namespace"):
            RedisBroker(UnusedRedis(), namespace=namespace)

    assert RedisBroker(UnusedRedis(), namespace="_legacy", allow_legacy_names=True)._namespace == "_legacy"
    assert RedisBroker(UnusedRedis(), consistency_pel_page_size=17)._consistency_pel_page_size == 17
    with pytest.raises(ValidationError, match="consistency_pel_page_size"):
        RedisBroker(UnusedRedis(), consistency_pel_page_size=0)


@pytest.mark.asyncio
async def test_redis_serializer_registry_decodes_historical_message() -> None:
    namespace = f"taskflow-registry-{uuid4()}"
    writer = RedisBroker.from_url(namespace=namespace, serializer=BinaryJsonSerializer())
    await writer.start()
    try:
        await writer.submit(queue="jobs", payload={"binary": True})
        reader = RedisBroker.from_url(
            namespace=namespace,
            serializer=JsonSerializer(),
            serializer_registry=SerializerRegistry([BinaryJsonSerializer()]),
        )
        await reader.start()
        try:
            assert (await receive(reader)).message.payload == {"binary": True}
        finally:
            await reader.close()
    finally:
        await clean(writer)
        await writer.close()
