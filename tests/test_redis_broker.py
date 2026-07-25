"""使用本机 Redis 的 v0.1 生命周期集成测试。"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from datetime import timedelta
from uuid import uuid4

import pytest

from taskflow import (
    ConsumerOptions,
    FinishOutcome,
    JsonSerializer,
    LeaseLostError,
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
from taskflow.types import utc_now
from tests.support import BinaryJsonSerializer

pytest.importorskip("redis.asyncio")


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
    submitted = await broker.submit(queue="jobs", payload={}, delay=timedelta(milliseconds=40),
                                    expires_at=utc_now() + timedelta(milliseconds=10))
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

    expired = await broker.submit(queue="jobs", payload={"kind": "expired"}, expires_at=utc_now() - timedelta(seconds=1))
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

    result = await broker.submit(queue="jobs", payload={"kind": "expiry"}, expires_at=utc_now() - timedelta(seconds=1))
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
    await broker.submit(queue="jobs", payload={}, expires_at=utc_now() + timedelta(seconds=30))
    delivery = await broker.consumer("jobs", options=ConsumerOptions(lease_seconds=1)).__anext__()
    extended = await delivery.extend_lease(seconds=300)
    assert delivery.message.expires_at is not None
    assert extended <= delivery.message.expires_at


@pytest.mark.asyncio
async def test_initial_expiry_and_expired_ack_keep_indexes_and_stats_consistent(broker: RedisBroker) -> None:
    initial = await broker.submit(queue="jobs", payload={}, expires_at=utc_now() - timedelta(seconds=1))
    assert await broker._redis.zscore(broker._queue_key("jobs", "expiry"), initial.message_id) is None

    active = await broker.submit(queue="jobs", payload={}, expires_at=utc_now() + timedelta(seconds=1))
    delivery = await broker.consumer("jobs", options=ConsumerOptions(lease_seconds=2)).__anext__()
    assert delivery.message.id == active.message_id
    await asyncio.sleep(1.05)
    await delivery.ack()
    state = await broker._redis.hgetall(broker._message_key(active.message_id))
    assert state["status"] == "expired"
    assert int((await broker.inspect("jobs")).acked_total) == 0
    assert all(field not in state for field in ("consumer_id", "delivery_id", "lease_token", "claimed_at", "lease_until"))


@pytest.mark.asyncio
async def test_queue_profiles_route_submissions_and_preserve_batch_order() -> None:
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
            SubmitRequest(queue="plain-jobs", payload={"n": 2}),
            SubmitRequest(queue="exact-jobs", payload={"n": 3}, dedup_scope="s", dedup_key="same", dedup_ttl=timedelta(minutes=1)),
        ])
        assert [result.accepted for result in results] == [True, True, False]
    finally:
        await clean(instance)
        await instance.close()


def test_redis_namespace_must_be_a_safe_persistent_identifier() -> None:
    class UnusedRedis:
        pass

    for namespace in ("", "with:colon", "has{tag}", "中文", "x" * 256):
        with pytest.raises(ValidationError, match="namespace"):
            RedisBroker(UnusedRedis(), namespace=namespace)


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
