"""SQLite backend 的 v0.1 契约测试。"""
from __future__ import annotations

import asyncio
import json
from datetime import timedelta

import pytest

from taskflow import (
    BrokerEvent,
    ConsumerOptions,
    LeaseLostError,
    SQLiteBroker,
    SQLiteSubmissionStore,
    SerializerRegistry,
    SubmitRequest,
    ValidationError,
)
from taskflow.middleware import Middleware
from taskflow.types import utc_now


class BinaryJsonSerializer:
    """故意产生非 UTF-8 bytes，用于验证 backend 不会假定文本编码。"""

    name = "binary-json"
    version = "7"

    def dumps(self, value: object) -> bytes:
        return b"\xff" + json.dumps(value, separators=(",", ":")).encode()

    def loads(self, payload: bytes) -> object:
        assert payload.startswith(b"\xff")
        return json.loads(payload[1:].decode())


async def receive(broker: SQLiteBroker, queue: str = "jobs"):
    """从指定队列领取一条消息，测试中不保留长生命周期 consumer。"""

    return await broker.consumer(queue).__anext__()


@pytest.mark.asyncio
async def test_submit_claim_ack_and_stats() -> None:
    async with SQLiteBroker() as broker:
        result = await broker.submit(queue="jobs", payload={"id": 1}, metadata={"trace": "a"})
        delivery = await receive(broker)
        assert result.accepted and delivery.message.payload == {"id": 1}
        assert delivery.attempt == 1
        await delivery.ack()
        await delivery.ack()
        stats = await broker.inspect("jobs")
        assert (stats.ready, stats.leased, stats.acked_total) == (0, 0, 1)


@pytest.mark.asyncio
async def test_submit_many_preserves_input_order() -> None:
    async with SQLiteBroker() as broker:
        results = await broker.submit_many([SubmitRequest(queue="jobs", payload={"n": value}) for value in range(3)])
        assert [result.accepted for result in results] == [True, True, True]
        assert [(await receive(broker)).message.payload["n"] for _ in range(3)] == [0, 1, 2]


@pytest.mark.asyncio
async def test_sqlite_submit_many_is_one_atomic_transaction() -> None:
    async with SQLiteBroker(id_factory=lambda: "same-id") as broker:
        with pytest.raises(Exception):
            await broker.submit_many([SubmitRequest(queue="jobs", payload={}), SubmitRequest(queue="jobs", payload={})])
        assert (await broker.inspect("jobs")).submitted_total == 0
        assert broker.submission_capabilities("jobs").batch_submit


@pytest.mark.asyncio
async def test_submit_many_passes_complete_prepared_submissions_to_store() -> None:
    class RecordingStore(SQLiteSubmissionStore):
        def __init__(self, broker: SQLiteBroker) -> None:
            super().__init__(broker)
            self.batches = []

        async def submit_many(self, submissions):  # type: ignore[no-untyped-def]
            self.batches.append(submissions)
            return await super().submit_many(submissions)

    async with SQLiteBroker() as broker:
        store = RecordingStore(broker)
        broker.submission_store = store
        results = await broker.submit_many([SubmitRequest(queue="jobs", payload={"n": 1})])
        prepared = store.batches[0][0]
        assert results[0].message_id == prepared.message_id
        assert prepared.envelope and prepared.status == "ready"
        assert (prepared.serializer_name, prepared.serializer_version) == ("json", "1")


@pytest.mark.asyncio
async def test_binary_serializer_and_identity_are_persisted(tmp_path) -> None:  # type: ignore[no-untyped-def]
    database = tmp_path / "messages.db"
    async with SQLiteBroker(database, serializer=BinaryJsonSerializer()) as broker:
        submitted = await broker.submit(queue="jobs", payload={"binary": True})
        row = await (await broker._connection.execute("SELECT envelope, serializer_name, serializer_version FROM messages WHERE id=?", (submitted.message_id,))).fetchone()  # type: ignore[union-attr]
        assert (row["serializer_name"], row["serializer_version"]) == ("binary-json", "7")
        assert broker._decode_message(row["envelope"], row["serializer_name"], row["serializer_version"]).payload == {"binary": True}

    async with SQLiteBroker(database) as incompatible:
        row = await (await incompatible._connection.execute("SELECT envelope, serializer_name, serializer_version FROM messages WHERE id=?", (submitted.message_id,))).fetchone()  # type: ignore[union-attr]
        with pytest.raises(ValidationError, match="serializer"):
            incompatible._decode_message(row["envelope"], row["serializer_name"], row["serializer_version"])

    async with SQLiteBroker(database, serializer_registry=SerializerRegistry([BinaryJsonSerializer()])) as migrated:
        assert (await receive(migrated)).message.payload == {"binary": True}


@pytest.mark.asyncio
async def test_implicit_concurrent_start_uses_one_initialized_connection() -> None:
    broker = SQLiteBroker()
    try:
        results = await asyncio.gather(*[broker.submit(queue="jobs", payload={"n": value}) for value in range(12)])
        assert all(result.accepted for result in results)
        assert (await broker.inspect("jobs")).ready == 12
    finally:
        await broker.close()


@pytest.mark.asyncio
async def test_worker_honors_concurrency_and_retries_handler_errors() -> None:
    seen: list[int] = []
    active = 0
    maximum = 0
    complete = asyncio.Event()

    async with SQLiteBroker() as broker:
        for value in range(4):
            await broker.submit(queue="jobs", payload={"n": value}, max_attempts=2)

        async def handler(message):  # type: ignore[no-untyped-def]
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(0.01)
            active -= 1
            seen.append(message.payload["n"])
            if len(seen) >= 4:
                complete.set()

        worker = broker.worker("jobs", handler, concurrency=2)
        task = asyncio.create_task(worker.run())
        await asyncio.wait_for(complete.wait(), timeout=1)
        await worker.close()
        await task
        assert maximum == 2
        assert sorted(seen) == [0, 1, 2, 3]


@pytest.mark.asyncio
async def test_metrics_and_structured_events_cover_submission_claim_and_ack() -> None:
    class Recorder:
        def __init__(self) -> None:
            self.increments = []
            self.observations = []

        async def increment(self, name, value=1, **labels):  # type: ignore[no-untyped-def]
            self.increments.append((name, value, labels))

        async def observe(self, name, value, **labels):  # type: ignore[no-untyped-def]
            self.observations.append((name, value, labels))

    events: list[BrokerEvent] = []
    middleware = Middleware()
    middleware.add("event", events.append)
    metrics = Recorder()
    async with SQLiteBroker(middleware=middleware, metrics=metrics) as broker:
        await broker.submit(queue="jobs", payload={})
        delivery = await receive(broker)
        await delivery.ack()
        await broker.inspect("jobs")
    assert {name for name, _, _ in metrics.increments} >= {"submitted_total", "claimed_total", "acked_total"}
    assert {item.name for item in events} >= {"submitted", "claimed", "ack"}
    assert all(item.serializer_name == "json" and item.serializer_version == "1" for item in events)
    assert {name for name, _, _ in metrics.observations} >= {"queue_ready", "queue_leased"}


@pytest.mark.asyncio
async def test_exact_dedup_is_atomic_and_ttl_can_expire() -> None:
    now = [utc_now()]
    async with SQLiteBroker(clock=lambda: now[0]) as broker:
        async def submit() -> bool:
            return (await broker.submit(queue="jobs", payload={}, dedup_scope="run", dedup_key="same", dedup_ttl=timedelta(seconds=5))).accepted

        assert sum(await asyncio.gather(*[submit() for _ in range(12)])) == 1
        now[0] += timedelta(seconds=6)
        assert (await submit()) is True


@pytest.mark.asyncio
async def test_retry_is_immediate_and_limit_routes_to_dlq() -> None:
    async with SQLiteBroker() as broker:
        await broker.submit(queue="jobs", payload={}, max_attempts=2)
        first = await receive(broker)
        await first.retry(reason="temporary")
        await first.retry(reason="temporary")
        second = await receive(broker)
        assert second.attempt == 2
        await second.retry(reason="still broken")
        letters = await broker.admin.list_dead_letters("jobs")
        assert len(letters) == 1 and letters[0].source == "retry_limit"


@pytest.mark.asyncio
async def test_reject_records_reason_and_exception() -> None:
    async with SQLiteBroker() as broker:
        await broker.submit(queue="jobs", payload={})
        delivery = await receive(broker)
        await delivery.reject(reason="bad input", error=ValueError("bad"))
        letter = (await broker.admin.list_dead_letters("jobs"))[0]
        assert letter.reason == "bad input" and letter.error_type == "ValueError"


@pytest.mark.asyncio
async def test_lease_reclaim_rejects_stale_delivery() -> None:
    now = [utc_now()]
    async with SQLiteBroker(clock=lambda: now[0]) as broker:
        await broker.submit(queue="jobs", payload={}, max_attempts=2)
        first = await broker.consumer("jobs", options=ConsumerOptions(lease_seconds=1)).__anext__()
        now[0] += timedelta(seconds=2)
        second = await receive(broker)
        assert second.attempt == 2
        with pytest.raises(LeaseLostError):
            await first.ack()
        await second.ack()
        assert (await broker.inspect("jobs")).reclaimed_total == 1


@pytest.mark.asyncio
async def test_extend_lease_is_capped_by_message_expiry() -> None:
    now = [utc_now()]
    async with SQLiteBroker(clock=lambda: now[0]) as broker:
        expires_at = now[0] + timedelta(seconds=5)
        await broker.submit(queue="jobs", payload={}, expires_at=expires_at)
        delivery = await broker.consumer("jobs", options=ConsumerOptions(lease_seconds=1)).__anext__()
        extended = await delivery.extend_lease(seconds=60)
        assert extended <= expires_at
        assert (expires_at - extended).total_seconds() < 0.001


@pytest.mark.asyncio
async def test_lease_timeout_at_limit_enters_dlq() -> None:
    now = [utc_now()]
    async with SQLiteBroker(clock=lambda: now[0]) as broker:
        await broker.submit(queue="jobs", payload={}, max_attempts=1)
        await broker.consumer("jobs", options=ConsumerOptions(lease_seconds=1)).__anext__()
        now[0] += timedelta(seconds=2)
        await broker.maintain("jobs")
        assert (await broker.admin.list_dead_letters("jobs"))[0].source == "lease_timeout"


@pytest.mark.asyncio
async def test_expired_messages_do_not_reach_consumer_and_can_replay() -> None:
    now = [utc_now()]
    async with SQLiteBroker(clock=lambda: now[0]) as broker:
        submitted = await broker.submit(queue="jobs", payload={"a": 1}, expires_at=now[0] + timedelta(seconds=1))
        now[0] += timedelta(seconds=2)
        await broker.maintain("jobs")
        expired = await broker.admin.list_expired("jobs")
        assert [item.message.id for item in expired] == [submitted.message_id]
        await broker.admin.replay_expired("jobs", submitted.message_id, expires_at=None)
        delivery = await receive(broker)
        assert delivery.message.id == submitted.message_id


@pytest.mark.asyncio
async def test_dead_letter_replay_and_delete() -> None:
    async with SQLiteBroker() as broker:
        submitted = await broker.submit(queue="jobs", payload={"v": 1})
        await (await receive(broker)).reject(reason="bad")
        await broker.admin.replay_dead_letter("jobs", submitted.message_id, payload={"v": 2})
        delivery = await receive(broker)
        assert delivery.message.payload == {"v": 2}
        await delivery.reject(reason="again")
        assert await broker.admin.delete_dead_letter("jobs", submitted.message_id)


@pytest.mark.asyncio
async def test_replay_can_keep_remove_or_replace_dedup_atomically() -> None:
    async with SQLiteBroker() as broker:
        first = await broker.submit(queue="jobs", payload={}, dedup_scope="old", dedup_key="one", dedup_ttl=timedelta(minutes=1))
        await (await receive(broker)).reject(reason="repair")
        await broker.admin.replay_dead_letter("jobs", first.message_id)
        kept = await broker.submit(queue="jobs", payload={}, dedup_scope="old", dedup_key="one", dedup_ttl=timedelta(minutes=1))
        assert not kept.accepted and kept.existing_message_id == first.message_id

        replayed = await receive(broker)
        await replayed.reject(reason="remove key")
        await broker.admin.replay_dead_letter("jobs", first.message_id, reuse_dedup=False)
        assert (await broker.submit(queue="jobs", payload={}, dedup_scope="old", dedup_key="one", dedup_ttl=timedelta(minutes=1))).accepted

        second = await broker.submit(queue="jobs", payload={}, dedup_scope="replace", dedup_key="taken", dedup_ttl=timedelta(minutes=1))
        active = await receive(broker)
        if active.message.id != first.message_id:
            await active.ack()
            active = await receive(broker)
        await active.reject(reason="replace key")
        with pytest.raises(ValidationError, match="其他消息"):
            await broker.admin.replay_dead_letter("jobs", first.message_id, reuse_dedup=False,
                dedup_scope="replace", dedup_key="taken", dedup_ttl=timedelta(minutes=1))
        assert any(letter.message.id == first.message_id for letter in await broker.admin.list_dead_letters("jobs"))
        assert second.accepted


@pytest.mark.asyncio
async def test_invalid_payload_and_dedup_parameters_are_rejected() -> None:
    async with SQLiteBroker() as broker:
        with pytest.raises(ValidationError):
            await broker.submit(queue="jobs", payload={"unserializable": {1}})
        with pytest.raises(ValidationError):
            await broker.submit(queue="jobs", payload={}, dedup_key="x")
        with pytest.raises(ValidationError):
            await broker.submit(queue="jobs", payload={}, dedup_scope="s", dedup_key="x", dedup_ttl=timedelta())


@pytest.mark.asyncio
@pytest.mark.parametrize("queue", ["", "contains space", "colon:name", "brace{name}", "中文", "x" * 256])
async def test_queue_name_must_be_a_safe_persistent_identifier(queue: str) -> None:
    async with SQLiteBroker() as broker:
        with pytest.raises(ValidationError, match="queue"):
            await broker.submit(queue=queue, payload={})
