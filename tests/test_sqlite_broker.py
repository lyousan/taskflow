"""SQLite backend 的 v0.1 契约测试。"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import timedelta

import pytest

from taskqx import (
    BrokerEvent,
    ConsumerOptions,
    FinishOutcome,
    LeaseLostError,
    SerializerRegistry,
    SQLiteBroker,
    SQLiteSubmissionStore,
    SubmitRequest,
    ValidationError,
)
from taskqx.middleware import Middleware
from taskqx.submission import PreparedSubmission
from taskqx.types import utc_now
from tests.support import BinaryJsonSerializer


async def receive(broker: SQLiteBroker, queue: str = "jobs"):
    """从指定队列领取一条消息，测试中不保留长生命周期 consumer。"""

    return await broker.consumer(queue).__anext__()


@pytest.mark.asyncio
async def test_submit_claim_ack_and_stats() -> None:
    async with SQLiteBroker() as broker:
        result = await broker.submit(
            queue="jobs", payload={"id": 1}, metadata={"trace": "a"}
        )
        delivery = await receive(broker)
        assert result.accepted and delivery.message.payload == {"id": 1}
        assert delivery.attempt == 1
        assert await delivery.ack() is FinishOutcome.ACKED
        assert await delivery.ack() is FinishOutcome.IDEMPOTENT
        stats = await broker.inspect("jobs")
        assert (stats.ready, stats.leased, stats.acked_total) == (0, 0, 1)


@pytest.mark.asyncio
async def test_submit_many_preserves_input_order() -> None:
    async with SQLiteBroker() as broker:
        results = await broker.submit_many(
            [SubmitRequest(queue="jobs", payload={"n": value}) for value in range(3)]
        )
        assert [result.accepted for result in results] == [True, True, True]
        assert [(await receive(broker)).message.payload["n"] for _ in range(3)] == [
            0,
            1,
            2,
        ]


@pytest.mark.asyncio
async def test_sqlite_submit_many_is_one_atomic_transaction() -> None:
    async with SQLiteBroker(id_factory=lambda: "same-id") as broker:
        with pytest.raises(sqlite3.IntegrityError):
            await broker.submit_many(
                [
                    SubmitRequest(queue="jobs", payload={}),
                    SubmitRequest(queue="jobs", payload={}),
                ]
            )
        assert (await broker.inspect("jobs")).submitted_total == 0
        assert broker._connection is not None
        row = await (
            await broker._connection.execute("SELECT COUNT(*) FROM messages")
        ).fetchone()  # type: ignore[union-attr]
        assert row is not None
        assert row[0] == 0
        assert broker.submission_capabilities("jobs").batch_submit
        assert broker.submission_capabilities("jobs").batch_atomic


@pytest.mark.asyncio
async def test_submit_many_passes_complete_prepared_submissions_to_store() -> None:
    class RecordingStore(SQLiteSubmissionStore):
        def __init__(self, broker: SQLiteBroker) -> None:
            super().__init__(broker)
            self.batches: list[list[PreparedSubmission]] = []

        async def submit_many(self, submissions):  # type: ignore[no-untyped-def]
            self.batches.append(submissions)
            return await super().submit_many(submissions)

    async with SQLiteBroker() as broker:
        store = RecordingStore(broker)
        broker.submission_store = store
        results = await broker.submit_many(
            [SubmitRequest(queue="jobs", payload={"n": 1})]
        )
        prepared = store.batches[0][0]
        assert results[0].message_id == prepared.message_id
        assert prepared.envelope and prepared.status == "ready"
        assert (prepared.serializer_name, prepared.serializer_version) == ("json", "1")


@pytest.mark.asyncio
async def test_binary_serializer_and_identity_are_persisted(tmp_path) -> None:  # type: ignore[no-untyped-def]
    database = tmp_path / "messages.db"
    async with SQLiteBroker(database, serializer=BinaryJsonSerializer()) as broker:
        submitted = await broker.submit(queue="jobs", payload={"binary": True})
        assert broker._connection is not None
        row = await (
            await broker._connection.execute(
                "SELECT envelope, serializer_name, serializer_version FROM messages WHERE id=?",
                (submitted.message_id,),
            )
        ).fetchone()  # type: ignore[union-attr]
        assert row is not None
        assert (row["serializer_name"], row["serializer_version"]) == (
            "binary-json",
            "7",
        )
        assert broker._decode_message(
            row["envelope"], row["serializer_name"], row["serializer_version"]
        ).payload == {"binary": True}

    async with SQLiteBroker(database) as incompatible:
        assert incompatible._connection is not None
        row = await (
            await incompatible._connection.execute(
                "SELECT envelope, serializer_name, serializer_version FROM messages WHERE id=?",
                (submitted.message_id,),
            )
        ).fetchone()  # type: ignore[union-attr]
        assert row is not None
        with pytest.raises(ValidationError, match="serializer"):
            incompatible._decode_message(
                row["envelope"], row["serializer_name"], row["serializer_version"]
            )

    async with SQLiteBroker(
        database, serializer_registry=SerializerRegistry([BinaryJsonSerializer()])
    ) as migrated:
        assert (await receive(migrated)).message.payload == {"binary": True}


@pytest.mark.asyncio
async def test_start_migrates_legacy_messages_schema_idempotently(tmp_path) -> None:  # type: ignore[no-untyped-def]
    database = tmp_path / "legacy.db"
    connection = sqlite3.connect(database)
    connection.execute("""
        CREATE TABLE messages (
            id TEXT PRIMARY KEY, queue TEXT NOT NULL, envelope BLOB NOT NULL,
            status TEXT NOT NULL, attempt INTEGER NOT NULL, max_attempts INTEGER NOT NULL,
            created_at REAL NOT NULL, expires_at REAL, consumer_id TEXT,
            delivery_id TEXT, lease_token TEXT, claimed_at REAL, lease_until REAL,
            last_action TEXT, last_reason TEXT
        )
    """)
    connection.commit()
    connection.close()
    async with SQLiteBroker(database) as broker:
        assert broker._connection is not None
        columns = {
            row[1]
            for row in await (
                await broker._connection.execute("PRAGMA table_info(messages)")
            ).fetchall()
        }  # type: ignore[union-attr]
        assert {
            "serializer_name",
            "serializer_version",
            "last_delivery_id",
            "last_consumer_id",
        } <= columns
    async with SQLiteBroker(database):
        pass


@pytest.mark.asyncio
async def test_implicit_concurrent_start_uses_one_initialized_connection() -> None:
    broker = SQLiteBroker()
    try:
        results = await asyncio.gather(
            *[broker.submit(queue="jobs", payload={"n": value}) for value in range(12)]
        )
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
            self.increments: list[tuple[str, object, dict[str, object]]] = []
            self.observations: list[tuple[str, object, dict[str, object]]] = []

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
        assert await delivery.ack() is FinishOutcome.ACKED
        await broker.inspect("jobs")
    assert {name for name, _, _ in metrics.increments} >= {
        "submitted_total",
        "claimed_total",
        "acked_total",
    }
    assert {item.name for item in events} >= {"submitted", "claimed", "ack"}
    assert all(
        item.serializer_name == "json" and item.serializer_version == "1"
        for item in events
    )
    assert {name for name, _, _ in metrics.observations} >= {
        "queue_ready",
        "queue_leased",
    }


@pytest.mark.asyncio
async def test_exact_dedup_is_atomic_and_ttl_can_expire() -> None:
    now = [utc_now()]
    async with SQLiteBroker(clock=lambda: now[0]) as broker:

        async def submit() -> bool:
            return (
                await broker.submit(
                    queue="jobs",
                    payload={},
                    dedup_scope="run",
                    dedup_key="same",
                    dedup_ttl=timedelta(seconds=5),
                )
            ).accepted

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
async def test_retry_limit_is_idempotent_for_the_original_delivery() -> None:
    async with SQLiteBroker() as broker:
        await broker.submit(queue="jobs", payload={}, max_attempts=1)
        delivery = await receive(broker)
        await delivery.retry(reason="limit")
        await delivery.retry(reason="limit")
        assert (await broker.admin.list_dead_letters("jobs"))[0].source == "retry_limit"


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
        first = await broker.consumer(
            "jobs", options=ConsumerOptions(lease_seconds=1)
        ).__anext__()
        now[0] += timedelta(seconds=2)
        second = await receive(broker)
        assert second.attempt == 2
        with pytest.raises(LeaseLostError):
            await first.ack()
        await second.ack()
        assert (await broker.inspect("jobs")).reclaimed_total == 1


@pytest.mark.asyncio
async def test_reclaim_clears_current_lease_and_preserves_audit_identity() -> None:
    now = [utc_now()]
    async with SQLiteBroker(clock=lambda: now[0]) as broker:
        submitted = await broker.submit(queue="jobs", payload={})
        delivery = await broker.consumer(
            "jobs", consumer_id="worker-a", options=ConsumerOptions(lease_seconds=1)
        ).__anext__()
        now[0] += timedelta(seconds=2)
        await broker.maintain("jobs")
        assert broker._connection is not None
        row = await (
            await broker._connection.execute(
                "SELECT * FROM messages WHERE id=?", (submitted.message_id,)
            )
        ).fetchone()  # type: ignore[union-attr]
        assert row is not None
        assert row["status"] == "ready"
        assert all(
            row[field] is None
            for field in (
                "consumer_id",
                "delivery_id",
                "lease_token",
                "claimed_at",
                "lease_until",
            )
        )
        assert row["last_delivery_id"] == delivery.delivery_id
        assert row["last_consumer_id"] == "worker-a"


@pytest.mark.asyncio
async def test_extend_lease_is_capped_by_message_expiry() -> None:
    now = [utc_now()]
    async with SQLiteBroker(clock=lambda: now[0]) as broker:
        expires_at = now[0] + timedelta(seconds=5)
        await broker.submit(queue="jobs", payload={}, expires_at=expires_at)
        delivery = await broker.consumer(
            "jobs", options=ConsumerOptions(lease_seconds=1)
        ).__anext__()
        extended = await delivery.extend_lease(seconds=60)
        assert extended <= expires_at
        assert (expires_at - extended).total_seconds() < 0.001


@pytest.mark.asyncio
async def test_extend_lease_expiry_commits_eq_transition_and_observability() -> None:
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

    now, metrics, sink = [utc_now()], Metrics(), Sink()
    async with SQLiteBroker(
        clock=lambda: now[0], metrics=metrics, event_sink=sink
    ) as broker:
        submitted = await broker.submit(
            queue="jobs", payload={}, expires_at=now[0] + timedelta(seconds=1)
        )
        delivery = await broker.consumer(
            "jobs", options=ConsumerOptions(lease_seconds=10)
        ).__anext__()
        now[0] += timedelta(seconds=2)
        with pytest.raises(LeaseLostError, match="过期"):
            await delivery.extend_lease(seconds=1)
        assert broker._connection is not None
        state = await (
            await broker._connection.execute(
                "SELECT status FROM messages WHERE id=?", (submitted.message_id,)
            )
        ).fetchone()  # type: ignore[union-attr]
        assert state is not None and state["status"] == "expired"
        assert [
            item.message.id for item in await broker.admin.list_expired("jobs")
        ] == [submitted.message_id]
    assert metrics.increments.count("expired_total") == 1
    assert [item.event_name for item in sink.events].count("expired") == 1  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_expired_delivery_ack_records_expiry_not_ack() -> None:
    class Recorder:
        def __init__(self) -> None:
            self.increments: list[str] = []

        async def increment(self, name, value=1, **labels):
            self.increments.append(name)  # type: ignore[no-untyped-def]

        async def observe(self, name, value, **labels):
            pass  # type: ignore[no-untyped-def]

    now = [utc_now()]
    middleware, metrics = Middleware(), Recorder()
    events: list[BrokerEvent] = []
    middleware.add("event", events.append)
    async with SQLiteBroker(
        clock=lambda: now[0], middleware=middleware, metrics=metrics
    ) as broker:
        await broker.submit(
            queue="jobs", payload={}, expires_at=now[0] + timedelta(seconds=1)
        )
        delivery = await broker.consumer(
            "jobs", options=ConsumerOptions(lease_seconds=10)
        ).__anext__()
        now[0] += timedelta(seconds=2)
        assert await delivery.ack() is FinishOutcome.EXPIRED
        stats = await broker.inspect("jobs")
    assert stats.acked_total == 0 and stats.expired == 1
    assert (
        "acked_total" not in metrics.increments
        and "expired_total" in metrics.increments
    )
    assert any(item.name == "expired" for item in events)
    assert not any(item.name == "ack" for item in events)


@pytest.mark.asyncio
async def test_worker_is_an_async_context_manager_and_rejects_zero_concurrency() -> (
    None
):
    async with SQLiteBroker() as broker:
        with pytest.raises(ValidationError):
            broker.worker("jobs", lambda _: None, concurrency=0)
        async with broker.worker("jobs", lambda _: None) as worker:
            assert worker.concurrency == 1


@pytest.mark.asyncio
async def test_post_commit_hooks_and_metrics_failures_do_not_change_result() -> None:
    class BrokenMetrics:
        async def increment(self, *args, **kwargs):
            raise RuntimeError("metrics down")  # type: ignore[no-untyped-def]

        async def observe(self, *args, **kwargs):
            raise RuntimeError("metrics down")  # type: ignore[no-untyped-def]

    middleware = Middleware()

    def broken_hook(*_):
        raise RuntimeError("hook down")  # type: ignore[no-untyped-def]

    middleware.add("after_ack", broken_hook)
    async with SQLiteBroker(middleware=middleware, metrics=BrokenMetrics()) as broker:
        await broker.submit(queue="jobs", payload={})
        await (await receive(broker)).ack()
        assert (await broker.inspect("jobs")).acked_total == 1


@pytest.mark.asyncio
async def test_lease_timeout_at_limit_enters_dlq() -> None:
    now = [utc_now()]
    async with SQLiteBroker(clock=lambda: now[0]) as broker:
        await broker.submit(queue="jobs", payload={}, max_attempts=1)
        await broker.consumer(
            "jobs", options=ConsumerOptions(lease_seconds=1)
        ).__anext__()
        now[0] += timedelta(seconds=2)
        await broker.maintain("jobs")
        assert (await broker.admin.list_dead_letters("jobs"))[
            0
        ].source == "lease_timeout"


@pytest.mark.asyncio
async def test_expired_messages_do_not_reach_consumer_and_can_replay() -> None:
    now = [utc_now()]
    async with SQLiteBroker(clock=lambda: now[0]) as broker:
        submitted = await broker.submit(
            queue="jobs", payload={"a": 1}, expires_at=now[0] + timedelta(seconds=1)
        )
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
        await broker.admin.replay_dead_letter(
            "jobs", submitted.message_id, payload={"v": 2}
        )
        delivery = await receive(broker)
        assert delivery.message.payload == {"v": 2}
        await delivery.reject(reason="again")
        assert await broker.admin.delete_dead_letter("jobs", submitted.message_id)


@pytest.mark.asyncio
async def test_replay_can_keep_remove_or_replace_dedup_atomically() -> None:
    async with SQLiteBroker() as broker:
        first = await broker.submit(
            queue="jobs",
            payload={},
            dedup_scope="old",
            dedup_key="one",
            dedup_ttl=timedelta(minutes=1),
        )
        await (await receive(broker)).reject(reason="repair")
        await broker.admin.replay_dead_letter("jobs", first.message_id)
        kept = await broker.submit(
            queue="jobs",
            payload={},
            dedup_scope="old",
            dedup_key="one",
            dedup_ttl=timedelta(minutes=1),
        )
        assert not kept.accepted and kept.existing_message_id == first.message_id

        replayed = await receive(broker)
        await replayed.reject(reason="remove key")
        await broker.admin.replay_dead_letter(
            "jobs", first.message_id, reuse_dedup=False
        )
        assert (
            await broker.submit(
                queue="jobs",
                payload={},
                dedup_scope="old",
                dedup_key="one",
                dedup_ttl=timedelta(minutes=1),
            )
        ).accepted

        second = await broker.submit(
            queue="jobs",
            payload={},
            dedup_scope="replace",
            dedup_key="taken",
            dedup_ttl=timedelta(minutes=1),
        )
        active = await receive(broker)
        if active.message.id != first.message_id:
            await active.ack()
            active = await receive(broker)
        await active.reject(reason="replace key")
        with pytest.raises(ValidationError, match="其他消息"):
            await broker.admin.replay_dead_letter(
                "jobs",
                first.message_id,
                reuse_dedup=False,
                dedup_scope="replace",
                dedup_key="taken",
                dedup_ttl=timedelta(minutes=1),
            )
        assert any(
            letter.message.id == first.message_id
            for letter in await broker.admin.list_dead_letters("jobs")
        )
        assert second.accepted


@pytest.mark.asyncio
async def test_invalid_payload_and_dedup_parameters_are_rejected() -> None:
    async with SQLiteBroker() as broker:
        with pytest.raises(ValidationError):
            await broker.submit(queue="jobs", payload={"unserializable": {1}})
        with pytest.raises(ValidationError):
            await broker.submit(queue="jobs", payload={}, dedup_key="x")
        with pytest.raises(ValidationError):
            await broker.submit(
                queue="jobs",
                payload={},
                dedup_scope="s",
                dedup_key="x",
                dedup_ttl=timedelta(),
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("ttl", [timedelta(), timedelta(milliseconds=-1)])
async def test_explicit_invalid_dedup_ttl_never_falls_back_to_default(
    ttl: timedelta,
) -> None:
    async with SQLiteBroker(default_dedup_ttl=timedelta(hours=1)) as broker:
        with pytest.raises(ValidationError):
            await broker.submit(
                queue="jobs", payload={}, dedup_scope="s", dedup_key="x", dedup_ttl=ttl
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "queue", ["", "contains space", "colon:name", "brace{name}", "中文", "x" * 256]
)
async def test_queue_name_must_be_a_safe_persistent_identifier(queue: str) -> None:
    async with SQLiteBroker() as broker:
        with pytest.raises(ValidationError, match="queue"):
            await broker.submit(queue=queue, payload={})


@pytest.mark.asyncio
async def test_ack_tombstone_cleanup_preserves_cumulative_stats() -> None:
    now = [utc_now()]
    async with SQLiteBroker(
        clock=lambda: now[0], default_ack_tombstone_ttl=timedelta(seconds=1)
    ) as broker:
        submitted = await broker.submit(
            queue="jobs", payload={}, workflow_id="billing", parent_id="origin"
        )
        assert await (await receive(broker)).ack() is FinishOutcome.ACKED
        assert await broker.inspect_message(submitted.message_id) is not None

        now[0] += timedelta(seconds=1)
        assert await broker.maintain("jobs") == 1
        assert await broker.inspect_message(submitted.message_id) is None
        tombstone = (await broker.list_message_summaries("jobs")).items[0]
        assert tombstone.message_id == submitted.message_id
        assert tombstone.payload_pruned and tombstone.acked_at is not None
        assert (tombstone.workflow_id, tombstone.parent_id) == ("billing", "origin")
        assert (await broker.inspect("jobs")).acked_total == 1


@pytest.mark.asyncio
async def test_default_ack_tombstone_ttl_is_five_minutes() -> None:
    now = [utc_now()]
    async with SQLiteBroker(clock=lambda: now[0]) as broker:
        submitted = await broker.submit(queue="jobs", payload={})
        assert await (await receive(broker)).ack() is FinishOutcome.ACKED

        now[0] += timedelta(minutes=5) - timedelta(microseconds=1)
        assert await broker.maintain("jobs") == 0
        assert await broker.inspect_message(submitted.message_id) is not None

        now[0] += timedelta(microseconds=1)
        assert await broker.maintain("jobs") == 1
        assert await broker.inspect_message(submitted.message_id) is None
