from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest

from taskflow import (
    BrokerEvent,
    ConsumerOptions,
    FixedBackoff,
    QueueConfig,
    RetryableError,
    RetryPolicy,
    SerializerRegistry,
    SerializerUnavailableError,
    SQLiteBroker,
    SQLiteSubmissionStore,
    SubmitRequest,
    ValidationError,
)
from taskflow.naming import validate_persistent_name
from taskflow.types import utc_now


@pytest.mark.asyncio
async def test_queue_config_applies_lease_attempt_and_payload_limit() -> None:
    async with SQLiteBroker(
        queues={"mail": QueueConfig(max_attempts=7, lease=timedelta(seconds=2), max_payload_bytes=8)},
    ) as broker:
        result = await broker.submit(queue="mail", payload={"x": 1})
        row = await (await broker._connection.execute("SELECT max_attempts FROM messages WHERE id=?", (result.message_id,))).fetchone()  # type: ignore[union-attr]
        assert row is not None
        assert row[0] == 7
        with pytest.raises(ValidationError, match="max_payload_bytes"):
            await broker.submit(queue="mail", payload={"too": "large"})


@pytest.mark.asyncio
async def test_queue_config_none_overrides_broker_dedup_default() -> None:
    async with SQLiteBroker(
        default_dedup_ttl=timedelta(hours=1), queues={"plain": QueueConfig(default_dedup_ttl=None)}
    ) as broker:
        with pytest.raises(ValidationError, match="dedup_ttl"):
            await broker.submit(queue="plain", payload={}, dedup_scope="s", dedup_key="k")


@pytest.mark.asyncio
async def test_queue_config_default_dedup_ttl_is_used() -> None:
    async with SQLiteBroker(queues={"dedup": QueueConfig(default_dedup_ttl=timedelta(minutes=1))}) as broker:
        first = await broker.submit(queue="dedup", payload={}, dedup_scope="scope", dedup_key="key")
        duplicate = await broker.submit(queue="dedup", payload={}, dedup_scope="scope", dedup_key="key")
    assert first.accepted and not duplicate.accepted and duplicate.existing_message_id == first.message_id


@pytest.mark.asyncio
async def test_queue_config_max_attempts_controls_worker_without_policy() -> None:
    calls = 0

    async def handler(_message) -> None:  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        raise RetryableError("always fails")

    async with SQLiteBroker(queues={"jobs": QueueConfig(max_attempts=7)}) as broker:
        worker = broker.worker("jobs", handler)
        await worker.start()
        await broker.submit(queue="jobs", payload={})
        for _ in range(500):
            if (await broker.inspect("jobs")).dead_letters:
                break
            await asyncio.sleep(0.001)
        await worker.close()
    assert calls == 7


@pytest.mark.asyncio
async def test_worker_arguments_override_queue_config() -> None:
    configured = QueueConfig(
        lease=timedelta(seconds=7), retry_policy=RetryPolicy(max_attempts=5, backoff=FixedBackoff(1)),
    )
    explicit = RetryPolicy(max_attempts=2)
    async with SQLiteBroker(queues={"jobs": configured}) as broker:
        worker = broker.worker("jobs", lambda _: None, options=ConsumerOptions(lease_seconds=3), retry_policy=explicit)
        assert worker._options.lease_seconds == 3  # type: ignore[attr-defined]
        assert worker._retry_policy is explicit  # type: ignore[attr-defined]
        assert broker.consumer("jobs").options.lease_seconds == 7


@pytest.mark.asyncio
async def test_explicit_queue_retry_policy_remains_a_stricter_worker_limit() -> None:
    calls = 0

    async def handler(_message) -> None:  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        raise RetryableError("always fails")

    async with SQLiteBroker(queues={"jobs": QueueConfig(max_attempts=7, retry_policy=RetryPolicy(max_attempts=2))}) as broker:
        worker = broker.worker("jobs", handler)
        await worker.start()
        await broker.submit(queue="jobs", payload={})
        for _ in range(300):
            if (await broker.inspect("jobs")).dead_letters:
                break
            await asyncio.sleep(0.001)
        await worker.close()
    assert calls == 2


@pytest.mark.asyncio
async def test_submit_many_rejects_mixed_profiles_before_any_write() -> None:
    async with SQLiteBroker(
        submission_stores={
            "default": lambda broker: SQLiteSubmissionStore(broker),
            "exact": lambda broker: SQLiteSubmissionStore(broker),
        },
        queue_submission_profiles={"exact-jobs": "exact"},
    ) as broker:
        with pytest.raises(ValidationError, match="混合"):
            await broker.submit_many([
                SubmitRequest(queue="jobs", payload={}),
                SubmitRequest(queue="exact-jobs", payload={}),
            ])
        assert (await broker.inspect("jobs")).submitted_total == 0
        assert (await broker.inspect("exact-jobs")).submitted_total == 0


@pytest.mark.asyncio
async def test_event_sink_receives_standard_fields() -> None:
    class Sink:
        def __init__(self) -> None:
            self.events: list[Any] = []

        async def emit(self, item) -> None:  # type: ignore[no-untyped-def]
            self.events.append(item)

    sink = Sink()
    async with SQLiteBroker(event_sink=sink) as broker:
        await broker.submit(queue="jobs", payload={})
        delivery = await broker.consumer("jobs").__anext__()
        await delivery.ack()
    assert [item.event_name for item in sink.events] == ["submitted", "claimed", "ack"]
    assert all(item.backend == "sqlite" and item.queue == "jobs" for item in sink.events)


@pytest.mark.asyncio
async def test_atomic_batch_reports_duplicate_through_submission_observer() -> None:
    class Sink:
        def __init__(self) -> None:
            self.events: list[Any] = []

        async def emit(self, item) -> None:  # type: ignore[no-untyped-def]
            self.events.append(item)

    sink = Sink()
    async with SQLiteBroker(event_sink=sink) as broker:
        await broker.submit(queue="jobs", payload={"id": 1}, dedup_scope="scope", dedup_key="first",
                            dedup_ttl=timedelta(minutes=1))
        results = await broker.submit_many([
            SubmitRequest(queue="jobs", payload={"id": 2}, dedup_scope="scope", dedup_key="first",
                          dedup_ttl=timedelta(minutes=1)),
            SubmitRequest(queue="jobs", payload={"id": 3}),
        ])
    assert [item.event_name for item in sink.events] == ["submitted", "duplicate", "submitted"]
    assert not results[0].accepted and results[1].accepted


@pytest.mark.asyncio
async def test_maintenance_events_are_published_after_state_commit() -> None:
    class Sink:
        def __init__(self) -> None:
            self.events: list[Any] = []

        async def emit(self, item) -> None:  # type: ignore[no-untyped-def]
            self.events.append(item)

    now = [utc_now()]
    sink = Sink()
    async with SQLiteBroker(clock=lambda: now[0], event_sink=sink) as broker:
        await broker.submit(queue="jobs", payload={}, delay=timedelta(seconds=10), expires_at=now[0] + timedelta(seconds=5))
        now[0] += timedelta(seconds=10)
        await broker.maintain("jobs")
        await broker.submit(queue="jobs", payload={}, max_attempts=1)
        await broker.consumer("jobs", options=ConsumerOptions(lease_seconds=1)).__anext__()
        now[0] += timedelta(seconds=2)
        await broker.maintain("jobs")
    assert [item.event_name for item in sink.events].count("expired") == 1
    assert any(item.event_name == "dead_lettered" and item.reason for item in sink.events)


@pytest.mark.asyncio
async def test_legacy_name_mode_and_metrics_compatibility() -> None:
    class OldMetrics:
        async def increment(self, name: str, value: int = 1, **labels: str) -> None: pass
        async def observe(self, name: str, value: float, **labels: str) -> None: pass

    async with SQLiteBroker(metrics=OldMetrics(), allow_legacy_names=True) as broker:
        await broker.submit(queue=".legacy", payload={})
        assert (await broker.inspect(".legacy")).ready == 1


@pytest.mark.asyncio
async def test_gauge_metrics_and_broken_event_sink_do_not_change_state() -> None:
    class Metrics:
        def __init__(self) -> None:
            self.gauges: list[str] = []

        async def increment(self, name: str, value: int = 1, **labels: str) -> None: pass
        async def observe(self, name: str, value: float, **labels: str) -> None: pass
        async def gauge(self, name: str, value: float, **labels: str) -> None:
            self.gauges.append(name)

    class BrokenSink:
        async def emit(self, _event) -> None:  # type: ignore[no-untyped-def]
            raise RuntimeError("observability unavailable")

    metrics = Metrics()
    async with SQLiteBroker(metrics=metrics, event_sink=BrokenSink()) as broker:
        await broker.submit(queue="jobs", payload={})
        assert await (await broker.consumer("jobs").__anext__()).ack()
        await broker.inspect("jobs")
    assert {"queue_ready", "queue_leased", "queue_delayed"} <= set(metrics.gauges)


def test_broker_event_constructor_remains_v02_compatible() -> None:
    event = BrokerEvent(name="submitted", timestamp=utc_now(), queue="jobs", message_id="id")
    assert event.name == event.event_name == "submitted"


def test_names_and_serializer_errors_are_explicit() -> None:
    for value in (".", "-", "a" * 129, "bad:name", "bad name"):
        with pytest.raises(ValidationError):
            validate_persistent_name(value, label="queue")
    with pytest.raises(SerializerUnavailableError):
        SerializerRegistry().resolve("json", "99")
