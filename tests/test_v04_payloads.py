"""v0.4 类型化 payload 与 poison-message 契约。"""
from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TypedDict

import pytest
from typing_extensions import NotRequired, Required

from taskflow import (
    ConsumerOptions,
    PayloadDecodingError,
    SQLiteBroker,
    SubmitRequest,
    ValidationError,
)
from taskflow.payloads import decode_payload, normalize_payload
from taskflow.types import utc_now


@dataclass(frozen=True)
class ResizeImage:
    image_id: str
    width: int
    height: int


class ResizePayload(TypedDict):
    image_id: str
    width: int
    height: int


class OptionalResizePayload(TypedDict, total=False):
    image_id: Required[str]
    width: NotRequired[int]
    nested: NotRequired[ResizePayload]


async def _wait_for(broker: SQLiteBroker, *, acked: int = 0, dead_letters: int = 0) -> None:
    for _ in range(200):
        stats = await broker.inspect("images")
        if stats.acked_total == acked and stats.dead_letters == dead_letters:
            return
        await asyncio.sleep(0.005)
    raise AssertionError("worker did not reach expected terminal state")


def test_dataclass_and_typeddict_payloads_are_strictly_decoded() -> None:
    payload, schema = normalize_payload(ResizeImage("img-1", 800, 600))
    assert schema is not None
    assert decode_payload(payload, ResizeImage, schema_name=schema.name, schema_version=schema.version) == ResizeImage("img-1", 800, 600)
    with pytest.raises(PayloadDecodingError):
        decode_payload({"image_id": "img-1", "width": "800", "height": 600}, ResizePayload,
                       schema_name=f"{ResizePayload.__module__}.{ResizePayload.__qualname__}", schema_version="1")


def test_typeddict_required_and_not_required_fields_are_strict() -> None:
    payload, schema = normalize_payload(
        {"image_id": "img-1", "width": 800, "nested": {"image_id": "nested", "width": 1, "height": 2}},
        payload_type=OptionalResizePayload,
    )
    assert schema is not None
    assert decode_payload(payload, OptionalResizePayload,
                          schema_name=schema.name, schema_version=schema.version) == payload
    assert normalize_payload({"image_id": "img-1"}, payload_type=OptionalResizePayload)[0] == {"image_id": "img-1"}
    with pytest.raises(ValidationError):
        normalize_payload({"width": 800}, payload_type=OptionalResizePayload)
    with pytest.raises(ValidationError):
        normalize_payload({"image_id": "img-1", "width": "800"}, payload_type=OptionalResizePayload)


def test_pydantic_v2_payload_round_trips_json_values_in_strict_mode() -> None:
    pydantic = pytest.importorskip("pydantic")
    Model = pydantic.create_model(
        "Model", image_id=(str, ...), created_at=(datetime, ...), width=(int, ...),
    )

    original = Model(image_id="img-1", created_at=datetime(2026, 1, 1, tzinfo=timezone.utc), width=800)
    payload, schema = normalize_payload(original)
    assert schema is not None
    assert decode_payload(payload, Model, schema_name=schema.name, schema_version=schema.version) == original
    with pytest.raises(PayloadDecodingError):
        decode_payload({**payload, "width": "800"}, Model, schema_name=schema.name, schema_version=schema.version)


@pytest.mark.asyncio
async def test_typed_worker_receives_dataclass_and_sends_invalid_payload_to_dlq() -> None:
    received: list[ResizeImage] = []

    async def handler(message) -> None:  # type: ignore[no-untyped-def]
        assert isinstance(message.payload, ResizeImage)
        received.append(message.payload)

    async with SQLiteBroker() as broker:
        worker = broker.worker("images", handler, payload_type=ResizeImage,
                               options=ConsumerOptions(lease_seconds=1, poll_interval=0.001))
        await worker.start()
        await broker.submit(queue="images", payload=ResizeImage("img-1", 800, 600))
        await _wait_for(broker, acked=1)
        await broker.submit(queue="images", payload={"image_id": "img-2", "width": 800, "height": 600})
        await _wait_for(broker, acked=1, dead_letters=1)
        await worker.close()
        dead_letter = (await broker.admin.list_dead_letters("images"))[0]

    assert received == [ResizeImage("img-1", 800, 600)]
    assert dead_letter.message.payload == {"image_id": "img-2", "width": 800, "height": 600}
    assert dead_letter.reason == "poison_payload"


@pytest.mark.asyncio
async def test_typeddict_submit_marks_schema_and_reaches_typed_worker() -> None:
    received: list[ResizePayload] = []

    async def handler(message) -> None:  # type: ignore[no-untyped-def]
        received.append(message.payload)

    async with SQLiteBroker() as broker:
        worker = broker.worker("images", handler, payload_type=ResizePayload,
                               options=ConsumerOptions(lease_seconds=1, poll_interval=0.001))
        await worker.start()
        await broker.submit(queue="images", payload={"image_id": "typed", "width": 10, "height": 20},
                            payload_type=ResizePayload)
        await _wait_for(broker, acked=1)
        await worker.close()
    assert received == [{"image_id": "typed", "width": 10, "height": 20}]


@pytest.mark.asyncio
async def test_pydantic_typed_worker_success_poison_and_schema_mismatch() -> None:
    pydantic = pytest.importorskip("pydantic")
    Nested = pydantic.create_model("Nested", value=(int, ...))
    Model = pydantic.create_model(
        "ImageJob", image_id=(str, ...), created_at=(datetime, ...), nested=(Nested, ...), note=(str | None, None),
    )
    WrongVersion = pydantic.create_model(
        "ImageJob", image_id=(str, ...), created_at=(datetime, ...), nested=(Nested, ...), note=(str | None, None),
    )
    WrongVersion.__taskflow_schema_version__ = "2"
    received: list[object] = []
    handled = asyncio.Event()

    async def handler(message) -> None:  # type: ignore[no-untyped-def]
        received.append(message.payload)
        handled.set()

    async with SQLiteBroker() as broker:
        worker = broker.worker("images", handler, payload_type=Model,
                               options=ConsumerOptions(lease_seconds=1, poll_interval=0.001))
        await worker.start()
        await broker.submit(queue="images", payload=Model(
            image_id="ok", created_at=datetime(2026, 1, 1, tzinfo=timezone.utc), nested={"value": 1}, note=None,
        ))
        await asyncio.wait_for(handled.wait(), timeout=1)
        await worker.close()
        assert len(received) == 1 and isinstance(received[0], Model)

        mismatch_worker = broker.worker("images", handler, payload_type=WrongVersion,
                                        options=ConsumerOptions(lease_seconds=1, poll_interval=0.001))
        await mismatch_worker.start()
        await broker.submit(queue="images", payload=Model(
            image_id="bad", created_at=datetime(2026, 1, 1, tzinfo=timezone.utc), nested={"value": 2}, note=None,
        ))
        await _wait_for(broker, acked=1, dead_letters=1)
        await mismatch_worker.close()
        letters = await broker.admin.list_dead_letters("images")

    assert letters[0].reason == "poison_payload"


@pytest.mark.asyncio
async def test_non_atomic_batch_returns_every_item_and_continues_after_store_failure(tmp_path: Path) -> None:
    ids = iter(("first", "first", "third"))
    async with SQLiteBroker(tmp_path / "batch.db", id_factory=lambda: next(ids)) as broker:
        results = await broker.submit_many([
            SubmitRequest(queue="images", payload={"n": 1}),
            SubmitRequest(queue="images", payload={"n": 2}),
            SubmitRequest(queue="images", payload={"n": 3}),
        ], atomic=False)
        assert [item.index for item in results] == [0, 1, 2]
        assert results[0].result is not None and results[0].result.accepted
        assert isinstance(results[1].error, sqlite3.IntegrityError)
        assert results[2].result is not None and results[2].result.accepted
        message = await broker.inspect_message("first")
        assert message is not None and message.payload == {"n": 1}
        message = await broker.inspect_message("third")
        assert message is not None and message.payload == {"n": 3}


@pytest.mark.asyncio
async def test_dead_letter_replay_normalizes_typed_payload_and_replaces_schema() -> None:
    async with SQLiteBroker() as broker:
        submitted = await broker.submit(queue="images", payload=ResizeImage("broken", 1, 2))
        delivery = await broker.consumer("images").__anext__()
        await delivery.reject(reason="repair")

        await broker.admin.replay_dead_letter(
            "images", submitted.message_id,
            payload={"image_id": "repaired", "width": 10, "height": 20},
            payload_type=ResizePayload,
            dedup_mode="remove",
        )
        replayed = await broker.inspect_message(submitted.message_id)

    assert replayed is not None
    assert replayed.payload == {"image_id": "repaired", "width": 10, "height": 20}
    assert replayed.payload_schema_name == f"{ResizePayload.__module__}.{ResizePayload.__qualname__}"
    assert replayed.payload_schema_version == "1"

    async with SQLiteBroker() as broker:
        submitted = await broker.submit(queue="images", payload=ResizeImage("typed", 1, 2))
        delivery = await broker.consumer("images").__anext__()
        await delivery.reject(reason="repair")
        await broker.admin.replay_dead_letter(
            "images", submitted.message_id,
            payload={"image_id": "raw", "width": 3, "height": 4},
            dedup_mode="remove",
        )
        raw_replay = await broker.inspect_message(submitted.message_id)

    assert raw_replay is not None
    assert raw_replay.payload_schema_name is None
    assert raw_replay.payload_schema_version is None


@pytest.mark.asyncio
async def test_dead_letter_replay_preserves_v03_none_payload_semantics_and_can_explicitly_replace_with_null() -> None:
    async with SQLiteBroker() as broker:
        submitted = await broker.submit(queue="images", payload=ResizeImage("kept", 1, 2))
        delivery = await broker.consumer("images").__anext__()
        await delivery.reject(reason="repair")

        await broker.admin.replay_dead_letter("images", submitted.message_id, payload=None, dedup_mode="remove")
        preserved = await broker.inspect_message(submitted.message_id)
        assert preserved is not None
        assert preserved.payload == {"image_id": "kept", "width": 1, "height": 2}
        assert preserved.payload_schema_name == f"{ResizeImage.__module__}.{ResizeImage.__qualname__}"

        delivery = await broker.consumer("images").__anext__()
        await delivery.reject(reason="repair again")
        await broker.admin.replay_dead_letter(
            "images", submitted.message_id, payload=None, replace_payload=True, dedup_mode="remove",
        )
        replaced = await broker.inspect_message(submitted.message_id)

    assert replaced is not None
    assert replaced.payload is None
    assert replaced.payload_schema_name is None
    assert replaced.payload_schema_version is None


def test_replay_dedup_mode_is_explicit_and_compatible() -> None:
    from taskflow.admin import resolve_replay_dedup_mode

    assert resolve_replay_dedup_mode(dedup_mode="keep", reuse_dedup=None, has_replacement=False) == "keep"
    assert resolve_replay_dedup_mode(dedup_mode="remove", reuse_dedup=None, has_replacement=False) == "remove"
    assert resolve_replay_dedup_mode(dedup_mode="replace", reuse_dedup=False, has_replacement=True) == "replace"
    with pytest.raises(ValidationError):
        resolve_replay_dedup_mode(dedup_mode="keep", reuse_dedup=False, has_replacement=False)
    with pytest.raises(ValidationError):
        resolve_replay_dedup_mode(dedup_mode="replace", reuse_dedup=None, has_replacement=False)


def test_cli_is_read_only_by_default_and_redacts_payload(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from taskflow.cli import main

    database = tmp_path / "cli.db"
    assert main(["--sqlite", str(database), "--json", "health"]) == 0
    assert '"healthy": true' in capsys.readouterr().out
    assert main(["--sqlite", str(database), "dlq", "replay", "images", "missing"]) == 2


def test_cli_redaction_include_payload_and_confirmed_replay(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from taskflow.cli import main

    database = tmp_path / "cli-replay.db"

    async def seed() -> str:
        async with SQLiteBroker(database) as broker:
            submitted = await broker.submit(queue="images", payload={"secret": "keep-private"})
            delivery = await broker.consumer("images").__anext__()
            await delivery.reject(reason="repair")
            return submitted.message_id

    message_id = asyncio.run(seed())
    assert main(["--sqlite", str(database), "--json", "message", "inspect", message_id]) == 0
    assert '"payload": "<redacted>"' in capsys.readouterr().out
    assert main(["--sqlite", str(database), "--json", "--include-payload", "message", "inspect", message_id]) == 0
    assert '"secret": "keep-private"' in capsys.readouterr().out
    assert main(["--sqlite", str(database), "dlq", "replay", "images", message_id]) == 2
    assert main(["--sqlite", str(database), "--json", "dlq", "replay", "images", message_id, "--yes"]) == 0
    output = capsys.readouterr().out
    assert '"backend": "sqlite"' in output and '"queue": "images"' in output


@pytest.mark.asyncio
async def test_expired_replay_can_atomically_move_to_target_queue() -> None:
    async with SQLiteBroker() as broker:
        submitted = await broker.submit(queue="images", payload={"id": "expired"},
                                        expires_at=utc_now() - timedelta(seconds=1))
        await broker.admin.replay_expired("images", submitted.message_id, expires_at=None,
                                          target_queue="images.repaired", dedup_mode="remove")
        delivery = await broker.consumer("images.repaired").__anext__()
        assert delivery.message.id == submitted.message_id
        await delivery.ack()
        assert not await broker.admin.list_expired("images")
