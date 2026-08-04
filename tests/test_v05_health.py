"""v0.5 structured broker health diagnostics."""

from __future__ import annotations

from pathlib import Path

import pytest

from taskqx import SQLiteBroker
from tests.support import BinaryJsonSerializer


def checks_by_name(report):  # type: ignore[no-untyped-def]
    return {check.name: check for check in report.checks}


@pytest.mark.asyncio
async def test_sqlite_health_reports_all_release_diagnostics() -> None:
    async with SQLiteBroker() as broker:
        report = await broker.health_check()

    checks = checks_by_name(report)
    assert report.healthy
    assert report.backend == "sqlite"
    assert report.namespace is None
    assert {
        "connection",
        "schema_version",
        "required_indexes",
        "serializer_registry",
        "namespace",
        "unrecoverable_errors",
    } <= checks.keys()
    assert all(check.status == "ok" for check in checks.values())


@pytest.mark.asyncio
async def test_sqlite_health_finds_missing_index_and_schema_mismatch() -> None:
    async with SQLiteBroker() as broker:
        assert broker._connection is not None
        await broker._connection.execute("DROP INDEX idx_messages_claim")
        await broker._connection.execute(
            "UPDATE taskqx_schema SET value='999' WHERE key='version'"
        )
        await broker._connection.commit()

        report = await broker.health_check()

    checks = checks_by_name(report)
    assert not report.healthy
    assert checks["required_indexes"].status == "error"
    assert checks["schema_version"].status == "error"


@pytest.mark.asyncio
async def test_sqlite_health_identifies_messages_with_unavailable_serializer(
    tmp_path: Path,
) -> None:
    database = tmp_path / "serializer-health.db"
    async with SQLiteBroker(database, serializer=BinaryJsonSerializer()) as writer:
        await writer.submit(queue="jobs", payload={"id": 1})

    async with SQLiteBroker(database) as reader:
        report = await reader.health_check()

    checks = checks_by_name(report)
    assert not report.healthy
    assert checks["serializer_registry"].status == "error"
    assert "binary-json@7" in (checks["serializer_registry"].detail or "")
    assert checks["unrecoverable_errors"].status == "error"
