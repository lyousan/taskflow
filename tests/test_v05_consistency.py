"""v0.5 integrity reports and explicit repair behavior."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from taskqx import SQLiteBroker
from taskqx.broker.sqlite_migrations import apply_sqlite_migrations


@pytest.mark.asyncio
async def test_sqlite_consistency_dry_run_and_explicit_repair() -> None:
    async with SQLiteBroker() as broker:
        submitted = await broker.submit(queue="jobs", payload={})
        delivery = await broker.consumer("jobs").__anext__()
        await delivery.reject(reason="broken")
        assert broker._connection is not None
        await broker._connection.execute(
            "DELETE FROM dead_letters WHERE message_id=?", (submitted.message_id,)
        )
        await broker._connection.commit()

        report = await broker.check_consistency("jobs")
        assert not report.consistent
        assert ("missing_dead_letter", submitted.message_id) in {
            (issue.name, issue.message_id) for issue in report.issues
        }
        proposed = await broker.repair_consistency("jobs")
        assert proposed.dry_run
        assert not (await broker.check_consistency("jobs")).consistent
        applied = await broker.repair_consistency("jobs", dry_run=False)
        assert not applied.dry_run
        assert (await broker.check_consistency("jobs")).consistent


def test_cli_consistency_requires_explicit_confirmation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from taskqx.cli import main

    database = tmp_path / "consistency-cli.db"

    async def corrupt() -> None:
        async with SQLiteBroker(database) as broker:
            submitted = await broker.submit(queue="jobs", payload={})
            delivery = await broker.consumer("jobs").__anext__()
            await delivery.reject(reason="broken")
            assert broker._connection is not None
            await broker._connection.execute(
                "DELETE FROM dead_letters WHERE message_id=?", (submitted.message_id,)
            )
            await broker._connection.commit()

    asyncio.run(corrupt())
    assert main(["--sqlite", str(database), "queue", "check-consistency", "jobs"]) == 1
    assert (
        main(
            [
                "--sqlite",
                str(database),
                "queue",
                "repair-consistency",
                "jobs",
                "--apply",
            ]
        )
        == 2
    )
    assert (
        main(
            [
                "--sqlite",
                str(database),
                "queue",
                "repair-consistency",
                "jobs",
                "--apply",
                "--yes",
            ]
        )
        == 0
    )
    assert main(["--sqlite", str(database), "queue", "check-consistency", "jobs"]) == 0
    assert "missing_dead_letter" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_sqlite_migration_failure_rolls_back_and_can_resume() -> None:
    async with SQLiteBroker() as broker:
        assert broker._connection is not None
        await broker._connection.execute(
            "UPDATE taskqx_schema SET value='1' WHERE key='version'"
        )
        await broker._connection.execute("DROP TABLE taskqx_schema_migrations")
        await broker._connection.commit()
        with pytest.raises(RuntimeError, match="injected SQLite migration failure"):
            await apply_sqlite_migrations(broker._connection, fail_at_version=2)
        version = await (
            await broker._connection.execute(
                "SELECT value FROM taskqx_schema WHERE key='version'"
            )
        ).fetchone()
        assert version is not None and version[0] == "1"
        assert (
            await (
                await broker._connection.execute(
                    "SELECT name FROM sqlite_master WHERE name='taskqx_schema_migrations'"
                )
            ).fetchone()
            is None
        )
        await apply_sqlite_migrations(broker._connection)
        version = await (
            await broker._connection.execute(
                "SELECT value FROM taskqx_schema WHERE key='version'"
            )
        ).fetchone()
        assert version is not None and version[0] == "2"
