"""Versioned, transactional SQLite schema migrations."""
from __future__ import annotations

from typing import Final

import aiosqlite

from ..errors import ValidationError

CURRENT_SQLITE_SCHEMA_VERSION: Final = 2


async def apply_sqlite_migrations(connection: aiosqlite.Connection, *, fail_at_version: int | None = None) -> None:
    """Apply forward migrations in one transaction; test injection proves rollback."""

    cursor = await connection.cursor()
    await cursor.execute("BEGIN IMMEDIATE")
    try:
        await cursor.execute("CREATE TABLE IF NOT EXISTS taskflow_schema (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        row = await (await cursor.execute("SELECT value FROM taskflow_schema WHERE key='version'")).fetchone()
        version = int(row[0]) if row is not None else 1
        if version > CURRENT_SQLITE_SCHEMA_VERSION:
            raise ValidationError(f"SQLite schema {version} is newer than supported {CURRENT_SQLITE_SCHEMA_VERSION}")
        while version < CURRENT_SQLITE_SCHEMA_VERSION:
            next_version = version + 1
            if next_version == 2:
                await cursor.execute("CREATE TABLE IF NOT EXISTS taskflow_schema_migrations (version INTEGER PRIMARY KEY, applied_at REAL NOT NULL)")
            if fail_at_version == next_version:
                raise RuntimeError(f"injected SQLite migration failure at version {next_version}")
            await cursor.execute("INSERT OR IGNORE INTO taskflow_schema_migrations(version, applied_at) VALUES (?, unixepoch())", (next_version,))
            await cursor.execute("INSERT INTO taskflow_schema(key, value) VALUES ('version', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(next_version),))
            version = next_version
        await cursor.execute("COMMIT")
    except Exception:
        await cursor.execute("ROLLBACK")
        raise
