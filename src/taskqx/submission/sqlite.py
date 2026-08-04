"""SQLite SubmissionStore: transactional batch admission and exact dedup."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Protocol

import aiosqlite

from ..broker._time import datetime_from_timestamp as _datetime
from ..broker._time import timestamp as _timestamp
from ..capabilities import DedupGuarantee, SubmissionCapabilities
from ..types import MessageStatus, SubmitDecision, SubmitResult
from .base import PreparedSubmission


class SQLiteSubmissionBackend(Protocol):
    """Stable SQLite transaction hooks required by the submission Store."""

    _lock: asyncio.Lock
    _connection: aiosqlite.Connection | None

    async def start(self) -> None: ...
    async def _counter(
        self, cursor: aiosqlite.Cursor, queue: str, column: str
    ) -> None: ...
    async def _expire(
        self,
        cursor: aiosqlite.Cursor,
        message_id: str,
        now: datetime,
        old_status: MessageStatus,
        attempt: int,
    ) -> None: ...


class SQLiteSubmissionStore:
    """Commit dedup admission, initial message state and counters in one transaction."""

    capabilities = SubmissionCapabilities(
        dedup_guarantee=DedupGuarantee.EXACT,
        per_key_dedup_ttl=True,
        stores_original_message_id=True,
        atomic_submit=True,
        batch_submit=True,
        batch_atomic=True,
    )

    def __init__(self, broker: SQLiteSubmissionBackend) -> None:
        self._broker = broker

    async def submit(self, submission: PreparedSubmission) -> SubmitResult:
        results = await self._submit_all([submission])
        return results[0]

    async def submit_many(
        self, submissions: list[PreparedSubmission]
    ) -> list[SubmitResult]:
        """Run every per-message dedup and state write under one ``BEGIN IMMEDIATE``."""
        return await self._submit_all(submissions)

    async def _submit_all(
        self, submissions: list[PreparedSubmission]
    ) -> list[SubmitResult]:
        if not submissions:
            return []
        broker = self._broker
        await broker.start()
        results: list[SubmitResult] = []
        async with broker._lock:
            assert broker._connection is not None
            cursor = await broker._connection.cursor()
            try:
                await cursor.execute("BEGIN IMMEDIATE")
                for submission in submissions:
                    results.append(
                        await self._submit_in_transaction(cursor, submission)
                    )
                await cursor.execute("COMMIT")
            except Exception:
                await cursor.execute("ROLLBACK")
                raise
        return results

    async def _submit_in_transaction(
        self, cursor: aiosqlite.Cursor, submission: PreparedSubmission
    ) -> SubmitResult:
        broker, now = self._broker, submission.created_at
        if submission.dedup_key is not None:
            assert (
                submission.dedup_scope is not None
                and submission.dedup_ttl_ms is not None
            )
            await cursor.execute(
                "DELETE FROM dedup_records WHERE scope=? AND dedup_key=? AND expires_at<=?",
                (submission.dedup_scope, submission.dedup_key, _timestamp(now)),
            )
            existing = await (
                await cursor.execute(
                    "SELECT message_id, expires_at FROM dedup_records WHERE scope=? AND dedup_key=?",
                    (submission.dedup_scope, submission.dedup_key),
                )
            ).fetchone()
            if existing is not None:
                return SubmitResult(
                    existing["message_id"],
                    False,
                    SubmitDecision.DUPLICATE,
                    existing["message_id"],
                    dedup_expires_at=_datetime(existing["expires_at"]),
                )
            await cursor.execute(
                "INSERT INTO dedup_records VALUES (?, ?, ?, ?)",
                (
                    submission.dedup_scope,
                    submission.dedup_key,
                    submission.message_id,
                    _timestamp(now + timedelta(milliseconds=submission.dedup_ttl_ms)),
                ),
            )
        await cursor.execute(
            """
            INSERT INTO messages (id, queue, envelope, serializer_name, serializer_version, status, attempt,
                max_attempts, created_at, available_at, expires_at, workflow_id, parent_id)
            VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?)
            """,
            (
                submission.message_id,
                submission.queue,
                submission.envelope,
                submission.serializer_name,
                submission.serializer_version,
                submission.status,
                submission.max_attempts,
                _timestamp(now),
                submission.available_at_ms / 1000
                if submission.available_at_ms is not None
                else None,
                submission.expires_at_ms / 1000
                if submission.expires_at_ms is not None
                else None,
                submission.workflow_id,
                submission.parent_id,
            ),
        )
        await broker._counter(cursor, submission.queue, "submitted_total")
        if submission.status == MessageStatus.EXPIRED.value:
            await broker._expire(
                cursor, submission.message_id, now, MessageStatus.READY, 0
            )
        return SubmitResult(
            submission.message_id,
            True,
            SubmitDecision.ACCEPTED,
            dedup_expires_at=(
                now + timedelta(milliseconds=submission.dedup_ttl_ms)
                if submission.dedup_ttl_ms is not None
                else None
            ),
        )
