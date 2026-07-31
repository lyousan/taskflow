"""SQLite-specific implementation of the public DLQ/EQ administration API."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

import aiosqlite

from ..admin import resolve_replay_dedup_mode
from ..errors import ValidationError
from ..pagination import decode_cursor, encode_cursor, validate_page_limit
from ..payloads import PAYLOAD_UNSET
from ..types import (
    DeadLetter,
    ExpiredMessage,
    MessageStatus,
    Page,
    TaskMessage,
    utc_now,
)
from ._time import datetime_from_timestamp as _datetime
from ._time import timestamp as _timestamp

if TYPE_CHECKING:
    from .sqlite import SQLiteBroker


class SQLiteAdmin:
    """DLQ 与 EQ 的显式、可审计管理接口。"""

    def __init__(self, broker: SQLiteBroker) -> None:
        self._broker = broker

    async def list_dead_letters(self, queue: str) -> list[DeadLetter]:
        await self._broker.start()
        async with self._broker._lock:
            assert self._broker._connection is not None
            rows = await (
                await self._broker._connection.execute(
                    "SELECT d.*, m.envelope, m.serializer_name, m.serializer_version "
                    "FROM dead_letters d JOIN messages m ON m.id=d.message_id "
                    "WHERE d.queue=? ORDER BY d.failed_at",
                    (queue,),
                )
            ).fetchall()
        return [
            DeadLetter(
                self._broker._decode_message(
                    row["envelope"], row["serializer_name"], row["serializer_version"]
                ),
                row["attempt"],
                row["reason"],
                row["source"],
                _datetime(row["failed_at"]) or utc_now(),
                row["error_type"],
                row["traceback"],
            )
            for row in rows
        ]

    async def list_expired(self, queue: str) -> list[ExpiredMessage]:
        await self._broker.start()
        async with self._broker._lock:
            assert self._broker._connection is not None
            rows = await (
                await self._broker._connection.execute(
                    "SELECT e.*, m.envelope, m.serializer_name, m.serializer_version "
                    "FROM expired_messages e JOIN messages m ON m.id=e.message_id "
                    "WHERE e.queue=? ORDER BY e.expired_at",
                    (queue,),
                )
            ).fetchall()
        return [
            ExpiredMessage(
                self._broker._decode_message(
                    row["envelope"], row["serializer_name"], row["serializer_version"]
                ),
                MessageStatus(row["status_at_expiry"]),
                _datetime(row["expired_at"]) or utc_now(),
                row["attempt"],
            )
            for row in rows
        ]

    async def page_dead_letters(
        self, queue: str, *, cursor: str | None = None, limit: int = 100
    ) -> Page[DeadLetter]:
        """Read a bounded, newest-first DLQ page."""

        rows, next_cursor, total = await self._page_rows(
            queue,
            cursor=cursor,
            limit=limit,
            table="dead_letters",
            timestamp="failed_at",
        )
        items = tuple(
            DeadLetter(
                self._broker._decode_message(
                    row["envelope"], row["serializer_name"], row["serializer_version"]
                ),
                row["attempt"],
                row["reason"],
                row["source"],
                _datetime(row["failed_at"]) or utc_now(),
                row["error_type"],
                row["traceback"],
            )
            for row in rows
        )
        return Page(items, next_cursor, total)

    async def page_expired(
        self, queue: str, *, cursor: str | None = None, limit: int = 100
    ) -> Page[ExpiredMessage]:
        """Read a bounded, newest-first expired-message page."""

        rows, next_cursor, total = await self._page_rows(
            queue,
            cursor=cursor,
            limit=limit,
            table="expired_messages",
            timestamp="expired_at",
        )
        items = tuple(
            ExpiredMessage(
                self._broker._decode_message(
                    row["envelope"], row["serializer_name"], row["serializer_version"]
                ),
                MessageStatus(row["status_at_expiry"]),
                _datetime(row["expired_at"]) or utc_now(),
                row["attempt"],
            )
            for row in rows
        )
        return Page(items, next_cursor, total)

    async def _page_rows(
        self, queue: str, *, cursor: str | None, limit: int, table: str, timestamp: str
    ) -> tuple[list[aiosqlite.Row], str | None, int]:
        limit = validate_page_limit(limit)
        cursor_values = decode_cursor(cursor, size=2)
        parameters: list[Any] = [queue]
        cursor_sql = ""
        if cursor_values is not None:
            record_time, message_id = cursor_values
            if (
                isinstance(record_time, bool)
                or not isinstance(record_time, (int, float))
                or not isinstance(message_id, str)
            ):
                raise ValidationError("cursor 不属于管理列表")
            cursor_sql = (
                f" AND ({timestamp} < ? OR ({timestamp} = ? AND message_id < ?))"
            )
            parameters.extend((record_time, record_time, message_id))
        await self._broker.start()
        async with self._broker._lock:
            assert self._broker._connection is not None
            rows = list(
                await (
                    await self._broker._connection.execute(
                        f"SELECT r.*, m.envelope, m.serializer_name, m.serializer_version "
                        f"FROM {table} r JOIN messages m ON m.id=r.message_id "
                        f"WHERE r.queue=?{cursor_sql} "
                        f"ORDER BY r.{timestamp} DESC, r.message_id DESC LIMIT ?",
                        (*parameters, limit + 1),
                    )
                ).fetchall()
            )
            total_row = await (
                await self._broker._connection.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE queue=?", (queue,)
                )
            ).fetchone()
            assert total_row is not None
        next_cursor = (
            encode_cursor(rows[limit - 1][timestamp], rows[limit - 1]["message_id"])
            if len(rows) > limit
            else None
        )
        return rows[:limit], next_cursor, int(total_row[0])

    async def delete_dead_letter(self, queue: str, message_id: str) -> bool:
        await self._broker.start()
        async with self._broker._lock:
            assert self._broker._connection is not None
            cursor = await self._broker._connection.execute(
                "DELETE FROM dead_letters WHERE queue=? AND message_id=?",
                (queue, message_id),
            )
            return cursor.rowcount > 0

    async def delete_expired(self, queue: str, message_id: str) -> bool:
        await self._broker.start()
        async with self._broker._lock:
            assert self._broker._connection is not None
            cursor = await self._broker._connection.execute(
                "DELETE FROM expired_messages WHERE queue=? AND message_id=?",
                (queue, message_id),
            )
            return cursor.rowcount > 0

    async def delete_queue(self, queue: str) -> int:
        """Permanently remove a queue and every message-related record it owns."""

        await self._broker.start()
        async with self._broker._lock:
            assert self._broker._connection is not None
            cursor = await self._broker._connection.cursor()
            try:
                await cursor.execute("BEGIN")
                row = await (
                    await cursor.execute(
                        "SELECT COUNT(*) FROM messages WHERE queue=?", (queue,)
                    )
                ).fetchone()
                assert row is not None
                await cursor.execute(
                    "DELETE FROM dedup_records WHERE message_id IN "
                    "(SELECT id FROM messages WHERE queue=?)",
                    (queue,),
                )
                await cursor.execute("DELETE FROM dead_letters WHERE queue=?", (queue,))
                await cursor.execute(
                    "DELETE FROM expired_messages WHERE queue=?", (queue,)
                )
                await cursor.execute("DELETE FROM messages WHERE queue=?", (queue,))
                await cursor.execute(
                    "DELETE FROM queue_counters WHERE queue=?", (queue,)
                )
                await cursor.execute("COMMIT")
            except Exception:
                await cursor.execute("ROLLBACK")
                raise
        return int(row[0])

    def _replay_dedup(
        self,
        message: TaskMessage,
        *,
        reuse_dedup: bool,
        dedup_scope: str | None,
        dedup_key: str | None,
        dedup_ttl: timedelta | None,
    ) -> tuple[TaskMessage, tuple[str, str, timedelta] | None]:
        has_override = (
            dedup_scope is not None or dedup_key is not None or dedup_ttl is not None
        )
        if reuse_dedup and has_override:
            raise ValidationError("reuse_dedup=True 时不能同时指定新的 dedup 参数")
        if reuse_dedup:
            return message, None
        if (dedup_scope is None) != (dedup_key is None):
            raise ValidationError("dedup_scope 与 dedup_key 必须同时提供")
        if dedup_key is None:
            return replace(message, dedup_scope=None, dedup_key=None), None
        ttl = self._broker._default_dedup_ttl if dedup_ttl is None else dedup_ttl
        if ttl is None or ttl.total_seconds() <= 0:
            raise ValidationError("替换 dedup 时必须提供正数 dedup_ttl 或配置默认值")
        assert dedup_scope is not None
        return replace(message, dedup_scope=dedup_scope, dedup_key=dedup_key), (
            dedup_scope,
            dedup_key,
            ttl,
        )

    async def _apply_replay_dedup(
        self,
        cursor: aiosqlite.Cursor,
        message_id: str,
        replacement: tuple[str, str, timedelta] | None,
        now: datetime,
        *,
        reuse_dedup: bool,
    ) -> None:
        if reuse_dedup:
            return
        await cursor.execute(
            "DELETE FROM dedup_records WHERE message_id=?", (message_id,)
        )
        if replacement is None:
            return
        scope, key, ttl = replacement
        await cursor.execute(
            "DELETE FROM dedup_records WHERE scope=? AND dedup_key=? AND expires_at<=?",
            (scope, key, _timestamp(now)),
        )
        existing = await (
            await cursor.execute(
                "SELECT message_id FROM dedup_records WHERE scope=? AND dedup_key=?",
                (scope, key),
            )
        ).fetchone()
        if existing is not None and existing["message_id"] != message_id:
            raise ValidationError("新的 dedup key 已关联到其他消息")
        await cursor.execute(
            "INSERT INTO dedup_records(scope, dedup_key, message_id, expires_at) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(scope, dedup_key) DO UPDATE SET message_id=excluded.message_id, expires_at=excluded.expires_at",
            (scope, key, message_id, _timestamp(now + ttl)),
        )

    async def replay_dead_letter(
        self,
        queue: str,
        message_id: str,
        *,
        reset_attempt: bool = True,
        target_queue: str | None = None,
        payload: Any = None,
        replace_payload: bool = False,
        payload_type: type[Any] | None = None,
        metadata: Mapping[str, Any] | None = None,
        reuse_dedup: bool | None = None,
        dedup_mode: str | None = None,
        dedup_scope: str | None = None,
        dedup_key: str | None = None,
        dedup_ttl: timedelta | None = None,
    ) -> None:
        await self._broker.start()
        mode = resolve_replay_dedup_mode(
            dedup_mode=dedup_mode,
            reuse_dedup=reuse_dedup,
            has_replacement=dedup_scope is not None
            or dedup_key is not None
            or dedup_ttl is not None,
        )
        async with self._broker._lock:
            assert self._broker._connection is not None
            cursor = await self._broker._connection.cursor()
            await cursor.execute("BEGIN IMMEDIATE")
            try:
                row = await (
                    await cursor.execute(
                        "SELECT m.* FROM messages m JOIN dead_letters d ON d.message_id=m.id "
                        "WHERE d.queue=? AND m.id=?",
                        (queue, message_id),
                    )
                ).fetchone()
                if row is None:
                    raise ValidationError("未找到指定死信")
                message = self._broker._decode_message(
                    row["envelope"], row["serializer_name"], row["serializer_version"]
                )
                replayed = self._broker._reconstruct_replay_message(
                    message,
                    queue=target_queue or message.queue,
                    payload=payload
                    if replace_payload or payload is not None
                    else PAYLOAD_UNSET,
                    payload_type=payload_type,
                    metadata=metadata,
                )
                replayed, replacement = self._replay_dedup(
                    replayed,
                    reuse_dedup=mode == "keep",
                    dedup_scope=dedup_scope,
                    dedup_key=dedup_key,
                    dedup_ttl=dedup_ttl,
                )
                await self._apply_replay_dedup(
                    cursor,
                    message_id,
                    replacement,
                    self._broker._now(),
                    reuse_dedup=mode == "keep",
                )
                await cursor.execute(
                    "UPDATE messages SET queue=?, envelope=?, status=?, attempt=?, consumer_id=NULL, "
                    "delivery_id=NULL, lease_token=NULL, claimed_at=NULL, lease_until=NULL, "
                    "last_action='replayed' WHERE id=?",
                    (
                        replayed.queue,
                        self._broker._message_json(replayed),
                        MessageStatus.READY.value,
                        0 if reset_attempt else row["attempt"],
                        message_id,
                    ),
                )
                await cursor.execute(
                    "DELETE FROM dead_letters WHERE message_id=?", (message_id,)
                )
                await cursor.execute("COMMIT")
            except Exception:
                await cursor.execute("ROLLBACK")
                raise

    async def replay_expired(
        self,
        queue: str,
        message_id: str,
        *,
        expires_at: datetime | None,
        reuse_dedup: bool | None = None,
        dedup_mode: str | None = None,
        target_queue: str | None = None,
        dedup_scope: str | None = None,
        dedup_key: str | None = None,
        dedup_ttl: timedelta | None = None,
    ) -> None:
        if expires_at is not None and expires_at.tzinfo is None:
            raise ValidationError("expires_at 必须带时区")
        await self._broker.start()
        mode = resolve_replay_dedup_mode(
            dedup_mode=dedup_mode,
            reuse_dedup=reuse_dedup,
            has_replacement=dedup_scope is not None
            or dedup_key is not None
            or dedup_ttl is not None,
        )
        async with self._broker._lock:
            assert self._broker._connection is not None
            cursor = await self._broker._connection.cursor()
            await cursor.execute("BEGIN IMMEDIATE")
            try:
                row = await (
                    await cursor.execute(
                        "SELECT m.* FROM messages m JOIN expired_messages e ON e.message_id=m.id "
                        "WHERE e.queue=? AND m.id=?",
                        (queue, message_id),
                    )
                ).fetchone()
                if row is None:
                    raise ValidationError("未找到指定过期消息")
                message = self._broker._reconstruct_replay_message(
                    self._broker._decode_message(
                        row["envelope"],
                        row["serializer_name"],
                        row["serializer_version"],
                    ),
                    queue=target_queue or row["queue"],
                    expires_at=expires_at,
                )
                message, replacement = self._replay_dedup(
                    message,
                    reuse_dedup=mode == "keep",
                    dedup_scope=dedup_scope,
                    dedup_key=dedup_key,
                    dedup_ttl=dedup_ttl,
                )
                await self._apply_replay_dedup(
                    cursor,
                    message_id,
                    replacement,
                    self._broker._now(),
                    reuse_dedup=mode == "keep",
                )
                await cursor.execute(
                    "UPDATE messages SET queue=?, envelope=?, status=?, expires_at=?, consumer_id=NULL, "
                    "delivery_id=NULL, lease_token=NULL, claimed_at=NULL, lease_until=NULL, "
                    "last_action='replayed' WHERE id=?",
                    (
                        message.queue,
                        self._broker._message_json(message),
                        MessageStatus.READY.value,
                        _timestamp(expires_at) if expires_at else None,
                        message_id,
                    ),
                )
                await cursor.execute(
                    "DELETE FROM expired_messages WHERE message_id=?", (message_id,)
                )
                await cursor.execute("COMMIT")
            except Exception:
                await cursor.execute("ROLLBACK")
                raise
