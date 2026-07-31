"""Redis-specific implementation of the public DLQ/EQ administration API."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

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
from .redis_calls import replay_dead_letter_call, replay_expired_call

if TYPE_CHECKING:
    from .redis import RedisBroker


class RedisAdmin:
    """Redis DLQ/EQ 的查询、删除与重放接口。"""

    def __init__(self, broker: RedisBroker) -> None:
        self._broker = broker

    async def list_dead_letters(self, queue: str) -> list[DeadLetter]:
        result: list[DeadLetter] = []
        for message_id in await self._broker._redis.lrange(
            self._broker._queue_key(queue, "dlq"), 0, -1
        ):
            data = await self._broker._redis.hgetall(
                self._broker._message_key(message_id)
            )
            result.append(
                DeadLetter(
                    self._broker._decode(
                        data["envelope"],
                        data.get("serializer_name"),
                        data.get("serializer_version"),
                    ),
                    int(data["attempt"]),
                    data.get("last_reason"),
                    data.get("dead_source", "reject"),
                    _datetime(float(data.get("failed_at", 0))) or utc_now(),
                    data.get("error_type") or None,
                )
            )
        return result

    async def list_expired(self, queue: str) -> list[ExpiredMessage]:
        result: list[ExpiredMessage] = []
        for message_id in await self._broker._redis.lrange(
            self._broker._queue_key(queue, "eq"), 0, -1
        ):
            data = await self._broker._redis.hgetall(
                self._broker._message_key(message_id)
            )
            result.append(
                ExpiredMessage(
                    self._broker._decode(
                        data["envelope"],
                        data.get("serializer_name"),
                        data.get("serializer_version"),
                    ),
                    MessageStatus(data["status_at_expiry"]),
                    _datetime(float(data["expired_at"])) or utc_now(),
                    int(data.get("attempt", 0)),
                )
            )
        return result

    async def page_dead_letters(
        self, queue: str, *, cursor: str | None = None, limit: int = 100
    ) -> Page[DeadLetter]:
        """Read a bounded newest-first DLQ list page."""

        records, next_cursor, total = await self._page_records(
            queue, "dlq", cursor=cursor, limit=limit
        )
        items = tuple(
            DeadLetter(
                self._broker._decode(
                    data["envelope"],
                    data.get("serializer_name"),
                    data.get("serializer_version"),
                ),
                int(data["attempt"]),
                data.get("last_reason"),
                data.get("dead_source", "reject"),
                _datetime(float(data.get("failed_at", 0))) or utc_now(),
                data.get("error_type") or None,
            )
            for data in records
        )
        return Page(items, next_cursor, total)

    async def page_expired(
        self, queue: str, *, cursor: str | None = None, limit: int = 100
    ) -> Page[ExpiredMessage]:
        """Read a bounded newest-first expired-message list page."""

        records, next_cursor, total = await self._page_records(
            queue, "eq", cursor=cursor, limit=limit
        )
        items = tuple(
            ExpiredMessage(
                self._broker._decode(
                    data["envelope"],
                    data.get("serializer_name"),
                    data.get("serializer_version"),
                ),
                MessageStatus(data["status_at_expiry"]),
                _datetime(float(data["expired_at"])) or utc_now(),
                int(data.get("attempt", 0)),
            )
            for data in records
        )
        return Page(items, next_cursor, total)

    async def _page_records(
        self, queue: str, kind: str, *, cursor: str | None, limit: int
    ) -> tuple[list[Mapping[str, str]], str | None, int]:
        limit = validate_page_limit(limit)
        values = decode_cursor(cursor, size=1)
        offset = 0 if values is None else values[0]
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValidationError("cursor 不属于 Redis 管理列表")
        key = self._broker._queue_key(queue, kind)
        message_ids, total = (
            await self._broker._redis.lrange(key, offset, offset + limit - 1),
            await self._broker._redis.llen(key),
        )
        records = [
            await self._broker._redis.hgetall(self._broker._message_key(message_id))
            for message_id in message_ids
        ]
        next_offset = offset + len(records)
        next_cursor = encode_cursor(next_offset) if next_offset < total else None
        return records, next_cursor, int(total)

    async def delete_dead_letter(self, queue: str, message_id: str) -> bool:
        return bool(
            await self._broker._redis.lrem(
                self._broker._queue_key(queue, "dlq"), 0, message_id
            )
        )

    async def delete_expired(self, queue: str, message_id: str) -> bool:
        return bool(
            await self._broker._redis.lrem(
                self._broker._queue_key(queue, "eq"), 0, message_id
            )
        )

    async def delete_queue(self, queue: str) -> int:
        """Permanently remove a queue and every message-related Redis key it owns."""

        self._broker._validate_queue(queue)
        message_ids: set[str] = set()
        message_keys: list[str] = []
        async for raw_key in self._broker._redis.scan_iter(
            match=f"{self._broker._namespace}:message:*"
        ):
            key = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
            fields = await self._broker._redis.hgetall(key)
            stored_queue = fields.get("queue", fields.get(b"queue"))
            if stored_queue == queue or stored_queue == queue.encode():
                message_keys.append(key)
                message_ids.add(key.removeprefix(f"{self._broker._namespace}:message:"))

        dedup_keys: list[str] = []
        async for raw_key in self._broker._redis.scan_iter(
            match=f"{self._broker._namespace}:dedup:*"
        ):
            key = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
            message_id = await self._broker._redis.get(key)
            if message_id in message_ids or message_id in {
                value.encode() for value in message_ids
            }:
                dedup_keys.append(key)

        queue_keys = [
            self._broker._queue_key(queue, kind)
            for kind in (
                "stream",
                "ready",
                "leases",
                "delayed",
                "expiry",
                "dlq",
                "eq",
                "stats",
            )
        ]
        keys = [*queue_keys, *message_keys, *dedup_keys]
        if keys:
            await self._broker._redis.unlink(*keys)
        return len(message_ids)

    def _replay_dedup(
        self,
        message: TaskMessage,
        *,
        reuse_dedup: bool,
        dedup_scope: str | None,
        dedup_key: str | None,
        dedup_ttl: timedelta | None,
    ) -> tuple[TaskMessage, str, str, int, bool]:
        has_override = (
            dedup_scope is not None or dedup_key is not None or dedup_ttl is not None
        )
        old_key = (
            self._broker._dedup_key(message.dedup_scope, message.dedup_key)
            if message.dedup_key is not None and message.dedup_scope is not None
            else ""
        )
        if reuse_dedup and has_override:
            raise ValidationError("reuse_dedup=True 时不能同时指定新的 dedup 参数")
        if reuse_dedup:
            return message, old_key, "", 0, True
        if (dedup_scope is None) != (dedup_key is None):
            raise ValidationError("dedup_scope 与 dedup_key 必须同时提供")
        if dedup_key is None:
            return (
                replace(message, dedup_scope=None, dedup_key=None),
                old_key,
                "",
                0,
                False,
            )
        ttl = self._broker._default_dedup_ttl if dedup_ttl is None else dedup_ttl
        if ttl is None or ttl.total_seconds() <= 0:
            raise ValidationError("替换 dedup 时必须提供正数 dedup_ttl 或配置默认值")
        assert dedup_scope is not None
        return (
            replace(message, dedup_scope=dedup_scope, dedup_key=dedup_key),
            old_key,
            self._broker._dedup_key(dedup_scope, dedup_key),
            int(ttl.total_seconds() * 1000),
            False,
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
        mode = resolve_replay_dedup_mode(
            dedup_mode=dedup_mode,
            reuse_dedup=reuse_dedup,
            has_replacement=dedup_scope is not None
            or dedup_key is not None
            or dedup_ttl is not None,
        )
        data = await self._broker._redis.hgetall(self._broker._message_key(message_id))
        if not data:
            raise ValidationError("未找到指定死信")
        message = self._broker._reconstruct_replay_message(
            self._broker._decode(
                data["envelope"],
                data.get("serializer_name"),
                data.get("serializer_version"),
            ),
            queue=target_queue or data["queue"],
            payload=payload
            if replace_payload or payload is not None
            else PAYLOAD_UNSET,
            payload_type=payload_type,
            metadata=metadata,
        )
        message, old_key, new_key, ttl, keep = self._replay_dedup(
            message,
            reuse_dedup=mode == "keep",
            dedup_scope=dedup_scope,
            dedup_key=dedup_key,
            dedup_ttl=dedup_ttl,
        )
        await self._broker._ensure_group(message.queue)
        result = int(
            await replay_dead_letter_call(
                self._broker,
                source_queue=queue,
                target_queue=message.queue,
                message_id=message_id,
                envelope=self._broker._encode(message),
                attempt="0" if reset_attempt else data["attempt"],
                expires_at=_timestamp(message.expires_at) if message.expires_at else 0,
                now=_timestamp(await self._broker._now()),
                old_dedup_key=old_key,
                new_dedup_key=new_key,
                keep=keep,
                replacement_ttl=ttl,
            ).execute(self._broker._redis)
        )
        self._ensure_replayed(result, "未找到指定死信")

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
        mode = resolve_replay_dedup_mode(
            dedup_mode=dedup_mode,
            reuse_dedup=reuse_dedup,
            has_replacement=dedup_scope is not None
            or dedup_key is not None
            or dedup_ttl is not None,
        )
        data = await self._broker._redis.hgetall(self._broker._message_key(message_id))
        if not data:
            raise ValidationError("未找到指定过期消息")
        message = self._broker._reconstruct_replay_message(
            self._broker._decode(
                data["envelope"],
                data.get("serializer_name"),
                data.get("serializer_version"),
            ),
            queue=target_queue or data["queue"],
            expires_at=expires_at,
        )
        message, old_key, new_key, ttl, keep = self._replay_dedup(
            message,
            reuse_dedup=mode == "keep",
            dedup_scope=dedup_scope,
            dedup_key=dedup_key,
            dedup_ttl=dedup_ttl,
        )
        await self._broker._ensure_group(message.queue)
        result = int(
            await replay_expired_call(
                self._broker,
                source_queue=queue,
                target_queue=message.queue,
                message_id=message_id,
                envelope=self._broker._encode(message),
                expires_at=_timestamp(expires_at) if expires_at else 0,
                now=_timestamp(await self._broker._now()),
                old_dedup_key=old_key,
                new_dedup_key=new_key,
                keep=keep,
                replacement_ttl=ttl,
            ).execute(self._broker._redis)
        )
        self._ensure_replayed(result, "未找到指定过期消息")

    @staticmethod
    def _ensure_replayed(result: int, missing_message: str) -> None:
        if result == -1:
            raise ValidationError("新的 dedup key 已关联到其他消息")
        if result == 0:
            raise ValidationError(missing_message)
