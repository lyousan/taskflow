"""面向本地开发与 CI 的 SQLite Taskflow backend。"""
from __future__ import annotations

import asyncio
import traceback as traceback_module
from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, TypeVar, cast

import aiosqlite

from ..capabilities import BackendCapabilities, DedupGuarantee, SubmissionCapabilities
from ..errors import BrokerClosedError, LeaseLostError, ValidationError
from ..middleware import Middleware
from ..naming import validate_persistent_name
from ..observability import MetricsSink, metric
from ..observability import event as emit_event
from ..protocols import TaskBroker
from ..retry import RetryPolicy
from ..serialization import JsonSerializer, Serializer, SerializerRegistry
from ..submission import PreparedSubmission
from ..types import (
    ConsumerOptions,
    DeadLetter,
    ExpiredMessage,
    FinishOutcome,
    MessageStatus,
    QueueStats,
    SubmitDecision,
    SubmitRequest,
    SubmitResult,
    TaskMessage,
    utc_now,
)
from ..worker import Handler, TaskWorker
from ._time import datetime_from_timestamp as _datetime
from ._time import new_id as _new_id
from ._time import timestamp as _timestamp
from .sqlite_components import SQLiteConsumer, SQLiteDelivery

BrokerT = TypeVar("BrokerT", bound="SQLiteBroker")


class SQLiteBroker:
    """SQLite 上的完整 v0.1 生命周期实现。

    此实现使用单进程异步锁串行化同一连接上的事务，因此非常适合本地
    脚本、测试和 CI；它不定位为高吞吐量的分布式生产消息系统。
    """

    capabilities = BackendCapabilities(
        distributed_consumers=False,
        high_throughput=False,
        batch_submit=True,
        batch_atomic=True,
    )

    def __init__(
        self,
        database: str | Path = ":memory:",
        *,
        default_max_attempts: int = 3,
        default_dedup_ttl: timedelta | None = None,
        serializer: Serializer | None = None,
        serializer_registry: SerializerRegistry | None = None,
        clock: Callable[[], datetime] = utc_now,
        id_factory: Callable[[], str] = _new_id,
        middleware: Middleware | None = None,
        metrics: MetricsSink | None = None,
        submission_store: Any | None = None,
        submission_stores: Mapping[str, Any] | None = None,
        queue_submission_profiles: Mapping[str, str] | None = None,
    ) -> None:
        if default_max_attempts < 1:
            raise ValidationError("default_max_attempts 必须大于等于 1")
        self._database = str(database)
        self._connection: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()
        self._start_lock = asyncio.Lock()
        self._clock, self._id_factory = clock, id_factory
        self._default_max_attempts = default_max_attempts
        self._default_dedup_ttl = default_dedup_ttl
        self._serializer = serializer or JsonSerializer()
        self.serializer_registry = serializer_registry or SerializerRegistry([self._serializer])
        self.middleware = middleware or Middleware()
        self.metrics = metrics
        self._configure_submission_stores(submission_store, submission_stores, queue_submission_profiles)
        self._closed = False
        self.admin = SQLiteAdmin(self)

    async def _initialize(self) -> None:
        """建立 schema；所有可变投递信息均与不可变 envelope 分开存储。"""

        assert self._connection is not None
        await self._connection.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY, queue TEXT NOT NULL, envelope BLOB NOT NULL,
                serializer_name TEXT NOT NULL DEFAULT 'json', serializer_version TEXT NOT NULL DEFAULT '1',
                status TEXT NOT NULL, attempt INTEGER NOT NULL, max_attempts INTEGER NOT NULL,
                created_at REAL NOT NULL, available_at REAL, expires_at REAL, consumer_id TEXT,
                delivery_id TEXT, lease_token TEXT, claimed_at REAL, lease_until REAL,
                last_delivery_id TEXT, last_consumer_id TEXT,
                last_action TEXT, last_reason TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_messages_claim ON messages(queue, status, created_at);
            CREATE INDEX IF NOT EXISTS idx_messages_lease ON messages(status, lease_until);
            CREATE INDEX IF NOT EXISTS idx_messages_expiry ON messages(status, expires_at);
            CREATE TABLE IF NOT EXISTS dedup_records (
                scope TEXT NOT NULL, dedup_key TEXT NOT NULL, message_id TEXT NOT NULL,
                expires_at REAL NOT NULL, PRIMARY KEY(scope, dedup_key)
            );
            CREATE TABLE IF NOT EXISTS dead_letters (
                message_id TEXT PRIMARY KEY, queue TEXT NOT NULL, attempt INTEGER NOT NULL,
                reason TEXT, source TEXT NOT NULL, failed_at REAL NOT NULL,
                error_type TEXT, traceback TEXT
            );
            CREATE TABLE IF NOT EXISTS expired_messages (
                message_id TEXT PRIMARY KEY, queue TEXT NOT NULL, attempt INTEGER NOT NULL,
                status_at_expiry TEXT NOT NULL, expired_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS queue_counters (
                queue TEXT PRIMARY KEY, submitted_total INTEGER NOT NULL DEFAULT 0,
                acked_total INTEGER NOT NULL DEFAULT 0, retried_total INTEGER NOT NULL DEFAULT 0,
                reclaimed_total INTEGER NOT NULL DEFAULT 0, dead_lettered_total INTEGER NOT NULL DEFAULT 0
            );
        """)
        columns = {row[1] for row in await (await self._connection.execute("PRAGMA table_info(messages)")).fetchall()}
        if "serializer_name" not in columns:
            await self._connection.execute("ALTER TABLE messages ADD COLUMN serializer_name TEXT NOT NULL DEFAULT 'json'")
        if "serializer_version" not in columns:
            await self._connection.execute("ALTER TABLE messages ADD COLUMN serializer_version TEXT NOT NULL DEFAULT '1'")
        if "last_delivery_id" not in columns:
            await self._connection.execute("ALTER TABLE messages ADD COLUMN last_delivery_id TEXT")
        if "last_consumer_id" not in columns:
            await self._connection.execute("ALTER TABLE messages ADD COLUMN last_consumer_id TEXT")
        if "available_at" not in columns:
            await self._connection.execute("ALTER TABLE messages ADD COLUMN available_at REAL")
        await self._connection.commit()

    async def start(self) -> None:
        """提供与远程 backend 一致的显式生命周期入口。"""

        self._ensure_open()
        if self._connection is None:
            async with self._start_lock:
                if self._connection is None:
                    connection = await aiosqlite.connect(self._database, isolation_level=None)
                    connection.row_factory = aiosqlite.Row
                    self._connection = connection
                    await self._initialize()

    async def close(self) -> None:
        """关闭 SQLite 连接；关闭后不再允许任何操作。"""

        if not self._closed:
            async with self._lock:
                assert self._connection is not None
                await self._connection.close()
                self._closed = True

    async def __aenter__(self: BrokerT) -> BrokerT:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise BrokerClosedError("broker 已关闭")

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None:
            raise ValidationError("注入的时钟必须返回带时区的时间")
        return value.astimezone(timezone.utc)

    def _validate_queue(self, queue: str) -> None:
        validate_persistent_name(queue, label="queue")

    def _configure_submission_stores(self, submission_store: Any | None,
                                     submission_stores: Mapping[str, Any] | None,
                                     queue_submission_profiles: Mapping[str, str] | None) -> None:
        if submission_store is not None and submission_stores is not None:
            raise ValidationError("submission_store 与 submission_stores 不能同时配置")
        configured = dict(submission_stores or {"default": submission_store or SQLiteSubmissionStore(self)})
        if "default" not in configured:
            raise ValidationError("submission_stores 必须包含 default profile")
        self._submission_stores = {
            name: store(self) if callable(store) and not hasattr(store, "submit") else store
            for name, store in configured.items()
        }
        if any(not hasattr(store, "submit") or not hasattr(store, "submit_many") for store in self._submission_stores.values()):
            raise ValidationError("每个 submission store 都必须实现 submit 与 submit_many")
        self._queue_submission_profiles = dict(queue_submission_profiles or {})
        for queue, profile in self._queue_submission_profiles.items():
            self._validate_queue(queue)
            if profile not in self._submission_stores:
                raise ValidationError(f"queue {queue!r} 使用了未知 submission profile {profile!r}")
        self.submission_store = self._submission_stores["default"]

    def submission_capabilities(self, queue: str) -> SubmissionCapabilities:
        """返回该 queue 实际路由到的 SubmissionStore 能力声明。"""
        self._validate_queue(queue)
        profile = self._queue_submission_profiles.get(queue, "default")
        return (self.submission_store if profile == "default" else self._submission_stores[profile]).capabilities

    def _submission_store_for(self, queue: str) -> Any:
        profile = self._queue_submission_profiles.get(queue, "default")
        return self.submission_store if profile == "default" else self._submission_stores[profile]

    def _message_json(self, message: TaskMessage) -> bytes:
        return self._serializer.dumps({
            "id": message.id, "queue": message.queue, "payload": message.payload,
            "metadata": dict(message.metadata), "dedup_key": message.dedup_key,
            "dedup_scope": message.dedup_scope, "workflow_id": message.workflow_id,
            "parent_id": message.parent_id, "created_at": _timestamp(message.created_at),
            "available_at": _timestamp(message.available_at) if message.available_at else None,
            "expires_at": _timestamp(message.expires_at) if message.expires_at else None,
            "max_attempts": message.max_attempts,
        })

    def _decode_message(self, envelope: bytes, serializer_name: str | None = None,
                        serializer_version: str | None = None) -> TaskMessage:
        decoder = self._serializer if serializer_name is None or (serializer_name == self._serializer.name and serializer_version == self._serializer.version) else self.serializer_registry.resolve(serializer_name, serializer_version or "")
        data = decoder.loads(envelope)
        return TaskMessage(
            id=data["id"], queue=data["queue"], payload=data["payload"], metadata=data["metadata"],
            dedup_key=data["dedup_key"], dedup_scope=data["dedup_scope"], workflow_id=data["workflow_id"],
            parent_id=data["parent_id"], created_at=_datetime(data["created_at"]) or utc_now(),
            available_at=_datetime(data.get("available_at")),
            expires_at=_datetime(data["expires_at"]), max_attempts=data["max_attempts"],
        )

    async def submit(self, *, queue: str, payload: Any, metadata: Mapping[str, Any] | None = None,
                     dedup_key: str | None = None, dedup_scope: str | None = None,
                     dedup_ttl: timedelta | None = None, delay: timedelta | None = None, expires_at: datetime | None = None,
                     max_attempts: int | None = None, workflow_id: str | None = None,
                     parent_id: str | None = None) -> SubmitResult:
        """构造完整 PreparedSubmission 并委托 SubmissionStore 进入原子边界。"""
        prepared, message = self._prepare_submission(queue=queue, payload=payload, metadata=metadata,
            dedup_key=dedup_key, dedup_scope=dedup_scope, dedup_ttl=dedup_ttl, delay=delay,
            expires_at=expires_at, max_attempts=max_attempts, workflow_id=workflow_id, parent_id=parent_id)
        await self.middleware.emit("before_submit", message)
        result = await self._submission_store_for(queue).submit(prepared)
        if result.accepted:
            await self.middleware.emit("after_submit", message, result)
            await metric(self.metrics, "submitted_total", queue=queue)
            await emit_event(self.middleware, "submitted", message, status=prepared.status, serializer_name=self._serializer.name, serializer_version=self._serializer.version)
        else:
            await metric(self.metrics, "duplicate_total", queue=queue)
            await emit_event(self.middleware, "duplicate", message, status=prepared.status, serializer_name=self._serializer.name, serializer_version=self._serializer.version)
        return result

    def _prepare_submission(self, *, queue: str, payload: Any, metadata: Mapping[str, Any] | None = None,
                     dedup_key: str | None = None, dedup_scope: str | None = None,
                     dedup_ttl: timedelta | None = None, delay: timedelta | None = None, expires_at: datetime | None = None,
                     max_attempts: int | None = None, workflow_id: str | None = None,
                     parent_id: str | None = None) -> tuple[PreparedSubmission, TaskMessage]:
        """校验请求、生成 ID 并序列化；不在此处执行持久化。"""
        self._ensure_open()
        self._validate_queue(queue)
        if (dedup_key is None) != (dedup_scope is None):
            raise ValidationError("dedup_key 与 dedup_scope 必须同时提供")
        ttl = self._default_dedup_ttl if dedup_ttl is None else dedup_ttl
        if dedup_key is not None and (ttl is None or ttl.total_seconds() <= 0):
            raise ValidationError("启用去重时必须提供正数 dedup_ttl 或配置默认值")
        attempts = max_attempts if max_attempts is not None else self._default_max_attempts
        if attempts < 1:
            raise ValidationError("max_attempts 必须大于等于 1")
        now = self._now()
        if delay is not None and delay.total_seconds() < 0:
            raise ValidationError("delay 不能为负数")
        available_at = now + delay if delay and delay.total_seconds() > 0 else None
        if expires_at is not None and _timestamp(expires_at) <= _timestamp(now):
            # 已过期消息仍可审计，但不应先作为 READY 出现。
            initial_status = MessageStatus.EXPIRED
        elif available_at is not None:
            initial_status = MessageStatus.DELAYED
        else:
            initial_status = MessageStatus.READY
        message = TaskMessage(self._id_factory(), queue, payload, metadata or {}, dedup_key, dedup_scope,
                              workflow_id, parent_id, now, expires_at, attempts, available_at)
        envelope = self._message_json(message)
        prepared = PreparedSubmission(message.id, queue, envelope, initial_status.value, now,
            int(_timestamp(expires_at) * 1000) if expires_at else None, dedup_scope, dedup_key,
            int(ttl.total_seconds() * 1000) if ttl else None, attempts,
            self._serializer.name, self._serializer.version,
            int(_timestamp(available_at) * 1000) if available_at else None)
        return prepared, message

    async def submit_many(self, messages: list[SubmitRequest]) -> list[SubmitResult]:
        """按输入顺序提交一组请求；每项都保留独立、确定的提交结果。"""

        prepared_messages = [self._prepare_submission(queue=request.queue, payload=request.payload, metadata=request.metadata,
            dedup_key=request.dedup_key, dedup_scope=request.dedup_scope, dedup_ttl=request.dedup_ttl, delay=request.delay,
            expires_at=request.expires_at, max_attempts=request.max_attempts,
            workflow_id=request.workflow_id, parent_id=request.parent_id) for request in messages]
        for _, message in prepared_messages:
            await self.middleware.emit("before_submit", message)
        results: list[SubmitResult | None] = [None] * len(prepared_messages)
        groups: dict[Any, list[tuple[int, PreparedSubmission]]] = {}
        for index, (prepared, _) in enumerate(prepared_messages):
            groups.setdefault(self._submission_store_for(prepared.queue), []).append((index, prepared))
        for store, group in groups.items():
            accepted = await store.submit_many([prepared for _, prepared in group])
            for (index, _), result in zip(group, accepted):
                results[index] = result
        finalized = [result for result in results if result is not None]
        for result, (prepared, message) in zip(results, prepared_messages):
            assert result is not None
            if result.accepted:
                await self.middleware.emit("after_submit", message, result)
                await metric(self.metrics, "submitted_total", queue=message.queue)
                await emit_event(self.middleware, "submitted", message, status=prepared.status, serializer_name=self._serializer.name, serializer_version=self._serializer.version)
        return finalized

    def consumer(self, queue: str, *, consumer_id: str | None = None,
                 options: ConsumerOptions | None = None) -> SQLiteConsumer:
        """创建一个显式 ACK 的异步消费者。"""

        self._ensure_open()
        self._validate_queue(queue)
        selected = options or ConsumerOptions()
        if selected.lease_seconds <= 0 or selected.poll_interval < 0 or selected.concurrency < 1:
            raise ValidationError("消费者参数必须有效")
        return SQLiteConsumer(self, queue, consumer_id or self._id_factory(), selected)

    def worker(self, queue: str, handler: Handler, *, concurrency: int | None = None,
               consumer_id: str | None = None, options: ConsumerOptions | None = None,
               retry_policy: RetryPolicy | None = None, heartbeat_seconds: float | None = None) -> TaskWorker:
        """创建一个真正受 ``concurrency`` 限制的 Worker。"""
        selected = options or ConsumerOptions()
        return TaskWorker(cast(TaskBroker, self), queue, handler, concurrency=concurrency if concurrency is not None else selected.concurrency,
                          consumer_id=consumer_id, options=selected, retry_policy=retry_policy,
                          heartbeat_seconds=heartbeat_seconds)

    async def run(self, queue: str, handler: Handler, *, concurrency: int | None = None,
                  consumer_id: str | None = None, options: ConsumerOptions | None = None,
                  retry_policy: RetryPolicy | None = None,
                  heartbeat_seconds: float | None = None) -> None:
        """运行 Worker，直到调用方取消任务或调用 Worker.close()。"""
        await self.worker(queue, handler, concurrency=concurrency, consumer_id=consumer_id,
                          options=options, retry_policy=retry_policy,
                          heartbeat_seconds=heartbeat_seconds).run()

    async def _claim(self, queue: str, consumer_id: str, lease_seconds: float) -> SQLiteDelivery | None:
        await self.start()
        now = self._now()
        async with self._lock:
            assert self._connection is not None
            cursor = await self._connection.cursor()
            await cursor.execute("BEGIN IMMEDIATE")
            try:
                await self._maintain(cursor, now, queue)
                row = await (await cursor.execute("SELECT * FROM messages WHERE queue=? AND status=? ORDER BY created_at, id LIMIT 1", (queue, MessageStatus.READY.value))).fetchone()
                if row is None:
                    await cursor.execute("COMMIT")
                    return None
                delivery_id, token = self._id_factory(), self._id_factory()
                lease_until = now + timedelta(seconds=lease_seconds)
                await cursor.execute("UPDATE messages SET status=?, attempt=attempt+1, consumer_id=?, delivery_id=?, lease_token=?, claimed_at=?, lease_until=?, last_action=NULL WHERE id=?",
                               (MessageStatus.LEASED.value, consumer_id, delivery_id, token, _timestamp(now), _timestamp(lease_until), row["id"]))
                updated = await (await cursor.execute("SELECT * FROM messages WHERE id=?", (row["id"],))).fetchone()
                assert updated is not None
                await cursor.execute("COMMIT")
            except Exception:
                await cursor.execute("ROLLBACK")
                raise
        message = self._decode_message(updated["envelope"], updated["serializer_name"], updated["serializer_version"])
        delivery = SQLiteDelivery(self, message, delivery_id, token, consumer_id, updated["attempt"], now, lease_until)
        await self.middleware.emit("after_claim", delivery)
        await metric(self.metrics, "claimed_total", queue=queue)
        await emit_event(self.middleware, "claimed", message, status=MessageStatus.LEASED.value, delivery=delivery, serializer_name=self._serializer.name, serializer_version=self._serializer.version)
        return delivery

    async def _counter(self, cursor: aiosqlite.Cursor, queue: str, column: str) -> None:
        await cursor.execute("INSERT INTO queue_counters(queue) VALUES (?) ON CONFLICT(queue) DO NOTHING", (queue,))
        await cursor.execute(f"UPDATE queue_counters SET {column}={column}+1 WHERE queue=?", (queue,))

    async def _dead_letter(self, cursor: aiosqlite.Cursor, row: aiosqlite.Row, now: datetime, source: str,
                           reason: str | None, error: BaseException | None = None,
                           *, last_action: str | None = None) -> None:
        await cursor.execute("INSERT OR REPLACE INTO dead_letters VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (row["id"], row["queue"], row["attempt"], reason, source, _timestamp(now),
                        type(error).__name__ if error else None,
                        "".join(traceback_module.format_exception(error)) if error else None))
        await cursor.execute("UPDATE messages SET status=?, last_delivery_id=delivery_id, last_consumer_id=consumer_id, consumer_id=NULL, delivery_id=NULL, lease_token=NULL, claimed_at=NULL, lease_until=NULL, last_action=?, last_reason=? WHERE id=?",
                       (MessageStatus.DEAD_LETTERED.value, last_action or source, reason, row["id"]))
        await self._counter(cursor, row["queue"], "dead_lettered_total")

    async def _expire(self, cursor: aiosqlite.Cursor, message_id: str, now: datetime, old_status: MessageStatus, attempt: int) -> None:
        row = await (await cursor.execute("SELECT queue, delivery_id, consumer_id FROM messages WHERE id=?", (message_id,))).fetchone()
        if row is None:
            return
        await cursor.execute("INSERT OR REPLACE INTO expired_messages VALUES (?, ?, ?, ?, ?)",
                       (message_id, row["queue"], attempt, old_status.value, _timestamp(now)))
        await cursor.execute("UPDATE messages SET status=?, last_delivery_id=delivery_id, last_consumer_id=consumer_id, consumer_id=NULL, delivery_id=NULL, lease_token=NULL, claimed_at=NULL, lease_until=NULL, last_action='expired' WHERE id=?",
                       (MessageStatus.EXPIRED.value, message_id))

    async def _maintain(self, cursor: aiosqlite.Cursor, now: datetime, queue: str | None = None) -> int:
        """在事务中回收超时租约，并把所有已过期消息移入 EQ。"""

        predicate, params = ("", []) if queue is None else (" AND queue=?", [queue])
        expired = await (await cursor.execute("SELECT id, status, attempt FROM messages WHERE status IN (?, ?, ?) AND expires_at IS NOT NULL AND expires_at<=?" + predicate,
                                              [MessageStatus.READY.value, MessageStatus.DELAYED.value, MessageStatus.LEASED.value, _timestamp(now), *params])).fetchall()
        for row in expired:
            await self._expire(cursor, row["id"], now, MessageStatus(row["status"]), row["attempt"])
        due = await (await cursor.execute("SELECT id FROM messages WHERE status=? AND available_at IS NOT NULL AND available_at<=?" + predicate,
                                          [MessageStatus.DELAYED.value, _timestamp(now), *params])).fetchall()
        for row in due:
            await cursor.execute("UPDATE messages SET status=?, available_at=NULL, last_action='due' WHERE id=?",
                                 (MessageStatus.READY.value, row["id"]))
        leases = await (await cursor.execute("SELECT * FROM messages WHERE status=? AND lease_until<=?" + predicate,
                                             [MessageStatus.LEASED.value, _timestamp(now), *params])).fetchall()
        for row in leases:
            if row["attempt"] >= row["max_attempts"]:
                await self._dead_letter(cursor, row, now, "lease_timeout", "租约超时且已达到最大尝试次数")
            else:
                await cursor.execute("UPDATE messages SET status=?, last_delivery_id=delivery_id, last_consumer_id=consumer_id, consumer_id=NULL, delivery_id=NULL, lease_token=NULL, claimed_at=NULL, lease_until=NULL, last_action='reclaimed', last_reason=? WHERE id=?",
                               (MessageStatus.READY.value, "租约超时", row["id"]))
                await self._counter(cursor, row["queue"], "reclaimed_total")
        return sum(1 for _ in expired) + sum(1 for _ in due) + sum(1 for _ in leases)

    async def maintain(self, queue: str | None = None) -> int:
        """按需运行维护；生产部署可周期性调用此方法。"""

        self._ensure_open()
        await self.start()
        now = self._now()
        async with self._lock:
            assert self._connection is not None
            cursor = await self._connection.cursor()
            await cursor.execute("BEGIN IMMEDIATE")
            try:
                count = await self._maintain(cursor, now, queue)
                await cursor.execute("COMMIT")
                return count
            except Exception:
                await cursor.execute("ROLLBACK")
                raise

    async def _finish(self, delivery: SQLiteDelivery, action: str, reason: str | None = None,
                      error: BaseException | None = None, delay: timedelta | None = None,
                      max_attempts: int | None = None) -> FinishOutcome:
        await self.start()
        now = self._now()
        async with self._lock:
            assert self._connection is not None
            cursor = await self._connection.cursor()
            await cursor.execute("BEGIN IMMEDIATE")
            try:
                row = await (await cursor.execute("SELECT * FROM messages WHERE id=?", (delivery.message.id,))).fetchone()
                if row is None:
                    raise LeaseLostError("消息不存在")
                if row["last_delivery_id"] == delivery.delivery_id and row["last_action"] == action and row["status"] != MessageStatus.LEASED.value:
                    await cursor.execute("COMMIT")
                    return FinishOutcome.IDEMPOTENT
                if (row["status"] != MessageStatus.LEASED.value or row["delivery_id"] != delivery.delivery_id or
                        row["lease_token"] != delivery._lease_token or row["lease_until"] <= _timestamp(now)):
                    raise LeaseLostError("租约已经失效，不能终结当前投递")
                if row["expires_at"] is not None and row["expires_at"] <= _timestamp(now):
                    await self._expire(cursor, row["id"], now, MessageStatus.LEASED, row["attempt"])
                    outcome = FinishOutcome.EXPIRED
                elif action == "ack":
                    await cursor.execute("UPDATE messages SET status=?, last_delivery_id=delivery_id, last_consumer_id=consumer_id, consumer_id=NULL, delivery_id=NULL, lease_token=NULL, claimed_at=NULL, lease_until=NULL, last_action=?, last_reason=NULL WHERE id=?",
                                   (MessageStatus.ACKED.value, action, row["id"]))
                    await self._counter(cursor, row["queue"], "acked_total")
                    outcome = FinishOutcome.ACKED
                elif action == "retry":
                    if delay is not None and delay.total_seconds() < 0:
                        raise ValidationError("delay 不能为负数")
                    limit = (min(row["max_attempts"], max_attempts)
                             if max_attempts is not None else row["max_attempts"])
                    if limit < 1:
                        raise ValidationError("max_attempts 必须大于等于 1")
                    if row["attempt"] >= limit:
                        await self._dead_letter(cursor, row, now, "retry_limit", reason, last_action="retry")
                        outcome = FinishOutcome.DEAD_LETTERED
                    else:
                        available_at = now + delay if delay and delay.total_seconds() > 0 else None
                        await cursor.execute("UPDATE messages SET status=?, available_at=?, last_delivery_id=delivery_id, last_consumer_id=consumer_id, consumer_id=NULL, delivery_id=NULL, lease_token=NULL, claimed_at=NULL, lease_until=NULL, last_action=?, last_reason=? WHERE id=?",
                                       ((MessageStatus.DELAYED if available_at else MessageStatus.READY).value,
                                        _timestamp(available_at) if available_at else None, action, reason, row["id"]))
                        await self._counter(cursor, row["queue"], "retried_total")
                        outcome = FinishOutcome.RETRIED
                else:
                    await self._dead_letter(cursor, row, now, "reject", reason, error)
                    outcome = FinishOutcome.DEAD_LETTERED
                await cursor.execute("COMMIT")
            except Exception:
                await cursor.execute("ROLLBACK")
                raise
        actual = {
            FinishOutcome.ACKED: ("ack", "acked_total"),
            FinishOutcome.RETRIED: ("retry", "retried_total"),
            FinishOutcome.DEAD_LETTERED: ("dead_lettered", "dead_lettered_total"),
            FinishOutcome.EXPIRED: ("expired", "expired_total"),
        }.get(outcome)
        if actual is not None:
            event_name, metric_name = actual
            await self.middleware.emit(f"after_{event_name}", delivery, reason)
            await metric(self.metrics, metric_name, queue=delivery.message.queue)
            await emit_event(self.middleware, event_name, delivery.message, status=outcome.value, delivery=delivery, reason=reason, serializer_name=self._serializer.name, serializer_version=self._serializer.version)
        return outcome

    async def _extend(self, delivery: SQLiteDelivery, seconds: float | None) -> datetime:
        await self.start()
        period = seconds if seconds is not None else delivery._lease_seconds
        if period <= 0:
            raise ValidationError("续租时长必须为正数")
        now = self._now()
        async with self._lock:
            assert self._connection is not None
            cursor = await self._connection.cursor()
            await cursor.execute("BEGIN IMMEDIATE")
            try:
                row = await (await cursor.execute("SELECT * FROM messages WHERE id=?", (delivery.message.id,))).fetchone()
                if row is None or row["status"] != MessageStatus.LEASED.value or row["delivery_id"] != delivery.delivery_id or row["lease_token"] != delivery._lease_token or row["lease_until"] <= _timestamp(now):
                    raise LeaseLostError("租约已经失效，不能续租")
                until = now + timedelta(seconds=period)
                if row["expires_at"] is not None:
                    expires = _datetime(row["expires_at"])
                    assert expires is not None
                    until = min(until, expires)
                if until <= now:
                    await self._expire(cursor, row["id"], now, MessageStatus.LEASED, row["attempt"])
                    raise LeaseLostError("消息已过期")
                await cursor.execute("UPDATE messages SET lease_until=? WHERE id=?", (_timestamp(until), row["id"]))
                await cursor.execute("COMMIT")
                return until
            except Exception:
                await cursor.execute("ROLLBACK")
                raise

    async def inspect(self, queue: str) -> QueueStats:
        """返回在一次锁定快照中读取到的队列统计。"""

        self._ensure_open()
        await self.start()
        await self.maintain(queue)
        async with self._lock:
            assert self._connection is not None
            cursor = await self._connection.cursor()
            async def count(status: MessageStatus) -> int:
                row = await (await cursor.execute("SELECT COUNT(*) FROM messages WHERE queue=? AND status=?", (queue, status.value))).fetchone()
                assert row is not None
                return row[0]
            earliest_row = await (await cursor.execute("SELECT MIN(created_at) FROM messages WHERE queue=? AND status=?", (queue, MessageStatus.READY.value))).fetchone()
            assert earliest_row is not None
            earliest = earliest_row[0]
            counters = await (await cursor.execute("SELECT * FROM queue_counters WHERE queue=?", (queue,))).fetchone()
            values = counters or {name: 0 for name in ("submitted_total", "acked_total", "retried_total", "reclaimed_total", "dead_lettered_total")}
            dead_row = await (await cursor.execute("SELECT COUNT(*) FROM dead_letters WHERE queue=?", (queue,))).fetchone()
            expired_row = await (await cursor.execute("SELECT COUNT(*) FROM expired_messages WHERE queue=?", (queue,))).fetchone()
            assert dead_row is not None and expired_row is not None
            ready, leased, delayed = await count(MessageStatus.READY), await count(MessageStatus.LEASED), await count(MessageStatus.DELAYED)
            await metric(self.metrics, "queue_ready", float(ready), queue=queue)
            await metric(self.metrics, "queue_leased", float(leased), queue=queue)
            return QueueStats(queue, ready, leased, dead_row[0], expired_row[0],
                              _datetime(earliest), values["submitted_total"], values["acked_total"], values["retried_total"],
                              values["reclaimed_total"], values["dead_lettered_total"], delayed)


class SQLiteSubmissionStore:
    """以单个 SQLite 事务完成准入、精确去重和初始状态写入。"""

    capabilities = SubmissionCapabilities(
        dedup_guarantee=DedupGuarantee.EXACT,
        per_key_dedup_ttl=True,
        stores_original_message_id=True,
        atomic_submit=True,
        batch_submit=True,
        batch_atomic=True,
    )

    def __init__(self, broker: SQLiteBroker) -> None:
        self._broker = broker

    async def submit(self, submission: PreparedSubmission) -> SubmitResult:
        await self._broker.start()
        now = submission.created_at
        async with self._broker._lock:
            assert self._broker._connection is not None
            cursor = await self._broker._connection.cursor()
            try:
                await cursor.execute("BEGIN IMMEDIATE")
                if submission.dedup_key is not None:
                    assert submission.dedup_scope is not None and submission.dedup_ttl_ms is not None
                    await cursor.execute("DELETE FROM dedup_records WHERE scope=? AND dedup_key=? AND expires_at<=?",
                        (submission.dedup_scope, submission.dedup_key, _timestamp(now)))
                    existing = await (await cursor.execute(
                        "SELECT message_id, expires_at FROM dedup_records WHERE scope=? AND dedup_key=?",
                        (submission.dedup_scope, submission.dedup_key))).fetchone()
                    if existing is not None:
                        await cursor.execute("COMMIT")
                        return SubmitResult(existing["message_id"], False, SubmitDecision.DUPLICATE,
                            existing["message_id"], dedup_expires_at=_datetime(existing["expires_at"]))
                    dedup_expires = now + timedelta(milliseconds=submission.dedup_ttl_ms)
                    await cursor.execute("INSERT INTO dedup_records VALUES (?, ?, ?, ?)",
                        (submission.dedup_scope, submission.dedup_key, submission.message_id, _timestamp(dedup_expires)))
                await cursor.execute("""
                    INSERT INTO messages (
                        id, queue, envelope, serializer_name, serializer_version, status, attempt,
                        max_attempts, created_at, available_at, expires_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
                    """, (submission.message_id, submission.queue, submission.envelope,
                        submission.serializer_name, submission.serializer_version, submission.status,
                        submission.max_attempts, _timestamp(submission.created_at),
                        submission.available_at_ms / 1000 if submission.available_at_ms is not None else None,
                        submission.expires_at_ms / 1000 if submission.expires_at_ms is not None else None))
                await self._broker._counter(cursor, submission.queue, "submitted_total")
                if submission.status == MessageStatus.EXPIRED.value:
                    await self._broker._expire(cursor, submission.message_id, now, MessageStatus.READY, 0)
                await cursor.execute("COMMIT")
            except Exception:
                await cursor.execute("ROLLBACK")
                raise
        dedup_expires_at = (now + timedelta(milliseconds=submission.dedup_ttl_ms)
                            if submission.dedup_ttl_ms is not None else None)
        return SubmitResult(submission.message_id, True, SubmitDecision.ACCEPTED,
                            dedup_expires_at=dedup_expires_at)

    async def submit_many(self, submissions: list[PreparedSubmission]) -> list[SubmitResult]:
        """以一个 ``BEGIN IMMEDIATE`` 完成整批准入；异常会回滚整批写入。"""
        if not submissions:
            return []
        await self._broker.start()
        results: list[SubmitResult] = []
        async with self._broker._lock:
            assert self._broker._connection is not None
            cursor = await self._broker._connection.cursor()
            try:
                await cursor.execute("BEGIN IMMEDIATE")
                for submission in submissions:
                    now = submission.created_at
                    if submission.dedup_key is not None:
                        assert submission.dedup_scope is not None and submission.dedup_ttl_ms is not None
                        await cursor.execute("DELETE FROM dedup_records WHERE scope=? AND dedup_key=? AND expires_at<=?", (submission.dedup_scope, submission.dedup_key, _timestamp(now)))
                        existing = await (await cursor.execute("SELECT message_id, expires_at FROM dedup_records WHERE scope=? AND dedup_key=?", (submission.dedup_scope, submission.dedup_key))).fetchone()
                        if existing is not None:
                            results.append(SubmitResult(existing["message_id"], False, SubmitDecision.DUPLICATE, existing["message_id"], dedup_expires_at=_datetime(existing["expires_at"])))
                            continue
                        await cursor.execute("INSERT INTO dedup_records VALUES (?, ?, ?, ?)", (submission.dedup_scope, submission.dedup_key, submission.message_id, _timestamp(now + timedelta(milliseconds=submission.dedup_ttl_ms))))
                    await cursor.execute("INSERT INTO messages (id, queue, envelope, serializer_name, serializer_version, status, attempt, max_attempts, created_at, available_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)", (submission.message_id, submission.queue, submission.envelope, submission.serializer_name, submission.serializer_version, submission.status, submission.max_attempts, _timestamp(now), submission.available_at_ms / 1000 if submission.available_at_ms is not None else None, submission.expires_at_ms / 1000 if submission.expires_at_ms is not None else None))
                    await self._broker._counter(cursor, submission.queue, "submitted_total")
                    if submission.status == MessageStatus.EXPIRED.value:
                        await self._broker._expire(cursor, submission.message_id, now, MessageStatus.READY, 0)
                    results.append(SubmitResult(submission.message_id, True, SubmitDecision.ACCEPTED, dedup_expires_at=(now + timedelta(milliseconds=submission.dedup_ttl_ms) if submission.dedup_ttl_ms is not None else None)))
                await cursor.execute("COMMIT")
            except Exception:
                await cursor.execute("ROLLBACK")
                raise
        return results


class SQLiteAdmin:
    """DLQ 与 EQ 的显式、可审计管理接口。"""

    def __init__(self, broker: SQLiteBroker) -> None:
        self._broker = broker

    async def list_dead_letters(self, queue: str) -> list[DeadLetter]:
        """按失败时间返回某队列的死信。"""
        await self._broker.start()
        async with self._broker._lock:
            assert self._broker._connection is not None
            rows = await (await self._broker._connection.execute("SELECT d.*, m.envelope, m.serializer_name, m.serializer_version FROM dead_letters d JOIN messages m ON m.id=d.message_id WHERE d.queue=? ORDER BY d.failed_at", (queue,))).fetchall()
        return [DeadLetter(self._broker._decode_message(row["envelope"], row["serializer_name"], row["serializer_version"]), row["attempt"], row["reason"], row["source"], _datetime(row["failed_at"]) or utc_now(), row["error_type"], row["traceback"]) for row in rows]

    async def list_expired(self, queue: str) -> list[ExpiredMessage]:
        """按过期时间返回某队列的 EQ 记录。"""
        await self._broker.start()
        async with self._broker._lock:
            assert self._broker._connection is not None
            rows = await (await self._broker._connection.execute("SELECT e.*, m.envelope, m.serializer_name, m.serializer_version FROM expired_messages e JOIN messages m ON m.id=e.message_id WHERE e.queue=? ORDER BY e.expired_at", (queue,))).fetchall()
        return [ExpiredMessage(self._broker._decode_message(row["envelope"], row["serializer_name"], row["serializer_version"]), MessageStatus(row["status_at_expiry"]), _datetime(row["expired_at"]) or utc_now(), row["attempt"]) for row in rows]

    async def delete_dead_letter(self, queue: str, message_id: str) -> bool:
        """删除一条 DLQ 审计记录，返回是否确实存在。"""
        await self._broker.start()
        async with self._broker._lock:
            assert self._broker._connection is not None
            cursor = await self._broker._connection.execute("DELETE FROM dead_letters WHERE queue=? AND message_id=?", (queue, message_id))
            return cursor.rowcount > 0

    def _replay_dedup(self, message: TaskMessage, *, reuse_dedup: bool,
                      dedup_scope: str | None, dedup_key: str | None,
                      dedup_ttl: timedelta | None) -> tuple[TaskMessage, tuple[str, str, timedelta] | None]:
        """解析 replay 的去重策略；scope 不随目标 queue 自动变化。"""
        has_override = dedup_scope is not None or dedup_key is not None or dedup_ttl is not None
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
        return replace(message, dedup_scope=dedup_scope, dedup_key=dedup_key), (dedup_scope, dedup_key, ttl)

    async def _apply_replay_dedup(self, cursor: aiosqlite.Cursor, message_id: str,
                                  replacement: tuple[str, str, timedelta] | None,
                                  now: datetime, *, reuse_dedup: bool) -> None:
        if reuse_dedup:
            return
        await cursor.execute("DELETE FROM dedup_records WHERE message_id=?", (message_id,))
        if replacement is None:
            return
        scope, key, ttl = replacement
        await cursor.execute("DELETE FROM dedup_records WHERE scope=? AND dedup_key=? AND expires_at<=?", (scope, key, _timestamp(now)))
        existing = await (await cursor.execute("SELECT message_id FROM dedup_records WHERE scope=? AND dedup_key=?", (scope, key))).fetchone()
        if existing is not None and existing["message_id"] != message_id:
            raise ValidationError("新的 dedup key 已关联到其他消息")
        await cursor.execute("INSERT INTO dedup_records(scope, dedup_key, message_id, expires_at) VALUES (?, ?, ?, ?) ON CONFLICT(scope, dedup_key) DO UPDATE SET message_id=excluded.message_id, expires_at=excluded.expires_at",
                             (scope, key, message_id, _timestamp(now + ttl)))

    async def replay_dead_letter(self, queue: str, message_id: str, *, reset_attempt: bool = True,
                                 target_queue: str | None = None, payload: Any = None,
                                 metadata: Mapping[str, Any] | None = None, reuse_dedup: bool = True,
                                 dedup_scope: str | None = None, dedup_key: str | None = None,
                                 dedup_ttl: timedelta | None = None) -> None:
        """将 DLQ 消息重新置为 READY，可选择重置尝试数和覆盖业务内容。"""
        await self._broker.start()
        async with self._broker._lock:
            assert self._broker._connection is not None
            cursor = await self._broker._connection.cursor()
            await cursor.execute("BEGIN IMMEDIATE")
            try:
                row = await (await cursor.execute("SELECT m.* FROM messages m JOIN dead_letters d ON d.message_id=m.id WHERE d.queue=? AND m.id=?", (queue, message_id))).fetchone()
                if row is None:
                    raise ValidationError("未找到指定死信")
                message = self._broker._decode_message(row["envelope"], row["serializer_name"], row["serializer_version"])
                replayed = replace(message, queue=target_queue or message.queue,
                                   payload=message.payload if payload is None else payload,
                                   metadata=message.metadata if metadata is None else metadata,
                                   expires_at=message.expires_at)
                self._broker._validate_queue(replayed.queue)
                replayed, dedup_replacement = self._replay_dedup(replayed, reuse_dedup=reuse_dedup,
                    dedup_scope=dedup_scope, dedup_key=dedup_key, dedup_ttl=dedup_ttl)
                await self._apply_replay_dedup(cursor, message_id, dedup_replacement, self._broker._now(), reuse_dedup=reuse_dedup)
                await cursor.execute("UPDATE messages SET queue=?, envelope=?, status=?, attempt=?, consumer_id=NULL, delivery_id=NULL, lease_token=NULL, claimed_at=NULL, lease_until=NULL, last_action='replayed' WHERE id=?",
                               (replayed.queue, self._broker._message_json(replayed), MessageStatus.READY.value,
                                0 if reset_attempt else row["attempt"], message_id))
                await cursor.execute("DELETE FROM dead_letters WHERE message_id=?", (message_id,))
                await cursor.execute("COMMIT")
            except Exception:
                await cursor.execute("ROLLBACK")
                raise

    async def delete_expired(self, queue: str, message_id: str) -> bool:
        """删除一条 EQ 审计记录，返回是否确实存在。"""
        await self._broker.start()
        async with self._broker._lock:
            assert self._broker._connection is not None
            cursor = await self._broker._connection.execute("DELETE FROM expired_messages WHERE queue=? AND message_id=?", (queue, message_id))
            return cursor.rowcount > 0

    async def replay_expired(self, queue: str, message_id: str, *, expires_at: datetime | None,
                             reuse_dedup: bool = True, dedup_scope: str | None = None,
                             dedup_key: str | None = None, dedup_ttl: timedelta | None = None) -> None:
        """以新的过期策略重新投递 EQ 消息；必须显式给出参数。"""
        if expires_at is not None and expires_at.tzinfo is None:
            raise ValidationError("expires_at 必须带时区")
        await self._broker.start()
        async with self._broker._lock:
            assert self._broker._connection is not None
            cursor = await self._broker._connection.cursor()
            await cursor.execute("BEGIN IMMEDIATE")
            try:
                row = await (await cursor.execute("SELECT m.* FROM messages m JOIN expired_messages e ON e.message_id=m.id WHERE e.queue=? AND m.id=?", (queue, message_id))).fetchone()
                if row is None:
                    raise ValidationError("未找到指定过期消息")
                message = replace(self._broker._decode_message(row["envelope"], row["serializer_name"], row["serializer_version"]), expires_at=expires_at)
                message, dedup_replacement = self._replay_dedup(message, reuse_dedup=reuse_dedup,
                    dedup_scope=dedup_scope, dedup_key=dedup_key, dedup_ttl=dedup_ttl)
                await self._apply_replay_dedup(cursor, message_id, dedup_replacement, self._broker._now(), reuse_dedup=reuse_dedup)
                await cursor.execute("UPDATE messages SET envelope=?, status=?, expires_at=?, consumer_id=NULL, delivery_id=NULL, lease_token=NULL, claimed_at=NULL, lease_until=NULL, last_action='replayed' WHERE id=?",
                               (self._broker._message_json(message), MessageStatus.READY.value, _timestamp(expires_at) if expires_at else None, message_id))
                await cursor.execute("DELETE FROM expired_messages WHERE message_id=?", (message_id,))
                await cursor.execute("COMMIT")
            except Exception:
                await cursor.execute("ROLLBACK")
                raise
