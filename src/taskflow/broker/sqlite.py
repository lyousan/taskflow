"""面向本地开发与 CI 的 SQLite Taskflow backend。"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal, TypeVar, overload

import aiosqlite

from ..capabilities import BackendCapabilities, SubmissionCapabilities
from ..config import QueueConfig
from ..consistency import ConsistencyIssue, ConsistencyReport, RepairReport
from ..errors import BrokerClosedError, SerializerUnavailableError, ValidationError
from ..health import HealthCheck, HealthReport
from ..middleware import Middleware
from ..naming import validate_persistent_name
from ..observability import EventSink, MetricsSink, metric
from ..payloads import PAYLOAD_UNSET, normalize_payload, reconstruct_payload
from ..retry import RetryPolicy
from ..serialization import JsonSerializer, Serializer, SerializerRegistry
from ..submission import (
    PreparedSubmission,
    SubmissionObserver,
    SubmissionRouter,
    SubmissionService,
)
from ..submission.sqlite import SQLiteSubmissionStore as _SQLiteSubmissionStore
from ..types import (
    BatchSubmitItemResult,
    ConsumerOptions,
    FinishOutcome,
    MessageStatus,
    QueueStats,
    SubmitRequest,
    SubmitResult,
    TaskMessage,
    utc_now,
)
from ..worker import Handler, TaskWorker
from ._time import datetime_from_timestamp as _datetime
from ._time import new_id as _new_id
from ._time import timestamp as _timestamp
from .sqlite_admin import SQLiteAdmin as _SQLiteAdmin
from .sqlite_components import SQLiteConsumer, SQLiteDelivery
from .sqlite_maintenance import MaintenanceEvent as _MaintenanceEvent
from .sqlite_maintenance import SQLiteMaintenance
from .sqlite_migrations import CURRENT_SQLITE_SCHEMA_VERSION, apply_sqlite_migrations
from .sqlite_state_machine import SQLiteStateMachine

BrokerT = TypeVar("BrokerT", bound="SQLiteBroker")
logger = logging.getLogger(__name__)

_SQLITE_SCHEMA_VERSION = str(CURRENT_SQLITE_SCHEMA_VERSION)
_REQUIRED_SQLITE_INDEXES = frozenset({
    "idx_messages_claim", "idx_messages_lease", "idx_messages_expiry",
})


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
        queues: Mapping[str, QueueConfig] | None = None,
        event_sink: EventSink | None = None,
        events: EventSink | None = None,
        allow_legacy_names: bool = False,
    ) -> None:
        if isinstance(default_max_attempts, bool) or not isinstance(default_max_attempts, int) or default_max_attempts < 1:
            raise ValidationError("default_max_attempts 必须大于等于 1")
        self._database = str(database)
        self._connection: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()
        self._start_lock = asyncio.Lock()
        self._clock, self._id_factory = clock, id_factory
        self._allow_legacy_names = allow_legacy_names
        self._default_max_attempts = default_max_attempts
        self._default_dedup_ttl = default_dedup_ttl
        self._default_queue_config = QueueConfig(max_attempts=default_max_attempts, default_dedup_ttl=default_dedup_ttl)
        self._queues = dict(queues or {})
        for queue, config in self._queues.items():
            validate_persistent_name(queue, label="queue", allow_legacy=self._allow_legacy_names)
            if not isinstance(config, QueueConfig):
                raise ValidationError("queues 的值必须是 QueueConfig")
        self._serializer = serializer or JsonSerializer()
        self.serializer_registry = serializer_registry or SerializerRegistry([self._serializer])
        self.middleware = middleware or Middleware()
        self.metrics = metrics
        if event_sink is not None and events is not None:
            raise ValidationError("event_sink 与 events 不能同时配置")
        self.event_sink = event_sink or events
        self._configure_submission_stores(submission_store, submission_stores, queue_submission_profiles)
        self._submission_observer = SubmissionObserver(
            backend="sqlite", middleware=self.middleware, metrics=self.metrics,
            event_sink=self.event_sink, serializer=self._serializer,
        )
        self._submission_service = SubmissionService(self, self._prepare_submission)
        self._maintenance = SQLiteMaintenance(self)
        self._state_machine = SQLiteStateMachine(self)
        self._closed = False
        self.admin = _SQLiteAdmin(self)

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
            CREATE TABLE IF NOT EXISTS taskflow_schema (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
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
        await apply_sqlite_migrations(self._connection)

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
        validate_persistent_name(queue, label="queue", allow_legacy=self._allow_legacy_names)

    def _queue_config(self, queue: str) -> QueueConfig:
        self._validate_queue(queue)
        return self._queues.get(queue, self._default_queue_config)

    def _configure_submission_stores(self, submission_store: Any | None,
                                     submission_stores: Mapping[str, Any] | None,
                                     queue_submission_profiles: Mapping[str, str] | None) -> None:
        self._submission_router = SubmissionRouter(
            self, default_store=_SQLiteSubmissionStore(self), submission_store=submission_store,
            submission_stores=submission_stores, queue_submission_profiles=queue_submission_profiles,
        )
        self._submission_stores = self._submission_router.stores
        self._queue_submission_profiles = self._submission_router.profiles
        self.submission_store = self._submission_router.default

    def submission_capabilities(self, queue: str) -> SubmissionCapabilities:
        """返回该 queue 实际路由到的 SubmissionStore 能力声明。"""
        self._validate_queue(queue)
        profile = self._submission_router.profile_for(queue)
        return self.submission_store.capabilities if profile == "default" else self._submission_router.capabilities(queue)

    def _submission_store_for(self, queue: str) -> Any:
        return self.submission_store if self._submission_router.profile_for(queue) == "default" else self._submission_router.for_queue(queue)

    def _message_json(self, message: TaskMessage) -> bytes:
        return self._serializer.dumps({
            "id": message.id, "queue": message.queue, "payload": message.payload,
            "metadata": dict(message.metadata), "dedup_key": message.dedup_key,
            "dedup_scope": message.dedup_scope, "workflow_id": message.workflow_id,
            "parent_id": message.parent_id, "created_at": _timestamp(message.created_at),
            "available_at": _timestamp(message.available_at) if message.available_at else None,
            "expires_at": _timestamp(message.expires_at) if message.expires_at else None,
            "max_attempts": message.max_attempts,
            "payload_schema_name": message.payload_schema_name,
            "payload_schema_version": message.payload_schema_version,
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
            payload_schema_name=data.get("payload_schema_name"),
            payload_schema_version=data.get("payload_schema_version"),
        )

    async def submit(self, *, queue: str, payload: Any, metadata: Mapping[str, Any] | None = None,
                     dedup_key: str | None = None, dedup_scope: str | None = None,
                     dedup_ttl: timedelta | None = None, delay: timedelta | None = None, expires_at: datetime | None = None,
                     max_attempts: int | None = None, workflow_id: str | None = None,
                     parent_id: str | None = None, payload_type: type[Any] | None = None) -> SubmitResult:
        """构造完整 PreparedSubmission 并委托 SubmissionStore 进入原子边界。"""
        return await self._submission_service.submit(
            queue=queue, payload=payload, metadata=metadata, dedup_key=dedup_key, dedup_scope=dedup_scope,
            dedup_ttl=dedup_ttl, delay=delay, expires_at=expires_at, max_attempts=max_attempts,
            workflow_id=workflow_id, parent_id=parent_id, payload_type=payload_type,
        )

    async def _prepare_submission(self, *, queue: str, payload: Any, metadata: Mapping[str, Any] | None = None,
                     dedup_key: str | None = None, dedup_scope: str | None = None,
                     dedup_ttl: timedelta | None = None, delay: timedelta | None = None, expires_at: datetime | None = None,
                     max_attempts: int | None = None, workflow_id: str | None = None,
                     parent_id: str | None = None, payload_type: type[Any] | None = None) -> tuple[PreparedSubmission, TaskMessage]:
        """校验请求、生成 ID 并序列化；不在此处执行持久化。"""
        self._ensure_open()
        self._validate_queue(queue)
        if (dedup_key is None) != (dedup_scope is None):
            raise ValidationError("dedup_key 与 dedup_scope 必须同时提供")
        config = self._queue_config(queue)
        ttl = (config.default_dedup_ttl if queue in self._queues else self._default_dedup_ttl) if dedup_ttl is None else dedup_ttl
        if ttl is not None and not isinstance(ttl, timedelta):
            raise ValidationError("dedup_ttl 必须是 timedelta")
        if dedup_key is not None and (ttl is None or ttl.total_seconds() <= 0):
            raise ValidationError("启用去重时必须提供正数 dedup_ttl 或配置默认值")
        attempts = max_attempts if max_attempts is not None else config.max_attempts
        if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 1:
            raise ValidationError("max_attempts 必须大于等于 1")
        now = self._now()
        if delay is not None and not isinstance(delay, timedelta):
            raise ValidationError("delay 必须是 timedelta")
        if delay is not None and delay.total_seconds() < 0:
            raise ValidationError("delay 不能为负数")
        if expires_at is not None and expires_at.tzinfo is None:
            raise ValidationError("expires_at 必须带时区")
        available_at = now + delay if delay and delay.total_seconds() > 0 else None
        if expires_at is not None and _timestamp(expires_at) <= _timestamp(now):
            # 已过期消息仍可审计，但不应先作为 READY 出现。
            initial_status = MessageStatus.EXPIRED
        elif available_at is not None:
            initial_status = MessageStatus.DELAYED
        else:
            initial_status = MessageStatus.READY
        encoded_payload, schema = normalize_payload(payload, payload_type=payload_type)
        message = TaskMessage(self._id_factory(), queue, encoded_payload, metadata or {}, dedup_key, dedup_scope,
                              workflow_id, parent_id, now, expires_at, attempts, available_at,
                              schema.name if schema else None, schema.version if schema else None)
        envelope = self._message_json(message)
        if config.max_payload_bytes is not None and len(self._serializer.dumps(encoded_payload)) > config.max_payload_bytes:
            raise ValidationError(f"payload 超过 queue {queue!r} 的 max_payload_bytes 限制")
        prepared = PreparedSubmission(message.id, queue, envelope, initial_status.value, now,
            int(_timestamp(expires_at) * 1000) if expires_at else None, dedup_scope, dedup_key,
            int(ttl.total_seconds() * 1000) if ttl else None, attempts,
            self._serializer.name, self._serializer.version,
            int(_timestamp(available_at) * 1000) if available_at else None)
        return prepared, message

    @overload
    async def submit_many(self, messages: list[SubmitRequest], *, atomic: Literal[True] = True) -> list[SubmitResult]: ...

    @overload
    async def submit_many(self, messages: list[SubmitRequest], *, atomic: Literal[False]) -> list[BatchSubmitItemResult]: ...

    async def submit_many(self, messages: list[SubmitRequest], *, atomic: bool = True) -> list[SubmitResult] | list[BatchSubmitItemResult]:
        """批量提交；non-atomic 模式对每一项独立准备、提交并返回结果。"""

        return await self._submission_service.submit_many(messages, atomic=atomic)

    async def _record_submitted(self, prepared: PreparedSubmission, message: TaskMessage,
                                result: SubmitResult) -> None:
        """在持久化成功后发出统一的提交观测事件。"""

        await self._submission_observer.record(prepared, message, result)

    def _reconstruct_replay_message(self, message: TaskMessage, *, queue: str,
                                    payload: Any = PAYLOAD_UNSET,
                                    payload_type: type[Any] | None = None,
                                    metadata: Mapping[str, Any] | None = None,
                                    expires_at: datetime | None | object = PAYLOAD_UNSET) -> TaskMessage:
        """Rebuild a replayed envelope through the same payload boundary as submit."""

        encoded, schema_name, schema_version = reconstruct_payload(
            existing_payload=message.payload,
            existing_schema_name=message.payload_schema_name,
            existing_schema_version=message.payload_schema_version,
            payload=payload,
            payload_type=payload_type,
        )
        replayed_expires_at: datetime | None
        if expires_at is PAYLOAD_UNSET:
            replayed_expires_at = message.expires_at
        else:
            assert expires_at is None or isinstance(expires_at, datetime)
            replayed_expires_at = expires_at
        replayed = replace(
            message, queue=queue, payload=encoded,
            metadata=message.metadata if metadata is None else metadata,
            expires_at=replayed_expires_at,
            payload_schema_name=schema_name, payload_schema_version=schema_version,
        )
        self._validate_queue(replayed.queue)
        config = self._queue_config(replayed.queue)
        if config.max_payload_bytes is not None and len(self._serializer.dumps(encoded)) > config.max_payload_bytes:
            raise ValidationError(f"payload 超过 queue {replayed.queue!r} 的 max_payload_bytes 限制")
        # Validate metadata and the complete envelope before the replay transaction starts.
        self._message_json(replayed)
        return replayed

    def consumer(self, queue: str, *, consumer_id: str | None = None,
                 options: ConsumerOptions | None = None) -> SQLiteConsumer:
        """创建一个显式 ACK 的异步消费者。"""

        self._ensure_open()
        self._validate_queue(queue)
        selected = options or ConsumerOptions(lease_seconds=self._queue_config(queue).lease.total_seconds())
        if selected.lease_seconds <= 0 or selected.poll_interval < 0 or selected.concurrency < 1:
            raise ValidationError("消费者参数必须有效")
        return SQLiteConsumer(self, queue, consumer_id or self._id_factory(), selected)

    def worker(self, queue: str, handler: Handler, *, concurrency: int | None = None,
               consumer_id: str | None = None, options: ConsumerOptions | None = None,
               retry_policy: RetryPolicy | None = None, heartbeat_seconds: float | None = None,
               payload_type: type[Any] | None = None) -> TaskWorker:
        """创建一个真正受 ``concurrency`` 限制的 Worker。"""
        selected = options or ConsumerOptions(lease_seconds=self._queue_config(queue).lease.total_seconds())
        if retry_policy is None and queue in self._queues:
            retry_policy = self._queue_config(queue).retry_policy
        return TaskWorker(self, queue, handler, concurrency=concurrency if concurrency is not None else selected.concurrency,
                          consumer_id=consumer_id, options=selected, retry_policy=retry_policy,
                          heartbeat_seconds=heartbeat_seconds, payload_type=payload_type)

    async def run(self, queue: str, handler: Handler, *, concurrency: int | None = None,
                  consumer_id: str | None = None, options: ConsumerOptions | None = None,
                  retry_policy: RetryPolicy | None = None,
                  heartbeat_seconds: float | None = None,
                  payload_type: type[Any] | None = None) -> None:
        """运行 Worker，直到调用方取消任务或调用 Worker.close()。"""
        await self.worker(queue, handler, concurrency=concurrency, consumer_id=consumer_id,
                          options=options, retry_policy=retry_policy,
                          heartbeat_seconds=heartbeat_seconds, payload_type=payload_type).run()

    async def _claim(self, queue: str, consumer_id: str, lease_seconds: float) -> SQLiteDelivery | None:
        return await self._state_machine.claim(queue, consumer_id, lease_seconds)

    async def _counter(self, cursor: aiosqlite.Cursor, queue: str, column: str) -> None:
        await self._state_machine.counter(cursor, queue, column)

    async def _dead_letter(self, cursor: aiosqlite.Cursor, row: aiosqlite.Row, now: datetime, source: str,
                           reason: str | None, error: BaseException | None = None,
                           *, last_action: str | None = None) -> None:
        await self._state_machine.dead_letter(cursor, row, now, source, reason, error, last_action=last_action)

    async def _expire(self, cursor: aiosqlite.Cursor, message_id: str, now: datetime, old_status: MessageStatus, attempt: int) -> None:
        await self._state_machine.expire(cursor, message_id, now, old_status, attempt)

    def _maintenance_event(self, row: aiosqlite.Row, name: str, status: str, *,
                           reason: str | None = None, error_type: str | None = None,
                           metric_name: str | None = None) -> _MaintenanceEvent:
        return self._maintenance.event(row, name, status, reason=reason, error_type=error_type, metric_name=metric_name)

    async def _emit_maintenance_events(self, events: list[_MaintenanceEvent]) -> None:
        await self._maintenance.emit(events)

    async def _maintain(self, cursor: aiosqlite.Cursor, now: datetime,
                        queue: str | None = None) -> tuple[int, list[_MaintenanceEvent]]:
        return await self._maintenance.maintain(cursor, now, queue)

    async def maintain(self, queue: str | None = None) -> int:
        """按需运行维护；生产部署可周期性调用此方法。"""

        return await self._maintenance.run(queue)

    async def _finish(self, delivery: SQLiteDelivery, action: str, reason: str | None = None,
                      error: BaseException | None = None, delay: timedelta | None = None,
                      max_attempts: int | None = None) -> FinishOutcome:
        return await self._state_machine.finish(delivery, action, reason, error, delay, max_attempts)

    async def _extend(self, delivery: SQLiteDelivery, seconds: float | None) -> datetime:
        return await self._state_machine.extend(delivery, seconds)

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
            await metric(self.metrics, "queue_delayed", float(delayed), queue=queue)
            return QueueStats(queue, ready, leased, dead_row[0], expired_row[0],
                              _datetime(earliest), values["submitted_total"], values["acked_total"], values["retried_total"],
                              values["reclaimed_total"], values["dead_lettered_total"], delayed)

    async def inspect_message(self, message_id: str) -> TaskMessage | None:
        """按稳定 message ID 查询原始业务消息，不改变其状态。"""
        await self.start()
        async with self._lock:
            assert self._connection is not None
            row = await (await self._connection.execute(
                "SELECT envelope, serializer_name, serializer_version FROM messages WHERE id=?", (message_id,))).fetchone()
        return None if row is None else self._decode_message(row["envelope"], row["serializer_name"], row["serializer_version"])

    async def health_check(self) -> HealthReport:
        """Run non-mutating checks for the SQLite connection and persisted data.

        Unlike the v0.4 CLI probe this verifies the schema metadata, required
        indexes and every serializer identity currently referenced by messages.
        """

        if self._closed:
            return HealthReport(False, "sqlite", None, (
                HealthCheck("connection", "error", "broker is closed"),
            ))
        try:
            await self.start()
            async with self._lock:
                assert self._connection is not None
                await (await self._connection.execute("SELECT 1")).fetchone()
                version_row = await (await self._connection.execute(
                    "SELECT value FROM taskflow_schema WHERE key='version'"
                )).fetchone()
                index_rows = await (await self._connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='messages'"
                )).fetchall()
                serializer_rows = await (await self._connection.execute(
                    "SELECT DISTINCT serializer_name, serializer_version FROM messages"
                )).fetchall()
        except Exception as exc:  # noqa: BLE001 - report driver failures as diagnostics
            return HealthReport(False, "sqlite", None, (
                HealthCheck("connection", "error", f"{type(exc).__name__}: {exc}"),
            ))

        checks: list[HealthCheck] = [HealthCheck("connection", "ok")]
        version = version_row["value"] if version_row is not None else None
        checks.append(HealthCheck(
            "schema_version", "ok" if version == _SQLITE_SCHEMA_VERSION else "error",
            None if version == _SQLITE_SCHEMA_VERSION else f"expected {_SQLITE_SCHEMA_VERSION}, found {version!r}",
        ))
        indexes = {row["name"] for row in index_rows}
        missing_indexes = sorted(_REQUIRED_SQLITE_INDEXES - indexes)
        checks.append(HealthCheck(
            "required_indexes", "ok" if not missing_indexes else "error",
            None if not missing_indexes else f"missing: {', '.join(missing_indexes)}",
        ))
        unavailable: list[str] = []
        for row in serializer_rows:
            name, version = row["serializer_name"], row["serializer_version"]
            try:
                self.serializer_registry.resolve(name, version)
            except SerializerUnavailableError:
                unavailable.append(f"{name}@{version}")
        checks.append(HealthCheck(
            "serializer_registry", "ok" if not unavailable else "error",
            None if not unavailable else f"unavailable: {', '.join(sorted(unavailable))}",
        ))
        checks.append(HealthCheck("namespace", "ok", "SQLite has no namespace"))
        checks.append(HealthCheck(
            "unrecoverable_errors", "ok" if not unavailable else "error",
            None if not unavailable else "messages require unavailable serializers",
        ))
        return HealthReport(
            all(check.status != "error" for check in checks), "sqlite", None, tuple(checks),
        )

    async def check_consistency(self, queue: str) -> ConsistencyReport:
        """Inspect persisted status/audit invariants without changing user data."""

        self._validate_queue(queue)
        await self.start()
        async with self._lock:
            assert self._connection is not None
            issues: list[ConsistencyIssue] = []
            queries = (
                ("missing_dead_letter", "SELECT id FROM messages m WHERE queue=? AND status='dead_lettered' AND NOT EXISTS (SELECT 1 FROM dead_letters d WHERE d.message_id=m.id)"),
                ("orphan_dead_letter", "SELECT d.message_id FROM dead_letters d LEFT JOIN messages m ON m.id=d.message_id WHERE d.queue=? AND (m.id IS NULL OR m.status!='dead_lettered')"),
                ("missing_expired", "SELECT id FROM messages m WHERE queue=? AND status='expired' AND NOT EXISTS (SELECT 1 FROM expired_messages e WHERE e.message_id=m.id)"),
                ("orphan_expired", "SELECT e.message_id FROM expired_messages e LEFT JOIN messages m ON m.id=e.message_id WHERE e.queue=? AND (m.id IS NULL OR m.status!='expired')"),
                ("leased_without_lease", "SELECT id FROM messages WHERE queue=? AND status='leased' AND (lease_until IS NULL OR delivery_id IS NULL OR lease_token IS NULL)"),
            )
            for name, query in queries:
                rows = await (await self._connection.execute(query, (queue,))).fetchall()
                issues.extend(ConsistencyIssue(name, row[0]) for row in rows)
            index_rows = await (await self._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='messages'"
            )).fetchall()
            for name in sorted(_REQUIRED_SQLITE_INDEXES - {row[0] for row in index_rows}):
                issues.append(ConsistencyIssue("missing_index", detail=name))
        return ConsistencyReport(queue, "sqlite", None, tuple(issues))

    async def repair_consistency(self, queue: str, *, dry_run: bool = True) -> RepairReport:
        """Propose repairs by default; apply only audit/index repairs when explicit."""

        report = await self.check_consistency(queue)
        repairable = tuple(issue for issue in report.issues if issue.name != "leased_without_lease")
        if dry_run or not repairable:
            return RepairReport(queue, "sqlite", None, dry_run, repairable)
        await self.start()
        now = _timestamp(self._now())
        async with self._lock:
            assert self._connection is not None
            cursor = await self._connection.cursor()
            await cursor.execute("BEGIN IMMEDIATE")
            try:
                for issue in repairable:
                    if issue.name == "missing_dead_letter":
                        row = await (await cursor.execute("SELECT attempt FROM messages WHERE id=?", (issue.message_id,))).fetchone()
                        if row is not None:
                            await cursor.execute("INSERT OR IGNORE INTO dead_letters VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (issue.message_id, queue, row[0], "consistency_repair", "repair", now, None, None))
                    elif issue.name == "missing_expired":
                        row = await (await cursor.execute("SELECT attempt FROM messages WHERE id=?", (issue.message_id,))).fetchone()
                        if row is not None:
                            await cursor.execute("INSERT OR IGNORE INTO expired_messages VALUES (?, ?, ?, ?, ?)", (issue.message_id, queue, row[0], MessageStatus.EXPIRED.value, now))
                    elif issue.name == "orphan_dead_letter":
                        await cursor.execute("DELETE FROM dead_letters WHERE queue=? AND message_id=?", (queue, issue.message_id))
                    elif issue.name == "orphan_expired":
                        await cursor.execute("DELETE FROM expired_messages WHERE queue=? AND message_id=?", (queue, issue.message_id))
                    elif issue.name == "missing_index" and issue.detail:
                        definitions = {
                            "idx_messages_claim": "CREATE INDEX IF NOT EXISTS idx_messages_claim ON messages(queue, status, created_at)",
                            "idx_messages_lease": "CREATE INDEX IF NOT EXISTS idx_messages_lease ON messages(status, lease_until)",
                            "idx_messages_expiry": "CREATE INDEX IF NOT EXISTS idx_messages_expiry ON messages(status, expires_at)",
                        }
                        await cursor.execute(definitions[issue.detail])
                await cursor.execute("COMMIT")
            except Exception:
                await cursor.execute("ROLLBACK")
                raise
        return RepairReport(queue, "sqlite", None, False, repairable)


# Compatibility export; the implementation belongs to submission.sqlite.
SQLiteSubmissionStore = _SQLiteSubmissionStore
