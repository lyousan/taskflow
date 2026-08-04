"""Redis 上的 v0.1 可靠消息 backend。"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
from collections.abc import Callable, Iterable, Mapping
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, overload

from typing_extensions import Self

from ..capabilities import BackendCapabilities, SubmissionCapabilities
from ..config import QueueConfig
from ..consistency import (
    ConsistencyIssue,
    ConsistencyReport,
    KeyspaceMigrationReport,
    RepairReport,
)
from ..errors import BrokerClosedError, SerializerUnavailableError, ValidationError
from ..health import HealthCheck, HealthReport
from ..middleware import Middleware
from ..naming import validate_persistent_name
from ..observability import EventSink, MetricsSink, metric
from ..pagination import decode_cursor, encode_cursor, validate_page_limit
from ..payloads import PAYLOAD_UNSET, normalize_payload, reconstruct_payload
from ..retry import RetryPolicy
from ..scheduler import BackendScheduler
from ..serialization import JsonSerializer, Serializer, SerializerRegistry
from ..submission import (
    PreparedSubmission,
    SubmissionObserver,
    SubmissionRouter,
    SubmissionService,
)
from ..submission.redis import (
    RedisStringDedupSubmissionStore as _RedisStringDedupSubmissionStore,
)
from ..submission.redis import (
    RedisStringDedupSubmissionStore as _SubmissionRedisStringDedupSubmissionStore,
)
from ..submission.redis import (
    RedisSubmissionStore as _SubmissionRedisSubmissionStore,
)
from ..types import (
    BatchSubmitItemResult,
    ConsumerOptions,
    FinishOutcome,
    MessageState,
    MessageStatus,
    MessageSummary,
    Page,
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
from .redis_admin import RedisAdmin
from .redis_components import RedisConsumer, RedisDelivery
from .redis_maintenance import RedisMaintenance
from .redis_observability import RedisObservability
from .redis_state_machine import RedisStateMachine

logger = logging.getLogger(__name__)

_MESSAGE_SUMMARY_FIELDS = (
    "queue",
    "status",
    "attempt",
    "created_at",
    "serializer_name",
    "serializer_version",
    "last_action",
    "last_reason",
    "consumer_id",
    "delivery_id",
    "claimed_at",
    "lease_until",
    "acked_at",
    "workflow_id",
    "parent_id",
    "payload_pruned",
)

_CLOCK_SKEW_WARNING_SECONDS = 5.0
_REDIS_SCHEMA_VERSION = "1"
_REDIS_KEYSPACE_VERSION = "2"


class RedisBroker:
    """使用 Redis Lua 原子迁移实现的异步 v0.1 backend。

    默认命名空间为 ``taskqx``。每个 broker 应使用独立 namespace，便于隔离测试
    或不同业务。当前目标是 Redis standalone / Sentinel，不承诺 Redis Cluster 跨槽事务。
    """

    capabilities = BackendCapabilities(
        distributed_consumers=True,
        high_throughput=True,
        batch_submit=True,
        batch_atomic=True,
        paginated_observation=True,
        stable_pagination_cursors=False,
        message_status_filter=True,
    )

    def __init__(
        self,
        redis: Any,
        *,
        namespace: str = "taskqx",
        default_max_attempts: int = 3,
        default_dedup_ttl: timedelta | None = None,
        serializer: Serializer | None = None,
        serializer_registry: SerializerRegistry | None = None,
        id_factory: Callable[[], str] = _new_id,
        middleware: Middleware | None = None,
        metrics: MetricsSink | None = None,
        pending_recovery_seconds: float = 1.0,
        submission_store: Any | None = None,
        submission_stores: Mapping[str, Any] | None = None,
        queue_submission_profiles: Mapping[str, str] | None = None,
        consistency_pel_page_size: int = 1_000,
        queues: Mapping[str, QueueConfig] | None = None,
        default_ack_tombstone_ttl: timedelta = timedelta(minutes=5),
        event_sink: EventSink | None = None,
        events: EventSink | None = None,
        allow_legacy_names: bool = False,
    ) -> None:
        if (
            isinstance(default_max_attempts, bool)
            or not isinstance(default_max_attempts, int)
            or default_max_attempts < 1
            or pending_recovery_seconds < 0
            or isinstance(consistency_pel_page_size, bool)
            or not isinstance(consistency_pel_page_size, int)
            or consistency_pel_page_size < 1
        ):
            raise ValidationError(
                "default_max_attempts 必须大于等于 1，pending_recovery_seconds 不能为负数，consistency_pel_page_size 必须为正数"
            )
        if (
            not isinstance(default_ack_tombstone_ttl, timedelta)
            or default_ack_tombstone_ttl.total_seconds() <= 0
        ):
            raise ValidationError("default_ack_tombstone_ttl 必须为正数 timedelta")
        validate_persistent_name(
            namespace, label="namespace", allow_legacy=allow_legacy_names
        )
        self._redis, self._namespace = redis, namespace
        self._allow_legacy_names = allow_legacy_names
        self._default_max_attempts, self._default_dedup_ttl = (
            default_max_attempts,
            default_dedup_ttl,
        )
        self._default_queue_config = QueueConfig(
            max_attempts=default_max_attempts, default_dedup_ttl=default_dedup_ttl
        )
        self._default_ack_tombstone_ttl = default_ack_tombstone_ttl
        self._queues = dict(queues or {})
        for queue, config in self._queues.items():
            validate_persistent_name(
                queue, label="queue", allow_legacy=self._allow_legacy_names
            )
            if not isinstance(config, QueueConfig):
                raise ValidationError("queues 的值必须是 QueueConfig")
        self._serializer, self._id_factory = serializer or JsonSerializer(), id_factory
        self.serializer_registry = serializer_registry or SerializerRegistry(
            [self._serializer]
        )
        self._pending_recovery_ms = int(pending_recovery_seconds * 1000)
        self._consistency_pel_page_size = consistency_pel_page_size
        if event_sink is not None and events is not None:
            raise ValidationError("event_sink 与 events 不能同时配置")
        self.middleware, self.metrics, self.event_sink, self._closed = (
            middleware or Middleware(),
            metrics,
            event_sink or events,
            False,
        )
        self._configure_submission_stores(
            submission_store, submission_stores, queue_submission_profiles
        )
        self._submission_observer = SubmissionObserver(
            backend="redis",
            middleware=self.middleware,
            metrics=self.metrics,
            event_sink=self.event_sink,
            serializer=self._serializer,
        )
        self._submission_service = SubmissionService(self, self._prepare_submission)
        self._clock_skew_checked = False
        self._observability = RedisObservability(self)
        self._state_machine = RedisStateMachine(self)
        self._maintenance = RedisMaintenance(self)
        self.admin = RedisAdmin(self)

    @classmethod
    def from_url(
        cls, url: str = "redis://127.0.0.1:6379/2", **kwargs: Any
    ) -> RedisBroker:
        """从 URL 创建 Redis asyncio client；需要安装 ``taskqx[redis]``。"""
        try:
            from redis.asyncio import Redis
        except ImportError as exc:
            raise ValidationError("Redis backend 需要安装 taskqx[redis]") from exc
        return cls(Redis.from_url(url, decode_responses=True), **kwargs)

    def _queue_key(self, queue: str, kind: str) -> str:
        return f"{self._namespace}:queue:{{{queue}}}:{kind}"

    def _message_key(self, queue: str, message_id: str) -> str:
        return f"{self._namespace}:queue:{{{queue}}}:message:{message_id}"

    def _legacy_message_key(self, message_id: str) -> str:
        return f"{self._namespace}:message:{message_id}"

    def _queue_catalog_key(self) -> str:
        return f"{self._namespace}:queues"

    def _message_index_key(self) -> str:
        return f"{self._namespace}:message-index"

    def _keyspace_version_key(self) -> str:
        return f"{self._namespace}:meta:keyspace_version"

    def _group_name(self) -> str:
        """返回当前 namespace 内稳定的 Consumer Group 名称。"""

        return "taskqx"

    async def _ensure_group(self, queue: str) -> None:
        """幂等创建队列对应的 Redis Stream Consumer Group 并登记 queue。"""

        try:
            await self._redis.xgroup_create(
                self._queue_key(queue, "stream"),
                self._group_name(),
                id="0",
                mkstream=True,
            )
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        await self._register_queue(queue)

    async def _register_queue(self, queue: str) -> None:
        await self._redis.sadd(self._queue_catalog_key(), queue)

    async def _catalog_queues(self) -> tuple[str, ...]:
        values = await self._redis.smembers(self._queue_catalog_key())
        return tuple(sorted(_as_text(value) for value in values))

    def _schema_key(self) -> str:
        return f"{self._namespace}:meta:schema_version"

    def _dedup_key(self, scope: str, key: str) -> str:
        """以固定长度摘要构造 dedup key，隔离特殊字符与 Cluster hash tag。"""

        scope_hash = hashlib.sha256(scope.encode()).hexdigest()
        key_hash = hashlib.sha256(key.encode()).hexdigest()
        return f"{self._namespace}:dedup:{scope_hash}:{key_hash}"

    def _validate_queue(self, queue: str) -> None:
        validate_persistent_name(
            queue, label="queue", allow_legacy=self._allow_legacy_names
        )

    def _queue_config(self, queue: str) -> QueueConfig:
        self._validate_queue(queue)
        return self._queues.get(queue, self._default_queue_config)

    def _ack_tombstone_ttl(self, queue: str) -> timedelta:
        return self._queue_config(queue).ack_tombstone_ttl or self._default_ack_tombstone_ttl

    def _configure_submission_stores(
        self,
        submission_store: Any | None,
        submission_stores: Mapping[str, Any] | None,
        queue_submission_profiles: Mapping[str, str] | None,
    ) -> None:
        self._submission_router = SubmissionRouter(
            self,
            default_store=_RedisStringDedupSubmissionStore(self),
            submission_store=submission_store,
            submission_stores=submission_stores,
            queue_submission_profiles=queue_submission_profiles,
        )
        self._submission_stores = self._submission_router.stores
        self._queue_submission_profiles = self._submission_router.profiles
        self.submission_store = self._submission_router.default

    def submission_capabilities(self, queue: str) -> SubmissionCapabilities:
        self._validate_queue(queue)
        profile = self._submission_router.profile_for(queue)
        return (
            self.submission_store.capabilities
            if profile == "default"
            else self._submission_router.capabilities(queue)
        )

    def _submission_store_for(self, queue: str) -> Any:
        return (
            self.submission_store
            if self._submission_router.profile_for(queue) == "default"
            else self._submission_router.for_queue(queue)
        )

    def _ensure_open(self) -> None:
        if self._closed:
            raise BrokerClosedError("broker 已关闭")

    async def _now(self) -> datetime:
        seconds, microseconds = await self._redis.time()
        return datetime.fromtimestamp(
            int(seconds) + int(microseconds) / 1_000_000, timezone.utc
        )

    async def start(self) -> None:
        """验证连接，避免首个业务提交才暴露连接配置错误。"""
        self._ensure_open()
        await self._redis.ping()
        await self._redis.setnx(self._schema_key(), _REDIS_SCHEMA_VERSION)
        for queue in self._queues:
            await self._ensure_group(queue)
        if not self._clock_skew_checked:
            server_now = await self._now()
            skew_seconds = abs((utc_now() - server_now).total_seconds())
            self._clock_skew_checked = True
            if skew_seconds >= _CLOCK_SKEW_WARNING_SECONDS:
                logger.warning(
                    "taskqx Redis server clock differs materially from the application clock; "
                    "Redis time remains authoritative for expiry and leases",
                    extra={
                        "clock_skew_seconds": skew_seconds,
                        "namespace": self._namespace,
                    },
                )

    async def close(self) -> None:
        """关闭底层异步 Redis client。"""
        if not self._closed:
            await self._redis.aclose()
            self._closed = True

    def select_namespace(self, namespace: str) -> None:
        """Switch this broker's observation and operation namespace."""

        self._ensure_open()
        validate_persistent_name(
            namespace, label="namespace", allow_legacy=self._allow_legacy_names
        )
        self._namespace = namespace

    async def list_namespaces(self) -> tuple[str, ...]:
        """Return namespaces discovered from Taskqx schema markers."""

        self._ensure_open()
        namespaces: set[str] = set()
        suffix = ":meta:schema_version"
        async for key in self._redis.scan_iter(match=f"*{suffix}"):
            decoded = key.decode() if isinstance(key, bytes) else key
            if decoded.endswith(suffix):
                namespaces.add(decoded[: -len(suffix)])
        return tuple(sorted(namespaces))

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    def _encode(self, message: TaskMessage) -> str:
        """将任意 bytes serializer 输出编码为 Redis 安全的 ASCII 文本。"""
        raw = self._serializer.dumps(
            {
                "id": message.id,
                "queue": message.queue,
                "payload": message.payload,
                "metadata": dict(message.metadata),
                "dedup_key": message.dedup_key,
                "dedup_scope": message.dedup_scope,
                "workflow_id": message.workflow_id,
                "parent_id": message.parent_id,
                "created_at": _timestamp(message.created_at),
                "available_at": _timestamp(message.available_at)
                if message.available_at
                else None,
                "expires_at": _timestamp(message.expires_at)
                if message.expires_at
                else None,
                "max_attempts": message.max_attempts,
                "payload_schema_name": message.payload_schema_name,
                "payload_schema_version": message.payload_schema_version,
            }
        )
        return base64.b64encode(raw).decode("ascii")

    def _decode(
        self,
        envelope: str,
        serializer_name: str | None = None,
        serializer_version: str | None = None,
    ) -> TaskMessage:
        decoder = (
            self._serializer
            if serializer_name is None
            or (
                serializer_name == self._serializer.name
                and serializer_version == self._serializer.version
            )
            else self.serializer_registry.resolve(
                serializer_name, serializer_version or ""
            )
        )
        value = decoder.loads(base64.b64decode(envelope.encode("ascii")))
        return TaskMessage(
            value["id"],
            value["queue"],
            value["payload"],
            value["metadata"],
            value["dedup_key"],
            value["dedup_scope"],
            value["workflow_id"],
            value["parent_id"],
            _datetime(value["created_at"]) or utc_now(),
            _datetime(value["expires_at"]),
            value["max_attempts"],
            _datetime(value.get("available_at")),
            value.get("payload_schema_name"),
            value.get("payload_schema_version"),
        )

    async def submit(
        self,
        submission: SubmitRequest | None = None,
        *,
        queue: str | None = None,
        payload: Any = PAYLOAD_UNSET,
        metadata: Mapping[str, Any] | None = None,
        dedup_key: str | None = None,
        dedup_scope: str | None = None,
        dedup_ttl: timedelta | None = None,
        delay: timedelta | None = None,
        expires_at: datetime | None = None,
        max_attempts: int | None = None,
        workflow_id: str | None = None,
        parent_id: str | None = None,
        payload_type: type[Any] | None = None,
    ) -> SubmitResult:
        """Submit one draft or use the legacy keyword form.

        Draft input and legacy keywords are mutually exclusive. A persisted
        message is always re-submitted through :meth:`submit_from`, which
        creates a new draft and therefore cannot reuse message identity.
        """

        if submission is not None:
            if (
                any(
                    value is not None
                    for value in (
                        queue,
                        metadata,
                        dedup_key,
                        dedup_scope,
                        dedup_ttl,
                        delay,
                        expires_at,
                        max_attempts,
                        workflow_id,
                        parent_id,
                        payload_type,
                    )
                )
                or payload is not PAYLOAD_UNSET
            ):
                raise ValidationError("提交草稿不能与关键字参数混用")
            if not isinstance(submission, SubmitRequest):
                raise ValidationError("submit 只接受 SubmitRequest 或关键字参数")
            return await self._submission_service.submit_request(submission)
        if queue is None or payload is PAYLOAD_UNSET:
            raise ValidationError("submit 必须提供 queue 和 payload")
        return await self._submission_service.submit(
            queue=queue,
            payload=payload,
            metadata=metadata,
            dedup_key=dedup_key,
            dedup_scope=dedup_scope,
            dedup_ttl=dedup_ttl,
            delay=delay,
            expires_at=expires_at,
            max_attempts=max_attempts,
            workflow_id=workflow_id,
            parent_id=parent_id,
            payload_type=payload_type,
        )

    async def submit_from(self, message: TaskMessage, **overrides: Any) -> SubmitResult:
        """Derive and submit a fresh draft from an immutable persisted message."""

        return await self.submit(message.clone(**overrides))

    async def _prepare_submission(
        self,
        *,
        queue: str,
        payload: Any,
        metadata: Mapping[str, Any] | None = None,
        dedup_key: str | None = None,
        dedup_scope: str | None = None,
        dedup_ttl: timedelta | None = None,
        delay: timedelta | None = None,
        expires_at: datetime | None = None,
        max_attempts: int | None = None,
        workflow_id: str | None = None,
        parent_id: str | None = None,
        payload_type: type[Any] | None = None,
        payload_schema_name: str | None = None,
        payload_schema_version: str | None = None,
    ) -> tuple[PreparedSubmission, TaskMessage]:
        """校验请求、生成 ID 并序列化；不在此处触碰 Redis 状态。"""
        self._ensure_open()
        self._validate_queue(queue)
        if (dedup_key is None) != (dedup_scope is None):
            raise ValidationError("dedup_key 与 dedup_scope 必须同时提供")
        config = self._queue_config(queue)
        ttl = (
            (
                config.default_dedup_ttl
                if queue in self._queues
                else self._default_dedup_ttl
            )
            if dedup_ttl is None
            else dedup_ttl
        )
        if ttl is not None and not isinstance(ttl, timedelta):
            raise ValidationError("dedup_ttl 必须是 timedelta")
        if dedup_key is not None and (ttl is None or ttl.total_seconds() <= 0):
            raise ValidationError("启用去重时必须提供正数 dedup_ttl 或配置默认值")
        attempts = max_attempts if max_attempts is not None else config.max_attempts
        if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 1:
            raise ValidationError("max_attempts 必须大于等于 1")
        now = await self._now()
        if delay is not None and not isinstance(delay, timedelta):
            raise ValidationError("delay 必须是 timedelta")
        if delay is not None and delay.total_seconds() < 0:
            raise ValidationError("delay 不能为负数")
        if expires_at is not None and expires_at.tzinfo is None:
            raise ValidationError("expires_at 必须带时区")
        available_at = now + delay if delay and delay.total_seconds() > 0 else None
        encoded_payload, schema = normalize_payload(payload, payload_type=payload_type)
        message = TaskMessage(
            self._id_factory(),
            queue,
            encoded_payload,
            metadata or {},
            dedup_key,
            dedup_scope,
            workflow_id,
            parent_id,
            now,
            expires_at,
            attempts,
            available_at,
            schema.name if schema else payload_schema_name,
            schema.version if schema else payload_schema_version,
        )
        envelope = self._encode(message)
        if (
            config.max_payload_bytes is not None
            and len(self._serializer.dumps(encoded_payload)) > config.max_payload_bytes
        ):
            raise ValidationError(
                f"payload 超过 queue {queue!r} 的 max_payload_bytes 限制"
            )
        initial_expired = bool(expires_at and expires_at <= now)
        prepared = PreparedSubmission(
            message.id,
            queue,
            base64.b64decode(envelope.encode("ascii")),
            MessageStatus.EXPIRED.value
            if initial_expired
            else (
                MessageStatus.DELAYED.value
                if available_at
                else MessageStatus.READY.value
            ),
            now,
            int(_timestamp(expires_at) * 1000) if expires_at else None,
            dedup_scope,
            dedup_key,
            int(ttl.total_seconds() * 1000) if ttl else None,
            attempts,
            self._serializer.name,
            self._serializer.version,
            int(_timestamp(available_at) * 1000) if available_at else None,
            workflow_id=message.workflow_id,
            parent_id=message.parent_id,
        )
        return prepared, message

    @overload
    async def submit_many(
        self, messages: list[SubmitRequest], *, atomic: Literal[True] = True
    ) -> list[SubmitResult]: ...

    @overload
    async def submit_many(
        self, messages: list[SubmitRequest], *, atomic: Literal[False]
    ) -> list[BatchSubmitItemResult]: ...

    async def submit_many(
        self, messages: list[SubmitRequest], *, atomic: bool = True
    ) -> list[SubmitResult] | list[BatchSubmitItemResult]:
        """批量提交；non-atomic 模式对每一项独立准备、提交并返回结果。"""

        return await self._submission_service.submit_many(messages, atomic=atomic)

    async def _record_submitted(
        self, prepared: PreparedSubmission, message: TaskMessage, result: SubmitResult
    ) -> None:
        """Publish submission observation after recording global read indexes."""

        if result.accepted:
            await self._register_queue(prepared.queue)
            await self._redis.setnx(
                self._keyspace_version_key(), _REDIS_KEYSPACE_VERSION
            )
            await self._redis.hset(
                self._message_index_key(), prepared.message_id, prepared.queue
            )
        await self._submission_observer.record(prepared, message, result)

    def _reconstruct_replay_message(
        self,
        message: TaskMessage,
        *,
        queue: str,
        payload: Any = PAYLOAD_UNSET,
        payload_type: type[Any] | None = None,
        metadata: Mapping[str, Any] | None = None,
        expires_at: datetime | None | object = PAYLOAD_UNSET,
    ) -> TaskMessage:
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
            message,
            queue=queue,
            payload=encoded,
            metadata=message.metadata if metadata is None else metadata,
            expires_at=replayed_expires_at,
            payload_schema_name=schema_name,
            payload_schema_version=schema_version,
        )
        self._validate_queue(replayed.queue)
        config = self._queue_config(replayed.queue)
        if (
            config.max_payload_bytes is not None
            and len(self._serializer.dumps(encoded)) > config.max_payload_bytes
        ):
            raise ValidationError(
                f"payload 超过 queue {replayed.queue!r} 的 max_payload_bytes 限制"
            )
        self._encode(replayed)
        return replayed

    def consumer(
        self,
        queue: str,
        *,
        consumer_id: str | None = None,
        options: ConsumerOptions | None = None,
    ) -> RedisConsumer:
        """创建多进程安全的异步消费者。"""
        self._ensure_open()
        self._validate_queue(queue)
        chosen = options or ConsumerOptions(
            lease_seconds=self._queue_config(queue).lease.total_seconds()
        )
        if (
            chosen.lease_seconds <= 0
            or chosen.poll_interval < 0
            or chosen.concurrency < 1
        ):
            raise ValidationError("消费者参数必须有效")
        return RedisConsumer(self, queue, consumer_id or self._id_factory(), chosen)

    def scheduler(
        self,
        *,
        queues: Iterable[str] | None = None,
        interval: timedelta = timedelta(seconds=1),
    ) -> BackendScheduler:
        """Create a backend-only lifecycle scheduler."""

        return BackendScheduler(self, queues=queues, interval=interval)

    async def _scheduler_queues(self) -> tuple[str, ...]:
        """Discover queues from the persisted catalog without maintenance."""

        self._ensure_open()
        catalog = await self._catalog_queues()
        if catalog:
            return catalog
        queues: set[str] = set()
        prefix = f"{self._namespace}:queue:{{"
        suffix = "}:stats"
        async for raw_key in self._redis.scan_iter(
            match=f"{self._namespace}:queue:*:stats"
        ):
            key = _as_text(raw_key)
            if key.startswith(prefix) and key.endswith(suffix):
                queues.add(key[len(prefix) : -len(suffix)])
        return tuple(sorted(queues))

    def worker(
        self,
        queue: str,
        handler: Handler,
        *,
        concurrency: int | None = None,
        consumer_id: str | None = None,
        options: ConsumerOptions | None = None,
        retry_policy: RetryPolicy | None = None,
        heartbeat_seconds: float | None = None,
        payload_type: type[Any] | None = None,
    ) -> TaskWorker:
        """创建一个真正受 ``concurrency`` 限制的 Worker。"""
        selected = options or ConsumerOptions(
            lease_seconds=self._queue_config(queue).lease.total_seconds()
        )
        if retry_policy is None and queue in self._queues:
            retry_policy = self._queue_config(queue).retry_policy
        return TaskWorker(
            self,
            queue,
            handler,
            concurrency=concurrency
            if concurrency is not None
            else selected.concurrency,
            consumer_id=consumer_id,
            options=selected,
            retry_policy=retry_policy,
            heartbeat_seconds=heartbeat_seconds,
            payload_type=payload_type,
        )

    async def run(
        self,
        queue: str,
        handler: Handler,
        *,
        concurrency: int | None = None,
        consumer_id: str | None = None,
        options: ConsumerOptions | None = None,
        retry_policy: RetryPolicy | None = None,
        heartbeat_seconds: float | None = None,
        payload_type: type[Any] | None = None,
    ) -> None:
        await self.worker(
            queue,
            handler,
            concurrency=concurrency,
            consumer_id=consumer_id,
            options=options,
            retry_policy=retry_policy,
            heartbeat_seconds=heartbeat_seconds,
            payload_type=payload_type,
        ).run()

    async def _claim(
        self, queue: str, consumer_id: str, lease_seconds: float
    ) -> RedisDelivery | None:
        return await self._state_machine.claim(queue, consumer_id, lease_seconds)

    async def _finish(
        self,
        delivery: RedisDelivery,
        action: str,
        reason: str | None = None,
        error: BaseException | None = None,
        delay: timedelta | None = None,
        max_attempts: int | None = None,
    ) -> FinishOutcome:
        return await self._state_machine.finish(
            delivery, action, reason, error, delay, max_attempts
        )

    async def _extend(self, delivery: RedisDelivery, seconds: float | None) -> datetime:
        return await self._state_machine.extend(delivery, seconds)

    async def _recover_uncommitted_pel(self, queue: str) -> int:
        return await self._state_machine.recover_uncommitted_pel(queue)

    async def _emit_maintenance_event(
        self,
        queue: str,
        message_id: str,
        name: str,
        status: str,
        *,
        reason: str | None = None,
        error_type: str | None = None,
        metric_name: str | None = None,
    ) -> None:
        await self._observability.maintenance(
            queue,
            message_id,
            name,
            status,
            reason=reason,
            error_type=error_type,
            metric_name=metric_name,
        )

    async def maintain(self, queue: str | None = None) -> int:
        """回收已到期租约，并把 READY/LEASED 的过期消息移入 EQ。"""
        return await self._maintenance.run(queue)

    async def inspect(self, queue: str) -> QueueStats:
        """读取队列实时计数与累计计数。"""
        await self.maintain(queue)
        stats = await self._redis.hgetall(self._queue_key(queue, "stats"))
        (
            ready,
            leased,
            delayed,
            first_ready,
            dead_letters,
            expired,
        ) = await asyncio.gather(
            self._redis.zcard(self._queue_key(queue, "ready")),
            self._redis.zcard(self._queue_key(queue, "leases")),
            self._redis.zcard(self._queue_key(queue, "delayed")),
            self._redis.zrange(self._queue_key(queue, "ready"), 0, 0, withscores=True),
            self._redis.llen(self._queue_key(queue, "dlq")),
            self._redis.llen(self._queue_key(queue, "eq")),
        )
        earliest = _datetime(float(first_ready[0][1])) if first_ready else None
        await metric(self.metrics, "queue_ready", float(ready), queue=queue)
        await metric(self.metrics, "queue_leased", float(leased), queue=queue)
        await metric(self.metrics, "queue_delayed", float(delayed), queue=queue)
        return QueueStats(
            queue,
            int(ready),
            int(leased),
            int(dead_letters),
            int(expired),
            earliest,
            int(stats.get("submitted_total", 0)),
            int(stats.get("acked_total", 0)),
            int(stats.get("retried_total", 0)),
            int(stats.get("reclaimed_total", 0)),
            int(stats.get("dead_lettered_total", 0)),
            int(delayed),
        )

    async def observe_queue(self, queue: str) -> QueueStats:
        """Read a queue snapshot without maintenance or metrics emission."""

        self._validate_queue(queue)
        stats = await self._redis.hgetall(self._queue_key(queue, "stats"))
        (
            ready,
            leased,
            delayed,
            first_ready,
            dead_letters,
            expired,
        ) = await asyncio.gather(
            self._redis.zcard(self._queue_key(queue, "ready")),
            self._redis.zcard(self._queue_key(queue, "leases")),
            self._redis.zcard(self._queue_key(queue, "delayed")),
            self._redis.zrange(self._queue_key(queue, "ready"), 0, 0, withscores=True),
            self._redis.llen(self._queue_key(queue, "dlq")),
            self._redis.llen(self._queue_key(queue, "eq")),
        )
        earliest = _datetime(float(first_ready[0][1])) if first_ready else None
        return QueueStats(
            queue,
            int(ready),
            int(leased),
            int(dead_letters),
            int(expired),
            earliest,
            int(stats.get("submitted_total", 0)),
            int(stats.get("acked_total", 0)),
            int(stats.get("retried_total", 0)),
            int(stats.get("reclaimed_total", 0)),
            int(stats.get("dead_lettered_total", 0)),
            int(delayed),
        )

    async def inspect_message(self, message_id: str) -> TaskMessage | None:
        """按稳定 message ID 查询原始业务消息，不改变其状态。"""

        queue = await self._message_queue(message_id)
        if queue is None:
            return None
        fields = await self._redis.hgetall(self._message_key(queue, message_id))
        if not fields:
            fields = await self._redis.hgetall(self._legacy_message_key(message_id))
        if not fields:
            return None
        if fields.get("payload_pruned") == "1" or "envelope" not in fields:
            return None
        return self._decode(
            fields["envelope"],
            fields.get("serializer_name"),
            fields.get("serializer_version"),
        )

    async def _message_queue(self, message_id: str) -> str | None:
        queue = await self._redis.hget(self._message_index_key(), message_id)
        if queue is not None:
            return _as_text(queue)
        for candidate in await self._catalog_queues():
            if await self._redis.exists(self._message_key(candidate, message_id)):
                return candidate
        if await self._redis.exists(self._legacy_message_key(message_id)):
            fields = await self._redis.hgetall(self._legacy_message_key(message_id))
            value = fields.get("queue", fields.get(b"queue"))
            return _as_text(value) if value is not None else None
        return None

    async def observe_message(self, message_id: str) -> TaskMessage | None:
        """Read one message without changing Redis state."""

        return await self.inspect_message(message_id)

    async def list_queues(
        self, *, cursor: str | None = None, limit: int = 100
    ) -> Page[QueueStats]:
        """List queues from their persisted stats keys.

        Redis SCAN cursors are deliberately documented as best-effort rather than
        stable snapshots; concurrent writes can reorder or repeat entries.
        """

        limit = validate_page_limit(limit)
        values = decode_cursor(cursor, size=1)
        offset = 0 if values is None else values[0]
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise ValidationError("cursor 不属于 Redis 队列目录")
        queues = await self._catalog_queues()
        if not queues:
            queues = await self._scheduler_queues()
        selected = queues[offset : offset + limit]
        next_offset = offset + len(selected)
        return Page(
            tuple([await self.observe_queue(queue) for queue in selected]),
            encode_cursor(next_offset) if next_offset < len(queues) else None,
            len(queues),
        )

    async def list_message_summaries(
        self,
        queue: str,
        *,
        status: MessageStatus | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Page[MessageSummary]:
        """Read a bounded Redis metadata scan without decoding payloads."""

        self._validate_queue(queue)
        limit = validate_page_limit(limit)
        if status is not None and not isinstance(status, MessageStatus):
            raise ValidationError("status 必须是 MessageStatus")
        scan_cursor = self._scan_cursor(cursor)
        items: list[MessageSummary] = []
        seen_keys: set[str] = set()
        while len(items) < limit:
            scan_cursor, keys = await self._redis.scan(
                cursor=scan_cursor,
                match=f"{self._namespace}:queue:{{{queue}}}:message:*",
                count=max(10, limit * 2),
            )
            for key in keys:
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                values = await self._redis.hmget(key, _MESSAGE_SUMMARY_FIELDS)
                fields = {
                    name: value
                    for name, value in zip(_MESSAGE_SUMMARY_FIELDS, values, strict=True)
                    if value is not None
                }
                if not fields:
                    continue
                message_status = MessageStatus(fields["status"])
                if status is not None and message_status is not status:
                    continue
                items.append(
                    self._message_summary_from_fields(
                        _as_text(key).rsplit(":", 1)[-1], fields, message_status
                    )
                )
                if len(items) == limit:
                    break
            if scan_cursor == 0:
                break
        next_cursor = encode_cursor(scan_cursor) if scan_cursor != 0 else None
        return Page(tuple(items), next_cursor, None)

    async def list_messages(
        self,
        queue: str,
        *,
        status: MessageStatus | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> Page[MessageState]:
        """Read a bounded best-effort Redis message scan for one queue."""

        self._validate_queue(queue)
        limit = validate_page_limit(limit)
        if status is not None and not isinstance(status, MessageStatus):
            raise ValidationError("status 必须是 MessageStatus")
        scan_cursor = self._scan_cursor(cursor)
        items: list[MessageState] = []
        seen_keys: set[str] = set()
        while len(items) < limit:
            scan_cursor, keys = await self._redis.scan(
                cursor=scan_cursor,
                match=f"{self._namespace}:queue:{{{queue}}}:message:*",
                count=max(10, limit * 2),
            )
            for key in keys:
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                fields = await self._redis.hgetall(key)
                if _as_text(fields.get("queue", fields.get(b"queue"))) != queue:
                    continue
                if fields.get("payload_pruned") == "1":
                    continue
                message_status = MessageStatus(fields["status"])
                if status is not None and message_status is not status:
                    continue
                items.append(self._message_state_from_fields(fields, message_status))
                if len(items) == limit:
                    break
            if scan_cursor == 0:
                break
        next_cursor = encode_cursor(scan_cursor) if scan_cursor != 0 else None
        return Page(tuple(items), next_cursor, None)

    @staticmethod
    def _scan_cursor(cursor: str | None) -> int:
        values = decode_cursor(cursor, size=1)
        value = 0 if values is None else values[0]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValidationError("cursor 不属于 Redis 扫描")
        return value

    def _message_state_from_fields(
        self, fields: Mapping[str, str], status: MessageStatus
    ) -> MessageState:
        def timestamp(name: str) -> datetime | None:
            value = fields.get(name)
            return _datetime(float(value)) if value else None

        return MessageState(
            message=self._decode(
                fields["envelope"],
                fields.get("serializer_name"),
                fields.get("serializer_version"),
            ),
            status=status,
            attempt=int(fields["attempt"]),
            last_action=fields.get("last_action") or None,
            last_reason=fields.get("last_reason") or None,
            consumer_id=fields.get("consumer_id") or None,
            delivery_id=fields.get("delivery_id") or None,
            claimed_at=timestamp("claimed_at"),
            lease_until=timestamp("lease_until"),
        )

    def _message_summary_from_fields(
        self, message_id: str, fields: Mapping[str, str], status: MessageStatus
    ) -> MessageSummary:
        def timestamp(name: str) -> datetime | None:
            value = fields.get(name)
            return _datetime(float(value)) if value else None

        return MessageSummary(
            message_id=message_id,
            queue=fields["queue"],
            status=status,
            attempt=int(fields["attempt"]),
            created_at=timestamp("created_at") or utc_now(),
            serializer_name=fields.get("serializer_name", "json"),
            serializer_version=fields.get("serializer_version", "1"),
            last_action=fields.get("last_action") or None,
            last_reason=fields.get("last_reason") or None,
            consumer_id=fields.get("consumer_id") or None,
            delivery_id=fields.get("delivery_id") or None,
            claimed_at=timestamp("claimed_at"),
            lease_until=timestamp("lease_until"),
            acked_at=timestamp("acked_at"),
            workflow_id=fields.get("workflow_id") or None,
            parent_id=fields.get("parent_id") or None,
            payload_pruned=fields.get("payload_pruned") == "1",
        )

    async def health_check(self) -> HealthReport:
        """Run strictly read-only Redis diagnostics without claiming or modifying messages."""

        if self._closed:
            return HealthReport(
                False,
                "redis",
                self._namespace,
                (HealthCheck("connection", "error", "broker is closed"),),
            )
        try:
            await self._redis.ping()
        except Exception as exc:  # noqa: BLE001 - report client failures as diagnostics
            return HealthReport(
                False,
                "redis",
                self._namespace,
                (HealthCheck("connection", "error", f"{type(exc).__name__}: {exc}"),),
            )

        checks: list[HealthCheck] = [HealthCheck("connection", "ok")]
        try:
            schema_version = await self._redis.get(self._schema_key())
            schema_version = (
                _as_text(schema_version) if schema_version is not None else None
            )
            checks.append(
                HealthCheck(
                    "schema_version",
                    "ok" if schema_version == _REDIS_SCHEMA_VERSION else "error",
                    None
                    if schema_version == _REDIS_SCHEMA_VERSION
                    else f"expected {_REDIS_SCHEMA_VERSION}, found {schema_version!r}",
                )
            )
        except Exception as exc:  # noqa: BLE001 - report client failures as diagnostics
            checks.append(
                HealthCheck("schema_version", "error", f"{type(exc).__name__}: {exc}")
            )

        checks.append(HealthCheck("namespace", "ok", self._namespace))
        missing_configured_groups: list[str] = []
        missing_dynamic_groups: list[str] = []
        unavailable: list[str] = []
        try:
            stream_suffix = "}:stream"
            stream_prefix = f"{self._namespace}:queue:{{"
            queues = set(self._queues)
            async for raw_key in self._redis.scan_iter(
                match=f"{self._namespace}:queue:*:stream"
            ):
                key = _as_text(raw_key)
                if not key.startswith(stream_prefix) or not key.endswith(stream_suffix):
                    continue
                queues.add(key[len(stream_prefix) : -len(stream_suffix)])
            for queue in sorted(queues):
                stream = self._queue_key(queue, "stream")
                if not await self._redis.exists(stream):
                    (
                        missing_configured_groups
                        if queue in self._queues
                        else missing_dynamic_groups
                    ).append(queue)
                    continue
                groups = await self._redis.xinfo_groups(stream)
                names = {
                    _as_text(group.get("name", group.get(b"name"))) for group in groups
                }
                if self._group_name() not in names:
                    (
                        missing_configured_groups
                        if queue in self._queues
                        else missing_dynamic_groups
                    ).append(queue)
            checks.append(
                HealthCheck(
                    "consumer_groups",
                    "error"
                    if missing_configured_groups
                    else "warning"
                    if missing_dynamic_groups
                    else "ok",
                    (
                        f"missing configured group {self._group_name()!r}: {', '.join(missing_configured_groups)}"
                        if missing_configured_groups
                        else f"missing dynamic group {self._group_name()!r}: {', '.join(missing_dynamic_groups)}"
                        if missing_dynamic_groups
                        else None
                    ),
                )
            )
        except Exception as exc:  # noqa: BLE001 - report client failures as diagnostics
            checks.append(
                HealthCheck("consumer_groups", "error", f"{type(exc).__name__}: {exc}")
            )

        legacy_messages = 0
        try:
            for pattern in (
                f"{self._namespace}:queue:{{*}}:message:*",
                f"{self._namespace}:message:*",
            ):
                async for raw_key in self._redis.scan_iter(match=pattern):
                    fields = await self._redis.hgetall(raw_key)
                    raw_name = fields.get("serializer_name", fields.get(b"serializer_name"))
                    raw_version = fields.get(
                        "serializer_version", fields.get(b"serializer_version")
                    )
                    if raw_name is None or raw_version is None:
                        legacy_messages += 1
                        continue
                    name, version = _as_text(raw_name), _as_text(raw_version)
                    try:
                        self.serializer_registry.resolve(name, version)
                    except SerializerUnavailableError:
                        unavailable.append(f"{name}@{version}")
            checks.append(
                HealthCheck(
                    "serializer_registry",
                    "ok" if not unavailable else "error",
                    None
                    if not unavailable
                    else f"unavailable: {', '.join(sorted(set(unavailable)))}",
                )
            )
            checks.append(
                HealthCheck(
                    "legacy_serializer_identity",
                    "warning" if legacy_messages else "ok",
                    f"{legacy_messages} legacy JSON message(s) use the compatible default reader"
                    if legacy_messages
                    else None,
                )
            )
        except Exception as exc:  # noqa: BLE001 - report client failures as diagnostics
            checks.append(
                HealthCheck(
                    "serializer_registry", "error", f"{type(exc).__name__}: {exc}"
                )
            )

        has_unrecoverable = any(
            check.name == "serializer_registry" and check.status == "error"
            for check in checks
        )
        checks.append(
            HealthCheck(
                "unrecoverable_errors",
                "error" if has_unrecoverable else "ok",
                "messages require unavailable serializers"
                if has_unrecoverable
                else None,
            )
        )
        return HealthReport(
            all(check.status != "error" for check in checks),
            "redis",
            self._namespace,
            tuple(checks),
        )

    async def check_consistency(self, queue: str) -> ConsistencyReport:
        """Check Redis state hashes against all derived indexes and Stream state."""

        self._validate_queue(queue)
        await self._redis.ping()
        keys = {
            kind: self._queue_key(queue, kind)
            for kind in ("ready", "leases", "delayed", "dlq", "eq", "stream")
        }
        ready_ids = {
            _as_text(value) for value in await self._redis.zrange(keys["ready"], 0, -1)
        }
        lease_ids = {
            _as_text(value) for value in await self._redis.zrange(keys["leases"], 0, -1)
        }
        delayed_ids = {
            _as_text(value)
            for value in await self._redis.zrange(keys["delayed"], 0, -1)
        }
        dlq_ids = [
            _as_text(value) for value in await self._redis.lrange(keys["dlq"], 0, -1)
        ]
        eq_ids = [
            _as_text(value) for value in await self._redis.lrange(keys["eq"], 0, -1)
        ]
        stream_entries = await self._redis.xrange(keys["stream"], "-", "+")
        stream_by_id = {
            _as_text(entry_id): _as_text(
                fields.get("message_id", fields.get(b"message_id"))
            )
            for entry_id, fields in stream_entries
        }
        stream_ids = set(stream_by_id.values())
        pending_entry_ids: set[str] = set()
        pending_available = True
        try:
            pending_entry_ids = await self._scan_pending_entry_ids(keys["stream"])
        except (
            Exception
        ) as exc:  # A missing group is separately reported by health_check().
            pending_available = False
            logger.debug("could not inspect Redis pending entries", exc_info=exc)

        # Indexes alone are not a source of truth: a message can lose *all* its
        # derived entries.  Include every hash belonging to this queue so that
        # such total-loss corruption is still visible and repairable.
        message_ids = (
            ready_ids
            | lease_ids
            | delayed_ids
            | set(dlq_ids)
            | set(eq_ids)
            | stream_ids
        )
        message_fields: dict[str, Any] = {}
        async for raw_key in self._redis.scan_iter(
            match=f"{self._namespace}:queue:{{{queue}}}:message:*"
        ):
            fields = await self._redis.hgetall(raw_key)
            message_id = _as_text(raw_key).rsplit(":", 1)[-1]
            message_ids.add(message_id)
            message_fields[message_id] = fields

        issues: list[ConsistencyIssue] = []
        for message_id in sorted(message_ids):
            fields = message_fields.get(message_id)
            if fields is None:
                fields = await self._redis.hgetall(self._message_key(queue, message_id))
            if not fields:
                issues.append(ConsistencyIssue("missing_message", message_id))
                continue
            status = _as_text(fields.get("status", fields.get(b"status")))
            entry_id = _as_text(fields.get("entry_id", fields.get(b"entry_id")))
            expected = {
                "ready": (("ready_index", ready_ids), ("stream_entry", stream_ids)),
                "leased": (("lease_index", lease_ids), ("stream_entry", stream_ids)),
                "delayed": (("delayed_index", delayed_ids),),
                "dead_lettered": (("dlq_entry", set(dlq_ids)),),
                "expired": (("eq_entry", set(eq_ids)),),
            }.get(status, ())
            for name, values in expected:
                if message_id not in values:
                    issues.append(ConsistencyIssue(f"missing_{name}", message_id))
            # The current entry ID, rather than merely any old entry for this
            # message, is the one that may be safely claimed or acknowledged.
            if (
                status in {"ready", "leased"}
                and message_id in stream_ids
                and stream_by_id.get(entry_id) != message_id
            ):
                issues.append(ConsistencyIssue("stale_entry_id", message_id, entry_id))
            if (
                status == "leased"
                and pending_available
                and entry_id not in pending_entry_ids
            ):
                issues.append(ConsistencyIssue("missing_pel", message_id, entry_id))
            if status != "ready" and message_id in ready_ids:
                issues.append(ConsistencyIssue("orphan_ready_index", message_id))
            if status != "leased" and message_id in lease_ids:
                issues.append(ConsistencyIssue("orphan_lease_index", message_id))
            if status != "delayed" and message_id in delayed_ids:
                issues.append(ConsistencyIssue("orphan_delayed_index", message_id))
            if status != "dead_lettered" and message_id in dlq_ids:
                issues.append(ConsistencyIssue("orphan_dlq_entry", message_id))
            if status != "expired" and message_id in eq_ids:
                issues.append(ConsistencyIssue("orphan_eq_entry", message_id))
        for name, audit_ids in (
            ("duplicate_dlq_entry", dlq_ids),
            ("duplicate_eq_entry", eq_ids),
        ):
            issues.extend(
                ConsistencyIssue(name, message_id)
                for message_id in set(audit_ids)
                if audit_ids.count(message_id) > 1
            )
        for entry_id in pending_entry_ids:
            pending_message_id = stream_by_id.get(entry_id)
            if pending_message_id is None:
                issues.append(ConsistencyIssue("orphan_pel", detail=entry_id))
                continue
            fields = await self._redis.hgetall(
                self._message_key(queue, pending_message_id)
            )
            if _as_text(fields.get("status", fields.get(b"status"))) != "leased":
                issues.append(
                    ConsistencyIssue("stale_pel", pending_message_id, entry_id)
                )
        return ConsistencyReport(queue, "redis", self._namespace, tuple(issues))

    async def _scan_pending_entry_ids(self, stream: str) -> set[str]:
        """Read the full PEL with exclusive-ID pagination, never a hidden cap."""

        minimum = "-"
        entry_ids: set[str] = set()
        while True:
            page = await self._redis.xpending_range(
                stream,
                self._group_name(),
                minimum,
                "+",
                self._consistency_pel_page_size,
            )
            if not page:
                return entry_ids
            normalized = [
                _as_text(item.get("message_id", item.get(b"message_id")))
                for item in page
            ]
            entry_ids.update(normalized)
            if len(page) < self._consistency_pel_page_size:
                return entry_ids
            minimum = f"({normalized[-1]}"

    async def repair_consistency(
        self, queue: str, *, dry_run: bool = True
    ) -> RepairReport:
        """Repair only derived Redis indexes; default dry-run never changes Redis."""

        report = await self.check_consistency(queue)
        repairable_names = {
            "missing_ready_index",
            "missing_stream_entry",
            "stale_entry_id",
            "missing_lease_index",
            "missing_delayed_index",
            "missing_dlq_entry",
            "missing_eq_entry",
            "orphan_ready_index",
            "orphan_lease_index",
            "orphan_delayed_index",
            "orphan_dlq_entry",
            "orphan_eq_entry",
            "duplicate_dlq_entry",
            "duplicate_eq_entry",
            "stale_pel",
            "orphan_pel",
        }
        repairable = tuple(
            issue for issue in report.issues if issue.name in repairable_names
        )
        if dry_run:
            return RepairReport(queue, "redis", self._namespace, True, repairable)
        for issue in repairable:
            if issue.name == "orphan_pel" and issue.detail:
                await self._redis.xack(
                    self._queue_key(queue, "stream"), self._group_name(), issue.detail
                )
                continue
            if issue.name == "stale_pel" and issue.detail:
                await self._redis.xack(
                    self._queue_key(queue, "stream"), self._group_name(), issue.detail
                )
                continue
            if issue.message_id is None:
                continue
            message_key = self._message_key(queue, issue.message_id)
            if issue.name == "missing_ready_index":
                await self._redis.zadd(
                    self._queue_key(queue, "ready"),
                    {issue.message_id: _timestamp(await self._now())},
                )
            elif issue.name in {"missing_stream_entry", "stale_entry_id"}:
                fields = await self._redis.hgetall(message_key)
                # A leased message without its PEL is deliberately not rebuilt:
                # fabricating a stream entry would invalidate its active lease.
                if _as_text(fields.get("status", fields.get(b"status"))) != "ready":
                    continue
                if issue.name == "stale_entry_id":
                    entries = await self._redis.xrange(
                        self._queue_key(queue, "stream"), "-", "+"
                    )
                    stale_ids = [
                        entry_id
                        for entry_id, entry_fields in entries
                        if _as_text(
                            entry_fields.get(
                                "message_id", entry_fields.get(b"message_id")
                            )
                        )
                        == issue.message_id
                    ]
                    if stale_ids:
                        await self._redis.xack(
                            self._queue_key(queue, "stream"),
                            self._group_name(),
                            *stale_ids,
                        )
                        await self._redis.xdel(
                            self._queue_key(queue, "stream"), *stale_ids
                        )
                entry_id = await self._redis.xadd(
                    self._queue_key(queue, "stream"),
                    {"message_id": issue.message_id, "envelope": fields["envelope"]},
                )
                await self._redis.hset(message_key, mapping={"entry_id": entry_id})
            elif issue.name == "missing_lease_index":
                fields = await self._redis.hgetall(message_key)
                await self._redis.zadd(
                    self._queue_key(queue, "leases"),
                    {issue.message_id: float(fields.get("lease_until", 0))},
                )
            elif issue.name == "missing_delayed_index":
                fields = await self._redis.hgetall(message_key)
                await self._redis.zadd(
                    self._queue_key(queue, "delayed"),
                    {issue.message_id: float(fields.get("available_at", 0))},
                )
            elif issue.name == "missing_dlq_entry":
                await self._redis.lpush(self._queue_key(queue, "dlq"), issue.message_id)
            elif issue.name == "missing_eq_entry":
                await self._redis.lpush(self._queue_key(queue, "eq"), issue.message_id)
            elif issue.name.startswith("orphan_ready"):
                await self._redis.zrem(
                    self._queue_key(queue, "ready"), issue.message_id
                )
            elif issue.name.startswith("orphan_lease"):
                await self._redis.zrem(
                    self._queue_key(queue, "leases"), issue.message_id
                )
            elif issue.name.startswith("orphan_delayed"):
                await self._redis.zrem(
                    self._queue_key(queue, "delayed"), issue.message_id
                )
            elif (
                issue.name.startswith("orphan_dlq")
                or issue.name == "duplicate_dlq_entry"
            ):
                await self._redis.lrem(
                    self._queue_key(queue, "dlq"), 0, issue.message_id
                )
                fields = await self._redis.hgetall(message_key)
                if (
                    _as_text(fields.get("status", fields.get(b"status")))
                    == "dead_lettered"
                ):
                    await self._redis.lpush(
                        self._queue_key(queue, "dlq"), issue.message_id
                    )
            elif (
                issue.name.startswith("orphan_eq") or issue.name == "duplicate_eq_entry"
            ):
                await self._redis.lrem(
                    self._queue_key(queue, "eq"), 0, issue.message_id
                )
                fields = await self._redis.hgetall(message_key)
                if _as_text(fields.get("status", fields.get(b"status"))) == "expired":
                    await self._redis.lpush(
                        self._queue_key(queue, "eq"), issue.message_id
                    )
        return RepairReport(queue, "redis", self._namespace, False, repairable)

    async def migrate_keyspace(
        self, *, dry_run: bool = True
    ) -> KeyspaceMigrationReport:
        """Move legacy message hashes into the queue-scoped v0.7 keyspace.

        Operators must stop producers, consumers, and schedulers before applying
        this explicit, resumable migration. A source hash is removed only after
        its queue-scoped replacement and global lookup index are present.
        """

        await self.start()
        migrated: list[str] = []
        resumed: list[str] = []
        conflicts: list[ConsistencyIssue] = []
        async for raw_key in self._redis.scan_iter(
            match=f"{self._namespace}:message:*"
        ):
            source_key = _as_text(raw_key)
            message_id = source_key.rsplit(":", 1)[-1]
            fields = await self._redis.hgetall(source_key)
            raw_queue = fields.get("queue", fields.get(b"queue"))
            if raw_queue is None:
                conflicts.append(
                    ConsistencyIssue("legacy_message_missing_queue", message_id)
                )
                continue
            queue = _as_text(raw_queue)
            try:
                self._validate_queue(queue)
            except ValidationError as exc:
                conflicts.append(
                    ConsistencyIssue("legacy_message_invalid_queue", message_id, str(exc))
                )
                continue
            target_key = self._message_key(queue, message_id)
            existing = await self._redis.hgetall(target_key)
            if existing and existing != fields:
                conflicts.append(
                    ConsistencyIssue("message_key_conflict", message_id, queue)
                )
                continue
            was_resumed = bool(existing)
            if dry_run:
                (resumed if was_resumed else migrated).append(message_id)
                continue
            if not existing:
                await self._redis.hset(target_key, mapping=fields)
            await self._register_queue(queue)
            await self._redis.hset(self._message_index_key(), message_id, queue)
            await self._redis.unlink(source_key)
            (resumed if was_resumed else migrated).append(message_id)
        if not dry_run and not conflicts:
            await self._redis.set(self._keyspace_version_key(), _REDIS_KEYSPACE_VERSION)
        return KeyspaceMigrationReport(
            self._namespace,
            dry_run,
            tuple(sorted(migrated)),
            tuple(sorted(resumed)),
            tuple(conflicts),
        )

    async def cleanup_deprecated_keys(self, *, dry_run: bool = True) -> tuple[str, ...]:
        """List or remove the documented pre-v0.5 legacy key prefixes."""

        await self.start()
        keys: list[str] = []
        for pattern in (f"{self._namespace}:legacy:*", f"{self._namespace}:v0:*"):
            async for key in self._redis.scan_iter(match=pattern):
                keys.append(_as_text(key))
        selected = tuple(sorted(set(keys)))
        if selected and not dry_run:
            await self._redis.unlink(*selected)
        return selected


def _as_text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


# Compatibility exports; implementations belong to submission.redis.
RedisSubmissionStore = _SubmissionRedisSubmissionStore
RedisStringDedupSubmissionStore = _SubmissionRedisStringDedupSubmissionStore
