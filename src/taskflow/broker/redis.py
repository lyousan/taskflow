"""Redis 上的 v0.1 可靠消息 backend。"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
from collections.abc import Callable, Mapping
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, overload

from typing_extensions import Self

from ..capabilities import BackendCapabilities, SubmissionCapabilities
from ..config import QueueConfig
from ..errors import BrokerClosedError, ValidationError
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
from .redis_admin import RedisAdmin
from .redis_components import RedisConsumer, RedisDelivery
from .redis_maintenance import RedisMaintenance
from .redis_observability import RedisObservability
from .redis_state_machine import RedisStateMachine

logger = logging.getLogger(__name__)

_CLOCK_SKEW_WARNING_SECONDS = 5.0


class RedisBroker:
    """使用 Redis Lua 原子迁移实现的异步 v0.1 backend。

    默认命名空间为 ``taskflow``。每个 broker 应使用独立 namespace，便于隔离测试
    或不同业务。当前目标是 Redis standalone / Sentinel，不承诺 Redis Cluster 跨槽事务。
    """

    capabilities = BackendCapabilities(
        distributed_consumers=True,
        high_throughput=True,
        batch_submit=True,
        batch_atomic=True,
    )

    def __init__(self, redis: Any, *, namespace: str = "taskflow", default_max_attempts: int = 3,
                 default_dedup_ttl: timedelta | None = None, serializer: Serializer | None = None,
                 serializer_registry: SerializerRegistry | None = None,
                 id_factory: Callable[[], str] = _new_id, middleware: Middleware | None = None,
                 metrics: MetricsSink | None = None,
                 pending_recovery_seconds: float = 1.0, submission_store: Any | None = None,
                 submission_stores: Mapping[str, Any] | None = None,
                 queue_submission_profiles: Mapping[str, str] | None = None,
                 queues: Mapping[str, QueueConfig] | None = None,
                 event_sink: EventSink | None = None,
                 events: EventSink | None = None,
                 allow_legacy_names: bool = False) -> None:
        if (isinstance(default_max_attempts, bool) or not isinstance(default_max_attempts, int)
                or default_max_attempts < 1 or pending_recovery_seconds < 0):
            raise ValidationError("default_max_attempts 必须大于等于 1，pending_recovery_seconds 不能为负数")
        validate_persistent_name(namespace, label="namespace", allow_legacy=allow_legacy_names)
        self._redis, self._namespace = redis, namespace
        self._allow_legacy_names = allow_legacy_names
        self._default_max_attempts, self._default_dedup_ttl = default_max_attempts, default_dedup_ttl
        self._default_queue_config = QueueConfig(max_attempts=default_max_attempts, default_dedup_ttl=default_dedup_ttl)
        self._queues = dict(queues or {})
        for queue, config in self._queues.items():
            validate_persistent_name(queue, label="queue", allow_legacy=self._allow_legacy_names)
            if not isinstance(config, QueueConfig):
                raise ValidationError("queues 的值必须是 QueueConfig")
        self._serializer, self._id_factory = serializer or JsonSerializer(), id_factory
        self.serializer_registry = serializer_registry or SerializerRegistry([self._serializer])
        self._pending_recovery_ms = int(pending_recovery_seconds * 1000)
        if event_sink is not None and events is not None:
            raise ValidationError("event_sink 与 events 不能同时配置")
        self.middleware, self.metrics, self.event_sink, self._closed = middleware or Middleware(), metrics, event_sink or events, False
        self._configure_submission_stores(submission_store, submission_stores, queue_submission_profiles)
        self._submission_observer = SubmissionObserver(
            backend="redis", middleware=self.middleware, metrics=self.metrics,
            event_sink=self.event_sink, serializer=self._serializer,
        )
        self._submission_service = SubmissionService(self, self._prepare_submission)
        self._clock_skew_checked = False
        self._observability = RedisObservability(self)
        self._state_machine = RedisStateMachine(self)
        self._maintenance = RedisMaintenance(self)
        self.admin = RedisAdmin(self)

    @classmethod
    def from_url(cls, url: str = "redis://127.0.0.1:6379/2", **kwargs: Any) -> RedisBroker:
        """从 URL 创建 Redis asyncio client；需要安装 ``taskflow[redis]``。"""
        try:
            from redis.asyncio import Redis
        except ImportError as exc:
            raise ValidationError("Redis backend 需要安装 taskflow[redis]") from exc
        return cls(Redis.from_url(url, decode_responses=True), **kwargs)

    def _queue_key(self, queue: str, kind: str) -> str:
        return f"{self._namespace}:queue:{{{queue}}}:{kind}"

    def _group_name(self) -> str:
        """返回当前 namespace 内稳定的 Consumer Group 名称。"""

        return "taskflow"

    async def _ensure_group(self, queue: str) -> None:
        """幂等创建队列对应的 Redis Stream Consumer Group。"""

        try:
            await self._redis.xgroup_create(self._queue_key(queue, "stream"), self._group_name(), id="0", mkstream=True)
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def _message_key(self, message_id: str) -> str:
        return f"{self._namespace}:message:{message_id}"

    def _dedup_key(self, scope: str, key: str) -> str:
        """以固定长度摘要构造 dedup key，隔离特殊字符与 Cluster hash tag。"""

        scope_hash = hashlib.sha256(scope.encode()).hexdigest()
        key_hash = hashlib.sha256(key.encode()).hexdigest()
        return f"{self._namespace}:dedup:{scope_hash}:{key_hash}"

    def _validate_queue(self, queue: str) -> None:
        validate_persistent_name(queue, label="queue", allow_legacy=self._allow_legacy_names)

    def _queue_config(self, queue: str) -> QueueConfig:
        self._validate_queue(queue)
        return self._queues.get(queue, self._default_queue_config)

    def _configure_submission_stores(self, submission_store: Any | None,
                                     submission_stores: Mapping[str, Any] | None,
                                     queue_submission_profiles: Mapping[str, str] | None) -> None:
        self._submission_router = SubmissionRouter(
            self, default_store=_RedisStringDedupSubmissionStore(self), submission_store=submission_store,
            submission_stores=submission_stores, queue_submission_profiles=queue_submission_profiles,
        )
        self._submission_stores = self._submission_router.stores
        self._queue_submission_profiles = self._submission_router.profiles
        self.submission_store = self._submission_router.default

    def submission_capabilities(self, queue: str) -> SubmissionCapabilities:
        self._validate_queue(queue)
        profile = self._submission_router.profile_for(queue)
        return self.submission_store.capabilities if profile == "default" else self._submission_router.capabilities(queue)

    def _submission_store_for(self, queue: str) -> Any:
        return self.submission_store if self._submission_router.profile_for(queue) == "default" else self._submission_router.for_queue(queue)

    def _ensure_open(self) -> None:
        if self._closed:
            raise BrokerClosedError("broker 已关闭")

    async def _now(self) -> datetime:
        seconds, microseconds = await self._redis.time()
        return datetime.fromtimestamp(int(seconds) + int(microseconds) / 1_000_000, timezone.utc)

    async def start(self) -> None:
        """验证连接，避免首个业务提交才暴露连接配置错误。"""
        self._ensure_open()
        await self._redis.ping()
        if not self._clock_skew_checked:
            server_now = await self._now()
            skew_seconds = abs((utc_now() - server_now).total_seconds())
            self._clock_skew_checked = True
            if skew_seconds >= _CLOCK_SKEW_WARNING_SECONDS:
                logger.warning(
                    "taskflow Redis server clock differs materially from the application clock; "
                    "Redis time remains authoritative for expiry and leases",
                    extra={"clock_skew_seconds": skew_seconds, "namespace": self._namespace},
                )

    async def close(self) -> None:
        """关闭底层异步 Redis client。"""
        if not self._closed:
            await self._redis.aclose()
            self._closed = True

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    def _encode(self, message: TaskMessage) -> str:
        """将任意 bytes serializer 输出编码为 Redis 安全的 ASCII 文本。"""
        raw = self._serializer.dumps({
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
        return base64.b64encode(raw).decode("ascii")

    def _decode(self, envelope: str, serializer_name: str | None = None,
                serializer_version: str | None = None) -> TaskMessage:
        decoder = self._serializer if serializer_name is None or (serializer_name == self._serializer.name and serializer_version == self._serializer.version) else self.serializer_registry.resolve(serializer_name, serializer_version or "")
        value = decoder.loads(base64.b64decode(envelope.encode("ascii")))
        return TaskMessage(value["id"], value["queue"], value["payload"], value["metadata"], value["dedup_key"],
                           value["dedup_scope"], value["workflow_id"], value["parent_id"],
                           _datetime(value["created_at"]) or utc_now(), _datetime(value["expires_at"]),
                           value["max_attempts"], _datetime(value.get("available_at")),
                           value.get("payload_schema_name"), value.get("payload_schema_version"))

    async def submit(self, *, queue: str, payload: Any, metadata: Mapping[str, Any] | None = None,
                     dedup_key: str | None = None, dedup_scope: str | None = None,
                     dedup_ttl: timedelta | None = None, delay: timedelta | None = None, expires_at: datetime | None = None,
                     max_attempts: int | None = None, workflow_id: str | None = None,
                     parent_id: str | None = None, payload_type: type[Any] | None = None) -> SubmitResult:
        """构造完整 PreparedSubmission 并由 Store 完成原子准入。"""
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
        """校验请求、生成 ID 并序列化；不在此处触碰 Redis 状态。"""
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
        now = await self._now()
        if delay is not None and not isinstance(delay, timedelta):
            raise ValidationError("delay 必须是 timedelta")
        if delay is not None and delay.total_seconds() < 0:
            raise ValidationError("delay 不能为负数")
        if expires_at is not None and expires_at.tzinfo is None:
            raise ValidationError("expires_at 必须带时区")
        available_at = now + delay if delay and delay.total_seconds() > 0 else None
        encoded_payload, schema = normalize_payload(payload, payload_type=payload_type)
        message = TaskMessage(self._id_factory(), queue, encoded_payload, metadata or {}, dedup_key, dedup_scope,
                              workflow_id, parent_id, now, expires_at, attempts, available_at,
                              schema.name if schema else None, schema.version if schema else None)
        envelope = self._encode(message)
        if config.max_payload_bytes is not None and len(self._serializer.dumps(encoded_payload)) > config.max_payload_bytes:
            raise ValidationError(f"payload 超过 queue {queue!r} 的 max_payload_bytes 限制")
        initial_expired = bool(expires_at and expires_at <= now)
        prepared = PreparedSubmission(message.id, queue, base64.b64decode(envelope.encode("ascii")),
            MessageStatus.EXPIRED.value if initial_expired else (MessageStatus.DELAYED.value if available_at else MessageStatus.READY.value), now,
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
        self._encode(replayed)
        return replayed

    def consumer(self, queue: str, *, consumer_id: str | None = None,
                 options: ConsumerOptions | None = None) -> RedisConsumer:
        """创建多进程安全的异步消费者。"""
        self._ensure_open()
        self._validate_queue(queue)
        chosen = options or ConsumerOptions(lease_seconds=self._queue_config(queue).lease.total_seconds())
        if chosen.lease_seconds <= 0 or chosen.poll_interval < 0 or chosen.concurrency < 1:
            raise ValidationError("消费者参数必须有效")
        return RedisConsumer(self, queue, consumer_id or self._id_factory(), chosen)

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
        await self.worker(queue, handler, concurrency=concurrency, consumer_id=consumer_id,
                          options=options, retry_policy=retry_policy,
                          heartbeat_seconds=heartbeat_seconds, payload_type=payload_type).run()

    async def _claim(self, queue: str, consumer_id: str, lease_seconds: float) -> RedisDelivery | None:
        return await self._state_machine.claim(queue, consumer_id, lease_seconds)

    async def _finish(self, delivery: RedisDelivery, action: str, reason: str | None = None,
                      error: BaseException | None = None, delay: timedelta | None = None,
                      max_attempts: int | None = None) -> FinishOutcome:
        return await self._state_machine.finish(delivery, action, reason, error, delay, max_attempts)

    async def _extend(self, delivery: RedisDelivery, seconds: float | None) -> datetime:
        return await self._state_machine.extend(delivery, seconds)

    async def _recover_uncommitted_pel(self, queue: str) -> int:
        return await self._state_machine.recover_uncommitted_pel(queue)

    async def _emit_maintenance_event(self, message_id: str, name: str, status: str, *,
                                      reason: str | None = None, error_type: str | None = None,
                                      metric_name: str | None = None) -> None:
        await self._observability.maintenance(message_id, name, status, reason=reason,
                                              error_type=error_type, metric_name=metric_name)

    async def maintain(self, queue: str | None = None) -> int:
        """回收已到期租约，并把 READY/LEASED 的过期消息移入 EQ。"""
        return await self._maintenance.run(queue)

    async def inspect(self, queue: str) -> QueueStats:
        """读取队列实时计数与累计计数。"""
        await self.maintain(queue)
        stats = await self._redis.hgetall(self._queue_key(queue, "stats"))
        ready, leased, delayed, first_ready, dead_letters, expired = await asyncio.gather(
            self._redis.zcard(self._queue_key(queue, "ready")), self._redis.zcard(self._queue_key(queue, "leases")),
            self._redis.zcard(self._queue_key(queue, "delayed")),
            self._redis.zrange(self._queue_key(queue, "ready"), 0, 0, withscores=True),
            self._redis.llen(self._queue_key(queue, "dlq")), self._redis.llen(self._queue_key(queue, "eq")))
        earliest = _datetime(float(first_ready[0][1])) if first_ready else None
        await metric(self.metrics, "queue_ready", float(ready), queue=queue)
        await metric(self.metrics, "queue_leased", float(leased), queue=queue)
        await metric(self.metrics, "queue_delayed", float(delayed), queue=queue)
        return QueueStats(queue, int(ready), int(leased), int(dead_letters), int(expired), earliest,
                          int(stats.get("submitted_total", 0)), int(stats.get("acked_total", 0)), int(stats.get("retried_total", 0)), int(stats.get("reclaimed_total", 0)), int(stats.get("dead_lettered_total", 0)), int(delayed))

    async def inspect_message(self, message_id: str) -> TaskMessage | None:
        """按稳定 message ID 查询原始业务消息，不改变其状态。"""
        fields = await self._redis.hgetall(self._message_key(message_id))
        if not fields:
            return None
        return self._decode(fields["envelope"], fields.get("serializer_name"), fields.get("serializer_version"))


# Compatibility exports; implementations belong to submission.redis.
RedisSubmissionStore = _SubmissionRedisSubmissionStore
RedisStringDedupSubmissionStore = _SubmissionRedisStringDedupSubmissionStore
