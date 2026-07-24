"""Redis 上的 v0.1 可靠消息 backend。"""
from __future__ import annotations

import asyncio
import base64
import hashlib
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any

from typing_extensions import Self

from ..capabilities import BackendCapabilities, DedupGuarantee, SubmissionCapabilities
from ..errors import BrokerClosedError, LeaseLostError, ValidationError
from ..middleware import Middleware
from ..naming import validate_persistent_name
from ..observability import MetricsSink, metric
from ..observability import event as emit_event
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
from .sqlite import _datetime, _new_id, _timestamp


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
                 queue_submission_profiles: Mapping[str, str] | None = None) -> None:
        if default_max_attempts < 1 or pending_recovery_seconds < 0:
            raise ValidationError("default_max_attempts 必须大于等于 1，pending_recovery_seconds 不能为负数")
        validate_persistent_name(namespace, label="namespace")
        self._redis, self._namespace = redis, namespace
        self._default_max_attempts, self._default_dedup_ttl = default_max_attempts, default_dedup_ttl
        self._serializer, self._id_factory = serializer or JsonSerializer(), id_factory
        self.serializer_registry = serializer_registry or SerializerRegistry([self._serializer])
        self._pending_recovery_ms = int(pending_recovery_seconds * 1000)
        self._configure_submission_stores(submission_store, submission_stores, queue_submission_profiles)
        self.middleware, self.metrics, self._closed = middleware or Middleware(), metrics, False
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
        validate_persistent_name(queue, label="queue")

    def _configure_submission_stores(self, submission_store: Any | None,
                                     submission_stores: Mapping[str, Any] | None,
                                     queue_submission_profiles: Mapping[str, str] | None) -> None:
        if submission_store is not None and submission_stores is not None:
            raise ValidationError("submission_store 与 submission_stores 不能同时配置")
        configured = dict(submission_stores or {"default": submission_store or RedisStringDedupSubmissionStore(self)})
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
        self._validate_queue(queue)
        profile = self._queue_submission_profiles.get(queue, "default")
        return (self.submission_store if profile == "default" else self._submission_stores[profile]).capabilities

    def _submission_store_for(self, queue: str) -> Any:
        profile = self._queue_submission_profiles.get(queue, "default")
        return self.submission_store if profile == "default" else self._submission_stores[profile]

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
            "expires_at": _timestamp(message.expires_at) if message.expires_at else None,
            "max_attempts": message.max_attempts,
        })
        return base64.b64encode(raw).decode("ascii")

    def _decode(self, envelope: str, serializer_name: str | None = None,
                serializer_version: str | None = None) -> TaskMessage:
        decoder = self._serializer if serializer_name is None or (serializer_name == self._serializer.name and serializer_version == self._serializer.version) else self.serializer_registry.resolve(serializer_name, serializer_version or "")
        value = decoder.loads(base64.b64decode(envelope.encode("ascii")))
        return TaskMessage(value["id"], value["queue"], value["payload"], value["metadata"], value["dedup_key"],
                           value["dedup_scope"], value["workflow_id"], value["parent_id"],
                           _datetime(value["created_at"]) or utc_now(), _datetime(value["expires_at"]), value["max_attempts"])

    async def submit(self, *, queue: str, payload: Any, metadata: Mapping[str, Any] | None = None,
                     dedup_key: str | None = None, dedup_scope: str | None = None,
                     dedup_ttl: timedelta | None = None, expires_at: datetime | None = None,
                     max_attempts: int | None = None, workflow_id: str | None = None,
                     parent_id: str | None = None) -> SubmitResult:
        """构造完整 PreparedSubmission 并由 Store 完成原子准入。"""
        prepared, message = await self._prepare_submission(queue=queue, payload=payload, metadata=metadata,
            dedup_key=dedup_key, dedup_scope=dedup_scope, dedup_ttl=dedup_ttl,
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

    async def _prepare_submission(self, *, queue: str, payload: Any, metadata: Mapping[str, Any] | None = None,
                     dedup_key: str | None = None, dedup_scope: str | None = None,
                     dedup_ttl: timedelta | None = None, expires_at: datetime | None = None,
                     max_attempts: int | None = None, workflow_id: str | None = None,
                     parent_id: str | None = None) -> tuple[PreparedSubmission, TaskMessage]:
        """校验请求、生成 ID 并序列化；不在此处触碰 Redis 状态。"""
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
        now = await self._now()
        message = TaskMessage(self._id_factory(), queue, payload, metadata or {}, dedup_key, dedup_scope,
                              workflow_id, parent_id, now, expires_at, attempts)
        envelope = self._encode(message)
        initial_expired = bool(expires_at and expires_at <= now)
        prepared = PreparedSubmission(message.id, queue, base64.b64decode(envelope.encode("ascii")),
            MessageStatus.EXPIRED.value if initial_expired else MessageStatus.READY.value, now,
            int(_timestamp(expires_at) * 1000) if expires_at else None, dedup_scope, dedup_key,
            int(ttl.total_seconds() * 1000) if ttl else None, attempts,
            self._serializer.name, self._serializer.version)
        return prepared, message

    async def submit_many(self, messages: list[SubmitRequest]) -> list[SubmitResult]:
        """先准备整批请求，再交由 Store 决定其批量语义。"""
        prepared_messages = [await self._prepare_submission(queue=item.queue, payload=item.payload, metadata=item.metadata,
            dedup_key=item.dedup_key, dedup_scope=item.dedup_scope, dedup_ttl=item.dedup_ttl,
            expires_at=item.expires_at, max_attempts=item.max_attempts,
            workflow_id=item.workflow_id, parent_id=item.parent_id) for item in messages]
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
                 options: ConsumerOptions | None = None) -> RedisConsumer:
        """创建多进程安全的异步消费者。"""
        self._ensure_open()
        self._validate_queue(queue)
        chosen = options or ConsumerOptions()
        if chosen.lease_seconds <= 0 or chosen.poll_interval < 0 or chosen.concurrency < 1:
            raise ValidationError("消费者参数必须有效")
        return RedisConsumer(self, queue, consumer_id or self._id_factory(), chosen)

    def worker(self, queue: str, handler: Handler, *, concurrency: int | None = None,
               options: ConsumerOptions | None = None) -> TaskWorker:
        """创建一个真正受 ``concurrency`` 限制的 Worker。"""
        selected = options or ConsumerOptions()
        return TaskWorker(self, queue, handler, concurrency=concurrency if concurrency is not None else selected.concurrency, options=selected)

    async def run(self, queue: str, handler: Handler, *, concurrency: int | None = None,
                  options: ConsumerOptions | None = None) -> None:
        await self.worker(queue, handler, concurrency=concurrency, options=options).run()

    async def _claim(self, queue: str, consumer_id: str, lease_seconds: float) -> RedisDelivery | None:
        await self.maintain(queue)
        await self._ensure_group(queue)
        now = await self._now()
        received = await self._redis.xreadgroup(self._group_name(), consumer_id, {self._queue_key(queue, "stream"): ">"}, count=1, block=1)
        if not received:
            return None
        _, entries = received[0]
        entry_id, fields = entries[0]
        message_id = fields["message_id"]
        delivery_id, token = self._id_factory(), self._id_factory()
        lease_until = now + timedelta(seconds=lease_seconds)
        script = """
            local state = redis.call('HGET', KEYS[1], 'status')
            if state ~= 'ready' or redis.call('HGET', KEYS[1], 'entry_id') ~= ARGV[8] then redis.call('XACK', KEYS[5], ARGV[7], ARGV[8]); redis.call('XDEL', KEYS[5], ARGV[8]); return 0 end
            local expires = tonumber(redis.call('HGET', KEYS[1], 'expires_at') or '0')
            if expires > 0 and expires <= tonumber(ARGV[1]) then
              redis.call('HSET', KEYS[1], 'status', 'expired', 'last_action', 'expired', 'expired_at', ARGV[1], 'status_at_expiry', 'ready')
              redis.call('XACK', KEYS[5], ARGV[7], ARGV[8]); redis.call('XDEL', KEYS[5], ARGV[8]); redis.call('LPUSH', KEYS[3], ARGV[2]); redis.call('ZREM', KEYS[4], ARGV[2]); redis.call('ZREM', KEYS[6], ARGV[2]); return -1
            end
            local attempt = redis.call('HINCRBY', KEYS[1], 'attempt', 1)
            redis.call('HSET', KEYS[1], 'status', 'leased', 'consumer_id', ARGV[3], 'delivery_id', ARGV[4],
              'lease_token', ARGV[5], 'claimed_at', ARGV[1], 'lease_until', ARGV[6], 'last_action', '', 'entry_id', ARGV[8])
            redis.call('ZADD', KEYS[2], ARGV[6], ARGV[2]); redis.call('ZREM', KEYS[6], ARGV[2]); return attempt
        """
        status = int(await self._redis.eval(script, 6, self._message_key(message_id), self._queue_key(queue, "leases"),
                                            self._queue_key(queue, "eq"), self._queue_key(queue, "expiry"), self._queue_key(queue, "stream"),
                                            self._queue_key(queue, "ready"),
                                            str(_timestamp(now)), message_id, consumer_id, delivery_id, token, str(_timestamp(lease_until)), self._group_name(), entry_id))
        if status <= 0:
            return None
        fields = await self._redis.hgetall(self._message_key(message_id))
        delivery = RedisDelivery(self, self._decode(fields["envelope"], fields.get("serializer_name"), fields.get("serializer_version")), delivery_id, token, consumer_id, status, now, lease_until)
        await self.middleware.emit("after_claim", delivery)
        await metric(self.metrics, "claimed_total", queue=queue)
        await emit_event(self.middleware, "claimed", delivery.message, status=MessageStatus.LEASED.value, delivery=delivery, serializer_name=self._serializer.name, serializer_version=self._serializer.version)
        return delivery

    async def _finish(self, delivery: RedisDelivery, action: str, reason: str | None = None,
                      error: BaseException | None = None) -> FinishOutcome:
        now = await self._now()
        queue, message_id = delivery.message.queue, delivery.message.id
        error_type = type(error).__name__ if error else ""
        script = """
            local status = redis.call('HGET', KEYS[1], 'status')
            local current = redis.call('HGET', KEYS[1], 'delivery_id')
            if status ~= 'leased' then
              if redis.call('HGET', KEYS[1], 'last_delivery_id') == ARGV[2] and redis.call('HGET', KEYS[1], 'last_action') == ARGV[1] then return 2 end
              return 0
            end
            if current ~= ARGV[2] or redis.call('HGET', KEYS[1], 'lease_token') ~= ARGV[3] or tonumber(redis.call('HGET', KEYS[1], 'lease_until') or '0') <= tonumber(ARGV[4]) then return 0 end
            local entry = redis.call('HGET', KEYS[1], 'entry_id')
            local expires = tonumber(redis.call('HGET', KEYS[1], 'expires_at') or '0')
            if expires > 0 and expires <= tonumber(ARGV[4]) then
              redis.call('HSET', KEYS[1], 'status', 'expired', 'last_action', 'expired', 'expired_at', ARGV[4], 'status_at_expiry', 'leased', 'last_delivery_id', ARGV[2])
              redis.call('HDEL', KEYS[1], 'consumer_id', 'delivery_id', 'lease_token', 'claimed_at', 'lease_until')
              redis.call('XACK', KEYS[4], ARGV[8], entry); redis.call('XDEL', KEYS[4], entry); redis.call('ZREM', KEYS[2], ARGV[5]); redis.call('ZREM', KEYS[3], ARGV[5]); redis.call('ZREM', KEYS[8], ARGV[5]); redis.call('LPUSH', KEYS[5], ARGV[5]); return 3
            end
            local attempt = tonumber(redis.call('HGET', KEYS[1], 'attempt'))
            local max_attempts = tonumber(redis.call('HGET', KEYS[1], 'max_attempts'))
            redis.call('ZREM', KEYS[2], ARGV[5])
            if ARGV[1] == 'ack' then
              redis.call('XACK', KEYS[4], ARGV[8], entry); redis.call('XDEL', KEYS[4], entry); redis.call('HSET', KEYS[1], 'status', 'acked', 'last_action', 'ack', 'last_delivery_id', ARGV[2]); redis.call('HDEL', KEYS[1], 'consumer_id', 'delivery_id', 'lease_token', 'claimed_at', 'lease_until'); redis.call('ZREM', KEYS[3], ARGV[5]); redis.call('HINCRBY', KEYS[6], 'acked_total', 1); return 4
            elseif ARGV[1] == 'retry' and attempt < max_attempts then
              redis.call('XACK', KEYS[4], ARGV[8], entry); redis.call('XDEL', KEYS[4], entry); local new_entry = redis.call('XADD', KEYS[4], '*', 'message_id', ARGV[5], 'envelope', redis.call('HGET', KEYS[1], 'envelope')); redis.call('HSET', KEYS[1], 'status', 'ready', 'entry_id', new_entry, 'last_action', 'retry', 'last_reason', ARGV[6], 'last_delivery_id', ARGV[2]); redis.call('HDEL', KEYS[1], 'consumer_id', 'delivery_id', 'lease_token', 'claimed_at', 'lease_until'); redis.call('ZADD', KEYS[8], ARGV[4], ARGV[5]); redis.call('HINCRBY', KEYS[6], 'retried_total', 1); return 5
            else
              local source = ARGV[1] == 'reject' and 'reject' or 'retry_limit'
              redis.call('XACK', KEYS[4], ARGV[8], entry); redis.call('XDEL', KEYS[4], entry); redis.call('HSET', KEYS[1], 'status', 'dead_lettered', 'last_action', ARGV[1], 'last_reason', ARGV[6], 'dead_source', source, 'failed_at', ARGV[4], 'error_type', ARGV[7], 'last_delivery_id', ARGV[2]); redis.call('HDEL', KEYS[1], 'consumer_id', 'delivery_id', 'lease_token', 'claimed_at', 'lease_until')
              redis.call('ZREM', KEYS[3], ARGV[5]); redis.call('LPUSH', KEYS[7], ARGV[5]); redis.call('HINCRBY', KEYS[6], 'dead_lettered_total', 1); return 6
            end
        """
        result = int(await self._redis.eval(script, 8, self._message_key(message_id), self._queue_key(queue, "leases"),
                                             self._queue_key(queue, "expiry"), self._queue_key(queue, "stream"),
                                             self._queue_key(queue, "eq"), self._queue_key(queue, "stats"), self._queue_key(queue, "dlq"), self._queue_key(queue, "ready"),
                                             action, delivery.delivery_id, delivery._lease_token, str(_timestamp(now)), message_id, reason or "", error_type, self._group_name()))
        if result == 0:
            await metric(self.metrics, "lease_lost_total", queue=queue)
            raise LeaseLostError("租约已经失效，不能终结当前投递")
        if result == 2:
            return FinishOutcome.IDEMPOTENT
        outcome = {3: FinishOutcome.EXPIRED, 4: FinishOutcome.ACKED, 5: FinishOutcome.RETRIED, 6: FinishOutcome.DEAD_LETTERED}[result]
        event_name, metric_name = {
            FinishOutcome.EXPIRED: ("expired", "expired_total"),
            FinishOutcome.ACKED: ("ack", "acked_total"),
            FinishOutcome.RETRIED: ("retry", "retried_total"),
            FinishOutcome.DEAD_LETTERED: ("dead_lettered", "dead_lettered_total"),
        }[outcome]
        await self.middleware.emit(f"after_{event_name}", delivery, reason)
        await metric(self.metrics, metric_name, queue=queue)
        await emit_event(self.middleware, event_name, delivery.message, status=outcome.value, delivery=delivery, reason=reason, serializer_name=self._serializer.name, serializer_version=self._serializer.version)
        return outcome

    async def _extend(self, delivery: RedisDelivery, seconds: float | None) -> datetime:
        period = seconds if seconds is not None else delivery._lease_seconds
        if period <= 0:
            raise ValidationError("续租时长必须为正数")
        now = await self._now()
        until = now + timedelta(seconds=period)
        fields = await self._redis.hgetall(self._message_key(delivery.message.id))
        if fields.get("expires_at") and float(fields["expires_at"]) > 0:
            until = min(until, _datetime(float(fields["expires_at"])) or until)
        script = """
            local time = redis.call('TIME')
            local now = tonumber(time[1]) + tonumber(time[2]) / 1000000
            if redis.call('HGET', KEYS[1], 'status') ~= 'leased' or redis.call('HGET', KEYS[1], 'delivery_id') ~= ARGV[1] or redis.call('HGET', KEYS[1], 'lease_token') ~= ARGV[2] or tonumber(redis.call('HGET', KEYS[1], 'lease_until') or '0') <= now then return 0 end
            local expires = tonumber(redis.call('HGET', KEYS[1], 'expires_at') or '0')
            if expires > 0 and expires <= now then
              local entry = redis.call('HGET', KEYS[1], 'entry_id')
              redis.call('XACK', KEYS[3], ARGV[5], entry); redis.call('XDEL', KEYS[3], entry)
              redis.call('HSET', KEYS[1], 'status', 'expired', 'last_action', 'expired', 'expired_at', now, 'status_at_expiry', 'leased', 'last_delivery_id', ARGV[1])
              redis.call('HDEL', KEYS[1], 'consumer_id', 'delivery_id', 'lease_token', 'claimed_at', 'lease_until')
              redis.call('ZREM', KEYS[2], ARGV[4]); redis.call('ZREM', KEYS[4], ARGV[4]); redis.call('LPUSH', KEYS[5], ARGV[4]); return 0
            end
            redis.call('HSET', KEYS[1], 'lease_until', ARGV[3]); redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4]); return 1
        """
        if until <= now or not await self._redis.eval(script, 5, self._message_key(delivery.message.id), self._queue_key(delivery.message.queue, "leases"), self._queue_key(delivery.message.queue, "stream"), self._queue_key(delivery.message.queue, "expiry"), self._queue_key(delivery.message.queue, "eq"), delivery.delivery_id, delivery._lease_token, str(_timestamp(until)), delivery.message.id, self._group_name()):
            raise LeaseLostError("租约已经失效，不能续租")
        return until

    async def _recover_uncommitted_pel(self, queue: str) -> int:
        """恢复已进入 PEL、但尚未来得及写入 lease 的崩溃窗口消息。

        ``XREADGROUP`` 与状态 Lua 不能放在同一 Redis 原子命令中。此补偿流程
        仅处理状态仍为 READY 且 entry ID 仍匹配的 PEL 记录；过期 worker 的迟到
        claim 会因 entry ID 不匹配被丢弃，避免重新入队后的双重领取。
        """

        stream = self._queue_key(queue, "stream")
        try:
            _, entries, _ = await self._redis.xautoclaim(
                stream,
                self._group_name(),
                "taskflow-reclaimer",
                min_idle_time=self._pending_recovery_ms,
                start_id="0-0",
                count=100,
            )
        except Exception as exc:
            if "NOGROUP" in str(exc):
                return 0
            raise
        script = """
            if redis.call('HGET', KEYS[1], 'status') ~= 'ready' or redis.call('HGET', KEYS[1], 'entry_id') ~= ARGV[2] then return 0 end
            redis.call('XACK', KEYS[2], ARGV[1], ARGV[2]); redis.call('XDEL', KEYS[2], ARGV[2])
            local next_entry = redis.call('XADD', KEYS[2], '*', 'message_id', ARGV[3], 'envelope', redis.call('HGET', KEYS[1], 'envelope'))
            redis.call('HSET', KEYS[1], 'entry_id', next_entry, 'last_action', 'pel_recovered')
            redis.call('HINCRBY', KEYS[3], 'reclaimed_total', 1)
            return 1
        """
        restored = 0
        for entry_id, fields in entries:
            message_id = fields["message_id"]
            restored += int(await self._redis.eval(
                script,
                3,
                self._message_key(message_id),
                stream,
                self._queue_key(queue, "stats"),
                self._group_name(),
                entry_id,
                message_id,
            ))
        return restored

    async def maintain(self, queue: str | None = None) -> int:
        """回收已到期租约，并把 READY/LEASED 的过期消息移入 EQ。"""
        if queue is None:
            return 0
        await self._ensure_group(queue)
        now = await self._now()
        reclaimed = await self._recover_uncommitted_pel(queue)
        expire_script = """
            local status = redis.call('HGET', KEYS[1], 'status')
            local expires = tonumber(redis.call('HGET', KEYS[1], 'expires_at') or '0')
            if (status ~= 'ready' and status ~= 'leased') or expires == 0 or expires > tonumber(ARGV[1]) then return 0 end
            local entry = redis.call('HGET', KEYS[1], 'entry_id')
            if entry and entry ~= '' then redis.call('XACK', KEYS[4], ARGV[2], entry); redis.call('XDEL', KEYS[4], entry) end
            redis.call('HSET', KEYS[1], 'status', 'expired', 'last_action', 'expired', 'expired_at', ARGV[1], 'status_at_expiry', status, 'last_delivery_id', redis.call('HGET', KEYS[1], 'delivery_id') or '')
            redis.call('HDEL', KEYS[1], 'consumer_id', 'delivery_id', 'lease_token', 'claimed_at', 'lease_until')
            redis.call('ZREM', KEYS[2], ARGV[3]); redis.call('ZREM', KEYS[3], ARGV[3]); redis.call('ZREM', KEYS[6], ARGV[3]); redis.call('LPUSH', KEYS[5], ARGV[3]); return 1
        """
        reclaim_script = """
            if redis.call('HGET', KEYS[1], 'status') ~= 'leased' or tonumber(redis.call('HGET', KEYS[1], 'lease_until') or '0') > tonumber(ARGV[1]) then return 0 end
            local entry = redis.call('HGET', KEYS[1], 'entry_id')
            local expires = tonumber(redis.call('HGET', KEYS[1], 'expires_at') or '0')
            redis.call('XACK', KEYS[4], ARGV[2], entry); redis.call('XDEL', KEYS[4], entry); redis.call('ZREM', KEYS[2], ARGV[3])
            if expires > 0 and expires <= tonumber(ARGV[1]) then
              redis.call('HSET', KEYS[1], 'status', 'expired', 'last_action', 'expired', 'expired_at', ARGV[1], 'status_at_expiry', 'leased', 'last_delivery_id', redis.call('HGET', KEYS[1], 'delivery_id') or ''); redis.call('HDEL', KEYS[1], 'consumer_id', 'delivery_id', 'lease_token', 'claimed_at', 'lease_until'); redis.call('ZREM', KEYS[3], ARGV[3]); redis.call('LPUSH', KEYS[7], ARGV[3]); return 1
            end
            local attempt = tonumber(redis.call('HGET', KEYS[1], 'attempt')); local maximum = tonumber(redis.call('HGET', KEYS[1], 'max_attempts'))
            if attempt >= maximum then
              redis.call('HSET', KEYS[1], 'status', 'dead_lettered', 'dead_source', 'lease_timeout', 'last_action', 'lease_timeout', 'last_delivery_id', redis.call('HGET', KEYS[1], 'delivery_id') or ''); redis.call('HDEL', KEYS[1], 'consumer_id', 'delivery_id', 'lease_token', 'claimed_at', 'lease_until'); redis.call('ZREM', KEYS[3], ARGV[3]); redis.call('LPUSH', KEYS[5], ARGV[3]); redis.call('HINCRBY', KEYS[6], 'dead_lettered_total', 1)
            else
              local next_entry = redis.call('XADD', KEYS[4], '*', 'message_id', ARGV[3], 'envelope', redis.call('HGET', KEYS[1], 'envelope')); redis.call('HSET', KEYS[1], 'status', 'ready', 'entry_id', next_entry, 'last_action', 'reclaimed', 'last_delivery_id', redis.call('HGET', KEYS[1], 'delivery_id') or ''); redis.call('HDEL', KEYS[1], 'consumer_id', 'delivery_id', 'lease_token', 'claimed_at', 'lease_until'); redis.call('ZADD', KEYS[8], ARGV[1], ARGV[3]); redis.call('HINCRBY', KEYS[6], 'reclaimed_total', 1)
            end; return 1
        """
        for message_id in await self._redis.zrangebyscore(self._queue_key(queue, "expiry"), "-inf", _timestamp(now)):
            await self._redis.eval(expire_script, 6, self._message_key(message_id), self._queue_key(queue, "leases"),
                                   self._queue_key(queue, "expiry"), self._queue_key(queue, "stream"), self._queue_key(queue, "eq"),
                                   self._queue_key(queue, "ready"),
                                   str(_timestamp(now)), self._group_name(), message_id)
        for message_id in await self._redis.zrangebyscore(self._queue_key(queue, "leases"), "-inf", _timestamp(now)):
            moved = await self._redis.eval(reclaim_script, 8, self._message_key(message_id), self._queue_key(queue, "leases"),
                                           self._queue_key(queue, "expiry"), self._queue_key(queue, "stream"), self._queue_key(queue, "dlq"),
                                           self._queue_key(queue, "stats"), self._queue_key(queue, "eq"), self._queue_key(queue, "ready"), str(_timestamp(now)), self._group_name(), message_id)
            reclaimed += int(moved)
        return reclaimed

    async def inspect(self, queue: str) -> QueueStats:
        """读取队列实时计数与累计计数。"""
        await self.maintain(queue)
        stats = await self._redis.hgetall(self._queue_key(queue, "stats"))
        ready, leased, first_ready, dead_letters, expired = await asyncio.gather(
            self._redis.zcard(self._queue_key(queue, "ready")), self._redis.zcard(self._queue_key(queue, "leases")),
            self._redis.zrange(self._queue_key(queue, "ready"), 0, 0, withscores=True),
            self._redis.llen(self._queue_key(queue, "dlq")), self._redis.llen(self._queue_key(queue, "eq")))
        earliest = _datetime(float(first_ready[0][1])) if first_ready else None
        await metric(self.metrics, "queue_ready", float(ready), queue=queue)
        await metric(self.metrics, "queue_leased", float(leased), queue=queue)
        return QueueStats(queue, int(ready), int(leased), int(dead_letters), int(expired), earliest,
                          int(stats.get("submitted_total", 0)), int(stats.get("acked_total", 0)), int(stats.get("retried_total", 0)), int(stats.get("reclaimed_total", 0)), int(stats.get("dead_lettered_total", 0)))


class _RedisStoreBackend:
    """让独立配置的 Store 只依赖 Redis client 与 namespace。"""

    def __init__(self, redis: Any, namespace: str) -> None:
        self._redis, self._namespace, self._closed = redis, namespace, False

    def _ensure_open(self) -> None:
        if self._closed:
            raise BrokerClosedError("broker 已关闭")

    def _queue_key(self, queue: str, kind: str) -> str:
        return f"{self._namespace}:queue:{{{queue}}}:{kind}"

    def _message_key(self, message_id: str) -> str:
        return f"{self._namespace}:message:{message_id}"

    def _dedup_key(self, scope: str, key: str) -> str:
        return f"{self._namespace}:dedup:{hashlib.sha256(scope.encode()).hexdigest()}:{hashlib.sha256(key.encode()).hexdigest()}"


class RedisSubmissionStore:
    """Redis Lua 提交 Store；默认不接受 dedup 请求。"""

    capabilities = SubmissionCapabilities(
        dedup_guarantee=DedupGuarantee.NONE,
        per_key_dedup_ttl=False,
        stores_original_message_id=False,
        atomic_submit=True,
        batch_submit=True,
        batch_atomic=True,
    )

    def __init__(self, broker: RedisBroker | Any, *, namespace: str = "taskflow") -> None:
        """接受已创建的 Broker，或直接接受 Redis client 用于 profile 配置。"""
        self._broker = broker if isinstance(broker, RedisBroker) else _RedisStoreBackend(broker, namespace)

    def _dedup_redis_key(self, submission: PreparedSubmission) -> str:
        if submission.dedup_key is not None:
            raise ValidationError("当前 SubmissionStore 不支持 dedup")
        return ""

    async def submit(self, submission: PreparedSubmission) -> SubmitResult:
        self._broker._ensure_open()
        dedup_key = self._dedup_redis_key(submission)
        envelope = base64.b64encode(submission.envelope).decode("ascii")
        expires_at = submission.expires_at_ms / 1000 if submission.expires_at_ms is not None else 0
        values = await self._broker._redis.eval("""
            if ARGV[1] ~= '' then
              local old = redis.call('GET', KEYS[1])
              if old then return {0, old, redis.call('PTTL', KEYS[1])} end
              redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
            end
            local entry = ''
            if ARGV[6] == 'ready' then
              entry = redis.call('XADD', KEYS[3], '*', 'message_id', ARGV[2], 'envelope', ARGV[4])
              redis.call('ZADD', KEYS[7], ARGV[12], ARGV[2])
            end
            redis.call('HSET', KEYS[2], 'envelope', ARGV[4], 'queue', ARGV[5], 'status', ARGV[6], 'entry_id', entry,
              'attempt', '0', 'max_attempts', ARGV[7], 'created_at', ARGV[8], 'expires_at', ARGV[9],
              'serializer_name', ARGV[10], 'serializer_version', ARGV[11])
            if ARGV[6] ~= 'ready' then
              redis.call('HSET', KEYS[2], 'expired_at', ARGV[8], 'status_at_expiry', 'ready', 'last_delivery_id', '')
              redis.call('LPUSH', KEYS[5], ARGV[2])
            end
            if ARGV[6] == 'ready' and ARGV[9] ~= '0' then redis.call('ZADD', KEYS[4], ARGV[9], ARGV[2]) end
            redis.call('HINCRBY', KEYS[6], 'submitted_total', 1)
            return {1, ARGV[2], ARGV[3], entry}
            """, 7, dedup_key, self._broker._message_key(submission.message_id),
            self._broker._queue_key(submission.queue, "stream"), self._broker._queue_key(submission.queue, "expiry"),
            self._broker._queue_key(submission.queue, "eq"), self._broker._queue_key(submission.queue, "stats"),
            self._broker._queue_key(submission.queue, "ready"), dedup_key, submission.message_id,
            str(submission.dedup_ttl_ms or 0), envelope, submission.queue, submission.status,
            str(submission.max_attempts), str(_timestamp(submission.created_at)), str(expires_at),
            submission.serializer_name, submission.serializer_version, str(_timestamp(submission.created_at)))
        if int(values[0]) == 0:
            ttl_left = int(values[2])
            return SubmitResult(str(values[1]), False, SubmitDecision.DUPLICATE, str(values[1]),
                dedup_expires_at=submission.created_at + timedelta(milliseconds=max(ttl_left, 0)))
        return SubmitResult(submission.message_id, True, SubmitDecision.ACCEPTED,
            stream_entry_id=str(values[3]),
            dedup_expires_at=(submission.created_at + timedelta(milliseconds=submission.dedup_ttl_ms)
                              if submission.dedup_ttl_ms is not None else None))

    async def submit_many(self, submissions: list[PreparedSubmission]) -> list[SubmitResult]:
        """一次 Lua 调用处理整批；每项保留独立 dedup 决策与结果。"""
        if not submissions:
            return []
        self._broker._ensure_open()
        keys: list[str] = []
        args: list[str] = [str(len(submissions))]
        for submission in submissions:
            dedup_key = self._dedup_redis_key(submission)
            keys.extend([dedup_key, self._broker._message_key(submission.message_id),
                self._broker._queue_key(submission.queue, "stream"), self._broker._queue_key(submission.queue, "expiry"),
                self._broker._queue_key(submission.queue, "eq"), self._broker._queue_key(submission.queue, "stats"),
                self._broker._queue_key(submission.queue, "ready")])
            args.extend([dedup_key, submission.message_id, str(submission.dedup_ttl_ms or 0),
                base64.b64encode(submission.envelope).decode("ascii"), submission.queue, submission.status,
                str(submission.max_attempts), str(_timestamp(submission.created_at)),
                str(submission.expires_at_ms / 1000 if submission.expires_at_ms is not None else 0),
                submission.serializer_name, submission.serializer_version, str(_timestamp(submission.created_at))])
        values = await self._broker._redis.eval("""
            local count = tonumber(ARGV[1]); local output = {}
            for index = 0, count - 1 do
              local key = index * 7; local arg = 2 + index * 12
              local duplicate = false
              if ARGV[arg] ~= '' then
                local old = redis.call('GET', KEYS[key + 1])
                if old then
                  table.insert(output, 0); table.insert(output, old); table.insert(output, redis.call('PTTL', KEYS[key + 1])); table.insert(output, '')
                  duplicate = true
                else
                  redis.call('SET', KEYS[key + 1], ARGV[arg + 1], 'PX', ARGV[arg + 2])
                end
              end
              if not duplicate then
                local entry = ''
                if ARGV[arg + 5] == 'ready' then
                  entry = redis.call('XADD', KEYS[key + 3], '*', 'message_id', ARGV[arg + 1], 'envelope', ARGV[arg + 3])
                  redis.call('ZADD', KEYS[key + 7], ARGV[arg + 11], ARGV[arg + 1])
                end
                redis.call('HSET', KEYS[key + 2], 'envelope', ARGV[arg + 3], 'queue', ARGV[arg + 4], 'status', ARGV[arg + 5], 'entry_id', entry, 'attempt', '0', 'max_attempts', ARGV[arg + 6], 'created_at', ARGV[arg + 7], 'expires_at', ARGV[arg + 8], 'serializer_name', ARGV[arg + 9], 'serializer_version', ARGV[arg + 10])
                if ARGV[arg + 5] ~= 'ready' then redis.call('HSET', KEYS[key + 2], 'expired_at', ARGV[arg + 7], 'status_at_expiry', 'ready', 'last_delivery_id', ''); redis.call('LPUSH', KEYS[key + 5], ARGV[arg + 1]) end
                if ARGV[arg + 5] == 'ready' and ARGV[arg + 8] ~= '0' then redis.call('ZADD', KEYS[key + 4], ARGV[arg + 8], ARGV[arg + 1]) end
                redis.call('HINCRBY', KEYS[key + 6], 'submitted_total', 1)
                table.insert(output, 1); table.insert(output, ARGV[arg + 1]); table.insert(output, ARGV[arg + 2]); table.insert(output, entry)
              end
            end
            return output
            """, len(keys), *keys, *args)
        results: list[SubmitResult] = []
        for index, submission in enumerate(submissions):
            accepted, message_id, ttl_or_entry, entry = values[index * 4:index * 4 + 4]
            if int(accepted) == 0:
                results.append(SubmitResult(str(message_id), False, SubmitDecision.DUPLICATE, str(message_id),
                    dedup_expires_at=submission.created_at + timedelta(milliseconds=max(int(ttl_or_entry), 0))))
            else:
                results.append(SubmitResult(submission.message_id, True, SubmitDecision.ACCEPTED,
                    stream_entry_id=str(entry), dedup_expires_at=(submission.created_at + timedelta(milliseconds=submission.dedup_ttl_ms) if submission.dedup_ttl_ms is not None else None)))
        return results


class RedisStringDedupSubmissionStore(RedisSubmissionStore):
    """以 Redis string + PX TTL 实现精确按键去重的提交 Store。"""

    capabilities = SubmissionCapabilities(
        dedup_guarantee=DedupGuarantee.EXACT,
        per_key_dedup_ttl=True,
        stores_original_message_id=True,
        atomic_submit=True,
        batch_submit=True,
        batch_atomic=True,
    )

    def _dedup_redis_key(self, submission: PreparedSubmission) -> str:
        if submission.dedup_key is None:
            return ""
        assert submission.dedup_scope is not None and submission.dedup_ttl_ms is not None
        return self._broker._dedup_key(submission.dedup_scope, submission.dedup_key)


class RedisDelivery:
    """Redis lease token 保护的一次投递上下文。"""
    def __init__(self, broker: RedisBroker, message: TaskMessage, delivery_id: str, token: str, consumer_id: str, attempt: int, claimed_at: datetime, lease_until: datetime) -> None:
        self._broker, self._lease_token, self._lease_seconds = broker, token, (lease_until - claimed_at).total_seconds()
        self.message, self.delivery_id, self.consumer_id, self.attempt = message, delivery_id, consumer_id, attempt
        self.claimed_at, self.lease_until = claimed_at, lease_until
    async def ack(self) -> FinishOutcome: return await self._broker._finish(self, "ack")
    async def retry(self, *, reason: str | None = None) -> FinishOutcome: return await self._broker._finish(self, "retry", reason)
    async def reject(self, *, reason: str, error: BaseException | None = None) -> FinishOutcome:
        if not reason: raise ValidationError("reject 必须提供非空 reason")
        return await self._broker._finish(self, "reject", reason, error)
    async def extend_lease(self, *, seconds: float | None = None) -> datetime:
        self.lease_until = await self._broker._extend(self, seconds); return self.lease_until


class RedisConsumer:
    """Redis Stream Consumer Group 的异步投递迭代器。"""
    def __init__(self, broker: RedisBroker, queue: str, consumer_id: str, options: ConsumerOptions) -> None:
        self._broker, self.queue, self.consumer_id, self.options, self._closed = broker, queue, consumer_id, options, False
    async def start(self) -> None: await self._broker.start()
    async def close(self) -> None: self._closed = True
    async def __aenter__(self) -> Self: await self.start(); return self
    async def __aexit__(self, *_: object) -> None: await self.close()
    def __aiter__(self) -> AsyncIterator[RedisDelivery]: return self
    async def __anext__(self) -> RedisDelivery:
        while not self._closed:
            delivery = await self._broker._claim(self.queue, self.consumer_id, self.options.lease_seconds)
            if delivery is not None: return delivery
            await asyncio.sleep(self.options.poll_interval)
        raise StopAsyncIteration


class RedisAdmin:
    """Redis DLQ/EQ 的查询、删除与重放接口。"""
    def __init__(self, broker: RedisBroker) -> None: self._broker = broker
    async def list_dead_letters(self, queue: str) -> list[DeadLetter]:
        result = []
        for message_id in await self._broker._redis.lrange(self._broker._queue_key(queue, "dlq"), 0, -1):
            data = await self._broker._redis.hgetall(self._broker._message_key(message_id))
            result.append(DeadLetter(self._broker._decode(data["envelope"], data.get("serializer_name"), data.get("serializer_version")), int(data["attempt"]), data.get("last_reason"), data.get("dead_source", "reject"), _datetime(float(data.get("failed_at", 0))) or utc_now(), data.get("error_type") or None))
        return result
    async def list_expired(self, queue: str) -> list[ExpiredMessage]:
        result = []
        for message_id in await self._broker._redis.lrange(self._broker._queue_key(queue, "eq"), 0, -1):
            data = await self._broker._redis.hgetall(self._broker._message_key(message_id))
            result.append(ExpiredMessage(
                self._broker._decode(data["envelope"], data.get("serializer_name"), data.get("serializer_version")),
                MessageStatus(data["status_at_expiry"]),
                _datetime(float(data["expired_at"])) or utc_now(),
                int(data.get("attempt", 0)),
            ))
        return result
    async def delete_dead_letter(self, queue: str, message_id: str) -> bool: return bool(await self._broker._redis.lrem(self._broker._queue_key(queue, "dlq"), 0, message_id))
    async def delete_expired(self, queue: str, message_id: str) -> bool: return bool(await self._broker._redis.lrem(self._broker._queue_key(queue, "eq"), 0, message_id))

    def _replay_dedup(self, message: TaskMessage, *, reuse_dedup: bool,
                      dedup_scope: str | None, dedup_key: str | None,
                      dedup_ttl: timedelta | None) -> tuple[TaskMessage, str, str, int, bool]:
        """返回新 envelope 的 dedup 元数据；scope 不随 queue 重写。"""
        has_override = dedup_scope is not None or dedup_key is not None or dedup_ttl is not None
        if message.dedup_key is not None:
            assert message.dedup_scope is not None
            old_redis_key = self._broker._dedup_key(message.dedup_scope, message.dedup_key)
        else:
            old_redis_key = ""
        if reuse_dedup and has_override:
            raise ValidationError("reuse_dedup=True 时不能同时指定新的 dedup 参数")
        if reuse_dedup:
            return message, old_redis_key, "", 0, True
        if (dedup_scope is None) != (dedup_key is None):
            raise ValidationError("dedup_scope 与 dedup_key 必须同时提供")
        if dedup_key is None:
            return replace(message, dedup_scope=None, dedup_key=None), old_redis_key, "", 0, False
        ttl = self._broker._default_dedup_ttl if dedup_ttl is None else dedup_ttl
        if ttl is None or ttl.total_seconds() <= 0:
            raise ValidationError("替换 dedup 时必须提供正数 dedup_ttl 或配置默认值")
        assert dedup_scope is not None
        return (replace(message, dedup_scope=dedup_scope, dedup_key=dedup_key), old_redis_key,
                self._broker._dedup_key(dedup_scope, dedup_key), int(ttl.total_seconds() * 1000), False)

    async def replay_dead_letter(self, queue: str, message_id: str, *, reset_attempt: bool = True,
                                 target_queue: str | None = None, payload: Any = None,
                                 metadata: Mapping[str, Any] | None = None, reuse_dedup: bool = True,
                                 dedup_scope: str | None = None, dedup_key: str | None = None,
                                 dedup_ttl: timedelta | None = None) -> None:
        """原子地删除 DLQ 审计记录并向目标 Stream 写入新的 READY entry。"""
        data = await self._broker._redis.hgetall(self._broker._message_key(message_id))
        if not data:
            raise ValidationError("未找到指定死信")
        message = self._broker._decode(data["envelope"], data.get("serializer_name"), data.get("serializer_version")); message = replace(message, queue=target_queue or message.queue, payload=message.payload if payload is None else payload, metadata=message.metadata if metadata is None else metadata)
        self._broker._validate_queue(message.queue)
        message, old_dedup_key, new_dedup_key, new_dedup_ttl, reuse_dedup = self._replay_dedup(message,
            reuse_dedup=reuse_dedup, dedup_scope=dedup_scope, dedup_key=dedup_key, dedup_ttl=dedup_ttl)
        await self._broker._ensure_group(message.queue)
        envelope = self._broker._encode(message)
        script = """
            if ARGV[7] == '0' and KEYS[7] ~= '' then
              local current = redis.call('GET', KEYS[7])
              if current and current ~= ARGV[1] then return -1 end
            end
            if redis.call('LREM', KEYS[2], 1, ARGV[1]) == 0 then return 0 end
            if ARGV[7] == '0' then
              if KEYS[6] ~= '' and redis.call('GET', KEYS[6]) == ARGV[1] then redis.call('DEL', KEYS[6]) end
              if KEYS[7] ~= '' then redis.call('SET', KEYS[7], ARGV[1], 'PX', ARGV[8]) end
            end
            local entry = redis.call('XADD', KEYS[3], '*', 'message_id', ARGV[1], 'envelope', ARGV[2])
            redis.call('HSET', KEYS[1], 'envelope', ARGV[2], 'queue', ARGV[3], 'status', 'ready', 'entry_id', entry, 'attempt', ARGV[4], 'last_action', 'replayed')
            if ARGV[5] ~= '0' then redis.call('ZADD', KEYS[4], ARGV[5], ARGV[1]) end
            redis.call('ZADD', KEYS[5], ARGV[6], ARGV[1])
            return 1
        """
        replayed = int(await self._broker._redis.eval(script, 7, self._broker._message_key(message_id), self._broker._queue_key(queue, "dlq"), self._broker._queue_key(message.queue, "stream"), self._broker._queue_key(message.queue, "expiry"), self._broker._queue_key(message.queue, "ready"), old_dedup_key, new_dedup_key, message_id, envelope, message.queue, "0" if reset_attempt else data["attempt"], str(_timestamp(message.expires_at)) if message.expires_at else "0", str(_timestamp(await self._broker._now())), "1" if reuse_dedup else "0", str(new_dedup_ttl)))
        if replayed == -1:
            raise ValidationError("新的 dedup key 已关联到其他消息")
        if replayed == 0:
            raise ValidationError("未找到指定死信")

    async def replay_expired(self, queue: str, message_id: str, *, expires_at: datetime | None,
                             reuse_dedup: bool = True, dedup_scope: str | None = None,
                             dedup_key: str | None = None, dedup_ttl: timedelta | None = None) -> None:
        """以新过期策略将 EQ 消息重新置为 READY。"""
        if expires_at is not None and expires_at.tzinfo is None:
            raise ValidationError("expires_at 必须带时区")
        data = await self._broker._redis.hgetall(self._broker._message_key(message_id))
        if not data:
            raise ValidationError("未找到指定过期消息")
        message = replace(self._broker._decode(data["envelope"], data.get("serializer_name"), data.get("serializer_version")), expires_at=expires_at)
        message, old_dedup_key, new_dedup_key, new_dedup_ttl, reuse_dedup = self._replay_dedup(message,
            reuse_dedup=reuse_dedup, dedup_scope=dedup_scope, dedup_key=dedup_key, dedup_ttl=dedup_ttl)
        await self._broker._ensure_group(queue)
        envelope = self._broker._encode(message)
        script = """
            if ARGV[5] == '0' and KEYS[7] ~= '' then
              local current = redis.call('GET', KEYS[7])
              if current and current ~= ARGV[1] then return -1 end
            end
            if redis.call('LREM', KEYS[2], 1, ARGV[1]) == 0 then return 0 end
            if ARGV[5] == '0' then
              if KEYS[6] ~= '' and redis.call('GET', KEYS[6]) == ARGV[1] then redis.call('DEL', KEYS[6]) end
              if KEYS[7] ~= '' then redis.call('SET', KEYS[7], ARGV[1], 'PX', ARGV[6]) end
            end
            local entry = redis.call('XADD', KEYS[3], '*', 'message_id', ARGV[1], 'envelope', ARGV[2])
            redis.call('HSET', KEYS[1], 'envelope', ARGV[2], 'status', 'ready', 'entry_id', entry, 'expires_at', ARGV[3], 'last_action', 'replayed')
            if ARGV[3] ~= '0' then redis.call('ZADD', KEYS[4], ARGV[3], ARGV[1]) end
            redis.call('ZADD', KEYS[5], ARGV[4], ARGV[1])
            return 1
        """
        replayed = int(await self._broker._redis.eval(script, 7, self._broker._message_key(message_id), self._broker._queue_key(queue, "eq"), self._broker._queue_key(queue, "stream"), self._broker._queue_key(queue, "expiry"), self._broker._queue_key(queue, "ready"), old_dedup_key, new_dedup_key, message_id, envelope, str(_timestamp(expires_at)) if expires_at else "0", str(_timestamp(await self._broker._now())), "1" if reuse_dedup else "0", str(new_dedup_ttl)))
        if replayed == -1:
            raise ValidationError("新的 dedup key 已关联到其他消息")
        if replayed == 0:
            raise ValidationError("未找到指定过期消息")
