"""Redis SubmissionStore implementations and their atomic Lua admission boundary."""
from __future__ import annotations

import hashlib
from datetime import timedelta
from typing import Any, Protocol, TypeGuard

from ..broker.redis_calls import RedisKeyspace, batch_submit_call, submit_call
from ..capabilities import DedupGuarantee, SubmissionCapabilities
from ..errors import BrokerClosedError, ValidationError
from ..types import SubmitDecision, SubmitResult
from .base import PreparedSubmission


class RedisSubmissionBackend(RedisKeyspace, Protocol):
    """Internal Store dependency contract, including its Redis client adapter."""

    _redis: Any

    def _ensure_open(self) -> None: ...
    def _dedup_key(self, scope: str, key: str) -> str: ...


class _RedisStoreBackend:
    """Minimal backend adapter for configuring a store from a Redis client."""

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

    def _group_name(self) -> str:
        return "taskflow"


def _is_submission_backend(value: object) -> TypeGuard[RedisSubmissionBackend]:
    return all(hasattr(value, name) for name in ("_redis", "_ensure_open", "_dedup_key", "_message_key", "_queue_key", "_group_name"))


class RedisSubmissionStore:
    """Redis Lua submission store; the default variant refuses dedup requests."""

    capabilities = SubmissionCapabilities(
        dedup_guarantee=DedupGuarantee.NONE, per_key_dedup_ttl=False,
        stores_original_message_id=False, atomic_submit=True, batch_submit=True, batch_atomic=True,
    )

    def __init__(self, broker: object, *, namespace: str = "taskflow") -> None:
        self._broker: RedisSubmissionBackend = broker if _is_submission_backend(broker) else _RedisStoreBackend(broker, namespace)

    def _dedup_redis_key(self, submission: PreparedSubmission) -> str:
        if submission.dedup_key is not None:
            raise ValidationError("当前 SubmissionStore 不支持 dedup")
        return ""

    async def submit(self, submission: PreparedSubmission) -> SubmitResult:
        self._broker._ensure_open()
        values = await submit_call(self._broker, submission, self._dedup_redis_key(submission)).execute(self._broker._redis)
        if int(values[0]) == 0:
            ttl_left = int(values[2])
            return SubmitResult(str(values[1]), False, SubmitDecision.DUPLICATE, str(values[1]),
                                dedup_expires_at=submission.created_at + timedelta(milliseconds=max(ttl_left, 0)))
        return SubmitResult(submission.message_id, True, SubmitDecision.ACCEPTED, stream_entry_id=str(values[3]),
                            dedup_expires_at=(submission.created_at + timedelta(milliseconds=submission.dedup_ttl_ms)
                                              if submission.dedup_ttl_ms is not None else None))

    async def submit_many(self, submissions: list[PreparedSubmission]) -> list[SubmitResult]:
        if not submissions:
            return []
        self._broker._ensure_open()
        values = await batch_submit_call(
            self._broker, submissions, [self._dedup_redis_key(submission) for submission in submissions],
        ).execute(self._broker._redis)
        results: list[SubmitResult] = []
        for index, submission in enumerate(submissions):
            accepted, message_id, ttl_or_entry, entry = values[index * 4:index * 4 + 4]
            if int(accepted) == 0:
                results.append(SubmitResult(str(message_id), False, SubmitDecision.DUPLICATE, str(message_id),
                    dedup_expires_at=submission.created_at + timedelta(milliseconds=max(int(ttl_or_entry), 0))))
            else:
                results.append(SubmitResult(submission.message_id, True, SubmitDecision.ACCEPTED,
                    stream_entry_id=str(entry), dedup_expires_at=(submission.created_at + timedelta(milliseconds=submission.dedup_ttl_ms)
                    if submission.dedup_ttl_ms is not None else None)))
        return results


class RedisStringDedupSubmissionStore(RedisSubmissionStore):
    """Exact dedup admission implemented as Redis string + PX TTL in the same Lua call."""

    capabilities = SubmissionCapabilities(
        dedup_guarantee=DedupGuarantee.EXACT, per_key_dedup_ttl=True,
        stores_original_message_id=True, atomic_submit=True, batch_submit=True, batch_atomic=True,
    )

    def _dedup_redis_key(self, submission: PreparedSubmission) -> str:
        if submission.dedup_key is None:
            return ""
        assert submission.dedup_scope is not None and submission.dedup_ttl_ms is not None
        return self._broker._dedup_key(submission.dedup_scope, submission.dedup_key)
