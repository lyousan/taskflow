"""Typed Redis Lua invocation layouts.

Keeping these builders beside the scripts makes the Python/Lua boundary explicit:
every ``KEYS[n]`` and ``ARGV[n]`` position is constructed in one place and can
be asserted without a Redis server.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any, Protocol

from ..submission.base import PreparedSubmission
from ._time import timestamp as _timestamp
from .redis_scripts import (
    BATCH_SUBMIT,
    CLAIM,
    DUE_DELAYED,
    EXPIRE,
    EXTEND_LEASE,
    FINISH,
    PEL_RECOVER,
    RECLAIM_LEASE,
    REPLAY_DEAD_LETTER,
    REPLAY_EXPIRED,
    SUBMIT,
)


@dataclass(frozen=True, slots=True)
class RedisScriptCall:
    """A fully specified ``EVAL`` call, excluding the Redis client itself."""

    script: str
    keys: tuple[str, ...]
    args: tuple[str, ...]

    async def execute(self, redis: Any) -> Any:
        return await redis.eval(self.script, len(self.keys), *self.keys, *self.args)


class RedisKeyspace(Protocol):
    """Stable internal key-building contract shared by Redis script callers."""

    def _message_key(self, message_id: str) -> str: ...
    def _queue_key(self, queue: str, kind: str) -> str: ...
    def _group_name(self) -> str: ...


def submit_call(backend: RedisKeyspace, submission: PreparedSubmission, dedup_key: str) -> RedisScriptCall:
    """Build the single-message submit call (eight keys, thirteen args)."""
    expires_at = submission.expires_at_ms / 1000 if submission.expires_at_ms is not None else 0
    now = str(_timestamp(submission.created_at))
    return RedisScriptCall(SUBMIT, (
        dedup_key, backend._message_key(submission.message_id),
        backend._queue_key(submission.queue, "stream"), backend._queue_key(submission.queue, "expiry"),
        backend._queue_key(submission.queue, "eq"), backend._queue_key(submission.queue, "stats"),
        backend._queue_key(submission.queue, "ready"), backend._queue_key(submission.queue, "delayed"),
    ), (
        dedup_key, submission.message_id, str(submission.dedup_ttl_ms or 0),
        base64.b64encode(submission.envelope).decode("ascii"), submission.queue, submission.status,
        str(submission.max_attempts), now, str(expires_at), submission.serializer_name,
        submission.serializer_version, now,
        str(submission.available_at_ms / 1000 if submission.available_at_ms is not None else 0),
    ))


def batch_submit_call(backend: RedisKeyspace, submissions: list[PreparedSubmission], dedup_keys: list[str]) -> RedisScriptCall:
    """Build one atomic batch-submit call (eight keys / thirteen args each)."""
    keys: list[str] = []
    args: list[str] = [str(len(submissions))]
    for submission, dedup_key in zip(submissions, dedup_keys, strict=True):
        call = submit_call(backend, submission, dedup_key)
        keys.extend(call.keys)
        args.extend(call.args)
    return RedisScriptCall(BATCH_SUBMIT, tuple(keys), tuple(args))


def claim_call(backend: RedisKeyspace, *, queue: str, message_id: str, now: float, consumer_id: str,
               delivery_id: str, token: str, lease_until: float, entry_id: str) -> RedisScriptCall:
    return RedisScriptCall(CLAIM, (
        backend._message_key(message_id), backend._queue_key(queue, "leases"),
        backend._queue_key(queue, "eq"), backend._queue_key(queue, "expiry"),
        backend._queue_key(queue, "stream"), backend._queue_key(queue, "ready"),
    ), (str(now), message_id, consumer_id, delivery_id, token, str(lease_until), backend._group_name(), entry_id))


def finish_call(backend: RedisKeyspace, *, queue: str, message_id: str, action: str, delivery_id: str,
                token: str, now: float, reason: str, error_type: str, retry_available_at: float | None,
                max_attempts: int | None) -> RedisScriptCall:
    return RedisScriptCall(FINISH, tuple(backend._queue_key(queue, kind) if kind != "message" else backend._message_key(message_id)
        for kind in ("message", "leases", "expiry", "stream", "eq", "stats", "dlq", "ready", "delayed")),
        (action, delivery_id, token, str(now), message_id, reason, error_type, backend._group_name(),
         str(retry_available_at) if retry_available_at is not None else "0", str(max_attempts or 0)))


def extend_lease_call(backend: RedisKeyspace, *, queue: str, message_id: str, delivery_id: str,
                      token: str, lease_until: float) -> RedisScriptCall:
    return RedisScriptCall(EXTEND_LEASE, (
        backend._message_key(message_id), backend._queue_key(queue, "leases"),
        backend._queue_key(queue, "stream"), backend._queue_key(queue, "expiry"),
        backend._queue_key(queue, "eq"),
    ), (delivery_id, token, str(lease_until), message_id, backend._group_name()))


def pel_recover_call(backend: RedisKeyspace, *, queue: str, message_id: str, entry_id: str) -> RedisScriptCall:
    return RedisScriptCall(PEL_RECOVER, (
        backend._message_key(message_id), backend._queue_key(queue, "stream"),
        backend._queue_key(queue, "stats"),
    ), (backend._group_name(), entry_id, message_id))


def due_delayed_call(backend: RedisKeyspace, *, queue: str, message_id: str, now: float) -> RedisScriptCall:
    return RedisScriptCall(DUE_DELAYED, tuple(backend._queue_key(queue, kind) if kind != "message" else backend._message_key(message_id)
        for kind in ("message", "delayed", "ready", "stream", "eq", "expiry")), (str(now), message_id))


def expire_call(backend: RedisKeyspace, *, queue: str, message_id: str, now: float) -> RedisScriptCall:
    return RedisScriptCall(EXPIRE, tuple(backend._queue_key(queue, kind) if kind != "message" else backend._message_key(message_id)
        for kind in ("message", "leases", "expiry", "stream", "eq", "ready", "delayed")), (str(now), backend._group_name(), message_id))


def reclaim_lease_call(backend: RedisKeyspace, *, queue: str, message_id: str, now: float) -> RedisScriptCall:
    return RedisScriptCall(RECLAIM_LEASE, tuple(backend._queue_key(queue, kind) if kind != "message" else backend._message_key(message_id)
        for kind in ("message", "leases", "expiry", "stream", "dlq", "stats", "eq", "ready")), (str(now), backend._group_name(), message_id))


def replay_dead_letter_call(backend: RedisKeyspace, *, source_queue: str, target_queue: str, message_id: str,
                            envelope: str, attempt: str, expires_at: float, now: float, old_dedup_key: str,
                            new_dedup_key: str, keep: bool, replacement_ttl: int) -> RedisScriptCall:
    return RedisScriptCall(REPLAY_DEAD_LETTER, (
        backend._message_key(message_id), backend._queue_key(source_queue, "dlq"),
        backend._queue_key(target_queue, "stream"), backend._queue_key(target_queue, "expiry"),
        backend._queue_key(target_queue, "ready"), old_dedup_key, new_dedup_key,
    ), (message_id, envelope, target_queue, attempt, str(expires_at), str(now), "1" if keep else "0", str(replacement_ttl)))


def replay_expired_call(backend: RedisKeyspace, *, source_queue: str, target_queue: str, message_id: str,
                        envelope: str, expires_at: float, now: float, old_dedup_key: str,
                        new_dedup_key: str, keep: bool, replacement_ttl: int) -> RedisScriptCall:
    return RedisScriptCall(REPLAY_EXPIRED, (
        backend._message_key(message_id), backend._queue_key(source_queue, "eq"),
        backend._queue_key(target_queue, "stream"), backend._queue_key(target_queue, "expiry"),
        backend._queue_key(target_queue, "ready"), old_dedup_key, new_dedup_key,
    ), (message_id, envelope, str(expires_at), str(now), "1" if keep else "0", str(replacement_ttl), target_queue))
