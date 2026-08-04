"""Shared public submission orchestration for broker façades."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Protocol

from ..errors import ValidationError
from ..middleware import Middleware
from ..types import BatchSubmitItemResult, SubmitRequest, SubmitResult, TaskMessage
from .base import PreparedSubmission

PrepareSubmission = Callable[..., Awaitable[tuple[PreparedSubmission, TaskMessage]]]


class _SubmissionStore(Protocol):
    async def submit(self, submission: PreparedSubmission) -> SubmitResult: ...
    async def submit_many(
        self, submissions: list[PreparedSubmission]
    ) -> list[SubmitResult]: ...


class SubmissionOwner(Protocol):
    """Stable broker façade needed by shared submission orchestration."""

    middleware: Middleware
    _queue_submission_profiles: Mapping[str, str]

    def _submission_store_for(self, queue: str) -> _SubmissionStore: ...
    async def _record_submitted(
        self, prepared: PreparedSubmission, message: TaskMessage, result: SubmitResult
    ) -> None: ...


class SubmissionService:
    """Apply middleware, profile rules and result observation around Store calls."""

    def __init__(self, owner: SubmissionOwner, prepare: PrepareSubmission) -> None:
        self._owner = owner
        self._prepare = prepare

    async def submit(self, **kwargs: Any) -> SubmitResult:
        prepared, message = await self._prepare(**kwargs)
        await self._owner.middleware.emit("before_submit", message)
        result = await self._owner._submission_store_for(prepared.queue).submit(
            prepared
        )
        await self._owner._record_submitted(prepared, message, result)
        return result

    async def submit_request(self, request: SubmitRequest) -> SubmitResult:
        """Submit one normalized draft through the existing atomic boundary."""

        return await self.submit(**_request_kwargs(request))

    async def submit_many(
        self, messages: list[SubmitRequest], *, atomic: bool
    ) -> list[SubmitResult] | list[BatchSubmitItemResult]:
        if not atomic:
            results: list[BatchSubmitItemResult] = []
            for index, request in enumerate(messages):
                try:
                    result = await self.submit(**_request_kwargs(request))
                except Exception as exc:  # noqa: BLE001 - a failed item must not abort later items.
                    results.append(BatchSubmitItemResult(index=index, error=exc))
                else:
                    results.append(BatchSubmitItemResult(index=index, result=result))
            return results

        prepared_messages = [
            await self._prepare(**_request_kwargs(request)) for request in messages
        ]
        for _, message in prepared_messages:
            await self._owner.middleware.emit("before_submit", message)
        profiles = {
            self._owner._queue_submission_profiles.get(prepared.queue, "default")
            for prepared, _ in prepared_messages
        }
        if len(profiles) > 1:
            raise ValidationError("submit_many 不支持混合 SubmissionStore profile")
        atomic_results: list[SubmitResult] = []
        if prepared_messages:
            store = self._owner._submission_store_for(prepared_messages[0][0].queue)
            atomic_results = await store.submit_many(
                [prepared for prepared, _ in prepared_messages]
            )
        for result, (prepared, message) in zip(
            atomic_results, prepared_messages, strict=True
        ):
            await self._owner._record_submitted(prepared, message, result)
        return atomic_results


def _request_kwargs(request: SubmitRequest) -> dict[str, Any]:
    return {
        "queue": request.queue,
        "payload": request.payload,
        "metadata": request.metadata,
        "dedup_key": request.dedup_key,
        "dedup_scope": request.dedup_scope,
        "dedup_ttl": request.dedup_ttl,
        "delay": request.delay,
        "expires_at": request.expires_at,
        "max_attempts": request.max_attempts,
        "workflow_id": request.workflow_id,
        "parent_id": request.parent_id,
        "payload_type": request.payload_type,
        "payload_schema_name": request.payload_schema_name,
        "payload_schema_version": request.payload_schema_version,
    }
