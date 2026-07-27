"""SubmissionStore profile routing shared by all broker backends."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from ..capabilities import SubmissionCapabilities
from ..errors import ValidationError
from ..naming import validate_persistent_name


class _QueueValidator(Protocol):
    _allow_legacy_names: bool

    def _validate_queue(self, queue: str) -> None: ...


class SubmissionRouter:
    """Validate and select configured SubmissionStore profiles.

    Store implementations are intentionally duck-typed at this extension
    boundary, so third-party stores do not have to inherit a framework class.
    The dynamic check is contained here rather than duplicated by brokers.
    """

    def __init__(self, owner: _QueueValidator, *, default_store: Any,
                 submission_store: Any | None, submission_stores: Mapping[str, Any] | None,
                 queue_submission_profiles: Mapping[str, str] | None) -> None:
        if submission_store is not None and submission_stores is not None:
            raise ValidationError("submission_store 与 submission_stores 不能同时配置")
        configured = dict(submission_stores or {"default": submission_store or default_store})
        if "default" not in configured:
            raise ValidationError("submission_stores 必须包含 default profile")
        self._stores = {
            name: store(owner) if callable(store) and not hasattr(store, "submit") else store
            for name, store in configured.items()
        }
        for name in self._stores:
            validate_persistent_name(name, label="submission profile", allow_legacy=owner._allow_legacy_names)
        if any(not hasattr(store, "submit") or not hasattr(store, "submit_many") or not hasattr(store, "capabilities")
               for store in self._stores.values()):
            raise ValidationError("每个 submission store 都必须实现 submit 与 submit_many")
        self._profiles = dict(queue_submission_profiles or {})
        for queue, profile in self._profiles.items():
            owner._validate_queue(queue)
            if profile not in self._stores:
                raise ValidationError(f"queue {queue!r} 使用了未知 submission profile {profile!r}")

    @property
    def default(self) -> Any:
        return self._stores["default"]

    @property
    def stores(self) -> Mapping[str, Any]:
        return self._stores

    @property
    def profiles(self) -> Mapping[str, str]:
        return self._profiles

    def profile_for(self, queue: str) -> str:
        return self._profiles.get(queue, "default")

    def for_queue(self, queue: str) -> Any:
        return self.default if self.profile_for(queue) == "default" else self._stores[self.profile_for(queue)]

    def capabilities(self, queue: str) -> SubmissionCapabilities:
        return self.for_queue(queue).capabilities
