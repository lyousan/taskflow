"""Taskflow：独立、可嵌入、异步优先的任务消息框架。"""
from .broker import (
    RedisBroker,
    RedisStringDedupSubmissionStore,
    RedisSubmissionStore,
    SQLiteBroker,
    SQLiteSubmissionStore,
)
from .capabilities import BackendCapabilities, DedupGuarantee, SubmissionCapabilities
from .errors import (
    BrokerClosedError,
    LeaseLostError,
    RejectMessage,
    RetryableError,
    TaskflowError,
    UnsupportedCapabilityError,
    ValidationError,
)
from .observability import BrokerEvent, MetricsSink
from .protocols import SubmissionStore, TaskBroker, TaskConsumer, TaskDelivery
from .retry import ExponentialBackoff, FixedBackoff, ImmediateBackoff, RetryPolicy
from .serialization import JsonSerializer, Serializer, SerializerRegistry
from .types import (
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
)
from .worker import TaskWorker

__all__ = [
    "BackendCapabilities",
    "BrokerClosedError",
    "BrokerEvent",
    "ConsumerOptions",
    "DeadLetter",
    "DedupGuarantee",
    "ExpiredMessage",
    "ExponentialBackoff",
    "FinishOutcome",
    "FixedBackoff",
    "ImmediateBackoff",
    "JsonSerializer",
    "LeaseLostError",
    "MessageStatus",
    "MetricsSink",
    "QueueStats",
    "RedisBroker",
    "RedisStringDedupSubmissionStore",
    "RedisSubmissionStore",
    "RejectMessage",
    "RetryPolicy",
    "RetryableError",
    "SQLiteBroker",
    "SQLiteSubmissionStore",
    "Serializer",
    "SerializerRegistry",
    "SubmissionCapabilities",
    "SubmissionStore",
    "SubmitDecision",
    "SubmitRequest",
    "SubmitResult",
    "TaskBroker",
    "TaskConsumer",
    "TaskDelivery",
    "TaskMessage",
    "TaskWorker",
    "TaskflowError",
    "UnsupportedCapabilityError",
    "ValidationError",
]
