"""Taskflow：独立、可嵌入、异步优先的任务消息框架。"""
from .broker import RedisBroker, RedisStringDedupSubmissionStore, RedisSubmissionStore, SQLiteBroker, SQLiteSubmissionStore
from .capabilities import BackendCapabilities, DedupGuarantee, SubmissionCapabilities
from .errors import (
    BrokerClosedError,
    LeaseLostError,
    TaskflowError,
    UnsupportedCapabilityError,
    ValidationError,
)
from .serialization import JsonSerializer, Serializer, SerializerRegistry
from .observability import BrokerEvent, MetricsSink
from .types import (
    ConsumerOptions,
    DeadLetter,
    ExpiredMessage,
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
    "BrokerEvent",
    "BrokerClosedError",
    "ConsumerOptions",
    "DeadLetter",
    "DedupGuarantee",
    "ExpiredMessage",
    "JsonSerializer",
    "LeaseLostError",
    "MessageStatus",
    "MetricsSink",
    "QueueStats",
    "RedisBroker",
    "RedisStringDedupSubmissionStore",
    "RedisSubmissionStore",
    "SQLiteBroker",
    "SQLiteSubmissionStore",
    "Serializer",
    "SerializerRegistry",
    "SubmissionCapabilities",
    "SubmitDecision",
    "SubmitRequest",
    "SubmitResult",
    "TaskMessage",
    "TaskWorker",
    "TaskflowError",
    "UnsupportedCapabilityError",
    "ValidationError",
]
