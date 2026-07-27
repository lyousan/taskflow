"""Taskflow：独立、可嵌入、异步优先的任务消息框架。"""
from .broker import (
    RedisBroker,
    RedisStringDedupSubmissionStore,
    RedisSubmissionStore,
    SQLiteBroker,
    SQLiteSubmissionStore,
)
from .capabilities import BackendCapabilities, DedupGuarantee, SubmissionCapabilities
from .config import QueueConfig
from .errors import (
    BrokerClosedError,
    LeaseLostError,
    PayloadDecodingError,
    RejectMessage,
    RetryableError,
    SerializerUnavailableError,
    TaskflowError,
    UnsupportedCapabilityError,
    ValidationError,
)
from .observability import (
    BrokerEvent,
    EventSink,
    GaugeMetricsSink,
    MetricsSink,
    TaskflowEvent,
)
from .payloads import PayloadSchema
from .protocols import SubmissionStore, TaskBroker, TaskConsumer, TaskDelivery
from .retry import ExponentialBackoff, FixedBackoff, ImmediateBackoff, RetryPolicy
from .serialization import JsonSerializer, Serializer, SerializerRegistry
from .types import (
    BatchSubmitItemResult,
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
    "BatchSubmitItemResult",
    "BrokerClosedError",
    "BrokerEvent",
    "ConsumerOptions",
    "DeadLetter",
    "DedupGuarantee",
    "EventSink",
    "ExpiredMessage",
    "ExponentialBackoff",
    "FinishOutcome",
    "FixedBackoff",
    "GaugeMetricsSink",
    "ImmediateBackoff",
    "JsonSerializer",
    "LeaseLostError",
    "MessageStatus",
    "MetricsSink",
    "PayloadDecodingError",
    "PayloadSchema",
    "QueueConfig",
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
    "SerializerUnavailableError",
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
    "TaskflowEvent",
    "UnsupportedCapabilityError",
    "ValidationError",
]
