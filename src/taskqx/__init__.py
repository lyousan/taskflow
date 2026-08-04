"""Taskqx：独立、可嵌入、异步优先的任务消息框架。"""

from .broker import (
    RedisBroker,
    RedisStringDedupSubmissionStore,
    RedisSubmissionStore,
    SQLiteBroker,
    SQLiteSubmissionStore,
)
from .capabilities import BackendCapabilities, DedupGuarantee, SubmissionCapabilities
from .config import QueueConfig
from .consistency import (
    ConsistencyIssue,
    ConsistencyReport,
    KeyspaceMigrationReport,
    RepairReport,
)
from .errors import (
    BrokerClosedError,
    LeaseLostError,
    PayloadDecodingError,
    RejectMessage,
    RetryableError,
    SerializerUnavailableError,
    TaskqxError,
    UnsupportedCapabilityError,
    ValidationError,
)
from .health import HealthCheck, HealthReport
from .observability import (
    BrokerEvent,
    EventSink,
    GaugeMetricsSink,
    MetricsSink,
    TaskqxEvent,
)
from .payloads import PayloadSchema
from .protocols import SubmissionStore, TaskBroker, TaskConsumer, TaskDelivery
from .retry import ExponentialBackoff, FixedBackoff, ImmediateBackoff, RetryPolicy
from .scheduler import BackendScheduler
from .serialization import JsonSerializer, Serializer, SerializerRegistry
from .types import (
    BatchSubmitItemResult,
    ConsumerOptions,
    DeadLetter,
    ExpiredMessage,
    FinishOutcome,
    MessageState,
    MessageStatus,
    MessageSummary,
    Page,
    QueueStats,
    SubmitDecision,
    SubmitRequest,
    SubmitResult,
    TaskMessage,
)
from .worker import TaskWorker

__all__ = [
    "BackendCapabilities",
    "BackendScheduler",
    "BatchSubmitItemResult",
    "BrokerClosedError",
    "BrokerEvent",
    "ConsistencyIssue",
    "ConsistencyReport",
    "ConsumerOptions",
    "DeadLetter",
    "DedupGuarantee",
    "EventSink",
    "ExpiredMessage",
    "ExponentialBackoff",
    "FinishOutcome",
    "FixedBackoff",
    "GaugeMetricsSink",
    "HealthCheck",
    "HealthReport",
    "ImmediateBackoff",
    "JsonSerializer",
    "KeyspaceMigrationReport",
    "LeaseLostError",
    "MessageState",
    "MessageStatus",
    "MessageSummary",
    "MetricsSink",
    "Page",
    "PayloadDecodingError",
    "PayloadSchema",
    "QueueConfig",
    "QueueStats",
    "RedisBroker",
    "RedisStringDedupSubmissionStore",
    "RedisSubmissionStore",
    "RejectMessage",
    "RepairReport",
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
    "TaskqxError",
    "TaskqxEvent",
    "UnsupportedCapabilityError",
    "ValidationError",
]
