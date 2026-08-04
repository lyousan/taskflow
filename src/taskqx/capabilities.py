"""Backend 与提交策略的能力声明。"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class DedupGuarantee(str, Enum):
    """去重结果的可靠程度。"""

    NONE = "none"
    EXACT = "exact"
    PROBABILISTIC = "probabilistic"


@dataclass(frozen=True, slots=True)
class BackendCapabilities:
    """一个 backend 明确承诺的功能集合。"""

    delayed_delivery: bool = True
    dead_letter: bool = True
    deduplication: bool = True
    lease_reclaim: bool = True
    batch_submit: bool = True
    batch_atomic: bool = False
    transactional_submit: bool = True
    priority: bool = False
    partition_ordering: bool = False
    distributed_consumers: bool = False
    high_throughput: bool = False
    paginated_observation: bool = False
    stable_pagination_cursors: bool = False
    message_status_filter: bool = False


@dataclass(frozen=True, slots=True)
class SubmissionCapabilities:
    """提交准入策略的语义说明。"""

    dedup_guarantee: DedupGuarantee
    per_key_dedup_ttl: bool
    stores_original_message_id: bool
    atomic_submit: bool
    batch_submit: bool
    batch_atomic: bool = False
