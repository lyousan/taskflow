# 提交与去重架构

本文补充 [PRD](PRD.md) 的提交、去重与扩展设计，是 v0.1 核心架构的设计基线。

## 设计结论

去重不是一个独立的“提交前检查”操作，而是消息提交时的一种**准入策略**。可靠提交的原子边界是：

```text
准入判断（可选去重）
  + 消息入队
  + 初始状态写入
  + expires_at 索引写入
```

因此公共一等抽象为 `SubmissionStore`，而不是只提供 `claim()` 的 `DedupStore`。`TaskBroker.submit()` 负责参数校验、ID 生成和序列化，然后委托 `SubmissionStore.submit()` 在 backend 原子边界内完成提交。

```text
TaskBroker
  ├── SubmissionStore：submit / submit_many
  └── DeliveryStore：claim / ack / retry / reject / reclaim / DLQ / EQ
```

## 公共契约预览

```python
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Protocol


class DedupGuarantee(str, Enum):
    NONE = "none"
    EXACT = "exact"
    PROBABILISTIC = "probabilistic"


class SubmitDecision(str, Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    PROBABLE_DUPLICATE = "probable_duplicate"


@dataclass(frozen=True)
class SubmissionCapabilities:
    dedup_guarantee: DedupGuarantee
    per_key_dedup_ttl: bool
    stores_original_message_id: bool
    atomic_submit: bool
    batch_submit: bool


@dataclass(frozen=True)
class PreparedSubmission:
    message_id: str
    queue: str
    envelope: bytes
    status: str
    created_at: datetime
    expires_at_ms: int | None
    dedup_scope: str | None
    dedup_key: str | None
    dedup_ttl_ms: int | None
    max_attempts: int
    serializer_name: str
    serializer_version: str


@dataclass(frozen=True)
class SubmitResult:
    message_id: str
    accepted: bool
    decision: SubmitDecision
    existing_message_id: str | None = None
    stream_entry_id: str | None = None
    dedup_expires_at: datetime | None = None


class SubmissionStore(Protocol):
    capabilities: SubmissionCapabilities

    async def submit(self, submission: PreparedSubmission) -> SubmitResult: ...
    async def submit_many(
        self,
        submissions: list[PreparedSubmission],
    ) -> list[SubmitResult]: ...
```

`PreparedSubmission` 是 Broker 内部已完成校验、ID 生成和 serializer bytes 编码的对象；应用代码不直接构造它。Store 必须仅依赖该对象完成原子准入、dedup、消息入队、初始状态和过期索引写入，不能依赖隐藏回调。

## 内置提交策略

| 实现 | 去重保证 | 单 key TTL | 保存首次消息 ID | v0.1 |
|---|---|---:|---:|---:|
| `RedisSubmissionStore` | 无去重 | 否 | 否 | 是 |
| `RedisStringDedupSubmissionStore` | 精确 | 是 | 是 | 是 |
| `SQLiteSubmissionStore` | 精确（未提供 key 时跳过） | 是 | 是 | 是 |
| `RedisBloomDedupSubmissionStore` | 概率性 | 否（仅 scope / bucket 级） | 否 | 否，v0.2 optional |

Broker 默认分别使用 `SQLiteSubmissionStore` 与 `RedisStringDedupSubmissionStore`。也可用
`submission_stores` 和 `queue_submission_profiles` 将不同队列路由到不同 profile；未配置
queue 使用 `default`，混合 queue 的 `submit_many()` 按 profile 分组执行并保持输入顺序。
每个 queue 的实际语义可通过 `submission_capabilities(queue)` 查询：

```python
broker = RedisBroker(
    redis,
    namespace="taskflow",
    submission_stores={
        "default": RedisSubmissionStore(redis, namespace="taskflow"),
        "exact": RedisStringDedupSubmissionStore(redis, namespace="taskflow"),
    },
    queue_submission_profiles={"crawl.fetch": "exact"},
)
```

自定义 Store 直接接收完整的 `PreparedSubmission`，因此可以实现自己的原子准入语义。

DLQ / EQ replay 默认 `reuse_dedup=True`，保留原 scope/key 与其记录。传入
`reuse_dedup=False` 会移除原记录；同时提供新的 `dedup_scope`、`dedup_key` 和正数
`dedup_ttl` 则会原子替换记录。目标 queue 的变化不会自动改写 dedup scope。

## 精确 String Dedup

业务调用：

```python
result = await broker.submit(
    queue="crawl.fetch",
    payload={"url": "https://example.com/page/1"},
    metadata={"trace_id": "trace-001", "schema_version": "1"},
    dedup_scope="crawl:batch:2026-01",
    dedup_key="example.com:/page/1",
    dedup_ttl=timedelta(days=7),
)
```

- `dedup_key`：哪些业务消息视为相同；例如规范化 URL 或订单号。
- `dedup_scope`：比较 key 的范围；例如批次、租户或业务域。
- `dedup_ttl`：该 scope + key 的提交抑制窗口。

String Dedup 使用一个独立 Redis String：

```text
SET taskflow:dedup:{scope-hash}:key-hash <message-id> NX PX <ttl-ms>
```

相同 scope + key：

```text
首次提交 -> ACCEPTED
后续提交 -> DUPLICATE，并返回首次接受的 message_id
```

在 Redis backend 中，`SET NX`、`XADD`、状态写入和过期索引写入必须由同一个 Lua Script 完成；在 SQLite backend 中，它们必须位于同一个数据库事务中。

`dedup_ttl` 与 `expires_at` 独立：

- `expires_at`：消息何时不应继续处理，过期后进入 EQ；
- `dedup_ttl`：多久内不接受同一业务 key 的新提交。

如果 `dedup_ttl` 短于消息剩余有效期，原消息尚未过期时可能接受新的同 key 消息；如果更长，消息进入 EQ / DLQ 后仍可能继续抑制新提交。v0.1 不自动释放 dedup key。

## Bloom Dedup

Bloom Filter 只能回答：

```text
False -> 一定不存在
True  -> 可能存在（也可能是 false positive）
```

因此 Bloom 提交实现可以原子地完成“判断 + 入队”，但它的结果必须显式标记为：

```python
SubmitDecision.PROBABLE_DUPLICATE
```

不能伪装成精确的 `DUPLICATE`。普通 Bloom Filter 不支持 per-key TTL、保存 `existing_message_id` 或单 key `release()`；适合按 scope 或时间桶设置整体 TTL 的海量 URL Frontier 场景。它进入 v0.2，作为可选 RedisBloom 依赖。

## 仍待确定

启用精确 dedup、但未传 `dedup_ttl` 时的默认保留策略仍待产品确认。推荐要求业务方显式传入 TTL；若保留 broker 默认值，默认值和清理策略必须写入部署文档。
