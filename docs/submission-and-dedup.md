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
    envelope_json: str
    state_json: str
    expires_at_ms: int | None
    dedup_scope: str | None
    dedup_key: str | None
    dedup_ttl_ms: int | None


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

`PreparedSubmission` 是 Broker 内部已完成校验和 JSON 序列化的对象；应用代码不直接构造它。

## 内置提交策略

| 实现 | 去重保证 | 单 key TTL | 保存首次消息 ID | v0.1 |
|---|---|---:|---:|---:|
| `RedisSubmissionStore` | 无去重 | 否 | 否 | 是 |
| `RedisStringDedupSubmissionStore` | 精确 | 是 | 是 | 是 |
| `SQLiteSubmissionStore` | 无去重或精确 | 精确模式支持 | 精确模式支持 | 是 |
| `RedisBloomDedupSubmissionStore` | 概率性 | 否（仅 scope / bucket 级） | 否 | 否，v0.2 optional |

业务通过 Broker 配置的具名 submission profile 选择策略，而不是在每次 `submit()` 调用中传入一个任意 Python store 对象：

```python
broker = RedisBroker(
    submission_stores={
        "default": RedisSubmissionStore(redis),
        "exact": RedisStringDedupSubmissionStore(redis),
    },
    queue_submission_profiles={
        "crawl.fetch": "exact",
    },
)
```

这样同一个队列的所有生产者使用一致的准入语义。

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
