# Taskflow 产品需求文档（PRD）

- **产品名**：Taskflow
- **定位**：独立、可嵌入、异步优先的 Python 任务消息与处理框架
- **形态**：Python package
- **本文版本**：v0.1（历史 MVP 设计基线）
- **状态**：历史设计文档，不是当前版本验收清单；当前 v0.2 验收见
  [`v0.2-acceptance.md`](v0.2-acceptance.md)，迁移语义见
  [`migration-v0.1-v0.2.md`](migration-v0.1-v0.2.md)。

相关设计补充：

- [提交与去重架构](submission-and-dedup.md)：`SubmissionStore`、精确 String Dedup、Bloom Dedup 的语义与扩展边界；
- [Redis 消息生命周期设计](redis-lifecycle.md)：Redis Streams 下的提交、租约、ACK、Retry、Reject、DLQ 与 EQ 原子状态迁移。

---

## 1. 背景与问题

外部业务脚本中反复出现大量相同的任务流管理逻辑，例如：

- 生成并派发任务；
- 多个 worker 并发消费；
- 显式确认任务完成（ACK）；
- worker 崩溃、超时或网络异常后的任务回收；
- 按策略重试和延迟重试；
- 超过重试上限后进入死信队列（DLQ）；
- 任务去重；
- 任务状态、队列堆积、失败原因的观测；
- 死信任务查看、重放、清理；
- 支持异步业务处理函数。

本项目最初的讨论来自 BodSolve 的外部业务方需求：外部脚本会向 BodSolve 提交采集任务、拉取或接收任务结果、解析结果、根据结果扩展下一批任务、重放失败任务，并持续重复这一过程。一个典型例子是采集 A 网站的类目商品列表：业务方从一组类目初始 URL 开始，将 URL 提交给 BodSolve；取得页面结果后解析分页 URL；对新 URL 进行规范化和去重；再提交新任务，直到该类目没有新页面。

该场景揭示的是通用的任务流管理需求，而不是 BodSolve 自身的内部任务调度需求。BodSolve 在此仅是可被任务处理函数调用的一个外部执行服务；Taskflow 不应复用或受限于 BodSolve 的 `Task`、`Action`、`Node`、`Impl`、存储模型或历史实现。未来同一套能力也应能服务于纯 HTTP 请求、文件处理、数据 ETL、回放、通知及任意异步脚本。

简单的 Redis `SET + LIST` 可以实现“URL 去重 + 队列”的最小流程，但无法稳健处理 ACK、worker 宕机恢复、重试、死信、延迟投递、可观测性及任务重放。现有通用任务框架通常以“调用一个 Python 函数”为中心；而本项目更需要以“可靠地传递和消费一条业务消息”为中心。因此需要建设独立的 Taskflow package，为业务系统提供统一、可替换 backend 的任务消息基础设施。

> **边界结论**：Taskflow 是独立 Python package，不属于也不依赖 BodSolve。BodSolve、采集系统或任意 Python 服务均可作为 Taskflow 的生产者、消费者或任务处理函数的依赖方。

---

## 2. 产品定位

Taskflow 是一个面向 `asyncio` 的任务消息框架，提供统一的任务投递、消费、确认、重试、死信、延迟、租约与去重能力，并允许替换底层消息 backend。

### 2.1 核心价值

1. **可靠任务生命周期**：标准化消息从提交到完成、重试或死信的完整生命周期。
2. **异步优先**：所有 I/O 主路径均为 async API，适合高并发网络、采集与数据处理场景。
3. **Backend 可替换**：上层业务依赖稳定抽象，不直接耦合 Redis、Kafka 或 SQLite。
4. **业务无关**：不包含爬虫、浏览器、HTTP、BodSolve、Web 框架等领域模型。
5. **可扩展**：允许业务注入序列化、去重、重试策略、中间件、可观测性及 backend。
6. **语义清晰**：明确提供 at-least-once 投递，不虚假承诺 exactly-once。

### 2.2 非目标

MVP 不实现或不承诺：

- 工作流 DAG、任务编排语言、可视化流程设计器；
- 分布式事务与 exactly-once 端到端处理；
- 业务任务状态机、业务结果持久化；
- Web 管理后台；
- 自动发现或自动注册 Python 函数；
- 与特定 Web 框架、执行器或消息系统的强绑定；
- Kafka backend 的完整生产实现（作为后续版本目标）；
- 高级优先级调度、限流、配额和跨队列公平调度（后续迭代）。

---

## 3. 基本概念与语义

### 3.1 Message（任务消息）

一条不可变的、可序列化的业务任务请求。其最小字段包括：

| 字段 | 说明 |
|---|---|
| `id` | 全局唯一消息 ID；默认 UUIDv7 或可注入 ID 生成器 |
| `queue` | 目标队列名称，例如 `crawl.fetch` |
| `payload` | JSON-compatible 的业务载荷；业务字段由业务方定义 |
| `metadata` | 可选 JSON-compatible 元数据，如 trace ID、调用方、消息 schema 版本；不承载业务主体数据 |
| `dedup_key` | 可选去重键；同一去重作用域内相同键只接受一次提交 |
| `dedup_scope` | 可选去重作用域，用于隔离不同业务、租户或批次的 `dedup_key` |
| `workflow_id` | 可选业务流程或批次标识 |
| `parent_id` | 可选父消息 ID，用于任务派生关系 |
| `created_at` | 创建时间 |
| `expires_at` | 可选过期时间；到期后转入 Expired Queue，不再投递 |
| `max_attempts` | 最大投递/执行次数 |

Taskflow 核心层的 `payload` 必须是 JSON-compatible 数据：`null`、布尔、数字、字符串、数组、对象。不得默认使用 `pickle`，避免安全性、跨版本与跨 backend 问题。

### 3.2 Delivery（投递）

Delivery 是某条 Message 被某个消费者领取后形成的一次处理上下文。

同一条 Message 因超时回收或重试，可能产生多次 Delivery。Delivery 至少包含：

| 字段 | 说明 |
|---|---|
| `message` | 对应 Message |
| `delivery_id` | 本次投递的唯一标识 |
| `consumer_id` | 领取消息的消费者标识 |
| `attempt` | 当前第几次投递 |
| `claimed_at` | 领取时间 |
| `lease_until` | 租约到期时间 |

### 3.3 Lease（租约）

消费者领取任务后，Taskflow 在有限时间内认为该消费者拥有处理权。若消费者未在租约结束前 ACK、Retry、Reject 或续租，系统可将任务回收并再次投递。

Lease 用于处理：

- worker 进程崩溃；
- 容器被终止；
- 网络中断；
- 任务卡死；
- 消费者没有正常关闭。

### 3.4 ACK、Retry、Reject 与 DLQ

| 操作 | 语义 |
|---|---|
| `ack()` | 业务处理成功；当前消息生命周期结束 |
| `retry()` | 当前处理失败，立即重新投递；延迟重试在 v0.2 提供 |
| `reject()` | 当前处理失败且不再自动重试；进入 DLQ |
| 租约超时回收 | 未确认的任务重新投递；超过最大次数时进入 DLQ |

### 3.5 投递保证

Taskflow 默认且正式承诺的投递语义为：

```text
at-least-once delivery（至少一次投递）
```

因此消息可能被重复处理。例如业务已执行成功，但 worker 在调用 `ack()` 前崩溃，任务会在 lease 超时后再次被投递。

业务必须以幂等方式处理重复消息。Taskflow 提供 `dedup_key` 与去重扩展来减少重复提交，但不能替代业务结果的幂等性设计。去重仅发生在提交阶段，不会阻止因 lease 回收导致的重复投递。

---

## 4. 用户角色与典型场景

### 4.1 角色

| 角色 | 诉求 |
|---|---|
| 任务生产者 | 创建任务、指定队列、最大尝试次数及去重键 |
| 任务消费者 | 高并发异步处理任务，并明确 ACK / Retry / Reject |
| 运维或开发人员 | 观察堆积、pending、重试、DLQ；重放失败消息 |
| Backend 实现者 | 在统一契约下实现 Redis、SQLite、Kafka 等适配器 |

### 4.2 典型场景：URL Frontier

```text
初始 URL
  -> 去重并提交 crawl.fetch
  -> worker 消费并调用外部采集服务
  -> 解析结果
  -> 发现新 URL
  -> 新 URL 去重并提交 crawl.fetch
  -> 成功 ACK / 临时异常延迟重试 / 不可恢复异常进入 DLQ
```

### 4.3 典型场景：异步数据处理

```text
业务服务提交 data.transform 消息
  -> 多 worker 并发处理
  -> 下游暂时不可用时指数退避
  -> 超过次数后进入 DLQ
  -> 修复后由管理命令重放 DLQ 消息
```

### 4.4 典型场景：本地脚本

开发者使用 SQLite backend，在无 Redis、无 Kafka 环境下运行和测试完整任务流。

---

## 5. 功能需求

### 5.1 任务提交

系统必须支持：

- 向指定队列提交单条消息；
- 批量提交消息；
- 自定义 payload、metadata；
- 指定最大尝试次数；
- 指定去重键和去重作用域；
- 返回提交结果，包括消息 ID、是否新建、是否因去重被抑制。

期望 API：

```python
result = await broker.submit(
    queue="crawl.fetch",
    payload={"url": "https://example.com/page/1"},
    metadata={"trace_id": "trace-001", "schema_version": "1"},
    dedup_key="example.com:https://example.com/page/1",
    dedup_scope="crawl:batch-2026-01",
    max_attempts=5,
)

assert result.message_id
assert result.accepted
```

批量提交：

```python
results = await broker.submit_many(messages)
```

### 5.2 异步消费

系统必须支持：

- 每个队列由一个或多个消费者并发消费；
- 可配置 `consumer_id`；
- 异步迭代消费模型；
- 可配置拉取超时、预取数量、并发度、lease 时长；
- 优雅关闭：停止领取新消息，并在配置策略下等待或释放处理中消息；
- 不同消费者实例可以组成逻辑消费者组。

期望 API：

```python
async with broker.consumer(
    "crawl.fetch",
    consumer_id="worker-a",
    options=ConsumerOptions(concurrency=20, lease_seconds=300),
) as consumer:
    async for delivery in consumer:
        await handle(delivery)
```

v0.2 应提供高层 worker helper：

```python
worker = TaskWorker(broker, queue="crawl.fetch", concurrency=20)

@worker.handler("fetch_page")
async def fetch_page(delivery: TaskDelivery) -> None:
    ...

await worker.run()
```

高层 helper 必须是对底层 `Delivery` API 的封装，不能隐藏 ACK、Retry、Reject 的真实语义。

### 5.3 显式确认与处理结果

消费者必须显式决定消息结果：

```python
await delivery.ack()
await delivery.retry(reason="upstream timeout")
await delivery.reject(reason="invalid payload")
await delivery.extend_lease(seconds=300)
```

要求：

- 同一个 Delivery 的终结操作必须幂等；
- 已终结 Delivery 的重复 ACK/Retry/Reject 不得造成重复投递；
- 无效、过期或已被回收的 lease 必须返回明确异常或确定性结果；
- v0.1 支持显式 `extend_lease()`；自动续租 `lease_heartbeat()` 进入 v0.2。

### 5.4 重试

Taskflow 必须提供：

- 最大尝试次数；
- v0.1 支持显式立即重试与重试原因记录；
- v0.2 支持固定延迟重试、指数退避、最大退避上限、可选随机抖动，以及显式指定下一次可投递时间；
- 业务方根据异常类型或业务结果决定是否重试。

v0.2 策略示例：

```python
RetryPolicy.exponential(
    max_attempts=5,
    initial_delay=1,
    max_delay=300,
    jitter=True,
)
```

尝试次数达到上限后，消息必须进入 DLQ，不能无限重试。

### 5.5 延迟投递（v0.2）

延迟投递和延迟重试不属于 v0.1；v0.1 不暴露 `delay`、`available_at` 或带延迟的 `retry()` 参数，避免在没有可靠延迟调度器时提供不完整语义。

v0.2 将支持：

```python
await broker.submit(..., delay=timedelta(minutes=5))
```

延迟到期后，延迟调度器必须可靠、幂等地将消息移动到可消费队列。调度器的内嵌或独立部署方式在 v0.2 设计时确定。

### 5.6 租约回收

系统必须支持后台或按需的 reclaim 机制：

1. 发现超过 `lease_until` 且未确认的 Delivery；
2. 判定该 Delivery 不再由原消费者可靠持有；
3. 增加尝试计数；
4. 未达最大次数时重新投递；
5. 达到最大次数时写入 DLQ；
6. 保留回收原因和原消费信息用于审计。

### 5.7 死信队列（DLQ）

每个逻辑队列必须存在对应 DLQ。DLQ 消息应保留：

- 原始 Message；
- 最后一次 Delivery 信息；
- 总尝试次数；
- 失败时间；
- 失败原因；
- 异常类型与可选 traceback；
- 进入 DLQ 的来源（`reject`、重试超限、租约超限等）。

管理能力：

```python
await broker.admin.list_dead_letters("crawl.fetch")
await broker.admin.replay_dead_letter("crawl.fetch", message_id)
await broker.admin.delete_dead_letter("crawl.fetch", message_id)
```

重放必须可以选择：

- 保留或重置 attempt；
- 重用或移除原 dedup key；
- 重新指定目标队列；
- 覆盖 payload 或 metadata（可选）。

### 5.8 过期队列（Expired Queue，EQ）

每个逻辑队列必须存在对应的 Expired Queue（EQ），用于保存超过 `expires_at` 而尚未完成的消息。EQ 与 DLQ 分开维护：过期是时间条件，不代表处理失败。

消息到期后必须停止投递并转入 EQ，至少保留原始 Message、过期时间、过期时的投递状态和最后一次 Delivery（如有）。管理接口应支持查询、删除和显式重新投递过期消息；重新投递时必须指定新的 `expires_at` 或明确移除过期时间。

### 5.9 去重

系统必须支持可选去重：

```python
await broker.submit(
    ...,
    dedup_scope="crawl:batch-1",
    dedup_key="canonical-url-hash",
    dedup_ttl=timedelta(days=7),
)
```

去重要求：

- 去重作用域由 `dedup_scope` 显式决定；
- `dedup_ttl` 决定该键的保留时长；未设置时使用 broker 配置的默认值（默认策略待确认）；
- 精确去重的 `SubmissionStore` 中，“声明去重键 + 写入消息”必须原子执行；
- 如果因已存在去重键而未提交，返回结果必须可辨识；
- 支持不启用去重；
- 不把 URL 规范化、业务 key 生成等领域逻辑内置到 Taskflow。

Taskflow 不保证去重等同于业务 exactly-once。业务仍需处理消费重复和副作用幂等。

### 5.10 队列管理与观测

MVP 需要最小管理能力：

```python
stats = await broker.inspect("crawl.fetch")
```

统计至少包括：

- ready 数量；
- pending / leased 数量；
- DLQ 数量；
- EQ 数量；
- v0.2 delayed 数量；
- 最早待处理消息时间；
- 消费者数量（backend 支持时）；
- 已处理、重试、回收、死信等累计计数（backend 支持时）。

必须提供结构化日志 hook 和指标 hook，便于接入 Prometheus、OpenTelemetry 或项目既有日志系统。

建议指标：

```text
taskflow_messages_submitted_total
taskflow_messages_acked_total
taskflow_messages_retried_total
taskflow_messages_reclaimed_total
taskflow_messages_dead_lettered_total
taskflow_delivery_duration_seconds
taskflow_queue_ready_messages
taskflow_queue_leased_messages
taskflow_queue_dead_letter_messages
taskflow_queue_expired_messages
taskflow_queue_delayed_messages  # v0.2
```

### 5.11 序列化与版本演进

- 默认 JSON serializer；
- 支持注册自定义 serializer；
- serializer 必须有稳定名称和版本；
- 消息必须记录 serializer 标识；
- serializer 失败时，不得无限重试，应允许按策略进入 DLQ；
- 不默认支持不安全的 Python pickle。

---

## 6. 核心接口草案

以下接口为设计方向，不是最终代码承诺。

```python
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Protocol
from typing_extensions import Self


class TaskBroker(Protocol):
    capabilities: BackendCapabilities

    async def start(self) -> None: ...
    async def close(self) -> None: ...

    async def submit(
        self,
        *,
        queue: str,
        payload: Any,
        metadata: dict[str, Any] | None = None,
        dedup_key: str | None = None,
        dedup_scope: str | None = None,
        dedup_ttl: timedelta | None = None,
        expires_at: datetime | None = None,
        max_attempts: int | None = None,
    ) -> SubmitResult: ...

    async def submit_many(self, messages: list[SubmitRequest]) -> list[SubmitResult]: ...

    def consumer(
        self,
        queue: str,
        *,
        consumer_id: str | None = None,
        options: ConsumerOptions | None = None,
    ) -> TaskConsumer: ...

    @property
    def admin(self) -> TaskAdmin: ...

    async def inspect(self, queue: str) -> QueueStats: ...


class TaskConsumer(Protocol):
    async def start(self) -> None: ...
    async def close(self) -> None: ...
    def __aiter__(self) -> Self: ...
    async def __anext__(self) -> TaskDelivery: ...


class TaskDelivery(Protocol):
    message: TaskMessage
    delivery_id: str
    consumer_id: str
    attempt: int
    claimed_at: datetime
    lease_until: datetime

    async def ack(self) -> FinishOutcome: ...
    async def retry(self, *, reason: str | None = None) -> FinishOutcome: ...  # v0.1：立即重投
    async def reject(self, *, reason: str, error: BaseException | None = None) -> FinishOutcome: ...
    async def extend_lease(self, *, seconds: float | None = None) -> datetime: ...
```

---

## 7. Backend 抽象与能力声明

### 7.1 Backend 要求

每个 backend 必须实现统一的核心语义：

- 提交；
- 消费；
- 显式 ACK；
- 重试；
- 死信；
- lease 或同等的故障恢复机制；
- 队列基础统计；
- 幂等的 Delivery 终结操作。

### 7.2 能力声明

由于 Redis、Kafka、SQLite 的原生模型不同，Taskflow 必须显式暴露 backend 能力，而不是假定所有 backend 的性能与语义完全一致。

```python
@dataclass(frozen=True)
class BackendCapabilities:
    delayed_delivery: bool
    dead_letter: bool
    deduplication: bool
    lease_reclaim: bool
    batch_submit: bool
    transactional_submit: bool
    priority: bool
    partition_ordering: bool
    distributed_consumers: bool
    high_throughput: bool
```

当调用未支持功能时，必须抛出明确的 `UnsupportedCapabilityError`，不得静默退化。

---

## 8. Backend 路线图

### 8.1 Redis Backend（MVP，必须实现）

#### 技术选型

- 客户端：`redis.asyncio`；
- 消息主队列：Redis Streams；
- 消费模型：Consumer Groups；
- ACK：`XACK`；
- pending：Consumer Group Pending Entries List（PEL）；reclaim：lease ZSet + 原子 Lua 状态迁移；
- DLQ、Expired Queue：独立 Redis Stream；
- v0.2 延迟任务：Sorted Set + 原子转移脚本；
- 消息状态：Hash；lease 与 expires_at 索引：Sorted Set；
- 提交准入：`SubmissionStore`，精确 String Dedup 使用 Redis String；
- 原子提交：Lua Script 或 Redis Function；
- 元数据：Stream fields 保存不可变 Message envelope，Hash 保存可变投递状态。

#### 推荐 Key 规范

假设 namespace 为 `taskflow`，队列为 `crawl.fetch`：

```text
taskflow:queue:{crawl.fetch}:stream
taskflow:queue:{crawl.fetch}:state
taskflow:queue:{crawl.fetch}:leases
taskflow:queue:{crawl.fetch}:expiry
taskflow:queue:{crawl.fetch}:dlq
taskflow:queue:{crawl.fetch}:eq
taskflow:queue:{crawl.fetch}:stats
taskflow:dedup:{scope-hash}:key-hash
```

实际 key 需统一编码，避免特殊字符、Redis Cluster hash slot 问题和命名冲突。

#### Redis Backend 关键约束

- 同一 consumer group 内，消息由一个 consumer 处理；
- ACK 必须在业务成功后进行；
- 每次 Delivery 必须使用唯一 lease token；ACK、Retry、Reject、extend lease 必须校验该 token，避免迟到消费者终结已回收消息；
- reclaimer 必须避免与正常 ACK 产生不一致；
- DLQ / EQ 写入与原消息终结必须通过 Lua 保证原子性；
- v0.2 的延迟任务转 READY 操作必须幂等；
- 需要处理 Redis 连接中断、超时及重连；
- 需要提供 Stream trimming/归档策略，防止无限增长；
- 需要明确 Redis 持久化（AOF/RDB）是部署责任还是产品建议，并形成文档。

### 8.2 SQLite Backend（MVP，必须实现）

用途：本地开发、单机任务脚本、测试与 CI。

要求：

- 使用 `aiosqlite`；
- 消息、租约、重试、DLQ、去重均持久化为表；
- 使用事务保证领取、ACK、Retry、Reject 的一致性；
- 支持单进程或有限多进程场景；
- 必须明确不适合作为高吞吐分布式生产消息系统；
- 与 Redis backend 共用一套 conformance tests。

建议表：

```text
messages
message_attempts
dead_letters
dedup_records
queue_metadata
```

### 8.3 Kafka Backend（后续版本）

Kafka backend 需要单独设计，不能简单套用 Redis Streams 细节。

需要先解决：

- queue 和 topic 的映射；
- partition key 与同 key 顺序；
- offset commit 与单条 ACK 的映射；
- consumer rebalance 时长任务的处理；
- retry topic、延迟策略与 DLQ topic；
- 去重状态的存储位置；
- 消息保留与重放策略。

Kafka backend 在未完成独立设计与压测前，不进入 MVP。

---

## 9. 任务状态模型

Taskflow 只维护消息投递状态，不维护业务领域状态。

建议消息状态：

```text
READY
LEASED
ACKED
DEAD_LETTERED
EXPIRED
DELAYED（v0.2）
```

状态流转：

```text
submit -> READY
READY -> claim -> LEASED
LEASED -> ack -> ACKED
LEASED -> retry -> READY
LEASED -> reject -> DEAD_LETTERED
LEASED -> lease timeout -> READY 或 DEAD_LETTERED
READY / LEASED -> expires -> EXPIRED -> Expired Queue

# v0.2
submit(delay) -> DELAYED -> due -> READY
LEASED -> retry(delay) -> DELAYED
DELAYED -> expires -> EXPIRED -> Expired Queue
```

说明：

- `ACKED` 状态可不长期保留完整消息，由 backend 按 retention 策略处理；
- backend 的内部状态不需要与上述名称逐字一致，但对外行为必须符合该模型；
- 业务状态如“网页抓取成功”“结果解析失败”属于业务系统，不属于 Taskflow。

---

## 10. 重试与错误分类

### 10.1 重试策略

Taskflow 应支持默认策略与消息级覆盖：

```python
RetryPolicy.fixed(delay=30, max_attempts=3)
RetryPolicy.exponential(
    initial_delay=1,
    factor=2,
    max_delay=300,
    max_attempts=5,
    jitter=True,
)
```

### 10.2 错误分类责任

Taskflow 提供策略机制，但不理解业务错误。业务方应明确区分：

| 类型 | 推荐动作 |
|---|---|
| 短暂网络错误 | retry |
| 外部服务限流 | 延迟 retry |
| 参数错误 | reject |
| 数据格式不合法 | reject |
| 临时资源不足 | retry |
| 不可恢复业务错误 | reject |
| worker 崩溃 | lease reclaim |

不得将所有 `Exception` 无条件无限重试。

### 10.3 异常处理 helper

高层 Worker 可提供可配置默认行为：

```python
WorkerOptions(
    on_unhandled_exception="retry",
    retry_policy=RetryPolicy.exponential(...),
)
```

但底层 API 必须允许业务明确控制结果，且默认行为应记录结构化日志和错误信息。

---

## 11. 可扩展性需求

### 11.1 Serializer

允许注入：

```python
class Serializer(Protocol):
    name: str
    version: str
    def dumps(self, value: Any) -> bytes: ...
    def loads(self, payload: bytes) -> Any: ...
```

默认 JSON；后续可支持 `orjson`、MessagePack、Protobuf，但序列化格式必须显式标识。

### 11.2 SubmissionStore 与提交准入

去重是提交时的一种准入策略，而不是独立于提交之外的前置检查。公共一等扩展点为 `SubmissionStore`：它必须在 backend 的原子边界内完成准入判断、消息入队、初始状态写入及 `expires_at` 索引写入。

```python
class SubmissionStore(Protocol):
    capabilities: SubmissionCapabilities

    async def submit(self, submission: PreparedSubmission) -> SubmitResult: ...
    async def submit_many(
        self,
        submissions: list[PreparedSubmission],
    ) -> list[SubmitResult]: ...
```

内置实现与语义：

- `RedisSubmissionStore`：无去重提交；
- `RedisStringDedupSubmissionStore`：精确去重、per-key TTL、保存首次消息 ID；Redis Lua 中原子执行 `SET NX [PX] + XADD + HSET + ZADD`；
- `SQLiteSubmissionStore`：在同一 SQLite transaction 中完成精确去重与消息写入；
- `RedisBloomDedupSubmissionStore`：v0.2 optional 概率性实现，必须返回 `PROBABLE_DUPLICATE`，不得伪装为精确重复。

SubmissionStore 可通过具名 submission profile 按 queue 配置。详细接口、TTL 语义及 Bloom 限制见[提交与去重架构](submission-and-dedup.md)。

### 11.3 Middleware / Hooks

提供异步 hooks，典型节点：

```text
before_submit
after_submit
before_receive
after_claim
before_ack
after_ack
after_retry
after_reject
on_reclaim
on_dead_letter
on_error
```

用途包括：

- trace ID 注入；
- 审计；
- 指标；
- 日志；
- payload 校验；
- 多租户/鉴权扩展。

Middleware 不得改变核心 ACK 和 lease 的一致性语义。

### 11.4 时间与 ID

- 时钟必须可注入，便于测试 delayed/retry/lease；
- ID 生成器必须可注入，便于测试和业务接入；
- 所有持久化时间统一使用 UTC；
- 对外展示由调用方自行转换时区。

---

## 12. 非功能需求

### 12.1 性能

Redis backend 的具体性能目标需在实现后以基准测试确定。MVP 至少应保证：

- 支持多 async consumer 并发消费；
- 批量提交避免逐条网络往返；
- 不在事件循环中执行阻塞 I/O；
- Redis 连接池可配置；
- 消费空闲时使用阻塞读取或合理等待，避免忙轮询。

### 12.2 可靠性

- 所有状态迁移应保持幂等；
- 重连后 consumer 可恢复；
- worker 异常退出不应永久丢失未 ACK 消息；
- maintenance loop 重复执行不应导致 lease reclaim、EQ 转移或 v0.2 延迟任务重复入队；
- DLQ 重放过程应可审计；
- Redis backend 的原子关键路径应通过 Lua/事务机制处理。

### 12.3 安全性

- 默认禁止 pickle；
- 日志不得默认泄露完整 payload、密钥、Cookie、Token；
- 提供 payload 脱敏 hook；
- Redis/Kafka 连接安全交给 driver 配置，但文档应说明 TLS、认证和 ACL；
- queue、consumer、dedup scope 名称需要校验，防止 key 注入或非法字符。

### 12.4 可测试性

- Core 层与 backend 层可独立单测；
- 所有 backend 必须通过同一套 conformance tests；
- 提供 SQLite backend 作为本地集成测试环境；
- Redis backend 使用临时 Redis 或容器化测试；
- 时间、随机数、ID 与网络 client 均可注入或替换。

---

## 13. 测试与验收标准

### 13.1 Backend Conformance Tests

每个 backend 必须验证至少以下行为：

1. 单消息提交和消费；
2. 批量提交；
3. 同一消息不会被同一 consumer group 的两个消费者同时正常领取；
4. ACK 后不再可消费；
5. v0.1 Retry 后消息可立即再次消费；
6. Reject 后消息进入 DLQ；
7. 超过最大重试次数进入 DLQ；
8. lease 超时后消息可被回收；
9. lease 回收达到上限后进入 DLQ；
10. 终结操作重复调用保持幂等，旧 lease token 的终结操作必须失败；
11. 精确去重开启时，并发重复提交仅接受一条；
12. 精确去重 TTL 到期后可重新提交；
13. 已过期消息不会交给业务消费者，并最终进入 EQ；
14. 消息 payload 和 metadata 可正确序列化/反序列化；
15. 优雅关闭后不泄露连接和后台 task；
16. 队列统计在允许的最终一致延迟内正确反映状态；
17. DLQ 与 EQ 均可查询、删除和重放。

### 13.2 MVP 验收场景

- Redis backend 下启动两个 worker，1000 条消息均被处理；
- 处理过程中强制终止一个 worker，其未 ACK 消息最终被另一个 worker 回收处理；
- 临时失败任务立即重试，超过阈值后进入 DLQ；
- 精确 String Dedup 下，同一个 dedup key 并发提交，只接受一条；
- 已过期消息不会被业务消费并进入 EQ；
- v0.2：延迟消息不会在到期前被消费；
- SQLite backend 运行同一业务测试，不需要 Redis；
- 所有 public API 具备类型标注、docstring 和异常说明。

---

## 14. 包结构建议

```text
taskflow/
├── pyproject.toml
├── README.md
├── docs/
│   ├── concepts.md
│   ├── redis-backend.md
│   ├── sqlite-backend.md
│   ├── reliability.md
│   ├── submission-and-dedup.md
│   ├── redis-lifecycle.md
│   └── migration.md
├── src/
│   └── taskflow/
│       ├── __init__.py
│       ├── types.py
│       ├── protocols.py
│       ├── errors.py
│       ├── capabilities.py
│       ├── policies.py
│       ├── serialization.py
│       ├── middleware.py
│       ├── submission/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── redis.py
│       │   └── sqlite.py
│       ├── broker/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── redis.py
│       │   └── sqlite.py
│       ├── admin/
│       │   ├── __init__.py
│       │   └── base.py
│       ├── observability/
│       │   ├── __init__.py
│       │   ├── logging.py
│       │   └── metrics.py
│       └── integrations/
│           └── taskiq.py             # 后续可选
└── tests/
    ├── conformance/
    ├── unit/
    ├── integration/
    └── benchmark/
```

依赖建议：

```toml
[project.optional-dependencies]
redis = ["redis>=5"]
sqlite = ["aiosqlite>=0.20"]
kafka = ["aiokafka>=0.10"]  # 后续
metrics = ["prometheus-client>=0.20"]
otel = ["opentelemetry-api>=1.0"]
```

核心 package 不应强依赖 Redis、SQLite、Kafka、Prometheus 或特定 Web 框架。

---

## 15. 版本规划

### v0.1.0：可用 MVP

- 核心抽象和类型；
- JSON serializer；
- Redis Streams backend；
- SQLite backend；
- 显式 ACK / Retry / Reject；
- lease、reclaim、最大尝试次数；
- DLQ、Expired Queue 与管理操作；
- 基础 dedup；
- backend conformance tests；
- 基础日志与指标 hooks；
- 完整 README 和 Redis/SQLite 使用文档。

### v0.2.0：工程化增强

- 高层 `TaskWorker`；
- 延迟投递、延迟重试与可靠延迟调度器；
- 中间件体系；
- 更完整的队列管理接口；
- 优雅关闭和 lease heartbeat；
- 更丰富的 retry policy；
- OpenTelemetry 集成；
- Redis Cluster 与更严格的 key slot 策略；
- CLI（inspect、DLQ list/replay/purge）。

### v0.3.0：扩展能力

- 优先级队列；
- 限流与并发配额 hook；
- 多队列消费；
- 更强的批处理与吞吐优化；
- Taskiq integration adapter。

### v1.0.0：稳定契约

- 核心 API 稳定；
- Redis 和 SQLite backend 语义与测试稳定；
- 文档、迁移策略和兼容性承诺明确；
- 生产部署建议、故障处理手册和压测基线完善。

### Kafka Backend

Kafka backend 仅在完成独立 RFC、语义定义、故障演练和性能验证后进入正式版本，不作为 v0.1 的交付目标。

---

## 16. 关键设计决策记录

1. **不以 Taskiq 为核心依赖**：Taskiq 的函数调用模型与 Taskflow 的消息生命周期模型不同；后者必须可独立控制 ACK、lease、reclaim、retry 和 DLQ。
2. **默认 at-least-once**：分布式环境无法仅通过消息框架可靠提供端到端 exactly-once；业务必须保证副作用幂等。
3. **Redis Streams 优先于 Redis List**：Consumer Group、pending、ACK、reclaim 语义更适合可靠消费。
4. **SQLite 是开发/测试 backend，不伪装为高吞吐分布式 MQ**。
5. **Kafka 后置**：其 partition/offset 模型与单消息 lease/ACK 存在显著差异，需要单独设计。
6. **任务消息与业务状态分离**：Taskflow 管消息投递，不维护爬取、解析、订单、支付等业务状态机。
7. **去重键由业务提供**：Taskflow 提供机制，不理解 URL canonicalization 或业务主键。
8. **能力显式声明**：backend 不支持的能力必须报错，不允许隐藏语义差异。

---

## 17. 已确认的立项决策与待确认事项

### 已确认

1. 最低 Python 版本为 **3.10**。核心类型系统可使用 `list[str]`、`X | None`、`Protocol`、`TypedDict`、`ParamSpec` 和 `from __future__ import annotations`；仅 Python 3.11+ 的注解能力如 `typing.Self` 必须避免使用或由 `typing_extensions` 兼容。
2. `attempt` 是总 Delivery 次数，首次领取为 1。
3. 消息到期后转入每个逻辑队列对应的 **Expired Queue（EQ）**，不进入 DLQ。
4. v0.1 不提供延迟投递、延迟重试或延迟调度器；它们进入 v0.2。
5. v0.1 RC 提供 `TaskWorker` / `broker.worker()` 作为稳定的高层执行 API；它仅封装显式 Delivery 的 ACK/Retry 语义，不包含 v0.2 的延迟重试与自动 heartbeat。
6. `task_type` 不作为 v0.1 的一等字段或 `submit()` 参数；队列承担消息路由职责。需要分类、schema 版本或业务标签时，由业务方写入 `metadata`。
7. `headers` 更名为 `metadata`；其值必须 JSON-compatible。
8. `dedup_namespace` 更名为 `dedup_scope`，以表达去重键的隔离范围。
9. 去重属于消息提交准入策略。`SubmissionStore` 是一等扩展点，负责在 backend 原子边界内完成准入判断与消息持久化；精确 String Dedup 是 v0.1 标准实现，Bloom Dedup 是 v0.2 的概率性 optional 实现。
10. Redis v0.1 使用 Stream、状态 Hash、lease / expiry ZSet、DLQ / EQ Stream 及 Lua Script 实现状态迁移；每次 Delivery 必须有唯一 lease token。

### 仍待确认

1. 默认 `max_attempts`、默认 lease 时长；
2. 未传 `dedup_ttl` 时 broker 的默认保留时长；
3. DLQ 和 EQ 重放默认是否重置 attempt（建议重置，同时保留原始 attempt 审计字段）；
4. Redis Stream 已 ACK 消息的 retention、trim 和归档策略；
5. 是否需要多租户 scope 与 ACL 约束；
6. 任务 payload 最大大小及大 payload 的外置存储策略；
7. 是否需要 FIFO / 同 key 有序性，以及该要求如何影响未来 Kafka backend；
8. 是否需要为任务定义 schema/version 校验。

---

## 18. 成功标准

Taskflow 成功的标志不是实现了更多 backend，而是业务方能够在不理解 Redis Streams、消费组、Lua、SQLite 锁或 Kafka offset 的情况下，可靠地完成：

```python
await broker.submit(...)

async for delivery in broker.consumer("queue"):
    try:
        await do_business_work(delivery.message.payload)
        await delivery.ack()
    except TemporaryError as exc:
        await delivery.retry(reason=str(exc))
    except Exception as exc:
        await delivery.reject(reason=str(exc), error=exc)
```

同时，系统能在 worker 崩溃、重复投递、外部服务临时故障、消息积压和处理失败时，提供可预测、可审计、可重放的行为。
