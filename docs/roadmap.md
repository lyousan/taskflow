# Taskqx 开发路线图

本文是 Taskqx 从 v0.1 MVP 发展到可直接用于一般生产项目的开发计划。它同时是版本规划、架构约束、任务拆分和验收标准，开发实现应以本文和现有设计文档为共同基线。

相关设计基线：

- [PRD](PRD.md)：产品范围、可靠性语义和非目标；
- [概念模型](concepts.md)：消息、Delivery、lease、DLQ、EQ；
- [Redis 生命周期](redis-lifecycle.md)：Redis 状态转换和原子边界；
- [SQLite backend](sqlite-backend.md)：SQLite 的适用范围和事务语义；
- [SubmissionStore 与去重](submission-and-dedup.md)：提交扩展点和 dedup 语义；
- [运维说明](operations.md)：当前 v0.1 的部署与排障边界。

---

## 1. 总体目标

Taskqx 最终应同时满足以下目标：

1. **开箱即用**：本地使用 SQLite，无需 Redis 即可提交和消费任务；
2. **人体工程学**：普通用户只需理解 `submit()`、`worker()`、handler 和重试策略；
3. **可靠性明确**：at-least-once、显式副作用确认、lease、retry、DLQ、EQ 和 dedup 语义不含糊；
4. **后端可替换**：SQLite 适用于本地/测试/单机，Redis 适用于多进程/多实例；
5. **扩展点稳定**：SubmissionStore、Serializer、Middleware、Metrics 和 Worker 策略均有明确 Protocol；
6. **可运维**：能够查看队列、消息、lease、DLQ/EQ、健康状态和关键指标；
7. **可发布**：具备类型、测试、lint、构建、迁移和版本兼容文档。

Taskqx **不承诺 exactly-once 业务处理**。所有版本都必须保留以下核心原则：

- 消息投递最多一次的保证不是目标，默认是 at-least-once；
- ACK 只能发生在业务副作用成功之后；
- handler 必须幂等，或使用业务去重；
- dedup 只约束提交准入，不等于业务处理 exactly-once；
- Redis/SQLite 的差异必须通过 capabilities 和文档显式暴露。

---

## 2. 版本总览

| 版本 | 主题 | 主要结果 |
|---|---|---|
| v0.2 | Worker 策略与任务执行增强 | 在 v0.1 RC Worker 基础上支持延迟重试、heartbeat 与更丰富策略 |
| v0.3 | 配置、扩展点与可观测性 | 按 queue 配置策略，提供稳定的 metrics/events 和 serializer 边界 |
| v0.4 | 性能、管理能力与类型化 | 批量提交、类型化任务、管理 API/CLI、replay 策略完整 |
| v0.5 | 生产化与兼容性 | 压测、故障演练、迁移、健康检查、发布质量和稳定 API |
| v0.6 | 交互式运维控制台 | Textual TUI、交互式 shell、队列/消息浏览与受保护管理操作 |
| v0.7 | 可诊断性、消息生命周期与调度体验 | 完整 Worker 异常日志、消息 clone/提交 API、独立 scheduler、ACK tombstone 与 Redis keyspace 迁移、持续 TUI 优化 |

版本不是简单的时间节点。每个版本只有在“功能、测试、文档、兼容性和验收”全部完成后才允许发布。

---

# 3. v0.2：高层 Worker 与任务执行体验

## 3.1 版本目标

v0.2 的目标是把 v0.1 的可靠性内核包装成可直接用于应用开发的任务 Worker，同时增加延迟重试。

用户应能够这样使用：

```python
from taskqx import SQLiteBroker
from taskqx.retry import ExponentialBackoff, RetryPolicy


async def handle_email(message):
    await send_email(message.payload)


async with SQLiteBroker("tasks.db") as broker:
    await broker.submit("emails", {"to": "user@example.com"})
    async with broker.worker(
        "emails",
        handle_email,
        concurrency=10,
        retry_policy=RetryPolicy(
            max_attempts=5,
            backoff=ExponentialBackoff(initial=1, maximum=60),
        ),
    ) as worker:
        await worker.run()
```

## 3.2 功能范围

### A. Worker API

新增高层 API：

```python
worker = broker.worker(
    queue: str,
    handler: Callable[[TaskMessage], Awaitable[None]],
    *,
    concurrency: int = 1,
    consumer_id: str | None = None,
    options: ConsumerOptions | None = None,
    retry_policy: RetryPolicy | None = None,
)
```

Worker 必须负责：

- 按 `concurrency` 限制同时处理的消息数；
- 自动 claim；
- handler 正常返回后 ACK；
- handler 异常后按策略 retry 或 reject；
- 达到最大尝试次数后进入 DLQ；
- 进程停止时停止领取新消息并等待当前任务；
- 取消任务时不伪造 ACK；
- 复用现有 lease 和 stale delivery 防护；
- 使用 middleware 发出与低层 API 一致的事件。

Worker 不应改变底层 Delivery 语义。高级 API 只是对底层 `consumer()` 和 `Delivery` 的安全封装。

### B. RetryPolicy

新增策略对象：

```python
@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    backoff: Backoff = ImmediateBackoff()
    retry_on: tuple[type[BaseException], ...] = (Exception,)
    reject_on: tuple[type[BaseException], ...] = ()
```

至少实现：

- `ImmediateBackoff`；
- `FixedBackoff`；
- `ExponentialBackoff`；
- 最大延迟限制；
- 可选 jitter；
- 明确 attempt 从 1 开始还是从 0 开始，并与现有状态保持一致。

### C. 延迟重试

延迟重试必须满足：

- retry 请求和 retry 状态迁移保持原子性；
- 消息在 delay 时间内不会被普通 claim；
- delay 到期后重新进入 READY；
- Redis 使用 server time 或等价的服务端时间语义；
- SQLite 使用同一数据库时间/事务语义；
- 进程崩溃后延迟消息仍可恢复；
- retry delay 与 `expires_at` 同时存在时，不能在过期后重新投递；
- DLQ/EQ 统计保持正确。

### D. 高层异常

新增：

```python
class RetryableError(Exception):
    pass


class RejectMessage(Exception):
    pass
```

Worker 默认行为应可配置：

- `RetryableError`：retry；
- `RejectMessage`：reject；
- 其他异常：默认 retry，或由 `RetryPolicy` 决定；
- `CancelledError`：不执行普通 reject/retry，交给 shutdown 逻辑处理。

## 3.3 v0.2 不做的事情

- 不实现 exactly-once；
- 不引入分布式锁；
- 不改变现有 SQLite/Redis 状态模型；
- 不实现按 queue 的不同 SubmissionStore；
- 不实现管理 CLI；
- 不在 Worker 内自动执行不可逆业务副作用。

## 3.4 v0.2 验收标准

- 能以 `worker(..., concurrency=N)` 同时处理最多 N 条消息；
- handler 成功后自动 ACK；
- handler 崩溃、异常和取消的行为有测试；
- retry backoff 在 SQLite 和 Redis 上均有测试；
- 延迟 retry 不会提前被 claim；
- 进程在 retry 等待期间重启后消息仍可处理；
- graceful shutdown 不丢失已领取但未 ACK 的消息；
- README 有 10 分钟快速上手示例；
- 至少有一个 SQLite Worker 示例和一个 Redis Worker 示例。

---

# 4. v0.3：配置、扩展点与可观测性

## 4.1 版本目标

v0.3 解决“能运行”之外的架构一致性问题：不同队列可以有不同策略，自定义 Store/Serializer/Metrics 有稳定接口，系统状态可观察。

## 4.2 功能范围

### A. QueueConfig

新增统一配置对象：

```python
@dataclass(frozen=True)
class QueueConfig:
    max_attempts: int = 3
    lease: timedelta = timedelta(minutes=5)
    retry_policy: RetryPolicy | None = None
    default_dedup_ttl: timedelta | None = None
    max_payload_bytes: int | None = None
```

Broker 支持：

```python
broker = SQLiteBroker(
    "tasks.db",
    queues={
        "emails": QueueConfig(max_attempts=5),
        "webhooks": QueueConfig(max_attempts=10),
    },
)
```

配置优先级必须固定并写入文档：

```text
单次 submit/worker 参数 > queue 配置 > broker 默认值
```

### B. 按 queue 选择 SubmissionStore

实现：

```python
broker = RedisBroker(
    redis,
    submission_stores={
        "default": RedisSubmissionStore(redis),
        "exact": RedisStringDedupSubmissionStore(redis),
    },
    queue_submission_profiles={
        "emails": "exact",
        "audit-events": "default",
    },
)
```

要求：

- profile 名称必须经过校验；
- 未配置 queue 使用 default profile；
- `submit()` 根据 queue 选择 Store；
- `submit_many()` 不允许静默混合不兼容 profile；
- capability 能反映实际被选中的 Store；
- 自定义 Store 不得依赖 Broker 的隐藏回调；
- profile 配置错误应在启动时尽早失败。

### C. 严格命名规则

queue、namespace、profile 使用统一规则：

```text
[A-Za-z0-9][A-Za-z0-9._-]{0,127}
```

禁止：

- 空白和控制字符；
- `{}`、`:`、`*`、`[`、`]`；
- 前后空格；
- 超过最大长度；
- 只包含 `.` 或 `-` 的无意义名称。

SQLite 和 Redis 必须共享同一套公共校验函数和测试。

### D. 标准化事件和指标

新增事件 Protocol：

```python
class EventSink(Protocol):
    async def emit(self, event: TaskqxEvent) -> None: ...
```

新增指标 Protocol：

```python
class MetricsSink(Protocol):
    async def increment(self, name: str, value: int = 1, **labels: str) -> None: ...
    async def observe(self, name: str, value: float, **labels: str) -> None: ...

class GaugeMetricsSink(MetricsSink, Protocol):
    async def gauge(self, name: str, value: float, **labels: str) -> None: ...
```

标准事件字段：

- `event_name`；
- `timestamp`；
- `queue`；
- `message_id`；
- `delivery_id`；
- `consumer_id`；
- `attempt`；
- `status`；
- `reason`；
- `error_type`；
- `backend`。

标准指标至少包括：

- `submitted_total`；
- `duplicate_total`；
- `claimed_total`；
- `acked_total`；
- `retried_total`；
- `reclaimed_total`；
- `dead_lettered_total`；
- `expired_total`；
- `lease_lost_total`；
- processing duration；
- queue ready/leased gauge。

指标 label 必须控制 cardinality，不能默认使用完整 dedup key 或 payload。

### E. Serializer 边界

v0.3 至少完成以下两种方案之一：

1. 实现 `SerializerRegistry`，按 `serializer_name + serializer_version` 选择解码器；
2. 明确声明一个 Broker 实例只能使用一个 serializer，并在文档中定义升级迁移步骤。

推荐实现 registry：

```python
registry.register("json", "1", JsonSerializer())
registry.register("msgpack", "1", MsgpackSerializer())
```

找不到 serializer 时必须返回明确的 `SerializerUnavailableError`，不能表现为普通 JSON 解码异常。

## 4.3 v0.3 验收标准

- 两个 queue 可以使用不同 SubmissionStore；
- 自定义 Store 能只依赖 `PreparedSubmission` 工作；
- 所有 queue/namespace 特殊字符测试通过；
- Worker、submit、claim、retry、DLQ、EQ 均有标准事件；
- 可注入自定义 MetricsSink；
- serializer 不匹配有明确错误和测试；
- 配置优先级、启动校验和 capability 文档完整。

---

# 5. v0.4：性能、类型化与管理能力

## 5.1 版本目标

v0.4 让 Taskqx 更适合中等规模应用：减少批量操作往返，提供类型化 payload，提供程序化和 CLI 管理能力，完善 DLQ/EQ replay，并在继续扩展前完成核心实现的瘦身与去腐化。

## 5.2 功能范围

### A. 类型化任务

支持 dataclass、TypedDict 或 Pydantic model，核心 API 示例：

```python
@dataclass
class ResizeImage:
    image_id: str
    width: int
    height: int


await broker.submit("image.resize", ResizeImage("img-1", 800, 600))

worker = broker.worker(
    "image.resize",
    handle_resize,
    payload_type=ResizeImage,
)
```

要求：

- 类型化只影响 payload 编码/解码，不改变消息生命周期；
- 解码失败进入明确的 poison-message 处理路径；
- schema/version 与 serializer version 分离；
- 原始 envelope 仍可在 DLQ/EQ 中保留；
- 不允许通过类型转换掩盖数据损坏。

### B. SQLite 批量提交

实现 `SQLiteSubmissionStore.submit_many()` 的单事务版本：

```text
BEGIN IMMEDIATE
  批量 dedup 清理
  批量 dedup 检查与写入
  批量 messages 写入
  批量 expires 索引写入
  批量 submitted counter
COMMIT
```

明确两种模式：

- `atomic=True`：任一条失败，整批回滚；
- `atomic=False`：逐条返回结果。

`SubmissionCapabilities.batch_submit` 必须准确表示 Store 支持的模式。

### C. Redis 批量提交

优先实现单次网络往返的批量提交；如无法提供完整批量回滚，必须保留逐条结果：

```python
results = await broker.submit_many(
    "emails",
    messages,
    atomic=False,
)
```

Redis Lua 或 pipeline 实现必须保证每一条消息内部的：

```text
dedup 准入 + message state + stream entry + expiry index
```

仍然是原子操作。

### D. 完整 replay 语义

DLQ/EQ replay 增加：

```python
await broker.admin.replay_dead_letter(
    queue="emails",
    message_id=message_id,
    reset_attempt=True,
    target_queue="emails.repaired",
    dedup_mode="replace",  # keep / remove / replace
    dedup_scope="repair-batch-1",
    dedup_key="user:123",
    dedup_ttl=timedelta(days=1),
)
```

要求：

- replay 与 DLQ/EQ 记录删除在同一原子边界内；
- dedup record 的保留、删除、替换策略明确；
- 目标队列不存在或配置不兼容时不破坏原记录；
- replay 后 Stream、state、ready/expiry index 一致；
- 重复 replay 幂等或返回明确的 not found。

### E. Admin API 与 CLI

程序化 API：

```python
stats = await broker.inspect("emails")
message = await broker.inspect_message(message_id)
dead_letters = await broker.admin.list_dead_letters("emails")
await broker.admin.replay_dead_letter(...)
```

CLI 示例：

```bash
taskqx queue inspect emails
taskqx queue list-dead-letters emails
taskqx message inspect <message-id>
taskqx dlq replay emails <message-id>
taskqx health
```

CLI 必须：

- 默认只读；
- 删除/replay 等破坏性操作要求显式确认或 `--yes`；
- 支持 JSON 输出；
- 显示 backend、namespace 和 queue；
- 不打印 payload 中的敏感字段，除非用户显式要求。

### F. 核心代码瘦身与去腐化

v0.3 后 `SQLiteBroker` 与 `RedisBroker` 已同时承担连接生命周期、提交路由、状态迁移、维护、观测、管理入口以及（Redis）内联 Lua 脚本等职责。v0.4 在新增功能时必须同步拆解这些边界，禁止继续将新能力堆入单个 Broker 文件。

目标结构与职责：

- `broker/sqlite.py`、`broker/redis.py`：仅保留 backend 生命周期、依赖装配及公共入口；
- `submission/`：Store、profile 路由、批量提交与 dedup 准入；
- `broker/*_components.py` 或独立 state-machine 模块：Delivery / Consumer 与 ACK、retry、reject、lease 状态迁移；
- `maintenance/`：delayed、expiry、lease reclaim、PEL recovery；
- `observability/`：事件构造、指标映射和 sink 隔离；
- Redis Lua 脚本移至具名、可单独测试的模块，避免在业务方法中内联大段字符串。

约束：

- 重构不得改变公开 API、持久化 schema、Redis key 格式或既有 at-least-once 语义；
- SQLite 和 Redis 的同类状态迁移必须由参数化 backend conformance suite 覆盖；
- 消除为绕过类型检查而新增的 `Any`、`cast` 和私有属性测试依赖；必要的动态边界应收敛到 adapter 层；
- 每次拆分均需保持 lint、mypy、完整测试、构建及覆盖率门槛通过；
- 新功能优先落到对应职责模块，不接受以“后续再拆”为由继续膨胀 Broker。

## 5.3 v0.4 验收标准

- SQLite / Redis Broker 的生命周期、提交、投递状态机、maintenance 与 observability 职责已拆分，新增功能不再直接扩大核心 Broker 文件；
- Redis Lua 脚本具有具名模块、独立单元测试及与 Python 调用参数一致的测试覆盖；
- 存在参数化跨 backend conformance suite，至少覆盖 submit、retry、delayed、expiry、lease reclaim、DLQ/EQ、heartbeat、cancellation 与 stats；
- 重构前已有公共 API、v0.2/v0.3 兼容行为、SQLite schema 与 Redis key 格式保持不变；
- 新增或变更代码没有以 `Any`、无意义 `cast` 或私有实现细节测试来规避公开类型契约；

- SQLite 批量提交有事务回滚测试和性能基准；
- Redis 批量提交网络往返明显少于逐条版本；
- 类型化 payload 有成功、失败、版本不兼容测试；
- replay 的 dedup keep/remove/replace 三种模式有测试；
- Admin API 与 CLI 能完成常见排障操作；
- 所有索引一致性检查通过；
- 文档中明确批量提交的原子性和性能能力。

---

# 6. v0.5：生产化、兼容性与发布质量

## 6.1 版本目标

v0.5 的目标不是继续增加大量业务功能，而是让库具备可靠发布和长期维护的条件。

## 6.2 功能范围

### A. 健康检查与故障诊断

新增：

```python
health = await broker.health_check()
```

至少检查：

- backend 连接；
- schema 版本；
- Redis Consumer Group；
- 必要索引；
- serializer registry；
- namespace 配置；
- 当前不可恢复错误。

返回结构化结果：

```python
HealthReport(
    healthy=True,
    backend="redis",
    checks=[
        HealthCheck(name="connection", status="ok"),
        HealthCheck(name="consumer_group", status="ok"),
    ],
)
```

### B. 索引一致性和修复

新增只读检查：

```python
report = await broker.check_consistency("emails")
```

检查：

- READY state 是否有 Stream entry；
- READY state 是否有 ready index；
- LEASED state 是否有 lease index；
- EXPIRED 是否在 EQ；
- DEAD_LETTERED 是否在 DLQ；
- entry 中的 message_id 是否存在；
- 重复的 DLQ/EQ 条目；
- stale PEL。

修复必须显式调用：

```python
await broker.repair_consistency("emails", dry_run=True)
```

默认只能 dry-run，不能自动删除用户数据。

### C. 压测与故障演练

建立可重复的 benchmark：

- SQLite submit 吞吐；
- SQLite batch submit 吞吐；
- Redis submit 吞吐；
- Redis batch submit 吞吐；
- claim/ACK 吞吐；
- retry/reclaim 吞吐；
- 大 payload；
- 高 dedup 冲突率；
- 多 consumer 并发。

建立故障测试：

- claim 后进程崩溃；
- ACK 前 Redis 连接断开；
- Lua 执行前后客户端断开；
- SQLite 进程中断；
- Worker graceful shutdown；
- serializer 不可用；
- DLQ replay 并发冲突。

### D. 数据库和 key 迁移

SQLite：

- schema version 表；
- 向前迁移脚本；
- 迁移失败 rollback；
- 备份建议；
- 旧版本兼容窗口。

Redis：

- key namespace version；
- Consumer Group 初始化/升级策略；
- 老字段兼容读取；
- 废弃 key 清理工具；
- 滚动升级期间的兼容行为。

### E. 发布质量

v0.5 发布前必须具备：

- `ruff check`；
- 类型检查；
- 完整 pytest；
- Redis 集成测试标识和 CI service；
- package build；
- 最低 Python 版本验证；
- API 文档；
- CHANGELOG；
- upgrade guide；
- 安全策略；
- license 和贡献指南；
- semver 兼容性声明。

## 6.3 v0.5 验收标准

- 可通过官方 CI 在无人工操作下运行 SQLite 和 Redis 测试；
- Redis 不可用、schema 不匹配、serializer 不可用时都有清晰错误；
- 能通过 health/consistency API 定位常见问题；
- 有基准数据和推荐容量边界；
- 有从 v0.1/v0.2/v0.3/v0.4 升级到 v0.5 的文档；
- 公共 API、废弃 API 和兼容策略明确；
- 以 v0.5 为第一个适合较长期依赖的稳定预发布版本。

---

# 7. v0.6：交互式运维控制台

v0.6 在不改变消息投递语义或后端状态机的前提下，把 v0.4/v0.5 已有的 Admin、health 和 consistency 能力提供为可交互的终端体验。完整交互、命令、数据边界及验收定义见 [`v0.6 TUI CLI 设计`](v0.6-tui-cli.md) 和 [`v0.6 验收清单`](v0.6-acceptance.md)。

## 7.1 版本目标

- 提供可选安装的全屏 TUI 和适合 SSH/脚本调试的 REPL；现有非交互式 `taskqx` 命令必须保持兼容。
- 让运维人员无需直接读取 Redis key 或 SQLite 表，即可完成健康诊断、队列观察、消息/DLQ 排障和一致性修复审阅。
- 所有写操作继续只经由公开 Broker/Admin API；TUI/REPL 绝不实现或绕过 backend-specific 状态迁移。
- 将“消息已重新投递”和“业务 handler 已成功处理”明确区分，避免把 replay enqueue 误报为业务成功。

## 7.2 功能范围

### A. 可选依赖与入口

新增 `taskqx[tui]` extra，以 Textual 实现全屏界面；REPL 可使用 `prompt_toolkit`。计划入口：

```bash
taskqx tui --sqlite taskqx.db
taskqx shell --redis-url redis://host/2 --namespace payments
```

不安装 extra 时，现有非交互 CLI 正常可用，并对 `tui`/`shell` 给出明确的安装提示。TUI 不能要求启动 worker，不能修改 broker 的启动、Consumer Group 初始化或维护语义。

### B. 只读看板与浏览

TUI 首页至少展示 backend、脱敏连接信息、namespace、health 状态、最后刷新时间和错误/警告摘要；队列页展示 READY、DELAYED、LEASED、DLQ、EQ 及累计计数。支持手动刷新、可配置轮询、筛选、排序、详情页和 JSON 原始报告查看。

消息详情显示状态、attempt、时间、consumer/delivery、失败原因、serializer/schema 与 dedup 元数据。payload 必须默认隐藏；展示 payload 需由用户显式触发，并在界面中显示敏感数据警告。

为保证跨 backend 一致的浏览体验，补充并稳定公开的分页 API：`list_queues()`、`list_messages(queue, *, status, cursor, limit)`，以及 EQ 列表/详情 API。不得通过扫描私有 Redis key 或 SQLite 表来伪造公共功能；如某 backend 无法提供相同语义，必须由 capability 明示。

### C. REPL、命令提示与可发现性

REPL 提供上下文 prompt、命令历史、Tab 补全、`help`、错误建议和 JSON 输出。首期命令至少覆盖 `health`、queue inspect/use、message inspect、DLQ list/show/replay、consistency check/repair；命令名、queue 和可查询 message ID 应支持动态补全。

TUI 提供 `?` 帮助、`r` 刷新、`/` 搜索、`:` 命令面板、`q` 返回/退出等一致快捷键；所有快捷键及非 TTY 降级行为写入 operations 文档。

### D. 受保护的管理工作流

DLQ replay、删除和 consistency repair 必须展示影响摘要、target queue、dedup mode 与风险提示。实际写入仍必须经过显式确认：TUI 需二次确认（输入操作词或资源名），REPL/非交互 CLI 继续要求 `--yes`，repair 继续要求 `--apply --yes`。

replay 完成页只能报告 `replay_enqueued`（已从 DLQ/EQ 原子转移并重新投递）；不得显示“处理成功”。TUI 可轮询消息后续状态，并明确标注结果由异步 worker 决定。

### E. 安全、性能与可访问性

- 不在状态栏、历史记录、崩溃报告或默认复制操作中泄露 payload、Redis 密码、完整 dedup key；
- 默认只读，所有危险操作有明确的不可逆提示和审计友好的结果摘要；
- 列表必须分页，自动刷新不得对大队列执行无界全量扫描；
- 支持窄终端、无色终端、非 TTY 失败提示和键盘全程操作；
- TUI 进程退出时取消刷新任务并关闭 broker，不影响运行中的外部 worker。

## 7.3 非目标

- 不在 TUI 内编写、部署、启动或停止业务 handler/worker；
- 不承诺实时监控、Web 控制台、多用户认证授权或远程命令执行；
- 不把 TUI 轮询视为 scheduler/maintenance 替代品；
- 不改变 at-least-once、ACK、lease、dedup 或 replay 的既有语义。

## 7.4 兼容性与发布

v0.6 保持 v0.5 的 Python API 与非交互 CLI 兼容。新增依赖必须为 optional extra；普通库用户不应因安装 TUI 而增加运行时依赖。发布前提供从 v0.5 升级的说明，明确 TUI 不引入 SQLite schema 或 Redis keyspace migration，且所有交互式写操作仍遵守 v0.5 安全确认边界；兼容性目标见 [`migration-v0.5-v0.6.md`](migration-v0.5-v0.6.md)。

---

# 8. v0.7：可诊断性、消息生命周期与调度体验

完整开发范围、拟议 API、兼容性策略、实施顺序、验收门槛和待确认决策见 [`v0.7 开发计划`](v0.7-development-plan.md)。本版本以 v0.6 的公开 API、TUI 安全边界和既有 `DELAYED` 状态机为基线；v0.7 将补足独立 backend scheduler，而非另建第二套延迟状态机。

## 8.1 版本目标

- Worker handler 异常、重试耗尽、DLQ 及状态迁移失败具有安全、可关联的结构化日志；retry 耗尽时必须保留原始异常与完整 traceback。
- Broker 支持以消息或提交草稿为输入；`TaskMessage.clone()` 让业务能够深拷贝并微调现有消息后安全派生新任务，而不复用原消息 identity。
- 独立 backend scheduler 在没有 Worker、claim 或 inspect 活动时，仍按期推进 DELAYED、expiry、lease reclaim 与 ACK tombstone 清理；它可独立部署且多实例幂等。
- Redis 消息 key 迁移为按 queue 作用域的格式，配套 queue catalog、ACK tombstone cleanup index、全局只读 lookup index 和可恢复 keyspace migration，以支持按 queue 维护和清空。
- TUI 持续按产品反馈改善 delayed/失败/ACKED 保留状态、scheduler 健康、刷新反馈与键盘体验，并始终保持 payload 保护和受控写入边界。

## 8.2 核心约束

- 不改变 at-least-once、ACK、lease、DLQ/EQ 或业务幂等性语义。
- 日志默认不得泄露 payload、凭据、完整 dedup key 或敏感 metadata；异常日志必须使用原始异常的 `exc_info`，不能仅记录字符串。
- 消息克隆产生可提交的独立草稿；Broker 为每次新提交生成新的 message ID 和创建时间，绝不覆盖来源消息。
- v0.7 在保留既有 `delay` / `DELAYED` 原子状态迁移的基础上，新增独立 scheduler；不得依赖 claim、inspect 或 Worker 活动来触发空闲队列的到期消息。
- Redis key 格式变更必须具有 version、dry-run、备份建议、可重入迁移与兼容读取窗口；新旧格式并存时同一消息只能由一套状态机处理。

---

# 9. 推荐代码组织

为了避免所有能力继续堆积在 Broker 类中，建议逐步采用以下结构：

```text
src/taskqx/
  broker/
    base.py
    sqlite.py
    redis.py
  worker/
    base.py
    runtime.py
    policies.py
  submission/
    base.py
    sqlite.py
    redis.py
  serialization/
    base.py
    json.py
    registry.py
  observability/
    events.py
    metrics.py
    middleware.py
  admin/
    api.py
    consistency.py
  cli/
    main.py
  retry/
    policy.py
    backoff.py
```

分层原则：

- `broker` 负责后端连接和生命周期；
- `submission` 负责提交原子边界；
- `worker` 负责高层执行模型；
- `serialization` 负责 envelope 和版本；
- `observability` 负责事件/指标协议；
- `admin` 负责查看、replay、修复；
- `cli` 只调用 Admin API，不重复实现业务逻辑。

Worker 不应直接拼 Redis key，Admin 不应绕过 Store 修改提交状态，CLI 不应包含 backend-specific 状态迁移逻辑。

---

# 10. 每个版本的开发工作流

每个功能都必须按以下顺序落地：

1. 更新设计文档和 Protocol；
2. 明确状态迁移和原子边界；
3. 先写 backend-independent 测试；
4. 分别实现 SQLite 和 Redis；
5. 增加崩溃、重试、并发和异常测试；
6. 增加公开 API 示例；
7. 更新 README、operations 和 migration 文档；
8. 运行单元测试、Redis 集成测试、lint、类型检查和构建；
9. 记录 capability 差异和已知限制；
10. 完成版本验收清单后再修改版本号。

禁止只以“测试通过”作为版本完成标准。以下内容缺一不可：

```text
实现 + 测试 + 文档 + 兼容性 + 运维说明
```

---

# 11. 优先级建议

如果开发资源有限，优先级应为：

## 必须优先

1. v0.2 Worker；
2. 真正实现 concurrency；
3. RetryPolicy 与延迟 retry；
4. queue/namespace 严格校验；
5. 按 queue 选择 SubmissionStore；
6. DLQ/EQ replay 的 dedup 策略。

## 第二优先级

7. 标准 EventSink/MetricsSink；
8. serializer registry；
9. SQLite 批量提交；
10. Redis 批量提交；
11. 类型化 payload；
12. Admin API。

## 发布前必须完成

13. CLI；
14. health check；
15. consistency check/repair；
16. 压测与故障演练；
17. schema/key migration；
18. CI、lint、类型检查和 package build。

---

# 12. 成熟度判断标准

Taskqx 可以被认为达到“适合一般生产项目使用”的阶段，需要满足：

- 新用户可以在 10 分钟内完成提交和 Worker 消费；
- 常见异常不需要用户手写 ACK/Retry 状态机；
- 后端差异不会导致静默的数据语义变化；
- 所有不可逆操作都有 Admin API 和审计信息；
- 消息状态在崩溃、重启和网络异常后可解释；
- 队列积压、DLQ、过期消息和 lease 问题可观测；
- 升级不会依赖手工修改 SQLite 表或 Redis key；
- 公共 API 有类型、文档、测试和兼容策略；
- 用户仍可以在需要时下沉到低层 Consumer/Delivery API。

最终理想的用户体验是：

```python
broker = Taskqx.sqlite("tasks.db")

await broker.submit("emails", SendEmail(...))

await broker.run(
    "emails",
    handler=handle_email,
    concurrency=10,
)
```

简单路径足够简单，复杂路径仍然保留可靠性内核和可扩展的低层接口。
