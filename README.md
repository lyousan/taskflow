# Taskqx

Taskqx 是独立、可嵌入、异步优先的 Python 任务消息框架。v0.7 提供可靠的
至少一次投递、显式 ACK、独立生命周期 scheduler、延迟重试、租约回收、ACK tombstone、DLQ、EQ
与精确提交去重，以及可直接运行异步 handler 的高层 Worker。

内置 `SQLiteBroker` 与 `RedisBroker`。SQLite 适用于本地脚本、测试和 CI，
不适合作为高吞吐、分布式生产队列；Redis 适用于多进程或多实例消费者。
Redis backend 使用 Streams 与 Consumer Group；主流状态变迁由 Lua 原子执行。

```python
import asyncio
from datetime import timedelta
from taskqx import SQLiteBroker
from taskqx import QueueConfig
from taskqx.retry import ExponentialBackoff, RetryPolicy


async def main() -> None:
    async with SQLiteBroker(
        "taskqx.db",
        queues={"crawl.fetch": QueueConfig(max_attempts=5)},
    ) as broker:
        await broker.submit(
            queue="crawl.fetch",
            payload={"url": "https://example.com"},
            dedup_scope="batch-1",
            dedup_key="example.com:/",
            dedup_ttl=timedelta(days=7),
        )

        async def fetch(message):
            # 在这里完成可重试且幂等的业务副作用。
            print(message.payload["url"])

        async with broker.worker(
            "crawl.fetch",
            fetch,
            concurrency=10,
            retry_policy=RetryPolicy(
                max_attempts=5,
                backoff=ExponentialBackoff(initial=1, maximum=60, jitter=True),
            ),
        ) as worker:
            await worker.run()


asyncio.run(main())
```

Redis 需要安装额外依赖：`pip install 'taskqx[redis]'`。使用实例可通过 URL 创建；
Worker API 与 SQLite 完全相同，适合多进程或多实例消费者：

```python
from taskqx import RedisBroker


async def main() -> None:
    broker = RedisBroker.from_url("redis://127.0.0.1:6379/2")
    async with broker:
        async with broker.worker("emails", handle_email, concurrency=20) as worker:
            await worker.run()


asyncio.run(main())
```

低层 `consumer()` / `Delivery` API 仍然可用，适合需要业务自行决定终结结果的场景：只应在
副作用成功后 `ack()`；临时错误使用 `retry()`，不可恢复错误使用 `reject()`。

## 关键语义

- Taskqx 承诺 at-least-once 投递。worker 在 `ack()` 前崩溃时，消息会在租约
  到期后再次投递，业务处理必须幂等。
- `retry(delay=...)` 与 `submit(delay=...)` 会原子地进入持久化的 `DELAYED` 状态；到期后
  scheduler（或 `maintain()`）幂等地将其变为 READY。生产环境应独立运行 scheduler，使空闲
  队列不依赖 claim、inspect 或 Worker 活动而推进；延迟等待期间进程重启不会丢失消息。
- `RetryPolicy` 的 attempt 从 1 开始计数，`max_attempts=3` 表示 handler 至多执行三次。
  `RetryableError` 会按策略重试，`RejectMessage` 会直接进入 DLQ；其他异常由
  `retry_on` / `reject_on` 决定。Worker 在 handler 运行时自动 heartbeat lease。
- 同一个 Delivery 的重复终结操作是幂等的；旧 lease 的迟到操作会抛出
  `LeaseLostError`。
- 去重只在提交阶段发生。启用去重必须同时传入 `dedup_scope`、`dedup_key` 和
  正数 `dedup_ttl`（或配置 broker 的默认 TTL）。
- `expires_at` 与 dedup TTL 相互独立。包括 DELAYED 在内的到期消息不会交给业务 handler，
  会进入 EQ。

## v0.7：scheduler、提交草稿与可诊断性

> v0.7 正在开发中，尚未发布。其 Redis keyspace 迁移、运行步骤与回滚边界见
> [v0.6→v0.7 升级说明](docs/migration-v0.6-v0.7.md)。

后台 scheduler 只推进既有消息生命周期，不领取消息或执行 handler。`queues=None` 会在每个
tick 发现 backend 已知的队列；多个实例可以并行运行。最长正常到期延迟为 `interval` 加一次
tick 耗时：

```python
from datetime import timedelta

scheduler = broker.scheduler(interval=timedelta(seconds=1))
await scheduler.run()  # 在独立进程或服务任务中运行；取消或 close() 时退出
```

ACK 会自动成为可查询的 tombstone，默认保留 5 分钟。可用 `QueueConfig(ack_tombstone_ttl=...)`
或 broker 的 `default_ack_tombstone_ttl=` 调整队列策略；不需要在每次 `submit()` 时传参。scheduler
（或显式 `maintain()`）只会在 tombstone 到期后删除仍为 ACKED 的消息，累计计数不受影响。

`TaskMessage.clone()` 返回深拷贝、可提交的 `SubmitRequest` 草稿，不会复用来源 message ID、
创建时间或投递状态。默认 `parent_id` 指向来源消息，dedup 三元组默认清除；传入
`parent_id=None` 是无 lineage 的独立重新提交。`broker.submit_from(message, ...)` 是等价的
显式入口。Worker、scheduler 和状态迁移失败使用 `taskqx.worker` / `taskqx.scheduler` logger
输出安全的关联字段和原始 traceback；默认不记录 payload、metadata 或完整 dedup key。

Redis 消息 hash 已迁移到 queue-scoped keyspace。升级 Redis 前必须停止生产者、Worker 和
scheduler，备份 namespace，先运行 `taskqx --redis-url URL redis migrate-keyspace`，审阅
dry-run 后再以 `--apply --yes` 执行；详见迁移指南。

开发路线与版本验收标准见 [`docs/roadmap.md`](docs/roadmap.md)。
v0.2 升级说明见 [`docs/migration-v0.2-v0.3.md`](docs/migration-v0.2-v0.3.md)。

## v0.4：类型化 payload、批量提交与管理

`submit()`、`worker()` 和 `run()` 均支持 `payload_type`。支持 dataclass、TypedDict
和 Pydantic v2 model（安装 `taskqx[pydantic]`）；类型只约束 payload 的编码和解码，
不改变 at-least-once 生命周期。TypedDict 的原始 `dict` 必须在提交时显式声明类型：

```python
class ResizePayload(TypedDict):
    image_id: str
    width: int
    height: int


await broker.submit(
    queue="image.resize",
    payload={"image_id": "img-1", "width": 800, "height": 600},
    payload_type=ResizePayload,
)
worker = broker.worker("image.resize", handle_resize, payload_type=ResizePayload)
```

Taskqx 将 schema name/version 随 envelope 保存。worker 收到不匹配、字段缺失或类型
损坏的类型化 payload 时，不会做隐式转换，而是以 `poison_payload` 原因进入 DLQ。
Admin replay 覆盖 payload 时可传 `payload_type`；未声明类型的原始 dict 覆盖会清除旧 schema，
避免类型元数据与实际 payload 不一致。为保持 v0.3 兼容，`payload=None` 表示保留原 payload；
只有 `replace_payload=True, payload=None` 才会明确重放 JSON `null`。

`submit_many(messages, atomic=True)` 在任一项无法准备或持久化时回滚整批。使用
`atomic=False` 时返回与输入同序的 `BatchSubmitItemResult`；每项各自包含 `result` 或
`error`，单项 validation、serializer、dedup 或 store 错误不会阻止后续项。

DLQ/EQ replay 的 `dedup_mode` 为 `keep`、`remove` 或 `replace`：保留原记录、删除原记录，
或以新的 scope/key/TTL 原子替换。破坏性 CLI 操作必须传 `--yes`。所有 CLI JSON 都显示
backend、namespace 和 queue；SQLite 的 namespace 为 `null`。`await broker.health_check()`
返回结构化的连接、schema、索引/Consumer Group 和 serializer 诊断；命令行 `taskqx health`
输出同一份报告，任一错误检查会返回非零状态。它不验证业务 handler、外部依赖或消息业务
语义。默认会隐藏 payload，只有
`--include-payload` 才显示，输出可能包含敏感数据。

完整行为、迁移和验收清单见 [v0.4 migration](docs/migration-v0.3-v0.4.md) 与
[v0.4 acceptance](docs/v0.4-acceptance.md)。

## v0.5：生产诊断与一致性修复

`check_consistency(queue)` 检查消息状态与 SQLite 审计表、或 Redis 的 ready/lease/delayed
索引、DLQ/EQ、Stream 和 PEL 是否一致。`repair_consistency(queue)` 默认只返回 dry-run
建议；必须显式传入 `dry_run=False` 才会修复安全的派生记录，绝不重放或删除业务 payload。
CLI 对应 `taskqx queue check-consistency QUEUE` 和 `taskqx queue repair-consistency QUEUE`；
实际修复需要 `--apply --yes`。

升级、兼容和回滚见 [v0.5 migration](docs/migration-v0.4-v0.5.md)，完整发布验收项见
[v0.5 acceptance](docs/v0.5-acceptance.md)。Taskqx 遵循 SemVer：v0.5 保持 v0.4 的公开 API
兼容；废弃 API 会先在文档和 CHANGELOG 中声明。安全问题请参阅 [SECURITY.md](SECURITY.md)，
贡献规范见 [CONTRIBUTING.md](CONTRIBUTING.md)。

## v0.6 开发计划：交互式运维 CLI

> v0.6 正在开发中，尚未发布。当前 TUI 已提供 health、按需加载的队列与消息/DLQ/EQ 浏览、分页搜索和受保护的管理操作；其余验收项仍按清单推进。

安装可选依赖后，可从 TTY 启动交互界面：

```bash
pip install "taskqx[tui]"
taskqx tui --sqlite taskqx.db
taskqx shell --sqlite taskqx.db
```

TUI 使用成熟的 [Textual](https://textual.textualize.io/) 实现：health 自动刷新，队列与记录按需读取；shell 使用 `prompt_toolkit`。TUI 支持队列/消息/DLQ/EQ 浏览、显式显示 payload、队列级或单条 replay/delete，以及先执行 dry-run 再确认的 consistency repair。所有写操作仅经公开 Admin API，并在影响摘要弹框中按 `y` 确认、按 `n` 或 `Esc` 取消；payload 默认隐藏。基础安装不导入这些依赖；非 TTY 会给出使用既有 JSON CLI 的提示。完整设计、交付门槛和兼容性目标分别见 [v0.6 TUI CLI 设计](docs/v0.6-tui-cli.md)、[v0.6 验收清单](docs/v0.6-acceptance.md) 与 [v0.5→v0.6 升级说明](docs/migration-v0.5-v0.6.md)。

## v0.3 配置与扩展点

`QueueConfig` 可为每个 queue 设置最大尝试次数、lease、重试策略、默认 dedup TTL
和 payload 大小上限。配置优先级固定为：单次 `submit()`/`worker()` 参数 > queue
配置 > broker 默认值。`submission_stores` 与 `queue_submission_profiles` 可将不同
队列路由到不同的 `SubmissionStore`；通过 `submission_capabilities(queue)` 查询实际
能力。queue、namespace 和 profile 统一使用 `[A-Za-z0-9][A-Za-z0-9._-]{0,127}`，且
不能是只有 `.` 或 `-` 的名称。

可注入 `EventSink` 和 `MetricsSink` 获取标准生命周期事件和指标；事件包含
`event_name`、backend、queue、message/delivery/consumer、attempt、status、reason 和
error_type，指标不会把完整 dedup key 或 payload 放入 label。`SerializerRegistry` 按
`serializer_name + serializer_version` 解码历史消息，未注册时抛出
`SerializerUnavailableError`。

## 开发与 Release 验证

Redis backend 是可选的运行时依赖；但完整测试、类型检查和 release CI 应安装两个 extra：

```bash
uv sync --extra dev --extra redis --locked
uv run ruff check src tests
uv run mypy src tests
uv run pytest --cov=taskqx -q
uv build
```

扩展开发者可从顶层导入 `TaskBroker`、`TaskConsumer`、`TaskDelivery` 与
`SubmissionStore` Protocol。

## 示例

常见场景的可独立运行示例见 [`examples/`](examples/README.md)：SQLite/Redis Worker、重试与延迟、
批量与 dedup、类型化 payload、显式 Delivery、DLQ 重放，以及 v0.5 health/consistency 诊断。

## 交互式运维

安装 `taskqx[tui]` 后可在 TTY 中运行 `taskqx tui --sqlite taskqx.db` 或
`taskqx shell --sqlite taskqx.db`。两者使用分页的公开 Broker/Admin API；默认脱敏
payload。TUI 的 replay、删除和一致性修复均展示影响摘要，并要求按 `y` 确认或按 `n`/`Esc`
取消。基础安装不导入交互依赖，非 TTY 请继续使用 JSON CLI。操作细节见
[`docs/operations.md`](docs/operations.md)。
