# Taskflow

Taskflow 是独立、可嵌入、异步优先的 Python 任务消息框架。v0.3 提供可靠的
至少一次投递、显式 ACK、延迟重试、租约回收、DLQ、EQ 与精确提交去重，以及可直接运行
异步 handler 的高层 Worker。

内置 `SQLiteBroker` 与 `RedisBroker`。SQLite 适用于本地脚本、测试和 CI，
不适合作为高吞吐、分布式生产队列；Redis 适用于多进程或多实例消费者。
Redis backend 使用 Streams 与 Consumer Group；主流状态变迁由 Lua 原子执行。

```python
import asyncio
from datetime import timedelta
from taskflow import SQLiteBroker
from taskflow import QueueConfig
from taskflow.retry import ExponentialBackoff, RetryPolicy


async def main() -> None:
    async with SQLiteBroker(
        "taskflow.db",
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
            "crawl.fetch", fetch, concurrency=10,
            retry_policy=RetryPolicy(
                max_attempts=5,
                backoff=ExponentialBackoff(initial=1, maximum=60, jitter=True),
            ),
        ) as worker:
            await worker.run()


asyncio.run(main())
```

Redis 需要安装额外依赖：`pip install 'taskflow[redis]'`。使用实例可通过 URL 创建；
Worker API 与 SQLite 完全相同，适合多进程或多实例消费者：

```python
from taskflow import RedisBroker

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

- Taskflow 承诺 at-least-once 投递。worker 在 `ack()` 前崩溃时，消息会在租约
  到期后再次投递，业务处理必须幂等。
- `retry(delay=...)` 与 `submit(delay=...)` 会原子地进入持久化的 `DELAYED` 状态；到期后
  maintenance 在 claim/inspect 时幂等地将其变为 READY。延迟等待期间进程重启不会丢失消息。
- `RetryPolicy` 的 attempt 从 1 开始计数，`max_attempts=3` 表示 handler 至多执行三次。
  `RetryableError` 会按策略重试，`RejectMessage` 会直接进入 DLQ；其他异常由
  `retry_on` / `reject_on` 决定。Worker 在 handler 运行时自动 heartbeat lease。
- 同一个 Delivery 的重复终结操作是幂等的；旧 lease 的迟到操作会抛出
  `LeaseLostError`。
- 去重只在提交阶段发生。启用去重必须同时传入 `dedup_scope`、`dedup_key` 和
  正数 `dedup_ttl`（或配置 broker 的默认 TTL）。
- `expires_at` 与 dedup TTL 相互独立。包括 DELAYED 在内的到期消息不会交给业务 handler，
  会进入 EQ。

开发路线与版本验收标准见 [`docs/roadmap.md`](docs/roadmap.md)。
v0.2 升级说明见 [`docs/migration-v0.2-v0.3.md`](docs/migration-v0.2-v0.3.md)。

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
uv run pytest --cov=taskflow -q
uv build
```

扩展开发者可从顶层导入 `TaskBroker`、`TaskConsumer`、`TaskDelivery` 与
`SubmissionStore` Protocol。
