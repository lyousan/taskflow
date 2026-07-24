# Taskflow

Taskflow 是独立、可嵌入、异步优先的 Python 任务消息框架。v0.1 提供可靠的
至少一次投递、显式 ACK、立即重试、租约回收、DLQ、EQ 与精确提交去重。

内置 `SQLiteBroker` 与 `RedisBroker`。SQLite 适用于本地脚本、测试和 CI，
不适合作为高吞吐、分布式生产队列；Redis 适用于多进程或多实例消费者。
Redis backend 使用 Streams 与 Consumer Group；主流状态变迁由 Lua 原子执行。

```python
import asyncio
from datetime import timedelta
from taskflow import ConsumerOptions, SQLiteBroker


async def main() -> None:
    async with SQLiteBroker("taskflow.db") as broker:
        await broker.submit(
            queue="crawl.fetch",
            payload={"url": "https://example.com"},
            dedup_scope="batch-1",
            dedup_key="example.com:/",
            dedup_ttl=timedelta(days=7),
        )
        async with broker.consumer("crawl.fetch", options=ConsumerOptions(lease_seconds=60)) as consumer:
            delivery = await anext(consumer)
            # 在业务副作用成功后再确认，处理函数必须保持幂等。
            await delivery.ack()


asyncio.run(main())
```

Redis 需要安装额外依赖：`pip install 'taskflow[redis]'`。使用实例可通过 URL 创建：

```python
from taskflow import RedisBroker

broker = RedisBroker.from_url("redis://127.0.0.1:6379/2")
```

## 关键语义

- Taskflow 承诺 at-least-once 投递。worker 在 `ack()` 前崩溃时，消息会在租约
  到期后再次投递，业务处理必须幂等。
- `retry()` 是立即重投；达到 `max_attempts` 后进入 DLQ。延迟重试属于 v0.2。
- 同一个 Delivery 的重复终结操作是幂等的；旧 lease 的迟到操作会抛出
  `LeaseLostError`。
- 去重只在提交阶段发生。启用去重必须同时传入 `dedup_scope`、`dedup_key` 和
  正数 `dedup_ttl`（或配置 broker 的默认 TTL）。
- `expires_at` 与 dedup TTL 相互独立。到期消息不会交给业务 handler，会进入 EQ。

开发路线与版本验收标准见 [`docs/roadmap.md`](docs/roadmap.md)。

运行测试：`pytest`。
