# Taskqx 示例

每个示例都可以独立运行，并且只使用公开 API。安装开发依赖后，从仓库根目录执行：

```bash
uv run python examples/01_sqlite_worker.py
```

Redis 示例需要本机 Redis，并安装 extra：

```bash
uv sync --extra redis
uv run python examples/02_redis_worker.py
```

| 文件 | 场景 |
|---|---|
| `01_sqlite_worker.py` | SQLite 上最小的 submit + Worker |
| `02_redis_worker.py` | Redis 多消费者 Worker |
| `03_retry_and_delay.py` | 延迟提交、RetryPolicy 与 retry |
| `04_batch_and_dedup.py` | 原子/非原子批量提交与提交去重 |
| `05_typed_payload.py` | dataclass 类型化 payload |
| `06_low_level_delivery.py` | 显式 ACK、retry、reject 的低层 Delivery API |
| `07_dlq_replay.py` | 查看并重放 DLQ 消息 |
| `08_health_and_consistency.py` | health、consistency check 与 dry-run repair |
| `09_scheduler.py` | 独立 scheduler 推进空闲队列的延迟消息 |

示例为了自行结束，使用 `asyncio.Event` 等待一条消息完成。在真实服务中，应让 Worker 持续运行，
并确保业务副作用在 `ack()` 之前完成且保持幂等。
