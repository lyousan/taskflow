# v0.2 Worker、延迟调度、观测与 serializer

`ConsumerOptions.concurrency` 由 `broker.worker()` / `broker.run()` 使用；两个入口均支持
`consumer_id`、`retry_policy` 和 `heartbeat_seconds`；单个
`consumer()` 始终一次返回一个 Delivery。Worker 以固定数量的领取循环限制 in-flight
消息数，handler 成功后 ACK。`RetryableError` 与满足 `RetryPolicy.retry_on` 的异常按策略
重试，`RejectMessage` 或满足 `reject_on` 的异常进入 DLQ；`CancelledError` 不会伪造 ACK、
Retry 或 Reject，消息会在租约到期后恢复。调用 `await worker.close()` 会停止新领取并等待
当前处理结束。它也支持 `async with broker.worker(...) as worker:`，退出上下文时会完成同样的
graceful shutdown。状态迁移失败会暴露给调用方，避免被静默吞掉。

Worker 默认每个 lease 的三分之一时间自动执行一次 heartbeat；可用
`heartbeat_seconds=` 覆盖。它只在 handler 持续运行时续租，不能改变 at-least-once 语义。

v0.2 没有独立的后台 scheduler。延迟消息在 claim、inspect 或显式 `maintain()` 时转为
READY；长期空闲队列建议运行每秒一次的 maintenance loop。多个实例可以同时维护同一队列，
转移操作是幂等的，不会重复入队。

`submit(delay=timedelta(...))` 和 `delivery.retry(delay=timedelta(...))` 将消息原子地放入
`DELAYED`。SQLite 在同一事务中提升到期消息；Redis 以 delayed sorted set 与 Lua 原子转移。
`maintain()`、`claim()` 和 `inspect()` 会驱动调度，因此不需要额外常驻调度进程。`expires_at`
早于到期时间时，消息会直接进入 EQ，绝不重新交给 handler。

Broker 可接收 `MetricsSink`。它包含 `increment()` 和 `observe()` 两个异步方法，并报告
提交、重复、领取、ACK、Retry、死信、lease lost、处理耗时和 ready/leased 队列快照。
Middleware 的 `event` 钩子接收 `BrokerEvent`，其中包含 queue、message/delivery ID、
attempt、consumer、reason、serializer、status 与时间戳。
默认 Middleware 和 MetricsSink 的异常会被记录并隔离，不会把已经提交的消息状态伪装成失败；
需要严格传播 hook 异常时可使用 `Middleware(fail_fast=True)`。

一个 Broker 仍有一个默认 encoder；`SerializerRegistry` 用持久化的 name/version 为历史
消息选择 decoder。未注册的标识会以清晰的 `ValidationError` 失败，而不会尝试错误解码。

完整的开发与 release 验证需同时安装开发工具和 Redis backend：

```bash
uv sync --extra dev --extra redis --locked
uv run ruff check src tests
uv run mypy src tests
uv run pytest --cov=taskflow -q
uv build
```
