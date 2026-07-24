# Worker、观测与 serializer

`ConsumerOptions.concurrency` 由 `broker.worker()` / `broker.run()` 使用；单个
`consumer()` 始终一次返回一个 Delivery。Worker 以固定数量的领取循环限制 in-flight
消息数，handler 成功后 ACK，抛异常后 Retry；调用 `await worker.close()` 会停止新领取并
等待当前处理结束。它也支持 `async with broker.worker(...) as worker:`，退出上下文时会完成
同样的 graceful shutdown。状态迁移（ACK / Retry）失败会暴露给调用方，避免被静默吞掉。

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
