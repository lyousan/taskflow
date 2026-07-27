# v0.4 Worker、配置、观测、类型化 payload 与管理

## QueueConfig 与扩展点

每个 broker 可以通过 `queues={"queue-name": QueueConfig(...)}` 配置队列默认策略：
最大尝试次数、lease 时长、可选 RetryPolicy、dedup TTL 和 payload 大小上限。未设置
queue RetryPolicy 时，Worker 沿用消息的 `max_attempts`；单次
`submit()`/`worker()` 参数优先于 queue 配置，queue 配置优先于 broker 默认值。
SubmissionStore profile 在 broker 创建时校验，并可用 `submission_capabilities(queue)`
查询实际去重和批量能力。

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

当前版本没有独立的后台 scheduler。延迟消息在 claim、inspect 或显式 `maintain()` 时转为
READY；长期空闲队列建议运行每秒一次的 maintenance loop。多个实例可以同时维护同一队列，
转移操作是幂等的，不会重复入队。

`submit(delay=timedelta(...))` 和 `delivery.retry(delay=timedelta(...))` 将消息原子地放入
`DELAYED`。SQLite 在同一事务中提升到期消息；Redis 以 delayed sorted set 与 Lua 原子转移。
`maintain()`、`claim()` 和 `inspect()` 会驱动调度，因此不需要额外常驻调度进程。`expires_at`
早于到期时间时，消息会直接进入 EQ，绝不重新交给 handler。

Redis 的 `expires_at` 和 lease 判断以 Redis server `TIME` 为准。请使用 NTP 同步 Redis 和
应用主机；启动时若两者相差至少 5 秒，broker 会记录告警。生产者需要生成相对过期时间时，
应确保其时钟已同步；backend 不会混用本地时间与 Redis 时间。

Broker 可接收 v0.2 兼容的 `MetricsSink`（`increment()`、`observe()`）。实现可选
`GaugeMetricsSink.gauge()` 时，ready/leased/delayed 快照会作为 gauge 报告；否则保持
observe 行为。它报告
提交、重复、领取、ACK、Retry、死信、lease lost、处理耗时和 ready/leased 队列快照。
Broker 也可接收 `EventSink`。其 `TaskflowEvent` 包含 `event_name`、backend、queue、message/delivery ID、
attempt、consumer、reason、serializer、status 与时间戳。
默认 Middleware 和 MetricsSink 的异常会被记录并隔离，不会把已经提交的消息状态伪装成失败；
需要严格传播 hook 异常时可使用 `Middleware(fail_fast=True)`。

一个 Broker 仍有一个默认 encoder；`SerializerRegistry` 用持久化的 name/version 为历史
消息选择 decoder。未注册的标识会以清晰的 `SerializerUnavailableError`（也是
`ValidationError` 子类）失败，而不会尝试错误解码。

## 类型化 payload 与排障

dataclass、TypedDict 和 Pydantic v2 model 可通过 `payload_type` 绑定到 submit 和 worker。
TypedDict 的提交值是普通 dict，必须显式声明 `payload_type`。类型化 worker 不会宽松转换字段：
schema 不匹配或字段损坏会以 `poison_payload` 写入 DLQ，原始 envelope 仍可通过 Admin API 查看。
Pydantic 是 optional extra（`taskflow[pydantic]`），正式支持范围是 v2；v1 不在兼容矩阵内。

`submit_many(..., atomic=False)` 适合导入或背填：调用方必须逐项检查
`BatchSubmitItemResult.result` 与 `.error`，不能假定整批都成功。`atomic=True` 适合要求整批
rollback 的业务场景。两种 backend 都保持单条提交内部的 dedup、message、stream/index 原子边界。

重放 DLQ/EQ 前应先 `inspect_message()`；使用 `dedup_mode=keep|remove|replace` 明确 dedup
记录策略。payload override 可以传 `payload_type`，会执行与 submit 相同的 schema 和 payload
size 校验。未类型化的 dict override 会清除旧 schema，以阻止错误类型标注。

CLI 默认只读且会隐藏 payload。`taskflow dlq replay ... --yes` 是显式的破坏性操作；
`--include-payload` 仅应在受控终端使用。每条输出带 backend、namespace（SQLite 为 null）和
相关 queue。`taskflow health` 仅为 backend connectivity probe，不能当作索引、Consumer Group
或业务 handler 的全量健康结论。

完整的开发与 release 验证需同时安装开发工具和 Redis backend：

```bash
uv sync --extra dev --extra redis --extra pydantic --locked
uv run ruff check src tests
uv run mypy src tests
uv run pytest --cov=taskflow -q
uv build
```

## v0.4 batch evidence

SQLite batch-submit timing is intentionally a reproducible benchmark rather than a
machine-dependent CI gate. Run it on the intended deployment host and record its
output with the release evidence:

```bash
uv run python benchmarks/v04_batch_submit.py --count 1000
```

`tests/test_v04_batch_performance.py` separately proves the Redis network boundary:
12 individual submissions perform 12 `EVAL` calls, while one `submit_many()` call
performs exactly one `EVAL` call for the same 12 prepared submissions.
