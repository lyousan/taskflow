# v0.1 → v0.2 迁移说明

v0.2 保留 v0.1 的低层 `consumer()` / `Delivery` API，并新增延迟消息、`RetryPolicy`
和自动 heartbeat。现有 `await delivery.retry(reason="...")` 调用仍然表示立即重试。

## 重试上限

有效最大执行次数永远不会超过消息在提交时声明的 `max_attempts`：

```text
显式 Worker RetryPolicy.max_attempts 与 message.max_attempts 的较小值
```

未传入 `retry_policy` 时，Worker 保持 v0.1 行为：普通 `Exception` 立即重试，次数由
消息自身的 `max_attempts` 决定。传入策略后，策略负责异常分类、退避和更严格的上限；
它不能放宽消息上限。

```python
worker = broker.worker(
    "jobs",
    handle,
    retry_policy=RetryPolicy.exponential(max_attempts=5, initial_delay=1, max_delay=60),
)
```

第三方 `TaskDelivery` 实现需要支持 v0.2 的 `retry(reason=..., delay=...,
max_attempts=...)` Protocol 参数。只实现 v0.1 签名的 backend 应在升级后重新运行
conformance 测试。

## 延迟调度模型

v0.2 没有独立常驻 scheduler。`claim()`、`inspect()` 和显式 `maintain(queue)` 会驱动
到期消息从 `DELAYED` 转为 READY；长期空闲队列应配置一个轻量 maintenance loop：

```python
while running:
    await broker.maintain("jobs")
    await asyncio.sleep(1)
```

迁移期间应确认部署环境允许该 loop，或接受“消息到期后等待下一次 broker 活动”的最终
一致性窗口。SQLite 与 Redis 的转移均为幂等操作，多实例重复维护不会重复投递。

## Worker handler 签名

高层 Worker 的 handler 接收 `TaskMessage`，不是 `TaskDelivery`：

```python
async def handle(message: TaskMessage) -> None:
    ...
```

需要显式 ACK、Retry、Reject 或续租时，请继续使用低层 `consumer()` API。
