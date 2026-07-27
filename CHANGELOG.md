# Changelog

## 0.4.0

- 新增 dataclass、TypedDict 和 Pydantic v2 类型化 payload 边界、schema identity 与 poison DLQ 路径。
- 新增 `SubmitRequest.payload_type`、non-atomic 批量逐项结果 `BatchSubmitItemResult`，以及 SQLite/Redis 对称行为。
- 新增 Admin inspect、DLQ/EQ replay 的 keep/remove/replace dedup 策略和安全 CLI。
- replay payload override 现在使用与 submit 相同的归一化、schema 和 payload size 校验。
- 保持 v0.3 `replay_dead_letter(payload=None)` 的“保留原 payload”语义；新增
  `replace_payload=True` 以显式将 payload 改为 JSON `null`。
- `TaskBroker` Protocol 同步 type payload API；Redis 以可选 extra 提供，Pydantic v2 以 `taskflow[pydantic]` 提供。

## 0.3.0

- 新增按 queue 的 `QueueConfig` 与固定配置优先级。
- 新增 SubmissionStore profile 路由和严格 queue/namespace/profile 命名校验。
- 新增标准 `EventSink`、`TaskflowEvent`、Metrics gauge 与 serializer unavailable 错误。
- Redis 的 expiry / lease 明确以 Redis server `TIME` 为权威；启动时会报告显著时钟偏差。
- 修复 SQLite lease 续租遇到 expiry 时的事务回滚，并补齐 SQLite / Redis claim、续租过期路径的
  EQ、`expired` 事件与 `expired_total` 指标一致性。

## 0.2.0

- 新增高层 `TaskWorker` 的 RetryPolicy、异常分类和自动 lease heartbeat。
- 新增 SQLite / Redis 延迟提交与延迟重试，支持 `DELAYED` 状态、过期转 EQ 和重启恢复。
- Worker 的有效重试上限为消息 `max_attempts` 与策略上限的较小值；未配置策略时保持
  v0.1 的消息级重试上限行为。
- 增加 Redis Worker、延迟生命周期、RetryPolicy 边界和取消恢复测试。
- 增加 `py.typed`、迁移说明和 v0.2 验收清单。
