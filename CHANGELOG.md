# Changelog

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
