# Changelog

## 0.2.0

- 新增高层 `TaskWorker` 的 RetryPolicy、异常分类和自动 lease heartbeat。
- 新增 SQLite / Redis 延迟提交与延迟重试，支持 `DELAYED` 状态、过期转 EQ 和重启恢复。
- Worker 的有效重试上限为消息 `max_attempts` 与策略上限的较小值；未配置策略时保持
  v0.1 的消息级重试上限行为。
- 增加 Redis Worker、延迟生命周期、RetryPolicy 边界和取消恢复测试。
- 增加 `py.typed`、迁移说明和 v0.2 验收清单。
