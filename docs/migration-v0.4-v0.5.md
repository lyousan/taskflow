# 从 v0.4 升级到 v0.5

v0.5 保持全部 v0.4 提交、投递、payload 与 at-least-once 语义。它新增诊断元数据和 API；
升级前仍应备份 SQLite 文件，Redis 应使用受控 namespace 进行滚动升级。

## 自动迁移与兼容窗口

- SQLite 首次由 v0.5 打开时创建 `taskflow_schema`，记录当前 schema version。旧 messages 表会沿用
  既有 bootstrap/增量 DDL 路径，不会重写 envelope 或业务 payload；随后 `sqlite_migrations.py` 的
  v0.5 versioned migration runner 在单一事务中推进 metadata migration。
- Redis `start()` 会用 `SETNX` 写入 `<namespace>:meta:schema_version`。已有 namespace 不会被覆盖；
  若值不是当前版本，`health_check()` 会返回明确的 `schema_version` error，升级流程应停止 worker
  并完成兼容性评估后再继续。
- 回滚到 v0.4 时，新增 SQLite 元数据表与 Redis meta key 会被旧版本忽略。v0.5 的 Health/Consistency
  API 当然不可用；不要在回滚期间依赖其修复结果。

## 新 API 与安全操作

```python
health = await broker.health_check()
report = await broker.check_consistency("emails")
proposal = await broker.repair_consistency("emails")  # dry-run
applied = await broker.repair_consistency("emails", dry_run=False)
```

修复只会重建派生 index、DLQ/EQ 审计条目；不会 ACK、重放、删除业务 payload 或改变 handler 副作用。
对 Redis 生产 namespace，应先保存 dry-run 输出并在维护窗口执行实际修复。

CLI 的实际修复必须同时显式给出 `--apply --yes`：

```bash
taskflow --redis-url redis://host/2 --namespace payments queue check-consistency emails
taskflow --redis-url redis://host/2 --namespace payments queue repair-consistency emails --apply --yes
```

## 发布前核对

1. 备份 SQLite，或记录 Redis namespace 与 key count。
2. 使用 v0.5 调用 `health_check()`；修复任何 `error` 后才启动 worker。
3. 对活跃 queue 执行 `check_consistency()`；先审阅 dry-run，再决定是否修复。
4. 滚动启动 worker；Taskflow 仍是 at-least-once，业务 handler 必须幂等。

## 从任意历史版本直升 v0.5

可从 v0.1/v0.2/v0.3/v0.4 升级；先按顺序阅读前置版本迁移说明，再执行本指南。v0.1 依次审阅 v0.1→v0.2、
v0.2→v0.3、v0.3→v0.4；v0.2 从第二步开始，v0.3 从第三步开始，v0.4 只需本指南。所有路径在副本上执行：
安装 v0.5 → `health_check()` → 对每个 queue `check_consistency()` → 审阅 dry-run repair → 启动幂等 canary
worker → 验证 submit/claim/ACK/DLQ/EQ → 再滚动生产。

versioned SQLite migration 失败会回滚到该 runner 开始前的版本；历史 schema bootstrap/legacy ALTER 已在
runner 之前独立提交，不能宣称由该事务回滚。对于 bootstrap 失败应从升级前 SQLite 备份恢复。Redis version key
不匹配不会被覆盖；回滚时停止 v0.5 worker，并从 SQLite 或 Redis namespace 备份恢复；旧 v0.4 会忽略 v0.5 元数据表/key。
