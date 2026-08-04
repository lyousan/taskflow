# 从 v0.6 升级到 v0.7（开发期）

> 本文描述 v0.7 开发预发布版本的升级步骤；在稳定版发布前，不应把它当作生产发布公告。

v0.7 保持 at-least-once、显式 ACK、lease、DELAYED、DLQ、EQ、dedup 和既有关键字
`submit()` API。SQLite 数据库不需要手工迁移。Redis message hash keyspace 是本版本唯一的
持久化破坏性变更，必须按本指南完成迁移。

## Python API 兼容性与新路径

现有 `submit(queue=..., payload=..., ...)` 和 `submit_many()` 调用继续有效。新代码可把
`SubmitRequest` 当作提交草稿；它与关键字调用使用同一验证和持久化路径，两种输入不能混用：

```python
source = delivery.message
request = source.clone(payload={**source.payload, "page": 2})
result = await broker.submit(request)

# 等价的便利入口：
result = await broker.submit_from(source, payload={**source.payload, "page": 2})
```

`clone()` 深拷贝 JSON-compatible payload、metadata 及其嵌套 dict/list；它不会复制 message ID、
创建时间、attempt、lease 或其他投递状态。每次提交都生成新的 ID 与创建时间。

- 默认 `parent_id=source.id`，形成可追踪 lineage；传入 `parent_id=None` 明确创建无 lineage 的独立重发。
- 默认清除 `dedup_key`、`dedup_scope` 与 `dedup_ttl`。若业务确实需要去重，必须显式传入完整三元组。
- queue、payload、metadata、workflow、expires_at、max_attempts、剩余 delay 及未替换 payload 的 schema identity 默认继承。
- `expires_at` 是绝对时间；clone 不会延长它。需要新的有效期时，必须显式传入新的 `expires_at`。

## 部署独立 scheduler

v0.6 中由 Worker、claim、inspect 或调用方 `maintain()` 推进的维护不能保证空闲队列及时到期。
v0.7 应在每个活跃 backend 部署独立 scheduler：

```python
from datetime import timedelta

scheduler = broker.scheduler(queues=None, interval=timedelta(seconds=1))
await scheduler.run()
```

它只推进持久化状态，不 claim 消息或运行 handler。`queues=None` 每个 tick 发现已知队列；显式
`queues` 仅维护给定队列。多个实例可并行运行；状态迁移为条件性且幂等。正常到期时间最多晚于
预期 `interval + 单次 tick 耗时`。`await scheduler.close()` 可用于正常停止；取消 `run()` 也会
停止循环。直接 `await scheduler.tick()` 适合测试或受控的一次维护，异常会返回调用方。

scheduler 负责推进 DELAYED due、READY/DELAYED/LEASED expiry、LEASED timeout reclaim，以及
ACK tombstone cleanup。`maintain(queue)` 保留为显式单次维护 API，但不再是生产环境的替代方案。

## ACK tombstone

v0.7 中每次 ACK 都自动保留为 tombstone，默认 5 分钟；不再支持按 `submit()` 覆盖。以下配置可
按 broker 默认值或 queue 调整正数 `timedelta`：

```python
QueueConfig(ack_tombstone_ttl=timedelta(hours=24))
SQLiteBroker(default_ack_tombstone_ttl=timedelta(hours=24))
```

ACK 会记录 `acked_at` 并立即写入 cleanup index。scheduler（或一次 `maintain()`）只在 tombstone
到期且消息仍为 ACKED 时永久清除序列化的业务 envelope；不会影响累计 ACK 统计，也不会删除其他终态。
轻量 tombstone 会保留 message ID、queue、ACKED 状态、attempt、创建/确认时间、serializer、最后操作/原因与
workflow/parent lineage，供 `list_message_summaries()` 和 TUI 查询；之后 `inspect_message()` 与
`observe_message()` 返回 `None`，不可重放或恢复 payload。请在保留期到期前导出任何需要留存的业务内容。

## Redis keyspace 迁移

新消息 hash 格式为：

```text
<namespace>:queue:{<queue>}:message:<message_id>
```

同时使用 `<namespace>:queues` queue catalog、`<namespace>:message-index` 全局只读定位索引，
以及 `<namespace>:queue:{<queue>}:retention` ACK tombstone cleanup index。v0.7 在运行时不会自动扫描或
迁移 namespace。

1. 停止**所有**使用该 namespace 的 producer、Worker、scheduler 和交互运维进程。
2. 备份整个 Redis namespace；保留可恢复的备份直到升级验证完成。
3. 对目标 namespace 运行 dry-run：

   ```bash
   taskqx --redis-url redis://127.0.0.1:6379/2 --namespace production redis migrate-keyspace --json
   ```

4. 审阅输出：`migrated` 是将移动的 legacy message IDs，`resumed` 是已有新 hash、可安全续跑的
   IDs，`conflicts` 必须为空。冲突、无 queue 的 legacy hash 或非法 queue 名称必须先人工处置；
   不得执行 apply。
5. 再次确认所有写入方仍已停止，然后执行：

   ```bash
   taskqx --redis-url redis://127.0.0.1:6379/2 --namespace production redis migrate-keyspace --apply --yes --json
   ```

6. 再运行同一 dry-run，确认 `migrated`、`resumed` 和 `conflicts` 均为空；启动 v0.7 scheduler 和
   Worker 后，对每个活跃 queue 运行 `taskqx ... queue check-consistency QUEUE`。

迁移可重入：单条消息的 queue-scoped hash 与 global index 都存在后，旧 hash 才会删除；进程中断后
在静止 namespace 上重复 apply 即可继续。若 apply 前发现问题，保持服务停止，从备份恢复 namespace
并继续运行 v0.6。apply 已开始后不能通过 v0.6 安全处理新格式的消息；应保持 v0.7 停止、从迁移前
备份恢复，再回退到 v0.6。旧格式只读兼容窗口延续到 v0.8，但任何单条消息不得同时由新旧状态机处理。

## 日志与运维

配置 `taskqx.worker` 和 `taskqx.scheduler` logger 收集失败诊断；库不调用 `basicConfig()`。
可重试 handler 异常以 WARNING 记录；retry 耗尽、reject、终结状态迁移失败和 scheduler tick 失败
以 ERROR 加原始 exception traceback 记录。日志的关联字段包括 backend、namespace、queue、
message/delivery/consumer ID、attempt、action、outcome、retry delay 和 error type；默认不会写入
payload、metadata、认证信息或完整 dedup key。
