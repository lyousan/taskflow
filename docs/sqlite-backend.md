# SQLite backend（v0.1）

`SQLiteBroker` 是本地脚本、测试与 CI 的持久化 backend。它以 SQLite 事务保护提交、
领取、确认、重试、拒绝、租约回收和过期迁移的一致性，并通过异步 API 暴露全部 I/O
路径。为了保证单连接上的事务正确性，当前实现以 `asyncio.Lock` 串行化操作。
`messages` 还持久化 `serializer_name` 与 `serializer_version`；若当前 Broker 的
serializer 不匹配历史消息的标识，读取会明确失败，而不会以错误的 serializer 解码。

因此它适用于单进程或有限进程的低到中等并发场景，不应作为高吞吐量、多节点生产
消息系统。生产部署应选用 Redis backend；Redis Streams 的目标状态机和原子边界见
[redis-lifecycle.md](redis-lifecycle.md)。

维护可以由 consumer 拉取时隐式执行，也可由服务周期性调用：

```python
await broker.maintain("crawl.fetch")
```

管理接口为 `broker.admin`，提供 `list_dead_letters`、`replay_dead_letter`、
`delete_dead_letter`、`list_expired`、`replay_expired` 与 `delete_expired`。重放 EQ
必须显式传入新的 `expires_at`，其中 `None` 代表移除过期限制。

`SQLiteSubmissionStore.submit_many()` 使用单个 `BEGIN IMMEDIATE` 事务：任何一条写入
失败都会回滚整批。Redis 同样以单个 Lua 调用处理一批已准备提交，并保留每条消息各自
的 dedup 决策和结果。
