# Redis backend（v0.3）

`RedisBroker` 面向 Redis standalone / Sentinel，使用 `redis.asyncio`。每个逻辑
队列拥有一个 Stream 和固定 Consumer Group：消费者以 `XREADGROUP` 领取新消息；
当前 Stream entry ID 仅是投递记录，业务身份始终是稳定的 `message_id`。

提交、ACK、Retry、Reject 通过 Lua 同时更新消息状态、Stream/PEL、租约索引、过期
索引、READY 时间索引和统计。Retry 与 lease reclaim 会 `XACK + XDEL` 旧 entry，再 `XADD` 新 entry，
所以同一消息的每次投递都有新 entry ID 和新 lease token。

延迟提交和 `retry(delay=...)` 原子地写入 queue 的 `delayed` Sorted Set 并将状态设为
`DELAYED`。维护逻辑以 Redis server time 驱动到期消息，并由 Lua 原子转入 Stream/READY；
重复维护不会重复入队，若消息已过期则转入 EQ。

## 时间权威与时钟同步

Redis server `TIME` 是 Redis backend 的唯一时间权威：提交时的 `expires_at`、租约和
维护状态迁移都与该时间比较，Lua 脚本不会采用 producer 或 worker 的本地时钟。因此部署
必须通过 NTP（或等价机制）同步 Redis 与应用主机。`RedisBroker.start()` 会在两者相差至少
5 秒时记录 warning；它不会改变既有消息状态或静默换用本地时间。集成测试和需要构造相对
到期时间的工具应从 `await broker._now()` 派生时间，而不是假定本地 `utc_now()` 与 Redis
主机一致。

`XREADGROUP` 与后续 lease 状态写入之间存在 Redis 命令边界。维护逻辑会以
`XAUTOCLAIM` 扫描“已在 PEL、状态仍为 READY”的记录，并以 Lua 将其重新写入
Stream；旧 entry ID 不能再 claim，防止进程在该窗口崩溃造成 PEL 永久滞留。

维护循环由 `consumer` 拉取前触发，也可显式调用 `await broker.maintain(queue)`。它
扫描 lease 与 expiry ZSet：超时 lease 会重新入 Stream 或进入 DLQ；到期消息进入 EQ。
Redis Cluster 不在 v0.2 兼容范围内，因为精确 dedup key 与队列 key 可能不在同一
hash slot，无法承诺跨槽 Lua 原子性。

`inspect()` 从 `ready` ZSet 读取 READY 数量与最早可消费时间，不扫描整个 Stream 或对
每个 entry 再读取一次消息 Hash。

新建 Redis key 的 namespace 和 queue 必须匹配
`[A-Za-z0-9][A-Za-z0-9._-]{0,127}`：首字符为 ASCII 字母或数字，长度最多 128。
这避免了 Redis hash tag 和分隔符产生歧义。已有 v0.2 名称仅可在显式
`allow_legacy_names=True` 时按旧规则 `[A-Za-z0-9._-]+`（最多 255 字符）读取；改名需迁移
对应 Redis keyspace，详见 [v0.2 → v0.3 迁移说明](migration-v0.2-v0.3.md)。
