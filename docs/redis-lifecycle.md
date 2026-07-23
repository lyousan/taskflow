# Redis 消息生命周期设计

本文补充 [PRD](PRD.md) 的 Redis Streams backend 设计，说明 v0.1 中 submit、claim、ack、retry、reject、lease reclaim 与过期队列（EQ）的状态和原子边界。

## Redis 数据模型

以 `crawl.fetch` 为例：

```text
taskflow:queue:{crawl.fetch}:stream
taskflow:queue:{crawl.fetch}:state
taskflow:queue:{crawl.fetch}:leases
taskflow:queue:{crawl.fetch}:expiry
taskflow:queue:{crawl.fetch}:dlq
taskflow:queue:{crawl.fetch}:eq
taskflow:queue:{crawl.fetch}:stats
```

| Key | 类型 | 作用 |
|---|---|---|
| `stream` | Stream | 主消息队列，Consumer Group 消费来源 |
| `state` | Hash | 每条消息的状态、attempt、当前 delivery 与 lease token |
| `leases` | ZSet | score 为 `lease_until_ms`，reclaimer 扫描来源 |
| `expiry` | ZSet | score 为 `expires_at_ms`，EQ 维护扫描来源 |
| `dlq` | Stream | 死信消息与失败审计 |
| `eq` | Stream | 过期消息与过期审计 |

主 Stream 的 entry 保存 `message_id` 和不可变 Message envelope JSON。`state` 保存可变投递状态；Stream entry ID 不是业务 `message_id`。

所有时间使用 Redis 服务端 `TIME` 生成的 UTC epoch milliseconds，不能以 worker 的本地时钟作为状态迁移依据。

## 状态与 attempt

```text
submit -> READY (attempt=0)
READY -> claim -> LEASED (attempt=1)
LEASED -> retry -> READY (attempt 保持 1)
READY -> claim -> LEASED (attempt=2)
LEASED -> ack -> ACKED
LEASED -> reject -> DEAD_LETTERED -> DLQ
READY / LEASED -> expires -> EXPIRED -> EQ
```

`attempt` 是总 Delivery 次数。`max_attempts=3` 表示业务 handler 最多真正领取三次；第三次调用 retry 或第三次 lease 回收时进入 DLQ。

v0.1 的 retry 是立即重新投递；`delay`、延迟 retry 与 DELAYED 状态属于 v0.2。

## Delivery 与 lease token

每次 claim 生成新的：

```text
delivery_id
lease_token
lease_until_ms
```

主动操作必须校验当前 `delivery_id + lease_token`。这样 worker-a 的 lease 被回收后，即使它恢复并迟到 ACK，也不能确认 worker-b 已重新领取的消息。旧消费者收到 `LeaseLostError`。

同一 delivery 的同一种终结操作必须幂等：例如重复 `ack()` 返回 `ALREADY_ACKED`。不同操作或不同 delivery 的迟到请求必须失败，不能改变当前状态。

## 提交

精确 String Dedup 提交由 Lua Script 原子执行：

```text
GET / SET dedup key NX PX ttl
  -> XADD stream
  -> HSET state READY
  -> ZADD expiry（有 expires_at 时）
  -> HINCRBY submitted_total
```

若 dedup key 已存在，脚本返回 `DUPLICATE`，不写入 Stream。详细架构见 [提交与去重架构](submission-and-dedup.md)。

## Claim

消费者先使用：

```text
XREADGROUP GROUP <group> <consumer> STREAMS <stream> >
```

Redis Consumer Group 的 PEL 记录领取信息。Broker 在将消息暴露给业务 handler 前，以脚本：

1. 确认 state 为 `READY`；
2. 检查 `expires_at_ms`，已过期则转 EQ，不交给业务；
3. 将 attempt 加一；
4. 生成 `delivery_id` 和 `lease_token`；
5. 写 state 为 `LEASED`；
6. `ZADD leases lease_until_ms message_id`。

## ACK

`ack()` 脚本按顺序：

1. 识别同一 delivery 的重复 ACK；
2. 校验当前 `LEASED` 状态、delivery ID 与 lease token；
3. 检查消息是否已到 `expires_at`；到期则转 EQ；
4. `XACK stream group entry_id`；
5. `XDEL stream entry_id`；
6. `ZREM leases message_id` 和 `ZREM expiry message_id`；
7. state 写为 `ACKED`，增加 `acked_total`。

ACK 与 reclaim 同时发生时，Redis 脚本串行执行：先完成的一方决定结果。reclaim 先完成时，迟到 ACK 因 token 不匹配而失败。

## Retry

v0.1：`retry(reason=...)` 为立即重投。脚本：

1. 幂等与 lease token 校验；
2. 检查过期，过期优先转 EQ；
3. 若 `attempt >= max_attempts`，写入 DLQ；
4. 否则 `XACK` + `XDEL` 旧 entry；
5. 用同一不可变 envelope `XADD` 新 entry；
6. 删除 lease ZSet 条目，state 改为 `READY`；
7. 保留 expiry ZSet 条目，记录 retry reason。

新 Stream entry ID 会变化，但 `message_id` 不变；下一次 claim 时 attempt 才加一。

## Reject

`reject(reason, error)` 脚本：

1. 幂等与 lease token 校验；
2. 已过期时优先转 EQ；
3. `XADD dlq`，写入原始消息、最后 Delivery、attempt、reason、异常类型和可选 traceback；
4. `XACK` + `XDEL` 主 Stream entry；
5. 清理 lease / expiry 索引；
6. state 改为 `DEAD_LETTERED`。

## Lease reclaim 与过期

maintenance loop 是 v0.1 必需组件，但它不是 v0.2 的延迟投递调度器。它负责：

```text
ZRANGEBYSCORE leases -inf now_ms  -> reclaim 超时 lease
ZRANGEBYSCORE expiry -inf now_ms  -> 转移过期消息至 EQ
```

lease reclaim 的优先级：

```text
已过期                -> EQ
未过期且达到上限      -> DLQ
其他情况              -> XACK + XDEL 旧 entry，再 XADD 回 READY
```

过期检查还必须发生在 claim、ack、retry、reject 和 extend_lease 时。因此即使维护循环尚未来得及搬运，已过期消息也不会重新交给业务处理。EQ 的物理转移允许有 maintenance interval 带来的最终一致延迟。

`extend_lease()` 同样校验 token，并且新的 `lease_until` 不得晚于 `expires_at`。

## Redis Cluster

v0.1 目标为 Redis standalone / Sentinel。精确 dedup 的 key 与 queue key 在 Redis Cluster 下可能不在同一 hash slot，不能承诺跨 slot Lua 原子提交。Redis Cluster 的 key-slot 设计和语义保证属于 v0.2。
