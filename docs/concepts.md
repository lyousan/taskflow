# 核心概念（v0.2）

`TaskMessage` 是不可变的 JSON 兼容业务请求；`SQLiteDelivery` 是消费者领取该
消息后产生的一次短生命周期处理上下文。一次消息可能因重试或租约回收产生多次
Delivery，因此副作用处理必须具有幂等性。

状态流转为：`READY -> LEASED -> ACKED`，或 `LEASED -> READY`（立即重试/回收），
或 `LEASED -> DEAD_LETTERED`。`submit(delay=...)` 与 `retry(delay=...)` 会进入
`DELAYED`，在 `available_at` 到期后幂等地转为 `READY`。`READY`、`DELAYED`、`LEASED`
消息在到期后转入 `EXPIRED`。

终结操作使用 delivery ID 与私有 lease token 双重校验。被回收后的旧 worker 即使
恢复，也会得到 `LeaseLostError`，无法确认新 worker 已领取的消息。

提交去重只抑制同一 `dedup_scope + dedup_key` 在 TTL 内的重复提交；它不会消除
at-least-once 投递带来的重复业务执行。
