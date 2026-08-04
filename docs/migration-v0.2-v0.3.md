# v0.2 → v0.3 迁移说明

v0.3 保持现有 `submit()`、`consumer()`、`worker()` 和 `Delivery` 生命周期兼容；SQLite
schema 与 Redis key 格式未改变。新部署使用严格名称规则；已有 v0.2 数据可先使用显式
兼容模式读取，不需要立即迁移 key 或表中数据。

可选地在 broker 创建时增加：

```python
SQLiteBroker(
    "tasks.db",
    queues={"emails": QueueConfig(max_attempts=5)},
    event_sink=my_event_sink,
)
```

命名校验收紧为首字符必须是 ASCII 字母或数字、总长度不超过 128。若已有 queue、Redis
namespace 或 profile 使用 v0.2 合法但 v0.3 不再推荐的名称（如 `_legacy`、`.queue`），
请在过渡期间传入 `allow_legacy_names=True`：

```python
RedisBroker.from_url(namespace="_legacy", allow_legacy_names=True)
SQLiteBroker("tasks.db", allow_legacy_names=True)
```

该模式是显式兼容开关，应用应仅在读取、迁移或仍须与旧名称互操作时启用；新
queue/namespace/profile 应改为严格格式。若要改名，必须在停机窗口内迁移 SQLite 的 queue
字段或 Redis 整个 namespace keyspace，不能假定改名后仍会自动读取旧数据。历史消息继续按持久化的 serializer
name/version 解码；缺少 decoder 时现在抛出 `SerializerUnavailableError`，应用可以据此
把消息转入显式 poison-message 处理路径。

`BrokerEvent` 保留 v0.2 的 `name=` 构造器、字段顺序和 `.name` 访问；新 EventSink 代码
建议使用标准 `TaskqxEvent.event_name`。
