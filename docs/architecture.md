# Taskflow 模块边界

Taskflow 的 backend 文件只负责 backend-specific 状态迁移；公共执行语义通过 Protocol
和独立模块复用。

```text
taskflow/
├── worker.py                 # 并发、异常分类、heartbeat、优雅关闭
├── retry.py                  # RetryPolicy 与 Backoff
├── protocols.py              # Broker / Consumer / Delivery / Store 契约
├── broker/
│   ├── _time.py              # SQLite / Redis 共享 UTC 时间和 ID 辅助函数
│   ├── sqlite.py             # SQLite 事务状态迁移、SubmissionStore、Admin
│   ├── sqlite_components.py  # SQLite Delivery / Consumer 适配器
│   ├── redis.py              # Redis Lua 状态迁移、SubmissionStore、Admin
│   └── redis_components.py   # Redis Delivery / Consumer 适配器
└── submission/               # PreparedSubmission 与提交扩展点
```

两个 backend 仍保留各自的 Admin 和 Lua/SQL 状态迁移，因为这些部分依赖不同的持久化
原子边界；Delivery、Consumer、时间转换和 Worker 配置则不再混在单个 broker 类中。
后续 v0.3 若继续扩展管理 API，应优先把 Admin 移入 `broker/admin/`，并将 Redis 脚本移入
`broker/redis_scripts/`，避免重新把状态迁移和公共 API 混合到同一文件。

## 测试组织

`tests/conformance/test_backend_v02.py` 是 SQLite / Redis 共用的高层行为契约，验证两种
backend 都必须满足的 Worker、retry、delayed 和 heartbeat 语义。`test_sqlite_broker.py`
与 `test_redis_broker.py` 只保留事务、Lua、PEL、索引和 replay 等 backend-specific 边界；
新增共享语义应先进入 conformance suite，避免两个测试文件逐渐漂移。
