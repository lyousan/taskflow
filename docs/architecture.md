# Taskflow 模块边界

Taskflow 的目标是让 backend 文件只负责生命周期装配和公共入口。当前 SQLite 与 Redis 的
投递状态机、maintenance、Redis lifecycle observability、Admin、Redis Lua 调用布局均已独立。
提交 Store、profile 路由、batch 编排与已提交观测已在 `submission/`；参数准备仍有
backend-specific envelope adapter，不能以本文件弱化 `docs/roadmap.md` 的 v0.4 发布门槛。

```text
taskflow/
├── worker.py                 # 并发、异常分类、heartbeat、优雅关闭
├── retry.py                  # RetryPolicy 与 Backoff
├── protocols.py              # Broker / Consumer / Delivery / Store 契约
├── broker/
│   ├── _time.py              # SQLite / Redis 共享 UTC 时间和 ID 辅助函数
│   ├── sqlite.py             # SQLite 生命周期装配与事务状态迁移
│   ├── sqlite_admin.py       # SQLite DLQ/EQ 查询、删除与 replay 事务
│   ├── sqlite_components.py  # SQLite Delivery / Consumer 适配器
│   ├── sqlite_maintenance.py # SQLite maintenance 事务与提交后观测编排
│   ├── redis.py              # Redis 生命周期装配、公共入口与兼容委派
│   ├── redis_admin.py        # Redis DLQ/EQ 查询、删除与 Lua replay 调用
│   ├── redis_calls.py        # RedisScriptCall 与所有 KEYS/ARGV builder
│   ├── redis_components.py   # Redis Delivery / Consumer 适配器
│   ├── redis_maintenance.py  # Redis delayed/expiry/reclaim 维护编排
│   ├── redis_observability.py# Redis 提交后 lifecycle event/metric 隔离
│   ├── redis_scripts.py      # 所有具名 Redis Lua 与 key/argv contract
│   └── redis_state_machine.py# Redis claim/finish/extend 与 PEL recovery
└── submission/
    ├── base.py               # PreparedSubmission 与 callback adapter
    ├── observability.py      # 已提交/重复/初始过期的统一观测 adapter
    ├── redis.py              # Redis Store、batch 与 exact-dedup 准入
    ├── routing.py            # SubmissionStore profile 校验与 queue 路由
    ├── service.py            # single/batch middleware、profile 与 observation 编排
    └── sqlite.py             # SQLite Store、batch 事务与 exact-dedup 准入
```

SQLite 与 Redis Admin/replay 均独立于 Broker；Redis Admin 只编排其具名 Lua 的多 key
原子边界，不在 CLI 或 Broker 中重复实现状态迁移。Delivery、Consumer、时间转换、Worker 配置、
SubmissionStore profile 路由、提交观测和 Redis 脚本文本不再由两个后端重复实现。
`SubmissionRouter` 是唯一允许 duck-typed Store 扩展进入的 adapter 边界，同时保留
`broker.submission_store` 的既有可替换行为。`SubmissionObserver` 保证 single、atomic batch
和 non-atomic batch 均只会为每一项已提交结果产生同一套 submitted/duplicate/expired 事件。
`*_maintenance` 在状态已提交后才发布指标和事件，Broker 的 `maintain()` 仅保留兼容委派入口。
每次 Redis `EVAL` 都通过 `RedisScriptCall` 调用，单元测试直接比较完整 `KEYS`、`ARGV` 与
`numkeys` 布局。SQLite delivery lifecycle observability 仍可进一步与 Redis 的独立 adapter
完全对称；这不改变当前持久化和提交后观测边界。

## 测试组织

`tests/conformance/test_backend_v04.py` 是 SQLite / Redis 共用的公开 Protocol 行为契约，
验证提交、Worker、retry、delayed、heartbeat、cancellation、typed payload、batch、expiry、
DLQ replay、EQ replay 和 stats 语义，并在 Redis fixture 结束时清理随机 namespace。`test_sqlite_broker.py`
与 `test_redis_broker.py` 只保留事务、Lua、PEL、索引和 replay 等 backend-specific 边界；
新增共享语义应先进入 conformance suite，避免两个测试文件逐渐漂移。
