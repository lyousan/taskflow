# 从 v0.3 升级到 v0.4

v0.4 不改变消息持久化格式、queue/namespace 命名或 at-least-once 语义。升级前请备份 SQLite
数据库，或为 Redis 部署使用独立 namespace；首次启动会继续兼容既有 envelope。

## API 变化

- `submit()`、`SubmitRequest`、`worker()` 和 `run()` 新增可选 `payload_type`。dataclass 可由
  实例推断；TypedDict 的原始 dict 必须显式传入类型；Pydantic 仅正式支持 v2，需安装
  `taskqx[pydantic]`。
- `submit_many(..., atomic=False)` 不再在第一项异常时抛出。它返回
  `list[BatchSubmitItemResult]`，每一项按输入顺序保存 `result` 或 `error`。`atomic=True`
  仍返回 `list[SubmitResult]` 并保持全有或全无。
- `replay_dead_letter(..., payload=..., payload_type=...)` 可安全重放类型化 payload。未传
  `payload_type` 的原始 dict 覆盖会清除旧 schema；请不要依赖旧版本的 stale schema 行为。
  v0.3 的 `payload=None` 仍表示“不覆盖 payload”。若需要明确将消息改为 JSON `null`，必须
  传入 `replace_payload=True, payload=None`，避免历史调用方被静默改写。
- replay 的 `dedup_mode` 为 `keep`、`remove`、`replace`。旧 `reuse_dedup=True/False` 参数
  仍可用；新代码应改用显式三态模式。

## 运行依赖

```bash
pip install 'taskqx[redis,pydantic]'
```

Redis 和 Pydantic 都是可选能力。没有安装 Pydantic 时，dataclass/TypedDict 仍完全可用；使用
Pydantic model 时会收到明确的 payload validation 错误。Pydantic v1 不属于 v0.4 支持范围。

## 运维与回滚

CLI replay 是改变状态的操作，必须加 `--yes`。CLI 默认 redaction payload；只有在受控终端中
临时添加 `--include-payload`。`taskqx health` 只探测 broker 连接，不能代替队列、索引或
Consumer Group 的完整健康检查。

若需回滚到 v0.3，请先停止 v0.4 worker，确保没有依赖 payload schema 的 v0.4 handler 在运行；
已有 schema 字段会被 v0.3 envelope reader 忽略，但 v0.3 不会执行类型化解码。
