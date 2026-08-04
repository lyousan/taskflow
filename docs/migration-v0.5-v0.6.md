# 从 v0.5 升级到 v0.6（计划）

> 本文是 v0.6 开发期的兼容性目标，不应被当作已发布版本的升级指引。

v0.6 计划新增可选的交互式 TUI 和 shell，不改变 v0.5 的消息格式、SQLite schema、Redis keyspace、Consumer Group、投递、ACK、lease、dedup 或 DLQ/EQ replay 语义。现有 Python API 和非交互 `taskqx` 子命令应保持兼容。

## 安装与回滚目标

TUI 依赖作为可选 extra 提供：

```bash
pip install "taskqx[tui]"
```

入口为 `taskqx tui --sqlite taskqx.db` 和 `taskqx shell --sqlite taskqx.db`，并要求在 TTY 中运行。基础安装不会导入 Textual 或 `prompt_toolkit`；未安装 extra 或非 TTY 时，命令会给出可操作提示并以非零状态退出。TUI 支持 health、分页队列/消息/DLQ/EQ 浏览、搜索、payload 显式展示，以及队列级或单条的 replay/delete；所有写入都先显示影响摘要，按 `y` 确认，按 `n` 或 `Esc` 取消。consistency repair 在确认前执行 dry-run。shell 提供持久 history、Tab 补全、分页 JSON 浏览及同样的管理工作流。因为交互层不创建持久化结构，卸载 extra 或回滚到 v0.5 不需要 SQLite/Redis 数据迁移。正式发布前仍应按 v0.5 的操作手册备份 SQLite 或记录 Redis namespace，并先执行 health/consistency 检查。

## 操作语义

- TUI/REPL 只封装公开 Admin API，不能绕过既有状态机；
- payload 默认脱敏，显示 payload 需要显式用户操作；
- replay 的完成结果表示 `replay_enqueued`，不表示业务 handler 已完成；
- 非交互 CLI 的 `--yes`、以及 repair 的 `--apply --yes` 继续有效；
- UI 内相同操作需要影响摘要和二次确认。

实现完成后，本文件将补充准确的版本要求、命令、已知限制和升级验证步骤。
