# Contributing

请保持 SQLite/Redis 行为对称、保留 at-least-once 语义，并为公共 API 添加类型、文档和跨 backend
测试。提交前运行：

```bash
uv run ruff check src tests
uv run mypy src tests
uv run pytest --cov=taskqx -q
```

不要通过 `Any` 或私有实现断言绕过公共契约。破坏性管理 API 必须具有 dry-run 或显式确认边界。
