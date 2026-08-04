# v0.5 基准数据与容量边界

运行 `uv run python benchmarks/v05_lifecycle.py --count 1000 --concurrency 32 --redis-url redis://127.0.0.1:6379/2`，
可重复覆盖 single/batch submit、claim/ACK、retry、lease reclaim、1 MiB payload、高 dedup 冲突和受限多 consumer 并发。
它不是机器无关的 CI 阈值，应在目标部署机保存输出。

## 基线记录（2026-07-28）

环境：macOS arm64、Python 3.10.20、SQLite WAL、本机 Redis、`count=1000`、32 consumer；数值为 wall time（秒）。

| backend | single submit | batch submit | claim/ACK | retry | reclaim | 1 MiB payload | dedup conflict | 32 consumer |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| SQLite | 0.323 | 0.161 | 0.906 | 0.004 | 0.024 | 0.004 | 0.321 | 0.872 |
| Redis | 0.825 | 0.298 | 3.584 | 0.007 | 0.026 | 0.025 | 0.476 | 1.085 |

## 推荐边界

- SQLite 仅适合单进程、本地脚本、测试和 CI；不要作为多主机消费者或高吞吐生产队列。数据库锁等待或持续接近 1 MiB payload 时使用 Redis。
- Redis 适合多进程/多实例。32 consumer 是本机连接池安全示例，不是默认上限；按 Redis `maxclients`、网络 RTT、payload 与 handler 时长重新压测。
- Taskqx 不承诺固定 msg/s 或容量。设置 queue payload 上限，监控 ready/leased/delayed，并为 maintenance、health、admin 操作保留连接池余量。
