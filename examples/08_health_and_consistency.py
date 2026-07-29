"""v0.5 的 health 与一致性诊断；repair 默认仅 dry-run。"""
from __future__ import annotations

import asyncio

from taskflow import SQLiteBroker


async def main() -> None:
    async with SQLiteBroker("taskflow-example.db") as broker:
        health = await broker.health_check()
        print("healthy:", health.healthy)
        for check in health.checks:
            print(check.name, check.status, check.detail or "")

        report = await broker.check_consistency("emails")
        print("consistent:", report.consistent)
        for issue in report.issues:
            print(issue.name, issue.message_id, issue.detail or "")

        proposal = await broker.repair_consistency("emails")
        print("dry-run repairs:", proposal.repairs)
        # 只有在审阅 proposal 后才执行：
        # await broker.repair_consistency("emails", dry_run=False)


if __name__ == "__main__":
    asyncio.run(main())
