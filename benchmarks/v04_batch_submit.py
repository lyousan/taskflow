"""Measure SQLite v0.4 single-submit versus atomic batch-submit wall time.

Run with ``uv run python benchmarks/v04_batch_submit.py --count 1000``.  This
is informational, intentionally has no machine-dependent pass/fail threshold.
"""
from __future__ import annotations

import argparse
import asyncio
import tempfile
import time
from pathlib import Path

from taskflow import SQLiteBroker, SubmitRequest


async def measure(count: int) -> tuple[float, float]:
    with tempfile.TemporaryDirectory(prefix="taskflow-v04-batch-") as directory:
        path = Path(directory) / "tasks.db"
        async with SQLiteBroker(path) as broker:
            started = time.perf_counter()
            for index in range(count):
                await broker.submit(queue="single", payload={"index": index})
            single_seconds = time.perf_counter() - started

            started = time.perf_counter()
            results = await broker.submit_many([
                SubmitRequest(queue="batch", payload={"index": index}) for index in range(count)
            ])
            batch_seconds = time.perf_counter() - started
            assert all(result.accepted for result in results)
    return single_seconds, batch_seconds


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=1_000)
    count = parser.parse_args().count
    if count < 1:
        raise SystemExit("--count must be positive")
    single, batch = asyncio.run(measure(count))
    print(f"count={count} single_seconds={single:.6f} batch_seconds={batch:.6f} speedup={single / batch:.2f}x")


if __name__ == "__main__":
    main()
