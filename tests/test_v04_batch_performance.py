"""Repeatable v0.4 batch-performance and Redis round-trip evidence."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from taskqx import RedisSubmissionStore
from taskqx.submission.base import PreparedSubmission


def _submission(index: int) -> PreparedSubmission:
    return PreparedSubmission(
        f"message-{index}",
        "jobs",
        b"{}",
        "ready",
        datetime.now(timezone.utc),
        None,
        None,
        None,
        None,
        3,
        "json",
        "1",
    )


class _EvalSpy:
    """Minimal Redis adapter recording only network-equivalent EVAL boundaries."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, int, tuple[str, ...]]] = []

    async def eval(self, script: str, numkeys: int, *items: str) -> list[object]:
        self.calls.append((script, numkeys, items))
        args = items[numkeys:]
        if numkeys == 8:
            return [1, args[1], args[2], "1-0"]
        count = int(args[0])
        result: list[object] = []
        for index in range(count):
            offset = 1 + index * 13
            result.extend([1, args[offset + 1], args[offset + 2], f"{index + 1}-0"])
        return result


@pytest.mark.asyncio
async def test_redis_batch_submit_uses_one_eval_vs_one_per_single_submit() -> None:
    single_client = _EvalSpy()
    single_store = RedisSubmissionStore(single_client, namespace="taskqx-rtt-single")
    for index in range(12):
        assert (await single_store.submit(_submission(index))).accepted
    assert len(single_client.calls) == 12

    batch_client = _EvalSpy()
    batch_store = RedisSubmissionStore(batch_client, namespace="taskqx-rtt-batch")
    results = await batch_store.submit_many([_submission(index) for index in range(12)])
    assert all(item.accepted for item in results)
    assert len(batch_client.calls) == 1
    assert len(batch_client.calls) < len(single_client.calls)
