"""Backend-only lifecycle scheduler.

A scheduler advances persisted message state without claiming messages or running
application handlers.  Multiple scheduler processes are safe: each backend
maintenance operation is conditional and idempotent.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from datetime import timedelta
from typing import Protocol

from typing_extensions import Self

from .errors import ValidationError

logger = logging.getLogger(__name__)


class SchedulerBackend(Protocol):
    async def maintain(self, queue: str | None = None) -> int: ...
    async def _scheduler_queues(self) -> Iterable[str]: ...


class BackendScheduler:
    """Periodically run backend maintenance for selected queues.

    ``queues=None`` discovers persisted queues before every tick.  ``run()``
    starts the scheduler and waits until ``close()`` is called or its task is
    cancelled.  A failing tick is logged and retried on the next interval;
    it never stops independent workers.
    """

    def __init__(
        self,
        backend: SchedulerBackend,
        *,
        queues: Iterable[str] | None = None,
        interval: timedelta = timedelta(seconds=1),
    ) -> None:
        if not isinstance(interval, timedelta) or interval.total_seconds() <= 0:
            raise ValidationError("scheduler interval 必须为正数 timedelta")
        self._backend = backend
        self._queues = tuple(queues) if queues is not None else None
        self._interval = interval.total_seconds()
        self._closed = False
        self._task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()

    async def start(self) -> None:
        """Start the maintenance loop; repeated calls are idempotent."""

        if self._closed:
            raise ValidationError("已关闭的 scheduler 不能重新启动")
        if self._task is None:
            self._task = asyncio.create_task(self._loop(), name="taskqx:scheduler")

    async def close(self) -> None:
        """Stop future ticks and wait for the current tick to complete."""

        if self._closed:
            return
        self._closed = True
        self._stopped.set()
        task = self._task
        if task is not None and task is not asyncio.current_task():
            await task

    async def tick(self) -> int:
        """Run one idempotent maintenance pass for every selected queue."""

        if self._closed:
            return 0
        queues = (
            self._queues
            if self._queues is not None
            else tuple(await self._backend._scheduler_queues())
        )
        count = 0
        for queue in queues:
            count += await self._backend.maintain(queue)
        return count

    async def run(self) -> None:
        """Run until closed or externally cancelled."""

        await self.start()
        try:
            await self._stopped.wait()
        finally:
            await self.close()

    async def _loop(self) -> None:
        try:
            while not self._closed:
                try:
                    await self.tick()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.exception(
                        "taskqx scheduler tick failed",
                        extra={
                            "backend": type(self._backend).__name__,
                            "namespace": getattr(self._backend, "_namespace", None),
                            "queue": None,
                            "message_id": None,
                            "delivery_id": None,
                            "consumer_id": None,
                            "attempt": None,
                            "max_attempts": None,
                            "action": "maintain",
                            "outcome": "failed",
                            "retry_delay": None,
                            "error_type": type(exc).__name__,
                        },
                    )
                try:
                    await asyncio.wait_for(self._stopped.wait(), timeout=self._interval)
                except asyncio.TimeoutError:
                    pass
        finally:
            self._closed = True
            self._stopped.set()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()
