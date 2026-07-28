"""Structured, backend-neutral diagnostic results."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

HealthStatus = Literal["ok", "warning", "error"]


@dataclass(frozen=True, slots=True)
class HealthCheck:
    """One non-mutating diagnostic check performed by a broker."""

    name: str
    status: HealthStatus
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class HealthReport:
    """The diagnostic result returned by :meth:`TaskBroker.health_check`."""

    healthy: bool
    backend: str
    namespace: str | None
    checks: tuple[HealthCheck, ...]
