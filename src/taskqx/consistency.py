"""Read-only integrity reports and explicit repair results."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ConsistencyIssue:
    """A broken persisted lifecycle invariant."""

    name: str
    message_id: str | None = None
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class ConsistencyReport:
    """All issues found for one queue; an empty list means consistent."""

    queue: str
    backend: str
    namespace: str | None
    issues: tuple[ConsistencyIssue, ...]

    @property
    def consistent(self) -> bool:
        return not self.issues


@dataclass(frozen=True, slots=True)
class RepairReport:
    """The repair actions proposed or applied for a consistency report."""

    queue: str
    backend: str
    namespace: str | None
    dry_run: bool
    repairs: tuple[ConsistencyIssue, ...]


@dataclass(frozen=True, slots=True)
class KeyspaceMigrationReport:
    """Result of an explicit Redis message-keyspace migration."""

    namespace: str
    dry_run: bool
    migrated: tuple[str, ...]
    resumed: tuple[str, ...]
    conflicts: tuple[ConsistencyIssue, ...]
