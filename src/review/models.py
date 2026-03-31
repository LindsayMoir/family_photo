"""Models for review task workflows."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ReviewTask:
    """A review task loaded from PostgreSQL."""

    id: int
    entity_type: str
    entity_id: int
    task_type: str
    status: str
    priority: int
    payload_json: dict[str, object]


@dataclass(frozen=True, slots=True)
class ReviewTaskSummary:
    """Counts of open review work grouped by task type."""

    task_counts: dict[str, int]
