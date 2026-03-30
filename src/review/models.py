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
class DetectionReviewCandidate:
    """A detection candidate attached to a sheet-level review task."""

    id: int
    region_type: str
    confidence: float
    accepted: bool
    crop_path: str | None


@dataclass(frozen=True, slots=True)
class SheetReviewBacklog:
    """Batch-level review backlog summary."""

    batch_name: str | None
    sheet_status_counts: dict[str, int]
    open_tasks: list[ReviewTask]
