"""Models for export audit reporting."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ExportAuditRecord:
    """One exported photo plus the metadata needed for classification."""

    photo_id: int
    batch_name: str
    sheet_scan_id: int
    crop_index: int
    raw_crop_path: Path
    working_path: Path
    export_path: Path
    status: str
    rotation_degrees: int | None
    accepted_detection_id: int | None
    detection_confidence: float | None
    detection_reviewed_by_human: bool
    detection_width: float | None
    detection_height: float | None
    has_open_orientation_review: bool
    orientation_review_reason: str | None


@dataclass(frozen=True, slots=True)
class ExportAuditFinding:
    """One audit classification for an exported photo."""

    photo_id: int
    batch_name: str
    sheet_scan_id: int
    crop_index: int
    category: str
    reason: str
    export_path: Path
    auto_rotation_suggestion: int
    auto_rotation_confidence: float
    review_priority: str
    suggested_issue: str | None
    suggested_issue_confidence: float | None
    suggested_issue_reason: str | None


@dataclass(frozen=True, slots=True)
class ExportAuditSummary:
    """Summary of an export audit run."""

    target: str
    audited_count: int
    category_counts: dict[str, int]
    findings: list[ExportAuditFinding]
    csv_path: Path | None
    dry_run: bool
