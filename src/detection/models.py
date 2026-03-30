"""Models for sheet-scan photo detection."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class SheetScanRecord:
    """A sheet scan loaded from PostgreSQL for detection."""

    id: int
    batch_name: str
    original_path: Path
    width_px: int
    height_px: int


@dataclass(frozen=True, slots=True)
class DetectionCandidate:
    """A single candidate photo detection."""

    region_type: str
    contour_points: tuple[tuple[int, int], ...]
    box_points: tuple[tuple[int, int], ...]
    center_x: float
    center_y: float
    width: float
    height: float
    angle: float
    area_ratio: float
    rectangularity: float
    confidence: float
    crop_path: Path | None = None
    ocr_text: str | None = None
    ocr_confidence: float | None = None
    ocr_engine: str | None = None


@dataclass(frozen=True, slots=True)
class SheetDetectionResult:
    """Detection output for one sheet scan."""

    sheet_scan_id: int
    detection_count: int
    review_required: bool
    review_reason: str | None
    preview_path: Path | None


@dataclass(frozen=True, slots=True)
class DetectionRunSummary:
    """Summary of a detect command run."""

    target: str
    processed_count: int
    detected_count: int
    review_required_count: int
    dry_run: bool
