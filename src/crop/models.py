"""Models for crop promotion."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class AcceptedPhotoDetection:
    """A photo detection ready to be promoted into a photo record."""

    detection_id: int
    sheet_scan_id: int
    crop_path: Path
    width_px: int
    height_px: int


@dataclass(frozen=True, slots=True)
class CropRunSummary:
    """Summary of a crop promotion run."""

    target: str
    promoted_count: int
    skipped_count: int
    dry_run: bool
