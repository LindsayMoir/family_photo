"""Application service for crop promotion."""

from __future__ import annotations

from pathlib import Path
import shutil

from config import AppConfig
from crop.models import CropRunSummary
from crop.repository import (
    get_accepted_photo_detections,
    get_existing_photo_indices,
    upsert_photo_from_detection,
)
from db.connection import connect


def run_crop(config: AppConfig, *, sheet_id: int, dry_run: bool) -> CropRunSummary:
    """Promote accepted photo detections into canonical photo records."""
    with connect(config) as conn:
        detections = get_accepted_photo_detections(conn, sheet_id=sheet_id)
        existing_crop_indices = get_existing_photo_indices(conn, sheet_id=sheet_id)

    if not detections:
        raise ValueError(f"No accepted photo detections found for sheet_id={sheet_id}.")

    skipped_count = len(existing_crop_indices.intersection(set(range(1, len(detections) + 1))))

    if dry_run:
        return CropRunSummary(
            target=f"sheet_id={sheet_id}",
            promoted_count=len(detections),
            skipped_count=skipped_count,
            dry_run=True,
        )

    promoted_count = 0
    with connect(config) as conn:
        for crop_index, detection in enumerate(detections, start=1):
            raw_crop_path = _canonical_raw_crop_path(config.photos_root, sheet_id, crop_index)
            _copy_crop_file(detection.crop_path, raw_crop_path)
            upsert_photo_from_detection(
                conn,
                detection=detection,
                crop_index=crop_index,
                raw_crop_path=raw_crop_path,
            )
            promoted_count += 1
        conn.commit()

    return CropRunSummary(
        target=f"sheet_id={sheet_id}",
        promoted_count=promoted_count,
        skipped_count=skipped_count,
        dry_run=False,
    )


def _canonical_raw_crop_path(photos_root: Path, sheet_id: int, crop_index: int) -> Path:
    return photos_root / "crops" / f"sheet_{sheet_id}" / f"crop_{crop_index}.jpg"


def _copy_crop_file(source_path: Path, destination_path: Path) -> None:
    if not source_path.exists():
        raise ValueError(f"Crop source file does not exist: {source_path}")
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, destination_path)
