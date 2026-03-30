"""Application service for sheet photo detection."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from config import AppConfig
from db.connection import connect
from detection.analysis import detect_sheet_regions, render_detection_preview, write_candidate_crop
from detection.models import (
    DetectionCandidate,
    DetectionRunSummary,
    SheetDetectionResult,
    SheetScanRecord,
)
from detection.repository import get_sheet_scans, replace_detections


DETECTION_METHOD = "opencv_contours_v1"
PIPELINE_VERSION = "detection_v1"


def run_detection(
    config: AppConfig,
    *,
    batch_name: str | None = None,
    sheet_id: int | None = None,
    limit: int | None = None,
    fast_mode: bool = False,
    dry_run: bool,
) -> DetectionRunSummary:
    """Run the first-pass photo detection pipeline."""
    with connect(config) as conn:
        sheets = get_sheet_scans(conn, batch_name=batch_name, sheet_id=sheet_id, limit=limit)

    if not sheets:
        target = batch_name if batch_name is not None else f"sheet_id={sheet_id}"
        raise ValueError(f"No sheet scans found for target '{target}'.")

    detection_results: list[SheetDetectionResult] = []
    total_candidates = 0
    review_required_count = 0

    if dry_run:
        for sheet in sheets:
            candidates = _materialize_region_outputs(
                config.photos_root,
                sheet,
                detect_sheet_regions(sheet.original_path, fast_mode=fast_mode),
                write_files=False,
            )
            review_required, review_reason = _review_decision(candidates)
            preview_path = _preview_path(config.photos_root, sheet) if review_required else None
            detection_results.append(
                SheetDetectionResult(
                    sheet_scan_id=sheet.id,
                    detection_count=len(candidates),
                    review_required=review_required,
                    review_reason=review_reason,
                    preview_path=preview_path,
                )
            )
            total_candidates += len(candidates)
            review_required_count += int(review_required)
    else:
        with connect(config) as conn:
            for sheet in sheets:
                _cleanup_region_outputs(config.photos_root, sheet)
                candidates = _materialize_region_outputs(
                    config.photos_root,
                    sheet,
                    detect_sheet_regions(sheet.original_path, fast_mode=fast_mode),
                    write_files=True,
                )
                review_required, review_reason = _review_decision(candidates)
                preview_path: Path | None = None

                preview_path = render_detection_preview(
                    sheet.original_path,
                    candidates,
                    _preview_path(config.photos_root, sheet),
                )

                detection_count = replace_detections(
                    conn,
                    sheet_scan_id=sheet.id,
                    candidates=candidates,
                    detection_method=DETECTION_METHOD,
                    pipeline_version=PIPELINE_VERSION,
                    review_required=review_required,
                    review_reason=review_reason,
                    preview_path=str(preview_path) if preview_path is not None else None,
                )
                detection_results.append(
                    SheetDetectionResult(
                        sheet_scan_id=sheet.id,
                        detection_count=detection_count,
                        review_required=review_required,
                        review_reason=review_reason,
                        preview_path=preview_path,
                    )
                )
                total_candidates += detection_count
                review_required_count += int(review_required)
            conn.commit()

    target = batch_name if batch_name is not None else f"sheet_id={sheet_id}"
    return DetectionRunSummary(
        target=target,
        processed_count=len(detection_results),
        detected_count=total_candidates,
        review_required_count=review_required_count,
        dry_run=dry_run,
    )


def _review_decision(candidates: list[DetectionCandidate]) -> tuple[bool, str | None]:
    candidate_count = len(candidates)
    if candidate_count == 0:
        return True, "no_candidates"
    photo_count = sum(1 for candidate in candidates if candidate.region_type == "photo")
    if photo_count > 6:
        return True, "too_many_photo_candidates"
    if any(candidate.confidence < 0.60 for candidate in candidates):
        return True, "low_confidence_candidates"
    return False, None


def _preview_path(photos_root: Path, sheet: SheetScanRecord) -> Path:
    return (
        photos_root
        / "derivatives"
        / "review"
        / "detections"
        / sheet.batch_name
        / f"sheet_{sheet.id}.jpg"
    )


def _materialize_region_outputs(
    photos_root: Path,
    sheet: SheetScanRecord,
    candidates: list[DetectionCandidate],
    *,
    write_files: bool,
) -> list[DetectionCandidate]:
    materialized: list[DetectionCandidate] = []
    for index, candidate in enumerate(candidates, start=1):
        crop_path = _region_crop_path(photos_root, sheet, candidate.region_type, index)
        if write_files:
            write_candidate_crop(sheet.original_path, candidate, crop_path)
        materialized.append(replace(candidate, crop_path=crop_path))
    return materialized


def _region_crop_path(photos_root: Path, sheet: SheetScanRecord, region_type: str, index: int) -> Path:
    return (
        photos_root
        / "derivatives"
        / "review"
        / "regions"
        / sheet.batch_name
        / f"sheet_{sheet.id}_{index}_{region_type}.jpg"
    )


def _cleanup_region_outputs(photos_root: Path, sheet: SheetScanRecord) -> None:
    region_dir = photos_root / "derivatives" / "review" / "regions" / sheet.batch_name
    if not region_dir.exists():
        return
    for path in region_dir.glob(f"sheet_{sheet.id}_*"):
        if path.is_file():
            path.unlink()
