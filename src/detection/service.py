"""Application service for sheet photo detection."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from config import AppConfig
from db.connection import connect
from detection.analysis import (
    detect_sheet_regions,
    has_duplicate_wide_photo_candidate,
    render_detection_preview,
    write_candidate_crop,
)
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
    enable_ocr: bool = False,
    dry_run: bool,
) -> DetectionRunSummary:
    """Run the first-pass photo detection pipeline."""
    with connect(config) as conn:
        sheets = get_sheet_scans(
            conn,
            batch_name=batch_name,
            sheet_id=sheet_id,
            limit=limit,
            pending_only=batch_name is not None and sheet_id is None,
        )

    if not sheets:
        target = batch_name if batch_name is not None else f"sheet_id={sheet_id}"
        raise ValueError(f"No pending sheet scans found for target '{target}'.")

    detection_results: list[SheetDetectionResult] = []
    total_candidates = 0
    review_required_count = 0

    if dry_run:
        for sheet in sheets:
            analysis_result = detect_sheet_regions(
                sheet.original_path,
                fast_mode=fast_mode,
                enable_ocr=enable_ocr,
            )
            candidates = _materialize_region_outputs(
                config.photos_root,
                sheet,
                analysis_result.candidates,
                write_files=False,
            )
            review_required, review_reason = _review_decision(
                sheet,
                candidates,
                image_path=sheet.original_path,
            )
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
                analysis_result = detect_sheet_regions(
                    sheet.original_path,
                    fast_mode=fast_mode,
                    enable_ocr=enable_ocr,
                )
                candidates = _materialize_region_outputs(
                    config.photos_root,
                    sheet,
                    analysis_result.candidates,
                    write_files=True,
                )
                review_required, review_reason = _review_decision(
                    sheet,
                    candidates,
                    image_path=sheet.original_path,
                )
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
                    ocr_request_reason=analysis_result.ocr_request_reason,
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


def _review_decision(
    sheet: SheetScanRecord,
    candidates: list[DetectionCandidate],
    *,
    image_path: Path,
) -> tuple[bool, str | None]:
    candidate_count = len(candidates)
    if candidate_count == 0:
        return True, "no_candidates"
    photo_candidates = [candidate for candidate in candidates if candidate.region_type == "photo"]
    photo_count = len(photo_candidates)
    if photo_count > 6:
        return True, "too_many_photo_candidates"
    if _has_suspicious_overlapping_photos(photo_candidates):
        return True, "suspicious_overlapping_photo_candidates"
    if _has_suspicious_wide_photo_candidate(sheet, photo_candidates):
        return True, "suspicious_wide_photo_candidate"
    if has_duplicate_wide_photo_candidate(image_path, photo_candidates):
        return True, "duplicate_wide_photo_candidate"
    if any(candidate.confidence < 0.60 for candidate in candidates):
        return True, "low_confidence_candidates"
    return False, None


def _has_suspicious_overlapping_photos(photo_candidates: list[DetectionCandidate]) -> bool:
    for index, left in enumerate(photo_candidates):
        for right in photo_candidates[index + 1 :]:
            overlap_ratio = _intersection_over_smaller_area(left, right)
            if overlap_ratio >= 0.22:
                return True
    return False


def _has_suspicious_wide_photo_candidate(
    sheet: SheetScanRecord,
    photo_candidates: list[DetectionCandidate],
) -> bool:
    if len(photo_candidates) < 3:
        return False

    for candidate in photo_candidates:
        x1, y1, x2, y2 = _bounds(candidate.box_points)
        width_ratio = (x2 - x1) / float(sheet.width_px)
        height_ratio = (y2 - y1) / float(sheet.height_px)
        if width_ratio >= 0.82 and height_ratio <= 0.40 and candidate.area_ratio >= 0.18:
            return True
    return False


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


def _intersection_over_smaller_area(left: DetectionCandidate, right: DetectionCandidate) -> float:
    left_bounds = _bounds(left.box_points)
    right_bounds = _bounds(right.box_points)

    inter_x1 = max(left_bounds[0], right_bounds[0])
    inter_y1 = max(left_bounds[1], right_bounds[1])
    inter_x2 = min(left_bounds[2], right_bounds[2])
    inter_y2 = min(left_bounds[3], right_bounds[3])

    if inter_x2 <= inter_x1 or inter_y2 <= inter_y1:
        return 0.0

    intersection = float((inter_x2 - inter_x1) * (inter_y2 - inter_y1))
    left_area = float((left_bounds[2] - left_bounds[0]) * (left_bounds[3] - left_bounds[1]))
    right_area = float((right_bounds[2] - right_bounds[0]) * (right_bounds[3] - right_bounds[1]))
    smaller_area = min(left_area, right_area)
    if smaller_area <= 0:
        return 0.0
    return intersection / smaller_area


def _bounds(points: tuple[tuple[int, int], ...]) -> tuple[int, int, int, int]:
    xs = [point[0] for point in points]
    ys = [point[1] for point in points]
    return min(xs), min(ys), max(xs), max(ys)
