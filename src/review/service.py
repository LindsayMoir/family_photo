"""Application service for review workflows."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from config import AppConfig
from db.connection import connect
from detection.analysis import write_candidate_crop
from detection.models import DetectionCandidate, SheetScanRecord
from detection.repository import get_sheet_scans
from orientation.service import run_orientation
from review.models import ReviewTask, SheetReviewBacklog
from review.repository import (
    accept_detection_review,
    create_manual_detection,
    get_next_sheet_review_task,
    get_review_task,
    get_next_review_task,
    get_sheet_status_counts,
    list_review_tasks,
    list_sheet_review_tasks,
    resolve_review_task,
)


def list_tasks(
    config: AppConfig,
    *,
    task_type: str | None,
    status: str | None,
    limit: int,
) -> list[ReviewTask]:
    """Return a filtered review task list."""
    with connect(config) as conn:
        return list_review_tasks(conn, task_type=task_type, status=status, limit=limit)


def list_sheet_tasks(
    config: AppConfig,
    *,
    batch_name: str | None = None,
    status: str | None,
    limit: int,
) -> list[ReviewTask]:
    """Return unresolved sheet-review tasks."""
    with connect(config) as conn:
        return list_sheet_review_tasks(conn, batch_name=batch_name, status=status, limit=limit)


def get_sheet_backlog(
    config: AppConfig,
    *,
    batch_name: str | None,
    status: str | None,
    limit: int,
) -> SheetReviewBacklog:
    """Return batch-level sheet status counts plus open review tasks."""
    with connect(config) as conn:
        return SheetReviewBacklog(
            batch_name=batch_name,
            sheet_status_counts=get_sheet_status_counts(conn, batch_name=batch_name),
            open_tasks=list_sheet_review_tasks(
                conn,
                batch_name=batch_name,
                status=status,
                limit=limit,
            ),
        )


def get_next_task(config: AppConfig, task_type: str | None) -> ReviewTask | None:
    """Return the next open review task."""
    with connect(config) as conn:
        return get_next_review_task(conn, task_type=task_type)


def get_next_sheet_task(
    config: AppConfig,
    *,
    batch_name: str | None = None,
    sheet_id: int | None = None,
) -> ReviewTask | None:
    """Return the next open sheet-level review task for a batch or sheet."""
    with connect(config) as conn:
        return get_next_sheet_review_task(conn, batch_name=batch_name, sheet_id=sheet_id)


def get_task(config: AppConfig, task_id: int) -> ReviewTask | None:
    """Return one review task with current task-specific detail."""
    with connect(config) as conn:
        return get_review_task(conn, task_id)


def export_ocr_text(config: AppConfig, task_id: int) -> Path:
    """Write OCR text for a review task to a sidecar file next to the crop image."""
    task = get_task(config, task_id)
    if task is None:
        raise ValueError(f"Review task {task_id} was not found.")
    if task.task_type != "review_ocr":
        raise ValueError(f"Review task {task_id} is not an OCR review task.")

    crop_path_value = task.payload_json.get("crop_path")
    if not crop_path_value:
        raise ValueError(f"Review task {task_id} does not have a crop path.")

    ocr_text_value = task.payload_json.get("ocr_text")
    if ocr_text_value is None:
        raise ValueError(f"Review task {task_id} does not have OCR text to export.")

    crop_path = Path(str(crop_path_value))
    sidecar_path = crop_path.with_suffix(".txt")
    sidecar_path.parent.mkdir(parents=True, exist_ok=True)
    sidecar_path.write_text(str(ocr_text_value), encoding="utf-8")
    return sidecar_path


def accept_detections(
    config: AppConfig,
    *,
    task_id: int,
    detection_ids: list[int],
    note: str | None,
) -> ReviewTask:
    """Accept selected detections for a sheet review task."""
    with connect(config) as conn:
        task = accept_detection_review(
            conn,
            task_id=task_id,
            detection_ids=detection_ids,
            note=note,
        )
        conn.commit()
    return task


def add_manual_detection(
    config: AppConfig,
    *,
    task_id: int,
    region_type: str,
    x1: int,
    y1: int,
    x2: int,
    y2: int,
    dry_run: bool,
) -> Path:
    """Create a manual detection from explicit bounding-box coordinates."""
    task = get_task(config, task_id)
    if task is None:
        raise ValueError(f"Review task {task_id} was not found.")
    if task.task_type != "review_detection" or task.entity_type != "sheet_scan":
        raise ValueError(f"Review task {task_id} is not a sheet detection review task.")
    if region_type not in {"photo", "text"}:
        raise ValueError(f"Unsupported region_type '{region_type}'.")

    with connect(config) as conn:
        sheets = get_sheet_scans(conn, sheet_id=task.entity_id)
        if not sheets:
            raise ValueError(f"Sheet scan {task.entity_id} was not found.")
        sheet = sheets[0]
        candidate = _build_manual_candidate(
            sheet=sheet,
            region_type=region_type,
            x1=x1,
            y1=y1,
            x2=x2,
            y2=y2,
        )
        crop_path = _manual_region_crop_path(
            config.photos_root,
            sheet,
            region_type=region_type,
            existing_detection_count=len(task.payload_json.get("detections", [])),
        )
        materialized_candidate = replace(candidate, crop_path=crop_path)
        if dry_run:
            return crop_path

        write_candidate_crop(sheet.original_path, materialized_candidate, crop_path)
        create_manual_detection(
            conn,
            sheet_scan_id=sheet.id,
            candidate=materialized_candidate,
        )
        conn.commit()
        return crop_path


def resolve_task(
    config: AppConfig,
    *,
    task_id: int,
    dismiss: bool,
    note: str | None,
    ocr_text: str | None,
) -> ReviewTask:
    """Resolve a review task and persist any OCR corrections."""
    with connect(config) as conn:
        task = resolve_review_task(
            conn,
            task_id=task_id,
            dismiss=dismiss,
            note=note,
            ocr_text=ocr_text,
        )
        conn.commit()
    return task


def apply_orientation_review(
    config: AppConfig,
    *,
    task_id: int,
    rotation_degrees: int,
    note: str | None,
    dry_run: bool,
) -> ReviewTask:
    """Apply a manual orientation decision and resolve the review task."""
    task = get_task(config, task_id)
    if task is None:
        raise ValueError(f"Review task {task_id} was not found.")
    if task.task_type != "review_orientation" or task.entity_type != "photo":
        raise ValueError(f"Review task {task_id} is not an orientation review task.")

    if dry_run:
        return task

    run_orientation(
        config,
        photo_id=task.entity_id,
        forced_rotation=rotation_degrees,
        dry_run=False,
    )
    return resolve_task(
        config,
        task_id=task_id,
        dismiss=False,
        note=note,
        ocr_text=None,
    )


def _build_manual_candidate(
    *,
    sheet: SheetScanRecord,
    region_type: str,
    x1: int,
    y1: int,
    x2: int,
    y2: int,
) -> DetectionCandidate:
    left = max(0, min(x1, x2))
    top = max(0, min(y1, y2))
    right = min(sheet.width_px, max(x1, x2))
    bottom = min(sheet.height_px, max(y1, y2))
    if right <= left or bottom <= top:
        raise ValueError("Bounding box must have positive width and height.")

    width = float(right - left)
    height = float(bottom - top)
    area_ratio = (width * height) / float(sheet.width_px * sheet.height_px)
    return DetectionCandidate(
        region_type=region_type,
        contour_points=((left, top), (right, top), (right, bottom), (left, bottom)),
        box_points=((left, top), (right, top), (right, bottom), (left, bottom)),
        center_x=float(left + (width / 2.0)),
        center_y=float(top + (height / 2.0)),
        width=width,
        height=height,
        angle=0.0,
        area_ratio=area_ratio,
        rectangularity=1.0,
        confidence=1.0,
    )


def _manual_region_crop_path(
    photos_root: Path,
    sheet: SheetScanRecord,
    *,
    region_type: str,
    existing_detection_count: int,
) -> Path:
    index = existing_detection_count + 1
    return (
        photos_root
        / "derivatives"
        / "review"
        / "regions"
        / sheet.batch_name
        / f"sheet_{sheet.id}_{index}_{region_type}.jpg"
    )
