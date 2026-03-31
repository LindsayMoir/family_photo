"""Application service for review workflows."""

from __future__ import annotations

from pathlib import Path

from config import AppConfig
from db.connection import connect
from orientation.service import run_orientation
from review.models import ReviewTask, ReviewTaskSummary
from review.repository import (
    dismiss_open_ocr_review_tasks,
    get_open_review_task_counts,
    get_next_review_task,
    get_review_task,
    list_review_tasks,
    resolve_open_orientation_review_task,
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


def get_next_task(config: AppConfig, task_type: str | None) -> ReviewTask | None:
    """Return the next open review task."""
    with connect(config) as conn:
        return get_next_review_task(conn, task_type=task_type)


def get_task_summary(
    config: AppConfig,
    *,
    batch_name: str | None = None,
    sheet_id: int | None = None,
) -> ReviewTaskSummary:
    """Return open review counts grouped by task type."""
    with connect(config) as conn:
        return ReviewTaskSummary(
            task_counts=get_open_review_task_counts(
                conn,
                batch_name=batch_name,
                sheet_id=sheet_id,
            )
        )


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


def resolve_task(
    config: AppConfig,
    *,
    task_id: int,
    dismiss: bool,
    note: str | None,
    ocr_text: str | None,
    export_action: str | None = None,
) -> ReviewTask:
    """Resolve a review task and persist any OCR corrections."""
    with connect(config) as conn:
        task = resolve_review_task(
            conn,
            task_id=task_id,
            dismiss=dismiss,
            note=note,
            ocr_text=ocr_text,
            export_action=export_action,
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


def resolve_export_audit_review(
    config: AppConfig,
    *,
    task_id: int,
    export_action: str,
    note: str | None,
    dry_run: bool,
) -> ReviewTask:
    """Resolve a spreadsheet-driven export audit task with an explicit operator action."""
    task = get_task(config, task_id)
    if task is None:
        raise ValueError(f"Review task {task_id} was not found.")
    if task.task_type != "review_export_audit" or task.entity_type != "photo":
        raise ValueError(f"Review task {task_id} is not an export audit review task.")
    if dry_run:
        return task

    return resolve_task(
        config,
        task_id=task_id,
        dismiss=False,
        note=note,
        ocr_text=None,
        export_action=export_action,
    )


def resolve_orientation_review_for_photo(
    config: AppConfig,
    *,
    photo_id: int,
    action: str,
) -> bool:
    """Resolve an open orientation review task for a photo."""
    with connect(config) as conn:
        resolved = resolve_open_orientation_review_task(conn, photo_id=photo_id, action=action)
        conn.commit()
    return resolved


def dismiss_ocr_reviews(
    config: AppConfig,
    *,
    batch_name: str | None,
    dry_run: bool,
) -> int:
    """Dismiss open OCR review tasks when OCR is out of scope."""
    if dry_run:
        with connect(config) as conn:
            counts = get_open_review_task_counts(conn, batch_name=batch_name)
        return counts.get("review_ocr", 0)

    with connect(config) as conn:
        dismissed_count = dismiss_open_ocr_review_tasks(conn, batch_name=batch_name)
        conn.commit()
    return dismissed_count
