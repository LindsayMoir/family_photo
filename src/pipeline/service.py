"""Batch pipeline orchestration across sheet and photo stages."""

from __future__ import annotations

from dataclasses import dataclass

from config import AppConfig
from crop.service import run_crop
from db.connection import connect
from deskew.service import run_deskew
from detection.repository import get_sheet_scans
from detection.service import run_detection
from enhance.service import run_enhancement
from frame_export.service import (
    resolve_frame_export_request,
    run_frame_export,
)
from orientation.service import run_orientation
from photo_repository import list_export_ready_photo_ids, list_photo_ids_for_sheet
from review.models import ReviewTask
from review.service import get_next_sheet_task, get_next_task


@dataclass(frozen=True, slots=True)
class ProcessSummary:
    """Summary of a process command run."""

    target: str
    sheets_processed: int
    photos_processed: int
    review_required_sheets: int
    dry_run: bool


@dataclass(frozen=True, slots=True)
class RunBatchSummary:
    """Summary of a supervisor-style batch run."""

    target: str
    sheets_processed: int
    photos_processed: int
    review_required_sheets: int
    exported_count: int
    blocking_task: ReviewTask | None
    dry_run: bool


def run_process(
    config: AppConfig,
    *,
    batch_name: str | None,
    sheet_id: int | None,
    limit: int | None,
    fast_mode: bool = False,
    dry_run: bool,
) -> ProcessSummary:
    """Run the current pipeline across one sheet or a batch of sheets."""
    with connect(config) as conn:
        sheets = get_sheet_scans(conn, batch_name=batch_name, sheet_id=sheet_id, limit=limit)

    if not sheets:
        target = batch_name if batch_name is not None else f"sheet_id={sheet_id}"
        raise ValueError(f"No sheet scans found for target '{target}'.")

    photos_processed = 0
    review_required_sheets = 0

    for sheet in sheets:
        detection_result = run_detection(
            config,
            sheet_id=sheet.id,
            fast_mode=fast_mode,
            dry_run=dry_run,
        )

        if detection_result.review_required_count > 0 or detection_result.detected_count == 0:
            review_required_sheets += 1
            continue

        run_crop(config, sheet_id=sheet.id, dry_run=dry_run)

        with connect(config) as conn:
            photo_ids = list_photo_ids_for_sheet(conn, sheet_id=sheet.id)

        for photo_id in photo_ids:
            run_deskew(config, photo_id=photo_id, dry_run=dry_run)
            orientation_result = run_orientation(config, photo_id=photo_id, dry_run=dry_run)
            if orientation_result.review_required:
                review_required_sheets += 1
                continue
            run_enhancement(config, photo_id=photo_id, dry_run=dry_run)
            photos_processed += 1

    target = batch_name if batch_name is not None else f"sheet_id={sheet_id}"
    return ProcessSummary(
        target=target,
        sheets_processed=len(sheets),
        photos_processed=photos_processed,
        review_required_sheets=review_required_sheets,
        dry_run=dry_run,
    )


def run_batch(
    config: AppConfig,
    *,
    batch_name: str | None,
    sheet_id: int | None,
    limit: int | None,
    fast_mode: bool,
    dry_run: bool,
) -> RunBatchSummary:
    """Advance the pipeline, export ready photos, and report the next blocking review task."""
    process_summary = run_process(
        config,
        batch_name=batch_name,
        sheet_id=sheet_id,
        limit=limit,
        fast_mode=fast_mode,
        dry_run=dry_run,
    )

    exported_count = 0
    with connect(config) as conn:
        ready_photo_ids = list_export_ready_photo_ids(
            conn,
            batch_name=batch_name,
            sheet_id=sheet_id,
        )

    if ready_photo_ids:
        export_width, export_height, export_profile = resolve_frame_export_request(
            preset_name="auto",
            width_px=None,
            height_px=None,
            profile_name=None,
        )
        export_summary = run_frame_export(
            config,
            batch_name=batch_name,
            sheet_id=sheet_id,
            photo_id=None,
            limit=None,
            width_px=export_width,
            height_px=export_height,
            profile_name=export_profile,
            dry_run=dry_run,
        )
        exported_count = export_summary.exported_count

    blocking_task = None
    if not dry_run:
        blocking_task = get_next_sheet_task(
            config,
            batch_name=batch_name,
            sheet_id=sheet_id,
        )
        if blocking_task is None:
            blocking_task = get_next_task(config, task_type="review_orientation")
    return RunBatchSummary(
        target=process_summary.target,
        sheets_processed=process_summary.sheets_processed,
        photos_processed=process_summary.photos_processed,
        review_required_sheets=process_summary.review_required_sheets,
        exported_count=exported_count,
        blocking_task=blocking_task,
        dry_run=dry_run,
    )
