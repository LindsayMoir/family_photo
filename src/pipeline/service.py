"""Batch pipeline orchestration across sheet and photo stages."""

from __future__ import annotations

from dataclasses import dataclass
import logging

from audit.service import run_export_audit
from config import AppConfig
from crop.service import run_crop
from db.connection import connect
from deskew.service import run_deskew
from detection.repository import SHEET_STATUS_INGESTED, count_sheet_scans_by_status, get_sheet_scans
from detection.service import run_detection
from enhance.service import run_enhancement
from frame_export.service import (
    resolve_frame_export_request,
    run_frame_export,
)
from orientation.service import run_orientation
from photo_repository import list_export_ready_photo_ids, list_photo_ids_for_sheet
from review.models import ReviewTask
from review.service import (
    get_next_task,
    get_task_summary,
)

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ProcessSummary:
    """Summary of a process command run."""

    target: str
    sheets_processed: int
    photos_processed: int
    dry_run: bool


@dataclass(frozen=True, slots=True)
class RunBatchSummary:
    """Summary of a supervisor-style batch run."""

    target: str
    sheets_processed: int
    photos_processed: int
    exported_count: int
    review_task_counts: dict[str, int]
    blocking_task: ReviewTask | None
    dry_run: bool


@dataclass(frozen=True, slots=True)
class RunUntilReviewSummary:
    """Summary of a keep-going supervisor pass for one batch."""

    target: str
    batch_runs: int
    pending_sheets: int
    staged_photo_count: int
    blocked: bool
    blocked_reason: str | None
    staging_csv_path: str
    dry_run: bool


def run_process(
    config: AppConfig,
    *,
    batch_name: str | None,
    sheet_id: int | None,
    limit: int | None,
    fast_mode: bool = False,
    enable_ocr: bool = False,
    dry_run: bool,
) -> ProcessSummary:
    """Run the current pipeline across one sheet or a batch of sheets."""
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

    photos_processed = 0

    for sheet in sheets:
        LOGGER.info("pipeline_sheet_start sheet_id=%s batch=%s stage=detection", sheet.id, sheet.batch_name)
        detection_result = run_detection(
            config,
            sheet_id=sheet.id,
            fast_mode=fast_mode,
            enable_ocr=enable_ocr,
            dry_run=dry_run,
        )
        LOGGER.info(
            "pipeline_sheet_detection_complete sheet_id=%s detected_count=%s",
            sheet.id,
            detection_result.detected_count,
        )

        LOGGER.info("pipeline_sheet_stage_start sheet_id=%s stage=crop", sheet.id)
        run_crop(config, sheet_id=sheet.id, dry_run=dry_run)

        with connect(config) as conn:
            photo_ids = list_photo_ids_for_sheet(conn, sheet_id=sheet.id)

        for photo_id in photo_ids:
            LOGGER.info("pipeline_photo_stage_start photo_id=%s stage=deskew", photo_id)
            run_deskew(config, photo_id=photo_id, dry_run=dry_run)
            LOGGER.info("pipeline_photo_stage_start photo_id=%s stage=orientation", photo_id)
            run_orientation(config, photo_id=photo_id, dry_run=dry_run)
            LOGGER.info("pipeline_photo_stage_start photo_id=%s stage=enhance", photo_id)
            run_enhancement(config, photo_id=photo_id, dry_run=dry_run)
            LOGGER.info("pipeline_photo_complete photo_id=%s", photo_id)
            photos_processed += 1

    target = batch_name if batch_name is not None else f"sheet_id={sheet_id}"
    return ProcessSummary(
        target=target,
        sheets_processed=len(sheets),
        photos_processed=photos_processed,
        dry_run=dry_run,
    )


def run_batch(
    config: AppConfig,
    *,
    batch_name: str | None,
    sheet_id: int | None,
    limit: int | None,
    fast_mode: bool,
    enable_ocr: bool,
    dry_run: bool,
) -> RunBatchSummary:
    """Advance the pipeline, export ready photos, and report the next blocking review task."""
    process_summary = run_process(
        config,
        batch_name=batch_name,
        sheet_id=sheet_id,
        limit=limit,
        fast_mode=fast_mode,
        enable_ocr=enable_ocr,
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

    review_task_counts: dict[str, int] = {}
    blocking_task = None
    if not dry_run:
        review_task_counts = get_task_summary(
            config,
            batch_name=batch_name,
            sheet_id=sheet_id,
        ).task_counts
        blocking_task = get_next_task(config, task_type=None)
    return RunBatchSummary(
        target=process_summary.target,
        sheets_processed=process_summary.sheets_processed,
        photos_processed=process_summary.photos_processed,
        exported_count=exported_count,
        review_task_counts=review_task_counts,
        blocking_task=blocking_task,
        dry_run=dry_run,
    )


def run_until_review(
    config: AppConfig,
    *,
    batch_name: str,
    fast_mode: bool,
    enable_ocr: bool,
    dry_run: bool,
) -> RunUntilReviewSummary:
    """Keep processing one batch until a true blocker or completion."""

    staging_csv_path = str(config.photos_root / "exports" / "staging" / "export_audit.csv")
    if dry_run:
        return RunUntilReviewSummary(
            target=batch_name,
            batch_runs=0,
            pending_sheets=0,
            staged_photo_count=0,
            blocked=False,
            blocked_reason=None,
            staging_csv_path=staging_csv_path,
            dry_run=True,
        )

    batch_runs = 0
    staged_photo_count = 0
    blocked = False
    blocked_reason: str | None = None

    for _ in range(1000):
        with connect(config) as conn:
            pending_sheets = count_sheet_scans_by_status(
                conn,
                batch_name=batch_name,
                status=SHEET_STATUS_INGESTED,
            )
        if pending_sheets == 0:
            return RunUntilReviewSummary(
                target=batch_name,
                batch_runs=batch_runs,
                pending_sheets=0,
                staged_photo_count=staged_photo_count,
                blocked=False,
                blocked_reason=None,
                staging_csv_path=staging_csv_path,
                dry_run=False,
            )

        progressed = False
        if pending_sheets > 0:
            process_summary = run_batch(
                config,
                batch_name=batch_name,
                sheet_id=None,
                limit=None,
                fast_mode=fast_mode,
                enable_ocr=enable_ocr,
                dry_run=False,
            )
            batch_runs += 1
            if process_summary.sheets_processed > 0 or process_summary.photos_processed > 0:
                progressed = True

        LOGGER.info("pipeline_batch_stage_start batch=%s stage=audit_exports", batch_name)
        audit_summary = run_export_audit(
            config,
            batch_name=batch_name,
            sheet_id=None,
            photo_id=None,
            limit=None,
            category=None,
            csv_path=None,
            dry_run=False,
        )
        staged_photo_count = audit_summary.audited_count
        LOGGER.info(
            "pipeline_batch_stage_complete batch=%s stage=audit_exports staged_photo_count=%s",
            batch_name,
            staged_photo_count,
        )

        if not progressed:
            blocked = True
            blocked_reason = "no further automatic progress was possible"
            break

    with connect(config) as conn:
        remaining_pending = count_sheet_scans_by_status(
            conn,
            batch_name=batch_name,
            status=SHEET_STATUS_INGESTED,
        )
    return RunUntilReviewSummary(
        target=batch_name,
        batch_runs=batch_runs,
        pending_sheets=remaining_pending,
        staged_photo_count=staged_photo_count,
        blocked=blocked,
        blocked_reason=blocked_reason,
        staging_csv_path=staging_csv_path,
        dry_run=False,
    )
