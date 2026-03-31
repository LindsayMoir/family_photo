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
    accept_detections,
    get_next_sheet_task,
    get_task,
    get_task_summary,
    list_sheet_tasks,
)

LOGGER = logging.getLogger(__name__)
REVIEW_SLICE_TASK_SCAN_LIMIT = 100


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
    review_task_counts: dict[str, int]
    blocking_task: ReviewTask | None
    dry_run: bool


@dataclass(frozen=True, slots=True)
class RunReviewSliceSummary:
    """Summary of advancing the next unresolved review-driven slice."""

    target: str
    requested_tasks: int
    actionable_tasks: int
    skipped_tasks_without_photo_detections: int
    photos_processed: int
    staged_photo_count: int
    staging_csv_path: str
    dry_run: bool


@dataclass(frozen=True, slots=True)
class RunUntilReviewSummary:
    """Summary of a keep-going supervisor pass for one batch."""

    target: str
    batch_runs: int
    review_slice_runs: int
    pending_sheets: int
    open_sheet_tasks: int
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
    review_required_sheets = 0

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
            "pipeline_sheet_detection_complete sheet_id=%s detected_count=%s review_required_count=%s",
            sheet.id,
            detection_result.detected_count,
            detection_result.review_required_count,
        )

        if detection_result.review_required_count > 0 or detection_result.detected_count == 0:
            LOGGER.info("pipeline_sheet_blocked sheet_id=%s reason=review_required_or_no_detections", sheet.id)
            review_required_sheets += 1
            continue

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
        blocking_task = get_next_sheet_task(
            config,
            batch_name=batch_name,
            sheet_id=sheet_id,
        )
    return RunBatchSummary(
        target=process_summary.target,
        sheets_processed=process_summary.sheets_processed,
        photos_processed=process_summary.photos_processed,
        review_required_sheets=process_summary.review_required_sheets,
        exported_count=exported_count,
        review_task_counts=review_task_counts,
        blocking_task=blocking_task,
        dry_run=dry_run,
    )


def run_review_slice(
    config: AppConfig,
    *,
    batch_name: str,
    limit: int,
    dry_run: bool,
) -> RunReviewSliceSummary:
    """Advance the next unresolved sheet-review slice through staging audit refresh."""
    if limit <= 0:
        raise ValueError("limit must be a positive integer.")

    open_tasks = list_sheet_tasks(
        config,
        batch_name=batch_name,
        status="open",
        limit=REVIEW_SLICE_TASK_SCAN_LIMIT,
    )
    if not open_tasks:
        raise ValueError(f"No open sheet review tasks found for batch '{batch_name}'.")

    actionable_tasks: list[tuple[int, int, list[int]]] = []
    requested_task_count = min(len(open_tasks), limit)
    skipped_tasks_without_photo_detections = 0

    for task in open_tasks:
        detailed_task = get_task(config, task.id)
        if detailed_task is None:
            continue
        detection_ids = [
            int(detection["id"])
            for detection in detailed_task.payload_json.get("detections", [])
            if detection.get("region_type") == "photo"
        ]
        if not detection_ids:
            skipped_tasks_without_photo_detections += 1
            continue
        actionable_tasks.append((task.id, task.entity_id, detection_ids))
        if len(actionable_tasks) >= limit:
            break

    if dry_run:
        return RunReviewSliceSummary(
            target=batch_name,
            requested_tasks=requested_task_count,
            actionable_tasks=len(actionable_tasks),
            skipped_tasks_without_photo_detections=skipped_tasks_without_photo_detections,
            photos_processed=0,
            staged_photo_count=0,
            staging_csv_path=str(config.photos_root / "exports" / "staging" / "export_audit.csv"),
            dry_run=True,
        )

    export_width, export_height, export_profile = resolve_frame_export_request(
        preset_name="auto",
        width_px=None,
        height_px=None,
        profile_name=None,
    )

    photos_processed = 0
    for task_id, sheet_id, detection_ids in actionable_tasks:
        LOGGER.info(
            "review_slice_task_start task_id=%s sheet_id=%s detection_count=%s",
            task_id,
            sheet_id,
            len(detection_ids),
        )
        accept_detections(
            config,
            task_id=task_id,
            detection_ids=detection_ids,
            note="Auto-accepted by run-next-slice",
        )
        run_crop(config, sheet_id=sheet_id, dry_run=False)

        with connect(config) as conn:
            photo_ids = list_photo_ids_for_sheet(conn, sheet_id=sheet_id)

        for photo_id in photo_ids:
            LOGGER.info("review_slice_photo_stage_start photo_id=%s stage=deskew", photo_id)
            run_deskew(config, photo_id=photo_id, dry_run=False)
            LOGGER.info("review_slice_photo_stage_start photo_id=%s stage=orientation", photo_id)
            run_orientation(config, photo_id=photo_id, dry_run=False)
            LOGGER.info("review_slice_photo_stage_start photo_id=%s stage=enhance", photo_id)
            run_enhancement(config, photo_id=photo_id, dry_run=False)
            LOGGER.info("review_slice_photo_stage_start photo_id=%s stage=frame_export", photo_id)
            run_frame_export(
                config,
                batch_name=None,
                sheet_id=None,
                photo_id=photo_id,
                limit=None,
                width_px=export_width,
                height_px=export_height,
                profile_name=export_profile,
                dry_run=False,
            )
            photos_processed += 1
        LOGGER.info("review_slice_task_complete task_id=%s sheet_id=%s", task_id, sheet_id)

    audit_summary = run_export_audit(
        config,
        batch_name=None,
        sheet_id=None,
        photo_id=None,
        limit=None,
        category=None,
        csv_path=None,
        dry_run=False,
    )

    return RunReviewSliceSummary(
        target=batch_name,
        requested_tasks=requested_task_count,
        actionable_tasks=len(actionable_tasks),
        skipped_tasks_without_photo_detections=skipped_tasks_without_photo_detections,
        photos_processed=photos_processed,
        staged_photo_count=audit_summary.audited_count,
        staging_csv_path=str(audit_summary.csv_path),
        dry_run=False,
    )


def run_until_review(
    config: AppConfig,
    *,
    batch_name: str,
    fast_mode: bool,
    review_slice_limit: int,
    enable_ocr: bool,
    dry_run: bool,
) -> RunUntilReviewSummary:
    """Keep processing one batch until a true blocker or a fresh staging handoff appears."""
    if review_slice_limit <= 0:
        raise ValueError("review_slice_limit must be a positive integer.")

    staging_csv_path = str(config.photos_root / "exports" / "staging" / "export_audit.csv")
    if dry_run:
        return RunUntilReviewSummary(
            target=batch_name,
            batch_runs=0,
            review_slice_runs=0,
            pending_sheets=0,
            open_sheet_tasks=0,
            staged_photo_count=0,
            blocked=False,
            blocked_reason=None,
            staging_csv_path=staging_csv_path,
            dry_run=True,
        )

    batch_runs = 0
    review_slice_runs = 0
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
        open_tasks = list_sheet_tasks(
            config,
            batch_name=batch_name,
            status="open",
            limit=5000,
        )
        if pending_sheets == 0 and not open_tasks:
            return RunUntilReviewSummary(
                target=batch_name,
                batch_runs=batch_runs,
                review_slice_runs=review_slice_runs,
                pending_sheets=0,
                open_sheet_tasks=0,
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

        open_tasks = list_sheet_tasks(
            config,
            batch_name=batch_name,
            status="open",
            limit=5000,
        )
        if open_tasks:
            slice_summary = run_review_slice(
                config,
                batch_name=batch_name,
                limit=review_slice_limit,
                dry_run=False,
            )
            review_slice_runs += 1
            staged_photo_count = slice_summary.staged_photo_count
            if slice_summary.actionable_tasks > 0 or slice_summary.photos_processed > 0:
                progressed = True
            elif pending_sheets == 0:
                blocked = True
                blocked_reason = "open sheet review tasks remain without photo detections"
                break

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
    remaining_open_tasks = len(
        list_sheet_tasks(
            config,
            batch_name=batch_name,
            status="open",
            limit=5000,
        )
    )
    return RunUntilReviewSummary(
        target=batch_name,
        batch_runs=batch_runs,
        review_slice_runs=review_slice_runs,
        pending_sheets=remaining_pending,
        open_sheet_tasks=remaining_open_tasks,
        staged_photo_count=staged_photo_count,
        blocked=blocked,
        blocked_reason=blocked_reason,
        staging_csv_path=staging_csv_path,
        dry_run=False,
    )
