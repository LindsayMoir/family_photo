"""CLI entrypoint for the family photo project."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys
from typing import Sequence

from app_logging import configure_logging
from config import AppConfig, ConfigError, load_config
from crop.service import run_crop
from db.schema import SCHEMA_PATH, read_initial_schema
from deskew.service import run_deskew
from detection.service import run_detection
from disposition.service import set_photo_export_disposition
from enhance.service import run_enhancement
from frame_export.service import (
    FRAME_PRESETS,
    resolve_frame_export_request,
    run_frame_export,
)
from ingest.service import run_ingest
from orientation.service import run_orientation
from pipeline.service import run_batch, run_process
from pipeline.types import CommandPlan
from review.service import (
    accept_detections,
    apply_orientation_review,
    add_manual_detection,
    export_ocr_text,
    get_next_task,
    get_sheet_backlog,
    get_task,
    list_sheet_tasks,
    list_tasks,
    resolve_task,
)


LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="family-photo",
        description="CLI tools for managing scanned family photo processing workflows.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("show-config", help="Display effective configuration.")

    init_db_parser = subparsers.add_parser("init-db", help="Print or locate the initial SQL schema.")
    init_db_parser.add_argument(
        "--print-sql",
        action="store_true",
        help="Print the SQL instead of the schema path.",
    )

    ingest_parser = subparsers.add_parser("ingest", help="Prepare an ingest operation.")
    _add_dry_run_argument(ingest_parser)
    ingest_parser.add_argument("--input", required=True, type=Path, help="Path to a scan file or directory.")
    ingest_parser.add_argument("--batch", required=True, help="Name of the ingest batch.")

    detect_parser = subparsers.add_parser("detect", help="Prepare detection for a batch or sheet.")
    _add_dry_run_argument(detect_parser)
    detect_target = detect_parser.add_mutually_exclusive_group(required=True)
    detect_target.add_argument("--batch", help="Batch name to process.")
    detect_target.add_argument("--sheet-id", type=int, help="Single sheet scan id to process.")
    detect_parser.add_argument(
        "--limit",
        type=int,
        help="Optional maximum number of sheet scans to process.",
    )
    detect_parser.add_argument(
        "--fast",
        action="store_true",
        help="Use faster printed-only OCR during detection.",
    )

    process_parser = subparsers.add_parser(
        "process",
        help="Run detect, crop, deskew, orient, and enhance for a sheet or batch.",
    )
    _add_dry_run_argument(process_parser)
    process_target = process_parser.add_mutually_exclusive_group(required=True)
    process_target.add_argument("--batch", help="Batch name to process.")
    process_target.add_argument("--sheet-id", type=int, help="Single sheet scan id to process.")
    process_parser.add_argument(
        "--limit",
        type=int,
        help="Optional maximum number of sheet scans to process.",
    )
    process_parser.add_argument(
        "--fast",
        action="store_true",
        help="Use faster printed-only OCR during detection.",
    )

    run_batch_parser = subparsers.add_parser(
        "run-batch",
        help="Advance a batch until blocked, export ready photos, and show the next review item.",
    )
    _add_dry_run_argument(run_batch_parser)
    run_batch_target = run_batch_parser.add_mutually_exclusive_group(required=True)
    run_batch_target.add_argument("--batch", help="Batch name to process.")
    run_batch_target.add_argument("--sheet-id", type=int, help="Single sheet scan id to process.")
    run_batch_parser.add_argument(
        "--limit",
        type=int,
        help="Optional maximum number of sheet scans to process.",
    )
    run_batch_parser.add_argument(
        "--fast",
        action="store_true",
        help="Use faster printed-only OCR during detection.",
    )

    crop_parser = subparsers.add_parser("crop", help="Promote accepted photo detections into photo records.")
    _add_dry_run_argument(crop_parser)
    crop_parser.add_argument("--sheet-id", required=True, type=int)

    deskew_parser = subparsers.add_parser("deskew", help="Prepare deskew processing for a photo.")
    _add_dry_run_argument(deskew_parser)
    deskew_parser.add_argument("--photo-id", required=True, type=int)

    orient_parser = subparsers.add_parser("orient", help="Prepare orientation processing for a photo.")
    _add_dry_run_argument(orient_parser)
    orient_parser.add_argument("--photo-id", required=True, type=int)
    orient_parser.add_argument(
        "--rotation-degrees",
        type=int,
        choices=[0, 90, 180, 270],
        help="Optional manual override for cardinal rotation.",
    )

    enhance_parser = subparsers.add_parser("enhance", help="Prepare enhancement processing for a photo.")
    _add_dry_run_argument(enhance_parser)
    enhance_parser.add_argument("--photo-id", required=True, type=int)

    disposition_parser = subparsers.add_parser(
        "set-photo-disposition",
        help="Include or exclude a photo from final frame exports.",
    )
    _add_dry_run_argument(disposition_parser)
    disposition_parser.add_argument("--photo-id", required=True, type=int)
    disposition_parser.add_argument(
        "--disposition",
        required=True,
        choices=["include", "exclude_low_value", "exclude_reject"],
    )
    disposition_parser.add_argument("--note", help="Optional operator note for the disposition decision.")

    export_frame_parser = subparsers.add_parser(
        "export-frame",
        help="Create fixed-size digital-frame exports from processed photos.",
    )
    _add_dry_run_argument(export_frame_parser)
    export_target = export_frame_parser.add_mutually_exclusive_group(required=True)
    export_target.add_argument("--batch", help="Batch name to export.")
    export_target.add_argument("--sheet-id", type=int, help="Single sheet scan id to export.")
    export_target.add_argument("--photo-id", type=int, help="Single photo id to export.")
    export_frame_parser.add_argument("--limit", type=int, help="Optional maximum number of photos to export.")
    export_frame_parser.add_argument(
        "--preset",
        choices=sorted(FRAME_PRESETS),
        default="auto",
        help="Named frame export preset.",
    )
    export_frame_parser.add_argument("--width", type=int, help="Target frame width in pixels.")
    export_frame_parser.add_argument("--height", type=int, help="Target frame height in pixels.")
    export_frame_parser.add_argument(
        "--profile",
        help="Output profile directory name under photos/exports.",
    )

    review_parser = subparsers.add_parser("review", help="Inspect or resolve review tasks.")
    _add_dry_run_argument(review_parser)
    review_subparsers = review_parser.add_subparsers(dest="review_command", required=True)
    review_list = review_subparsers.add_parser("list", help="List review tasks.")
    _add_dry_run_argument(review_list)
    review_list.add_argument(
        "--task-type",
        choices=["review_detection", "review_ocr", "review_orientation"],
        help="Optional filter for a specific review task type.",
    )
    review_list.add_argument(
        "--status",
        choices=["open", "in_progress", "resolved", "dismissed"],
        help="Optional filter for task status.",
    )
    review_list.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Maximum number of review tasks to show.",
    )
    review_next = review_subparsers.add_parser("next", help="Show the next open review task.")
    _add_dry_run_argument(review_next)
    review_next.add_argument(
        "--task-type",
        choices=["review_detection", "review_ocr", "review_orientation"],
        help="Optional filter for a specific review task type.",
    )
    review_show = review_subparsers.add_parser(
        "show",
        help="Show one review task with current linked detail.",
    )
    _add_dry_run_argument(review_show)
    review_show.add_argument("--task-id", required=True, type=int)
    review_export_ocr = review_subparsers.add_parser(
        "export-ocr",
        help="Write OCR text for a review_ocr task to a .txt sidecar file.",
    )
    _add_dry_run_argument(review_export_ocr)
    review_export_ocr.add_argument("--task-id", required=True, type=int)
    review_accept_detection = review_subparsers.add_parser(
        "accept-detection",
        help="Accept selected detections for a sheet-level review task.",
    )
    _add_dry_run_argument(review_accept_detection)
    review_accept_detection.add_argument("--task-id", required=True, type=int)
    review_accept_detection.add_argument(
        "--detection-id",
        required=True,
        type=int,
        action="append",
        help="Detection id to accept. Repeat for multiple detections.",
    )
    review_accept_detection.add_argument("--note", help="Optional reviewer note.")
    review_set_orientation = review_subparsers.add_parser(
        "set-orientation",
        help="Apply a manual orientation choice for a review_orientation task.",
    )
    _add_dry_run_argument(review_set_orientation)
    review_set_orientation.add_argument("--task-id", required=True, type=int)
    review_set_orientation.add_argument(
        "--rotation-degrees",
        required=True,
        type=int,
        choices=[0, 90, 180, 270],
    )
    review_set_orientation.add_argument("--note", help="Optional reviewer note.")
    review_add_detection = review_subparsers.add_parser(
        "add-detection",
        help="Create a manual detection for a sheet-level review task.",
    )
    _add_dry_run_argument(review_add_detection)
    review_add_detection.add_argument("--task-id", required=True, type=int)
    review_add_detection.add_argument(
        "--region-type",
        required=True,
        choices=["photo", "text"],
        help="Region type for the manual detection.",
    )
    review_add_detection.add_argument("--x1", required=True, type=int)
    review_add_detection.add_argument("--y1", required=True, type=int)
    review_add_detection.add_argument("--x2", required=True, type=int)
    review_add_detection.add_argument("--y2", required=True, type=int)
    review_resolve = review_subparsers.add_parser("resolve", help="Resolve a review task placeholder.")
    _add_dry_run_argument(review_resolve)
    review_resolve.add_argument("--task-id", required=True, type=int)
    review_resolve.add_argument("--ocr-text", help="Corrected OCR text for review_ocr tasks.")
    review_resolve.add_argument("--note", help="Optional reviewer note.")
    review_resolve.add_argument(
        "--dismiss",
        action="store_true",
        help="Dismiss the review task instead of resolving it.",
    )
    review_sheets = review_subparsers.add_parser(
        "sheets",
        help="List unresolved sheet-level review cases with preview paths.",
    )
    _add_dry_run_argument(review_sheets)
    review_sheets.add_argument(
        "--status",
        choices=["open", "in_progress", "resolved", "dismissed"],
        help="Optional filter for task status.",
    )
    review_sheets.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Maximum number of sheet review tasks to show.",
    )
    review_backlog = review_subparsers.add_parser(
        "backlog",
        help="Show batch-level sheet status counts and unresolved review tasks.",
    )
    _add_dry_run_argument(review_backlog)
    review_backlog.add_argument("--batch", help="Optional batch name filter.")
    review_backlog.add_argument(
        "--status",
        choices=["open", "in_progress", "resolved", "dismissed"],
        default="open",
        help="Review task status to list.",
    )
    review_backlog.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Maximum number of review tasks to show.",
    )

    detect_faces_parser = subparsers.add_parser(
        "detect-faces",
        help="Prepare face detection for a photo.",
    )
    _add_dry_run_argument(detect_faces_parser)
    detect_faces_parser.add_argument("--photo-id", required=True, type=int)

    suggest_parser = subparsers.add_parser(
        "suggest-labels",
        help="Prepare label suggestion for a photo.",
    )
    _add_dry_run_argument(suggest_parser)
    suggest_parser.add_argument("--photo-id", required=True, type=int)

    reprocess_parser = subparsers.add_parser(
        "reprocess",
        help="Prepare a reprocessing request from a named stage.",
    )
    _add_dry_run_argument(reprocess_parser)
    reprocess_parser.add_argument("--photo-id", required=True, type=int)
    reprocess_parser.add_argument("--from-stage", required=True)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        config = load_config()
    except ConfigError as exc:
        parser.exit(status=2, message=f"Configuration error: {exc}\n")

    configure_logging(config.log_level)
    LOGGER.debug("Loaded config for environment=%s", config.environment)

    try:
        return dispatch_command(args, config)
    except ValueError as exc:
        parser.exit(status=2, message=f"Argument error: {exc}\n")
    except RuntimeError as exc:
        parser.exit(status=1, message=f"Runtime error: {exc}\n")


def dispatch_command(args: argparse.Namespace, config: AppConfig) -> int:
    """Dispatch the parsed command."""
    if args.command == "show-config":
        _handle_show_config(config)
        return 0
    if args.command == "init-db":
        _handle_init_db(args.print_sql)
        return 0
    if args.command == "ingest":
        input_path = _validate_existing_path(args.input)
        _handle_ingest(config, input_path, args.batch, args.dry_run)
        return 0
    if args.command == "detect":
        _handle_detect(
            config,
            batch_name=args.batch,
            sheet_id=args.sheet_id,
            limit=args.limit,
            fast_mode=args.fast,
            dry_run=args.dry_run,
        )
        return 0
    if args.command == "process":
        _handle_process(
            config,
            batch_name=args.batch,
            sheet_id=args.sheet_id,
            limit=args.limit,
            fast_mode=args.fast,
            dry_run=args.dry_run,
        )
        return 0
    if args.command == "run-batch":
        _handle_run_batch(
            config,
            batch_name=args.batch,
            sheet_id=args.sheet_id,
            limit=args.limit,
            fast_mode=args.fast,
            dry_run=args.dry_run,
        )
        return 0
    if args.command == "crop":
        _handle_crop(config, sheet_id=args.sheet_id, dry_run=args.dry_run)
        return 0
    if args.command == "deskew":
        _handle_deskew(config, photo_id=args.photo_id, dry_run=args.dry_run)
        return 0
    if args.command == "orient":
        _handle_orient(
            config,
            photo_id=args.photo_id,
            rotation_degrees=args.rotation_degrees,
            dry_run=args.dry_run,
        )
        return 0
    if args.command == "enhance":
        _handle_enhance(config, photo_id=args.photo_id, dry_run=args.dry_run)
        return 0
    if args.command == "set-photo-disposition":
        _handle_set_photo_disposition(
            config,
            photo_id=args.photo_id,
            disposition=args.disposition,
            note=args.note,
            dry_run=args.dry_run,
        )
        return 0
    if args.command == "export-frame":
        _handle_export_frame(
            config,
            batch_name=args.batch,
            sheet_id=args.sheet_id,
            photo_id=args.photo_id,
            limit=args.limit,
            preset_name=args.preset,
            width_px=args.width,
            height_px=args.height,
            profile_name=args.profile,
            dry_run=args.dry_run,
        )
        return 0
    if args.command in {"detect-faces", "suggest-labels"}:
        photo_target = getattr(args, "photo_id", None)
        sheet_target = getattr(args, "sheet_id", None)
        target = f"photo_id={photo_target}" if photo_target is not None else f"sheet_id={sheet_target}"
        _print_plan(
            CommandPlan(
                command_name=args.command,
                target=target,
                dry_run=args.dry_run,
                notes=("implementation placeholder only; stage execution is not wired yet",),
            )
        )
        return 0
    if args.command == "review":
        _handle_review(config, args, args.dry_run)
        return 0
    if args.command == "reprocess":
        _print_plan(
            CommandPlan(
                command_name="reprocess",
                target=f"photo_id={args.photo_id}",
                dry_run=args.dry_run,
                notes=(f"from_stage={args.from_stage}", "future implementation will resume from the selected stage"),
            )
        )
        return 0
    raise ValueError(f"Unsupported command '{args.command}'.")


def _add_dry_run_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Describe the work without executing future processing steps.",
    )


def _handle_show_config(config: AppConfig) -> None:
    print(f"environment={config.environment}")
    print(f"database_url={config.database_url}")
    print(f"expected_database_name={config.expected_database_name}")
    print(f"photos_root={config.photos_root}")
    print(f"log_level={config.log_level}")


def _handle_init_db(print_sql: bool) -> None:
    if print_sql:
        print(read_initial_schema())
        return
    print(SCHEMA_PATH)


def _handle_ingest(config: AppConfig, input_path: Path, batch_name: str, dry_run: bool) -> None:
    result = run_ingest(config, input_path, batch_name, dry_run)
    print("command=ingest")
    print(f"batch={result.batch_name}")
    print(f"input={result.input_path}")
    print(f"dry_run={str(result.dry_run).lower()}")
    print(f"discovered_count={result.discovered_count}")
    print(f"inserted_count={result.inserted_count}")
    print(f"updated_count={result.updated_count}")


def _handle_detect(
    config: AppConfig,
    *,
    batch_name: str | None,
    sheet_id: int | None,
    limit: int | None,
    fast_mode: bool,
    dry_run: bool,
) -> None:
    result = run_detection(
        config,
        batch_name=batch_name,
        sheet_id=sheet_id,
        limit=limit,
        fast_mode=fast_mode,
        dry_run=dry_run,
    )
    print("command=detect")
    print(f"target={result.target}")
    print(f"dry_run={str(result.dry_run).lower()}")
    print(f"fast_mode={str(fast_mode).lower()}")
    print(f"processed_count={result.processed_count}")
    print(f"detected_count={result.detected_count}")
    print(f"review_required_count={result.review_required_count}")


def _handle_process(
    config: AppConfig,
    *,
    batch_name: str | None,
    sheet_id: int | None,
    limit: int | None,
    fast_mode: bool,
    dry_run: bool,
) -> None:
    result = run_process(
        config,
        batch_name=batch_name,
        sheet_id=sheet_id,
        limit=limit,
        fast_mode=fast_mode,
        dry_run=dry_run,
    )
    print("command=process")
    print(f"target={result.target}")
    print(f"dry_run={str(result.dry_run).lower()}")
    print(f"fast_mode={str(fast_mode).lower()}")
    print(f"sheets_processed={result.sheets_processed}")
    print(f"photos_processed={result.photos_processed}")
    print(f"review_required_sheets={result.review_required_sheets}")


def _handle_run_batch(
    config: AppConfig,
    *,
    batch_name: str | None,
    sheet_id: int | None,
    limit: int | None,
    fast_mode: bool,
    dry_run: bool,
) -> None:
    if dry_run:
        target = batch_name if batch_name is not None else f"sheet_id={sheet_id}"
        _print_plan(
            CommandPlan(
                command_name="run-batch",
                target=target,
                dry_run=True,
                notes=(
                    f"fast_mode={str(fast_mode).lower()}",
                    "run detect, crop, deskew, orient, enhance, and export-frame",
                    "show the next blocking review task when manual input is required",
                ),
            )
        )
        return

    result = run_batch(
        config,
        batch_name=batch_name,
        sheet_id=sheet_id,
        limit=limit,
        fast_mode=fast_mode,
        dry_run=dry_run,
    )
    print("command=run-batch")
    print(f"target={result.target}")
    print(f"dry_run={str(result.dry_run).lower()}")
    print(f"fast_mode={str(fast_mode).lower()}")
    print(f"sheets_processed={result.sheets_processed}")
    print(f"photos_processed={result.photos_processed}")
    print(f"review_required_sheets={result.review_required_sheets}")
    print(f"exported_count={result.exported_count}")
    if result.blocking_task is None:
        print("next_review_task=none")
        return
    print(f"next_review_task_id={result.blocking_task.id}")
    print(f"next_review_task_type={result.blocking_task.task_type}")
    print(f"next_review_entity_type={result.blocking_task.entity_type}")
    print(f"next_review_entity_id={result.blocking_task.entity_id}")
    print(f"next_review_reason={result.blocking_task.payload_json.get('review_reason', '')}")
    print(f"next_review_preview_path={result.blocking_task.payload_json.get('preview_path', '')}")


def _handle_crop(config: AppConfig, *, sheet_id: int, dry_run: bool) -> None:
    result = run_crop(config, sheet_id=sheet_id, dry_run=dry_run)
    print("command=crop")
    print(f"target={result.target}")
    print(f"dry_run={str(result.dry_run).lower()}")
    print(f"promoted_count={result.promoted_count}")
    print(f"skipped_count={result.skipped_count}")


def _handle_deskew(config: AppConfig, *, photo_id: int, dry_run: bool) -> None:
    result = run_deskew(config, photo_id=photo_id, dry_run=dry_run)
    print("command=deskew")
    print(f"photo_id={result.photo_id}")
    print(f"dry_run={str(result.dry_run).lower()}")
    print(f"angle_degrees={result.angle_degrees}")
    print(f"confidence={result.confidence}")
    print(f"output_path={result.output_path}")


def _handle_orient(
    config: AppConfig,
    *,
    photo_id: int,
    rotation_degrees: int | None,
    dry_run: bool,
) -> None:
    result = run_orientation(
        config,
        photo_id=photo_id,
        forced_rotation=rotation_degrees,
        dry_run=dry_run,
    )
    print("command=orient")
    print(f"photo_id={result.photo_id}")
    print(f"dry_run={str(result.dry_run).lower()}")
    if rotation_degrees is not None:
        print("manual_override=true")
    print(f"rotation_degrees={result.rotation_degrees}")
    print(f"confidence={result.confidence}")
    print(f"review_required={str(result.review_required).lower()}")
    print(f"output_path={result.output_path}")


def _handle_enhance(config: AppConfig, *, photo_id: int, dry_run: bool) -> None:
    result = run_enhancement(config, photo_id=photo_id, dry_run=dry_run)
    print("command=enhance")
    print(f"photo_id={result.photo_id}")
    print(f"dry_run={str(result.dry_run).lower()}")
    print(f"enhancement_version={result.enhancement_version}")
    print(f"output_path={result.output_path}")


def _handle_set_photo_disposition(
    config: AppConfig,
    *,
    photo_id: int,
    disposition: str,
    note: str | None,
    dry_run: bool,
) -> None:
    result = set_photo_export_disposition(
        config,
        photo_id=photo_id,
        disposition=disposition,
        note=note,
        dry_run=dry_run,
    )
    print("command=set-photo-disposition")
    print(f"photo_id={result.photo_id}")
    print(f"dry_run={str(result.dry_run).lower()}")
    print(f"disposition={result.disposition}")
    if result.note is not None:
        print(f"note={result.note}")
    print(f"removed_export_count={result.removed_export_count}")


def _handle_export_frame(
    config: AppConfig,
    *,
    batch_name: str | None,
    sheet_id: int | None,
    photo_id: int | None,
    limit: int | None,
    preset_name: str,
    width_px: int | None,
    height_px: int | None,
    profile_name: str | None,
    dry_run: bool,
) -> None:
    resolved_width, resolved_height, resolved_profile = resolve_frame_export_request(
        preset_name=preset_name,
        width_px=width_px,
        height_px=height_px,
        profile_name=profile_name,
    )
    result = run_frame_export(
        config,
        batch_name=batch_name,
        sheet_id=sheet_id,
        photo_id=photo_id,
        limit=limit,
        width_px=resolved_width,
        height_px=resolved_height,
        profile_name=resolved_profile,
        dry_run=dry_run,
    )
    print("command=export-frame")
    print(f"target={result.target}")
    print(f"dry_run={str(result.dry_run).lower()}")
    print(f"exported_count={result.exported_count}")
    print(f"output_dir={result.output_dir}")
    if resolved_profile == "frame_auto":
        print("frame_size=auto")
    else:
        print(f"frame_size={result.width_px}x{result.height_px}")


def _handle_review(config: AppConfig, args: argparse.Namespace, dry_run: bool) -> None:
    if args.review_command == "sheets":
        if dry_run:
            _print_plan(
                CommandPlan(
                    command_name="review sheets",
                    target="sheet_review_queue",
                    dry_run=True,
                    notes=("dry-run does not query the database",),
                )
            )
            return
        tasks = list_sheet_tasks(
            config,
            status=args.status or "open",
            limit=args.limit,
        )
        if not tasks:
            print("sheet_review_tasks=none")
            return
        print("id\tstatus\tpriority\tsheet_id\treason\tpreview_path")
        for task in tasks:
            print(
                f"{task.id}\t{task.status}\t{task.priority}\t{task.entity_id}\t"
                f"{task.payload_json.get('review_reason', '')}\t"
                f"{task.payload_json.get('preview_path', '')}"
            )
        return
    if args.review_command == "backlog":
        if dry_run:
            _print_plan(
                CommandPlan(
                    command_name="review backlog",
                    target=args.batch or "all_batches",
                    dry_run=True,
                    notes=("dry-run does not query the database",),
                )
            )
            return
        backlog = get_sheet_backlog(
            config,
            batch_name=args.batch,
            status=args.status,
            limit=args.limit,
        )
        print(f"batch={backlog.batch_name or 'all'}")
        for sheet_status, count in backlog.sheet_status_counts.items():
            print(f"sheet_status_count={sheet_status}:{count}")
        if not backlog.open_tasks:
            print("sheet_review_tasks=none")
            return
        print("id\tstatus\tpriority\tsheet_id\treason\tpreview_path")
        for task in backlog.open_tasks:
            print(
                f"{task.id}\t{task.status}\t{task.priority}\t{task.entity_id}\t"
                f"{task.payload_json.get('review_reason', '')}\t"
                f"{task.payload_json.get('preview_path', '')}"
            )
        return
    if args.review_command == "list":
        if dry_run:
            _print_plan(
                CommandPlan(
                    command_name="review list",
                    target="review_queue",
                    dry_run=True,
                    notes=("dry-run does not query the database",),
                )
            )
            return
        tasks = list_tasks(
            config,
            task_type=args.task_type,
            status=args.status or "open",
            limit=args.limit,
        )
        if not tasks:
            print("review_tasks=none")
            return
        print("id\ttype\tstatus\tpriority\tentity\tpreview")
        for task in tasks:
            preview = _task_preview(task.payload_json)
            print(
                f"{task.id}\t{task.task_type}\t{task.status}\t{task.priority}\t"
                f"{task.entity_type}:{task.entity_id}\t{preview}"
            )
        return
    if args.review_command == "next":
        if dry_run:
            _print_plan(
                CommandPlan(
                    command_name="review next",
                    target="review_queue",
                    dry_run=True,
                    notes=("dry-run does not query the database",),
                )
            )
            return
        task = get_next_task(config, task_type=args.task_type)
        if task is None:
            print("review_task=none")
            return
        print(f"task_id={task.id}")
        print(f"task_type={task.task_type}")
        print(f"entity_type={task.entity_type}")
        print(f"entity_id={task.entity_id}")
        print(f"status={task.status}")
        print(f"priority={task.priority}")
        for key, value in task.payload_json.items():
            print(f"{key}={value}")
        return
    if args.review_command == "set-orientation":
        if dry_run:
            _print_plan(
                CommandPlan(
                    command_name="review set-orientation",
                    target=f"task_id={args.task_id}",
                    dry_run=True,
                    notes=(f"rotation_degrees={args.rotation_degrees}",),
                )
            )
            return
        task = apply_orientation_review(
            config,
            task_id=args.task_id,
            rotation_degrees=args.rotation_degrees,
            note=args.note,
            dry_run=False,
        )
        print(f"task_id={task.id}")
        print(f"status={task.status}")
        print(f"task_type={task.task_type}")
        print(f"photo_id={task.entity_id}")
        return
    if args.review_command == "show":
        if dry_run:
            _print_plan(
                CommandPlan(
                    command_name="review show",
                    target=f"task_id={args.task_id}",
                    dry_run=True,
                    notes=("dry-run does not query the database",),
                )
            )
            return
        task = get_task(config, task_id=args.task_id)
        if task is None:
            print("review_task=none")
            return
        print(f"task_id={task.id}")
        print(f"task_type={task.task_type}")
        print(f"entity_type={task.entity_type}")
        print(f"entity_id={task.entity_id}")
        print(f"status={task.status}")
        print(f"priority={task.priority}")
        for key, value in task.payload_json.items():
            if key == "detections" and isinstance(value, list):
                for detection in value:
                    if not isinstance(detection, dict):
                        continue
                    print(
                        "detection="
                        f"id:{detection.get('id')},"
                        f"type:{detection.get('region_type')},"
                        f"confidence:{detection.get('confidence')},"
                        f"accepted:{detection.get('accepted')},"
                        f"crop_path:{detection.get('crop_path')}"
                    )
                continue
            print(f"{key}={value}")
        return
    if args.review_command == "export-ocr":
        if dry_run:
            _print_plan(
                CommandPlan(
                    command_name="review export-ocr",
                    target=f"task_id={args.task_id}",
                    dry_run=True,
                    notes=("dry-run does not write sidecar files",),
                )
            )
            return
        output_path = export_ocr_text(config, task_id=args.task_id)
        print(f"task_id={args.task_id}")
        print(f"output_path={output_path}")
        return
    if args.review_command == "accept-detection":
        if dry_run:
            _print_plan(
                CommandPlan(
                    command_name="review accept-detection",
                    target=f"task_id={args.task_id}",
                    dry_run=True,
                    notes=(f"detection_ids={','.join(str(value) for value in args.detection_id)}",),
                )
            )
            return
        task = accept_detections(
            config,
            task_id=args.task_id,
            detection_ids=args.detection_id,
            note=args.note,
        )
        print(f"task_id={task.id}")
        print(f"task_type={task.task_type}")
        print(f"status={task.status}")
        return
    if args.review_command == "add-detection":
        output_path = add_manual_detection(
            config,
            task_id=args.task_id,
            region_type=args.region_type,
            x1=args.x1,
            y1=args.y1,
            x2=args.x2,
            y2=args.y2,
            dry_run=dry_run,
        )
        print(f"task_id={args.task_id}")
        print(f"dry_run={str(dry_run).lower()}")
        print(f"output_path={output_path}")
        return
    if args.review_command == "resolve":
        if dry_run:
            _print_plan(
                CommandPlan(
                    command_name="review resolve",
                    target=f"task_id={args.task_id}",
                    dry_run=True,
                    notes=("dry-run does not modify the database",),
                )
            )
            return
        task = resolve_task(
            config,
            task_id=args.task_id,
            dismiss=args.dismiss,
            note=args.note,
            ocr_text=args.ocr_text,
        )
        print(f"task_id={task.id}")
        print(f"task_type={task.task_type}")
        print(f"status={task.status}")
        return
    raise ValueError(f"Unsupported review command '{args.review_command}'.")


def _task_preview(payload: dict[str, object]) -> str:
    preview_value = payload.get("ocr_preview") or payload.get("review_reason") or payload.get("crop_path")
    if preview_value is None:
        return ""
    preview = str(preview_value).replace("\n", " ").strip()
    return preview[:80]


def _print_plan(plan: CommandPlan) -> None:
    print(f"command={plan.command_name}")
    print(f"target={plan.target}")
    print(f"dry_run={str(plan.dry_run).lower()}")
    for note in plan.notes:
        print(f"note={note}")


def _validate_existing_path(path: Path) -> Path:
    resolved = path.expanduser()
    if not resolved.exists():
        raise ValueError(f"Input path does not exist: {resolved}")
    return resolved


if __name__ == "__main__":
    sys.exit(main())
