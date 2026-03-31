"""Digital frame export stage for processed photos."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import shutil
import csv

import cv2

from config import AppConfig
from db.connection import connect
from photo_repository import (
    delete_photo_artifact,
    get_photo_record,
    insert_photo_artifact,
    list_photo_artifact_paths,
    list_export_ready_photo_ids,
)


FRAME_EXPORT_ARTIFACT_TYPE = "frame_export"
STAGING_FRAME_EXPORT_ARTIFACT_TYPE = "frame_export_staging"
FRAME_EXPORT_PIPELINE_VERSION = "frame_export_v3"
DEFAULT_FRAME_WIDTH = 1920
DEFAULT_FRAME_HEIGHT = 1080
DEFAULT_FRAME_PROFILE = "frame_1920x1080"
PORTRAIT_FRAME_WIDTH = 1080
PORTRAIT_FRAME_HEIGHT = 1920
PORTRAIT_FRAME_PROFILE = "frame_1080x1920"
STAGING_LANDSCAPE_PROFILE = "staging/landscape"
STAGING_PORTRAIT_PROFILE = "staging/portrait"

FRAME_PRESETS: dict[str, tuple[int, int, str]] = {
    "auto": (DEFAULT_FRAME_WIDTH, DEFAULT_FRAME_HEIGHT, "frame_auto"),
    "landscape": (DEFAULT_FRAME_WIDTH, DEFAULT_FRAME_HEIGHT, DEFAULT_FRAME_PROFILE),
    "portrait": (PORTRAIT_FRAME_WIDTH, PORTRAIT_FRAME_HEIGHT, PORTRAIT_FRAME_PROFILE),
}


@dataclass(frozen=True, slots=True)
class FrameExportSummary:
    """Summary of a frame export run."""

    target: str
    exported_count: int
    output_dir: Path
    width_px: int
    height_px: int
    dry_run: bool


@dataclass(frozen=True, slots=True)
class PromoteExportsSummary:
    """Summary of promoting reviewed staging exports into final frame folders."""

    csv_path: Path
    promoted_count: int
    skipped_count: int
    dry_run: bool


@dataclass(frozen=True, slots=True)
class DeleteStagingSummary:
    """Summary of deleting staging exports based on operator review."""

    deleted_count: int
    skipped_count: int
    dry_run: bool


@dataclass(frozen=True, slots=True)
class StagePhotoExportsSummary:
    """Summary of forcing specific photos into staging exports."""

    exported_count: int
    dry_run: bool


def run_frame_export(
    config: AppConfig,
    *,
    batch_name: str | None,
    sheet_id: int | None,
    photo_id: int | None,
    limit: int | None,
    width_px: int,
    height_px: int,
    profile_name: str,
    dry_run: bool,
) -> FrameExportSummary:
    """Create fixed-size digital-frame derivatives from processed photos."""
    if width_px <= 0 or height_px <= 0:
        raise ValueError("Frame export width and height must be positive integers.")

    with connect(config) as conn:
        photo_ids = list_export_ready_photo_ids(
            conn,
            batch_name=batch_name,
            sheet_id=sheet_id,
            photo_id=photo_id,
            limit=limit,
        )

    if not photo_ids:
        target = _target_name(batch_name=batch_name, sheet_id=sheet_id, photo_id=photo_id)
        raise ValueError(f"No photos found for target '{target}'.")

    output_dir = config.photos_root / "exports" / "staging"
    if dry_run:
        return FrameExportSummary(
            target=_target_name(batch_name=batch_name, sheet_id=sheet_id, photo_id=photo_id),
            exported_count=len(photo_ids),
            output_dir=output_dir,
            width_px=width_px,
            height_px=height_px,
            dry_run=True,
        )

    exported_count = 0
    with connect(config) as conn:
        for current_photo_id in photo_ids:
            photo = get_photo_record(conn, photo_id=current_photo_id)
            resolved_width, resolved_height, resolved_profile = _resolve_export_for_photo(
                photo.working_path,
                requested_width=width_px,
                requested_height=height_px,
                requested_profile=profile_name,
            )
            output_path = config.photos_root / "exports" / resolved_profile / f"photo_{current_photo_id}.jpg"
            _write_frame_export(
                input_path=photo.working_path,
                output_path=output_path,
                width_px=resolved_width,
                height_px=resolved_height,
            )
            _cleanup_old_frame_exports(
                conn,
                photo_id=current_photo_id,
                keep_path=output_path,
                artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
            )
            insert_photo_artifact(
                conn,
                photo_id=current_photo_id,
                artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
                path=output_path,
                pipeline_stage="frame_export",
                pipeline_version=FRAME_EXPORT_PIPELINE_VERSION,
            )
            exported_count += 1
        conn.commit()

    return FrameExportSummary(
        target=_target_name(batch_name=batch_name, sheet_id=sheet_id, photo_id=photo_id),
        exported_count=exported_count,
        output_dir=output_dir,
        width_px=width_px,
        height_px=height_px,
        dry_run=False,
    )


def stage_photo_exports(
    config: AppConfig,
    *,
    photo_ids: list[int],
    dry_run: bool,
) -> StagePhotoExportsSummary:
    """Write staging exports for specific photo ids regardless of current photo status."""
    if dry_run:
        return StagePhotoExportsSummary(exported_count=len(photo_ids), dry_run=True)

    exported_count = 0
    with connect(config) as conn:
        for photo_id in photo_ids:
            photo = get_photo_record(conn, photo_id=photo_id)
            resolved_width, resolved_height, resolved_profile = _resolve_export_for_photo(
                photo.working_path,
                requested_width=DEFAULT_FRAME_WIDTH,
                requested_height=DEFAULT_FRAME_HEIGHT,
                requested_profile="frame_auto",
            )
            output_path = config.photos_root / "exports" / resolved_profile / f"photo_{photo_id}.jpg"
            _write_frame_export(
                input_path=photo.working_path,
                output_path=output_path,
                width_px=resolved_width,
                height_px=resolved_height,
            )
            _cleanup_old_frame_exports(
                conn,
                photo_id=photo_id,
                keep_path=output_path,
                artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
            )
            insert_photo_artifact(
                conn,
                photo_id=photo_id,
                artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
                path=output_path,
                pipeline_stage="frame_export",
                pipeline_version=FRAME_EXPORT_PIPELINE_VERSION,
            )
            exported_count += 1
        conn.commit()

    return StagePhotoExportsSummary(exported_count=exported_count, dry_run=False)


def resolve_frame_preset(
    *,
    preset_name: str,
    width_px: int | None,
    height_px: int | None,
    profile_name: str | None,
) -> tuple[int, int, str]:
    """Resolve frame export dimensions and profile name from a preset."""
    if preset_name not in FRAME_PRESETS:
        raise ValueError(f"Unsupported frame preset '{preset_name}'.")

    preset_width, preset_height, preset_profile = FRAME_PRESETS[preset_name]
    resolved_width = width_px if width_px is not None else preset_width
    resolved_height = height_px if height_px is not None else preset_height
    resolved_profile = profile_name if profile_name is not None else preset_profile
    return resolved_width, resolved_height, resolved_profile


def resolve_frame_export_request(
    *,
    preset_name: str,
    width_px: int | None,
    height_px: int | None,
    profile_name: str | None,
) -> tuple[int, int, str]:
    """Resolve top-level export request parameters."""
    return resolve_frame_preset(
        preset_name=preset_name,
        width_px=width_px,
        height_px=height_px,
        profile_name=profile_name,
    )


def promote_staging_exports(
    config: AppConfig,
    *,
    csv_path: Path,
    dry_run: bool,
) -> PromoteExportsSummary:
    """Promote staging exports with no flagged issues into final frame folders."""
    if not csv_path.exists():
        raise ValueError(f"Export audit CSV was not found: {csv_path}")

    rows = _read_promotable_rows(csv_path)
    promoted_count = 0
    skipped_count = 0

    if dry_run:
        for row in rows:
            if _row_needs_help(row):
                skipped_count += 1
                continue
            promoted_count += 1
        return PromoteExportsSummary(
            csv_path=csv_path,
            promoted_count=promoted_count,
            skipped_count=skipped_count,
            dry_run=True,
        )

    with connect(config) as conn:
        for row in rows:
            if _row_needs_help(row):
                skipped_count += 1
                continue

            photo_id = int(row["photo_id"])
            staging_path = Path(row["export_path"])
            if not staging_path.exists():
                skipped_count += 1
                continue

            final_path = config.photos_root / "exports" / _final_profile_for_staging_path(staging_path) / staging_path.name
            final_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(staging_path, final_path)
            _cleanup_old_frame_exports(
                conn,
                photo_id=photo_id,
                keep_path=final_path,
                artifact_type=FRAME_EXPORT_ARTIFACT_TYPE,
            )
            insert_photo_artifact(
                conn,
                photo_id=photo_id,
                artifact_type=FRAME_EXPORT_ARTIFACT_TYPE,
                path=final_path,
                pipeline_stage="frame_export",
                pipeline_version=FRAME_EXPORT_PIPELINE_VERSION,
            )
            delete_photo_artifact(
                conn,
                photo_id=photo_id,
                artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
                path=staging_path,
            )
            if staging_path.exists():
                os.remove(staging_path)
            promoted_count += 1
        conn.commit()

    return PromoteExportsSummary(
        csv_path=csv_path,
        promoted_count=promoted_count,
        skipped_count=skipped_count,
        dry_run=False,
    )


def delete_staging_exports(
    config: AppConfig,
    *,
    csv_path: Path,
    dry_run: bool,
) -> DeleteStagingSummary:
    """Delete staging exports flagged with issue=DELETE and remove their staging artifacts."""
    if not csv_path.exists():
        raise ValueError(f"Export audit CSV was not found: {csv_path}")

    rows = _read_promotable_rows(csv_path)
    deleted_count = 0
    skipped_count = 0

    if dry_run:
        for row in rows:
            if _row_issue(row) != "DELETE":
                continue
            deleted_count += 1
        return DeleteStagingSummary(
            deleted_count=deleted_count,
            skipped_count=0,
            dry_run=True,
        )

    with connect(config) as conn:
        for row in rows:
            if _row_issue(row) != "DELETE":
                continue
            photo_id = int(row["photo_id"])
            staging_path = Path(row["export_path"])
            delete_photo_artifact(
                conn,
                photo_id=photo_id,
                artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
                path=staging_path,
            )
            if staging_path.exists():
                os.remove(staging_path)
                deleted_count += 1
            else:
                skipped_count += 1
        conn.commit()

    return DeleteStagingSummary(
        deleted_count=deleted_count,
        skipped_count=skipped_count,
        dry_run=False,
    )


def _write_frame_export(
    *,
    input_path: Path,
    output_path: Path,
    width_px: int,
    height_px: int,
) -> None:
    image = cv2.imread(str(input_path))
    if image is None:
        raise ValueError(f"Unable to load photo for frame export: {input_path}")

    source_height, source_width = image.shape[:2]
    scale = max(width_px / float(source_width), height_px / float(source_height))
    resized_width = max(1, int(round(source_width * scale)))
    resized_height = max(1, int(round(source_height * scale)))
    resized = cv2.resize(image, (resized_width, resized_height), interpolation=cv2.INTER_AREA)

    x_offset = max(0, (resized_width - width_px) // 2)
    y_offset = max(0, (resized_height - height_px) // 2)
    composite = resized[y_offset:y_offset + height_px, x_offset:x_offset + width_px]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not cv2.imwrite(str(output_path), composite):
        raise ValueError(f"Failed to write frame export image: {output_path}")


def _target_name(
    *,
    batch_name: str | None,
    sheet_id: int | None,
    photo_id: int | None,
) -> str:
    if batch_name is not None:
        return batch_name
    if sheet_id is not None:
        return f"sheet_id={sheet_id}"
    if photo_id is not None:
        return f"photo_id={photo_id}"
    return "all_photos"


def _resolve_export_for_photo(
    input_path: Path,
    *,
    requested_width: int,
    requested_height: int,
    requested_profile: str,
) -> tuple[int, int, str]:
    if requested_profile != "frame_auto":
        if requested_height > requested_width:
            return requested_width, requested_height, STAGING_PORTRAIT_PROFILE
        return requested_width, requested_height, STAGING_LANDSCAPE_PROFILE

    image = cv2.imread(str(input_path))
    if image is None:
        raise ValueError(f"Unable to load photo for frame export: {input_path}")
    height, width = image.shape[:2]
    if height > width:
        return PORTRAIT_FRAME_WIDTH, PORTRAIT_FRAME_HEIGHT, STAGING_PORTRAIT_PROFILE
    return DEFAULT_FRAME_WIDTH, DEFAULT_FRAME_HEIGHT, STAGING_LANDSCAPE_PROFILE


def _cleanup_old_frame_exports(
    conn,
    *,
    photo_id: int,
    keep_path: Path,
    artifact_type: str,
) -> None:
    existing_paths = list_photo_artifact_paths(
        conn,
        photo_id=photo_id,
        artifact_type=artifact_type,
    )
    for existing_path in existing_paths:
        if existing_path == keep_path:
            continue
        delete_photo_artifact(
            conn,
            photo_id=photo_id,
            artifact_type=artifact_type,
            path=existing_path,
        )
        if existing_path.exists():
            os.remove(existing_path)


def _read_promotable_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or "photo_id" not in reader.fieldnames:
            raise ValueError("Export audit CSV must include a photo_id column.")
        rows: list[dict[str, str]] = []
        for row in reader:
            normalized = {key: value or "" for key, value in row.items()}
            if normalized.get("row_type", "photo").strip().lower() != "photo":
                continue
            rows.append(normalized)
        return rows


def _row_needs_help(row: dict[str, str]) -> bool:
    return row.get("needs_help", "").strip().lower() in {"1", "true", "t", "yes", "y", "x"}


def _row_issue(row: dict[str, str]) -> str:
    return row.get("issue", "").strip().upper()


def _final_profile_for_staging_path(staging_path: Path) -> str:
    if staging_path.parent.name == "portrait":
        return PORTRAIT_FRAME_PROFILE
    return DEFAULT_FRAME_PROFILE
