"""Digital frame export stage for processed photos."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

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
FRAME_EXPORT_PIPELINE_VERSION = "frame_export_v2"
DEFAULT_FRAME_WIDTH = 1920
DEFAULT_FRAME_HEIGHT = 1080
DEFAULT_FRAME_PROFILE = "frame_1920x1080"
PORTRAIT_FRAME_WIDTH = 1080
PORTRAIT_FRAME_HEIGHT = 1920
PORTRAIT_FRAME_PROFILE = "frame_1080x1920"

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

    output_dir = config.photos_root / "exports"
    if profile_name != "frame_auto":
        output_dir = output_dir / profile_name
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
            _cleanup_old_frame_exports(conn, photo_id=current_photo_id, keep_path=output_path)
            insert_photo_artifact(
                conn,
                photo_id=current_photo_id,
                artifact_type=FRAME_EXPORT_ARTIFACT_TYPE,
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
        return requested_width, requested_height, requested_profile

    image = cv2.imread(str(input_path))
    if image is None:
        raise ValueError(f"Unable to load photo for frame export: {input_path}")
    height, width = image.shape[:2]
    if height > width:
        return PORTRAIT_FRAME_WIDTH, PORTRAIT_FRAME_HEIGHT, PORTRAIT_FRAME_PROFILE
    return DEFAULT_FRAME_WIDTH, DEFAULT_FRAME_HEIGHT, DEFAULT_FRAME_PROFILE


def _cleanup_old_frame_exports(
    conn,
    *,
    photo_id: int,
    keep_path: Path,
) -> None:
    existing_paths = list_photo_artifact_paths(
        conn,
        photo_id=photo_id,
        artifact_type=FRAME_EXPORT_ARTIFACT_TYPE,
    )
    for existing_path in existing_paths:
        if existing_path == keep_path:
            continue
        delete_photo_artifact(
            conn,
            photo_id=photo_id,
            artifact_type=FRAME_EXPORT_ARTIFACT_TYPE,
            path=existing_path,
        )
        if existing_path.exists():
            os.remove(existing_path)
