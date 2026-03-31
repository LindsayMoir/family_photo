"""Apply automated fixes for spreadsheet-driven export audit issues."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import cv2

from config import AppConfig
from db.connection import connect
from deskew.service import run_deskew
from enhance.service import run_enhancement
from frame_export.service import (
    FRAME_EXPORT_PIPELINE_VERSION,
    STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
    STAGING_LANDSCAPE_PROFILE,
    STAGING_PORTRAIT_PROFILE,
    resolve_frame_export_request,
    run_frame_export,
)
from orientation.service import run_orientation
from photo_repository import (
    delete_photo_artifact,
    get_photo_artifact_path,
    get_photo_record,
    insert_photo_artifact,
)
from review.models import ReviewTask
from review.service import list_tasks, resolve_export_audit_review, resolve_orientation_review_for_photo


ROTATION_BY_ISSUE = {
    "RR90": 90,
    "RL90": 270,
    "R180": 180,
    "FLIP": 180,
}


@dataclass(frozen=True, slots=True)
class ExportAuditFixSummary:
    """Summary of auto-fixes applied from export audit issues."""

    fixed_count: int
    unresolved_count: int
    created_photo_count: int
    dry_run: bool


@dataclass(frozen=True, slots=True)
class PhotoRepairContext:
    """Minimal metadata required to repair and restage one photo."""

    photo_id: int
    sheet_scan_id: int
    crop_index: int
    raw_crop_path: Path


@dataclass(frozen=True, slots=True)
class StagedPhotoContext:
    """Paths needed to repair an operator-reviewed staged export."""

    photo_id: int
    working_path: Path
    staging_path: Path


def apply_export_audit_fixes(
    config: AppConfig,
    *,
    dry_run: bool,
) -> ExportAuditFixSummary:
    """Apply all supported open export-audit fixes and restage the results."""
    tasks = list_tasks(
        config,
        task_type="review_export_audit",
        status="open",
        limit=500,
    )
    if dry_run:
        return ExportAuditFixSummary(
            fixed_count=sum(1 for task in tasks if _is_supported_issue(task)),
            unresolved_count=sum(1 for task in tasks if not _is_supported_issue(task)),
            created_photo_count=0,
            dry_run=True,
        )

    fixed_count = 0
    unresolved_count = 0
    created_photo_count = 0
    export_width, export_height, export_profile = resolve_frame_export_request(
        preset_name="auto",
        width_px=None,
        height_px=None,
        profile_name=None,
    )

    for task in tasks:
        issue = str(task.payload_json.get("issue", "")).strip().upper()
        note = str(task.payload_json.get("notes", "")).strip() or None
        try:
            if issue in ROTATION_BY_ISSUE:
                _apply_rotation_fix(
                    config,
                    task=task,
                    rotation_degrees=ROTATION_BY_ISSUE[issue],
                    note=note,
                    export_width=export_width,
                    export_height=export_height,
                    export_profile=export_profile,
                )
                fixed_count += 1
                continue
            if issue in {"MERGE", "CROP", "DUP"}:
                created_photo_count += _apply_split_fix(
                    config,
                    task=task,
                    note=note,
                    export_width=export_width,
                    export_height=export_height,
                    export_profile=export_profile,
                )
                fixed_count += 1
                continue
            if issue == "SKEW":
                _apply_skew_fix(
                    config,
                    task=task,
                    note=note,
                    export_width=export_width,
                    export_height=export_height,
                    export_profile=export_profile,
                )
                fixed_count += 1
                continue
        except ValueError:
            unresolved_count += 1
            continue

        unresolved_count += 1

    return ExportAuditFixSummary(
        fixed_count=fixed_count,
        unresolved_count=unresolved_count,
        created_photo_count=created_photo_count,
        dry_run=False,
    )


def _is_supported_issue(task: ReviewTask) -> bool:
    issue = str(task.payload_json.get("issue", "")).strip().upper()
    return issue in ROTATION_BY_ISSUE or issue in {"MERGE", "CROP", "DUP", "SKEW"}


def _apply_rotation_fix(
    config: AppConfig,
    *,
    task: ReviewTask,
    rotation_degrees: int,
    note: str | None,
    export_width: int,
    export_height: int,
    export_profile: str,
) -> None:
    del export_width, export_height, export_profile
    staged_context = _get_staged_photo_context(config, task=task)
    _apply_direct_transform(
        config,
        context=staged_context,
        transform=_rotate_image,
        transform_value=rotation_degrees,
    )
    resolve_export_audit_review(
        config,
        task_id=task.id,
        export_action="fix_rotation",
        note=note,
        dry_run=False,
    )
    resolve_orientation_review_for_photo(
        config,
        photo_id=task.entity_id,
        action="fixed_via_export_audit",
    )


def _apply_skew_fix(
    config: AppConfig,
    *,
    task: ReviewTask,
    note: str | None,
    export_width: int,
    export_height: int,
    export_profile: str,
) -> None:
    forced_angle = _parse_forced_skew_angle(note)
    del export_width, export_height, export_profile
    staged_context = _get_staged_photo_context(config, task=task)
    _apply_direct_transform(
        config,
        context=staged_context,
        transform=_deskew_image,
        transform_value=forced_angle,
    )
    resolve_export_audit_review(
        config,
        task_id=task.id,
        export_action="fix_skew",
        note=note,
        dry_run=False,
    )
    resolve_orientation_review_for_photo(
        config,
        photo_id=task.entity_id,
        action="fixed_via_export_audit",
    )


def _apply_split_fix(
    config: AppConfig,
    *,
    task: ReviewTask,
    note: str | None,
    export_width: int,
    export_height: int,
    export_profile: str,
) -> int:
    context = _get_photo_repair_context(config, photo_id=task.entity_id)
    source_path = config.photos_root.parent / context.raw_crop_path
    image = cv2.imread(str(source_path))
    if image is None:
        raise ValueError(f"Unable to load merged raw crop: {source_path}")

    regions = _detect_split_regions(image)
    if len(regions) < 2:
        raise ValueError(f"Unable to split merged raw crop for photo_id={context.photo_id}.")

    new_photo_ids = _rewrite_split_photos(
        config,
        context=context,
        image=image,
        regions=regions,
    )
    all_photo_ids = [context.photo_id, *new_photo_ids]

    for photo_id in all_photo_ids:
        run_deskew(config, photo_id=photo_id, dry_run=False)
        orientation_probe = run_orientation(config, photo_id=photo_id, dry_run=True)
        if orientation_probe.review_required:
            run_orientation(
                config,
                photo_id=photo_id,
                forced_rotation=orientation_probe.rotation_degrees,
                dry_run=False,
            )
        else:
            run_orientation(config, photo_id=photo_id, dry_run=False)
        run_enhancement(config, photo_id=photo_id, dry_run=False)
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

    resolve_export_audit_review(
        config,
        task_id=task.id,
        export_action="fix_crop",
        note=note,
        dry_run=False,
    )
    return len(new_photo_ids)


def _get_photo_repair_context(config: AppConfig, *, photo_id: int) -> PhotoRepairContext:
    with connect(config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT sheet_scan_id, crop_index, raw_crop_path
                FROM photos
                WHERE id = %s
                """,
                (photo_id,),
            )
            row = cur.fetchone()
    if row is None:
        raise ValueError(f"Photo {photo_id} was not found.")
    return PhotoRepairContext(
        photo_id=photo_id,
        sheet_scan_id=int(row[0]),
        crop_index=int(row[1]),
        raw_crop_path=Path(str(row[2])),
    )


def _get_staged_photo_context(config: AppConfig, *, task: ReviewTask) -> StagedPhotoContext:
    with connect(config) as conn:
        photo = get_photo_record(conn, photo_id=task.entity_id)
        artifact_path = get_photo_artifact_path(
            conn,
            photo_id=task.entity_id,
            artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
        )

    if artifact_path is None:
        payload_export_path = str(task.payload_json.get("export_path", "")).strip()
        if payload_export_path:
            artifact_path = Path(payload_export_path)
    if artifact_path is None:
        raise ValueError(f"Photo {task.entity_id} does not have a staged export to repair.")

    return StagedPhotoContext(
        photo_id=task.entity_id,
        working_path=photo.working_path,
        staging_path=artifact_path,
    )


def _parse_forced_skew_angle(note: str | None) -> float | None:
    if not note:
        return None
    match = re.search(r"(-?\d+(?:\.\d+)?)", note)
    if match is None:
        return None
    angle = float(match.group(1))
    lowered = note.lower()
    if "left" in lowered:
        return -abs(angle)
    if "right" in lowered:
        return abs(angle)
    return angle


def _apply_direct_transform(
    config: AppConfig,
    *,
    context: StagedPhotoContext,
    transform,
    transform_value: int | float | None,
) -> None:
    working_absolute_path = config.photos_root.parent / context.working_path
    staging_absolute_path = context.staging_path

    working_image = _load_image(working_absolute_path)
    staging_image = _load_image(staging_absolute_path)

    transformed_working = transform(working_image, transform_value)
    transformed_staging = transform(staging_image, transform_value)

    _write_image(working_absolute_path, transformed_working)
    new_staging_path = _write_staging_image(
        config,
        photo_id=context.photo_id,
        original_staging_path=staging_absolute_path,
        image=transformed_staging,
    )

    with connect(config) as conn:
        _sync_photo_fix_metadata(
            conn,
            photo_id=context.photo_id,
            transform=transform,
            transform_value=transform_value,
        )
        if new_staging_path != staging_absolute_path:
            delete_photo_artifact(
                conn,
                photo_id=context.photo_id,
                artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
                path=staging_absolute_path,
            )
        insert_photo_artifact(
            conn,
            photo_id=context.photo_id,
            artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
            path=new_staging_path,
            pipeline_stage="frame_export",
            pipeline_version=FRAME_EXPORT_PIPELINE_VERSION,
        )
        conn.commit()


def _sync_photo_fix_metadata(
    conn,
    *,
    photo_id: int,
    transform,
    transform_value: int | float | None,
) -> None:
    if transform is _rotate_image:
        delta = int(transform_value or 0) % 360
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE photos
                SET rotation_degrees = MOD(COALESCE(rotation_degrees, 0) + %s, 360),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (delta, photo_id),
            )
        return

    if transform is _deskew_image:
        angle = float(transform_value or 0.0)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE photos
                SET deskew_angle = COALESCE(deskew_angle, 0) + %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (angle, photo_id),
            )
        return

    raise ValueError("Unsupported transform metadata sync.")


def _load_image(path: Path):
    image = cv2.imread(str(path))
    if image is None:
        raise ValueError(f"Unable to load image for staged repair: {path}")
    return image


def _write_image(path: Path, image) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not cv2.imwrite(str(path), image, [int(cv2.IMWRITE_JPEG_QUALITY), 95]):
        raise ValueError(f"Failed to write repaired image: {path}")


def _write_staging_image(
    config: AppConfig,
    *,
    photo_id: int,
    original_staging_path: Path,
    image,
) -> Path:
    target_profile = _staging_profile_for_image(image)
    target_path = config.photos_root / "exports" / target_profile / original_staging_path.name
    _write_image(target_path, image)
    if target_path != original_staging_path and original_staging_path.exists():
        original_staging_path.unlink()
    return target_path


def _staging_profile_for_image(image) -> str:
    height, width = image.shape[:2]
    if height > width:
        return STAGING_PORTRAIT_PROFILE
    return STAGING_LANDSCAPE_PROFILE


def _rotate_image(image, rotation_degrees: int | float | None):
    normalized = int(rotation_degrees or 0) % 360
    if normalized == 0:
        return image
    if normalized == 90:
        return cv2.rotate(image, cv2.ROTATE_90_CLOCKWISE)
    if normalized == 180:
        return cv2.rotate(image, cv2.ROTATE_180)
    if normalized == 270:
        return cv2.rotate(image, cv2.ROTATE_90_COUNTERCLOCKWISE)
    raise ValueError(f"Unsupported direct rotation: {rotation_degrees}")


def _deskew_image(image, angle_degrees: int | float | None):
    angle = float(angle_degrees or 0.0)
    if abs(angle) < 1e-6:
        return image

    height, width = image.shape[:2]
    center = (width / 2.0, height / 2.0)
    matrix = cv2.getRotationMatrix2D(center, angle, 1.0)
    cosine = abs(matrix[0, 0])
    sine = abs(matrix[0, 1])
    bound_width = max(1, int(round((height * sine) + (width * cosine))))
    bound_height = max(1, int(round((height * cosine) + (width * sine))))
    matrix[0, 2] += (bound_width / 2.0) - center[0]
    matrix[1, 2] += (bound_height / 2.0) - center[1]
    return cv2.warpAffine(
        image,
        matrix,
        (bound_width, bound_height),
        flags=cv2.INTER_LINEAR,
        borderMode=cv2.BORDER_REPLICATE,
    )


def _detect_split_regions(image) -> list[tuple[int, int, int, int]]:
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (5, 5), 0)
    _, threshold = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    if threshold.mean() > 127:
        threshold = 255 - threshold

    contours, _ = cv2.findContours(threshold, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    image_area = float(image.shape[0] * image.shape[1])
    regions: list[tuple[int, int, int, int]] = []
    for contour in contours:
        x, y, width, height = cv2.boundingRect(contour)
        area = float(width * height)
        if area < image_area * 0.03:
            continue
        regions.append((x, y, x + width, y + height))

    regions.sort(key=lambda region: (region[0], region[1]))
    return _merge_overlapping_regions(regions)


def _merge_overlapping_regions(
    regions: list[tuple[int, int, int, int]]
) -> list[tuple[int, int, int, int]]:
    if not regions:
        return []

    merged: list[list[int]] = [[*regions[0]]]
    for region in regions[1:]:
        current = merged[-1]
        if region[0] <= current[2] and region[1] <= current[3] and region[3] >= current[1]:
            current[0] = min(current[0], region[0])
            current[1] = min(current[1], region[1])
            current[2] = max(current[2], region[2])
            current[3] = max(current[3], region[3])
            continue
        merged.append([*region])
    return [(left, top, right, bottom) for left, top, right, bottom in merged]


def _rewrite_split_photos(
    config: AppConfig,
    *,
    context: PhotoRepairContext,
    image,
    regions: list[tuple[int, int, int, int]],
) -> list[int]:
    new_photo_ids: list[int] = []
    with connect(config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(MAX(crop_index), 0)
                FROM photos
                WHERE sheet_scan_id = %s
                """,
                (context.sheet_scan_id,),
            )
            max_crop_index = int(cur.fetchone()[0])

            for region_index, (x1, y1, x2, y2) in enumerate(regions, start=1):
                crop = image[y1:y2, x1:x2]
                if crop.size == 0:
                    continue
                if region_index == 1:
                    target_photo_id = context.photo_id
                    crop_index = context.crop_index
                else:
                    max_crop_index += 1
                    crop_index = max_crop_index
                    cur.execute(
                        """
                        INSERT INTO photos (
                            sheet_scan_id,
                            accepted_detection_id,
                            crop_index,
                            raw_crop_path,
                            working_path,
                            width_px,
                            height_px,
                            status,
                            export_disposition
                        )
                        VALUES (%s, NULL, %s, %s, %s, %s, %s, 'crop_complete', 'include')
                        RETURNING id
                        """,
                        (
                            context.sheet_scan_id,
                            crop_index,
                            "",
                            "",
                            int(crop.shape[1]),
                            int(crop.shape[0]),
                        ),
                    )
                    target_photo_id = int(cur.fetchone()[0])
                    new_photo_ids.append(target_photo_id)

                relative_crop_path = Path("photos") / "crops" / f"sheet_{context.sheet_scan_id}" / f"crop_{crop_index}.jpg"
                absolute_crop_path = config.photos_root.parent / relative_crop_path
                absolute_crop_path.parent.mkdir(parents=True, exist_ok=True)
                if not cv2.imwrite(str(absolute_crop_path), crop):
                    raise ValueError(f"Failed to write split crop: {absolute_crop_path}")

                cur.execute(
                    """
                    UPDATE photos
                    SET accepted_detection_id = NULL,
                        raw_crop_path = %s,
                        working_path = %s,
                        width_px = %s,
                        height_px = %s,
                        status = 'crop_complete',
                        deskew_angle = NULL,
                        deskew_confidence = NULL,
                        rotation_degrees = NULL,
                        enhancement_version = NULL,
                        export_disposition = 'include',
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (
                        str(relative_crop_path),
                        str(relative_crop_path),
                        int(crop.shape[1]),
                        int(crop.shape[0]),
                        target_photo_id,
                    ),
                )

                insert_photo_artifact(
                    conn,
                    photo_id=target_photo_id,
                    artifact_type="raw_crop",
                    path=relative_crop_path,
                    pipeline_stage="crop",
                    pipeline_version="auto_split_v1",
                )
        conn.commit()
    return new_photo_ids
