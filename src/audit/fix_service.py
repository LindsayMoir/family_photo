"""Apply automated fixes for spreadsheet-driven export audit issues."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import cv2
import numpy as np

from config import AppConfig
from db.connection import connect
from deskew.service import run_deskew
from disposition.service import set_photo_export_disposition
from enhance.service import run_enhancement
from frame_export.service import (
    FRAME_EXPORT_PIPELINE_VERSION,
    STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
    STAGING_LANDSCAPE_PROFILE,
    STAGING_PORTRAIT_PROFILE,
    resolve_frame_export_request,
    run_frame_export,
    stage_photo_exports,
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
SPLIT_ISSUES = {"MERGE", "CROP", "DUP"}
SUPPORTED_ISSUES = set(ROTATION_BY_ISSUE) | SPLIT_ISSUES | {"SKEW"}


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


@dataclass(frozen=True, slots=True)
class ManualSplitSummary:
    """Summary of applying a manual split and restaging the resulting children."""

    photo_id: int
    input_paths: tuple[Path, ...]
    staged_photo_ids: tuple[int, ...]
    resolved_task_id: int | None
    dry_run: bool


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

    for task in tasks:
        issue_codes = _task_issue_codes(task)
        note = str(task.payload_json.get("notes", "")).strip() or None
        if not issue_codes or not _issue_codes_supported(issue_codes):
            unresolved_count += 1
            continue

        try:
            created_photo_count += _apply_issue_codes(
                config,
                task=task,
                issue_codes=issue_codes,
                note=note,
            )
            fixed_count += 1
        except ValueError:
            unresolved_count += 1

    return ExportAuditFixSummary(
        fixed_count=fixed_count,
        unresolved_count=unresolved_count,
        created_photo_count=created_photo_count,
        dry_run=False,
    )


def manual_split_photo(
    config: AppConfig,
    *,
    photo_id: int,
    input_paths: list[Path],
    note: str | None,
    dry_run: bool,
) -> ManualSplitSummary:
    """Replace one photo with operator-provided split images and restage the results."""
    resolved_input_paths = _resolve_manual_split_inputs(input_paths)
    if dry_run:
        return ManualSplitSummary(
            photo_id=photo_id,
            input_paths=tuple(resolved_input_paths),
            staged_photo_ids=(photo_id,),
            resolved_task_id=None,
            dry_run=True,
        )

    context = _get_photo_repair_context(config, photo_id=photo_id)
    crops = [_load_manual_split_image(path) for path in resolved_input_paths]
    expected_staging_profiles = [_staging_profile_for_image(crop) for crop in crops]
    new_photo_ids = _rewrite_split_photo_arrays(
        config,
        context=context,
        crops=crops,
        pipeline_version="manual_split_v1",
        reuse_original_photo=False,
    )
    if not new_photo_ids:
        raise ValueError(f"Manual split did not create any child photos for photo_id={photo_id}.")

    set_photo_export_disposition(
        config,
        photo_id=context.photo_id,
        disposition="exclude_reject",
        note="replaced by manual split children",
        dry_run=False,
    )
    _reprocess_split_photo_ids(config, photo_ids=new_photo_ids)
    _enforce_staging_profiles(
        config,
        photo_ids=new_photo_ids,
        expected_profiles=expected_staging_profiles,
    )

    resolved_task_id = _find_open_export_audit_task_id(config, photo_id=photo_id)
    if resolved_task_id is not None:
        resolve_export_audit_review(
            config,
            task_id=resolved_task_id,
            export_action="fix_crop",
            note=note or "manual split applied",
            dry_run=False,
        )
    resolve_orientation_review_for_photo(
        config,
        photo_id=context.photo_id,
        action="excluded_via_manual_split",
    )
    for current_photo_id in new_photo_ids:
        resolve_orientation_review_for_photo(
            config,
            photo_id=current_photo_id,
            action="fixed_via_manual_split",
        )

    return ManualSplitSummary(
        photo_id=photo_id,
        input_paths=tuple(resolved_input_paths),
        staged_photo_ids=tuple(new_photo_ids),
        resolved_task_id=resolved_task_id,
        dry_run=False,
    )


def _is_supported_issue(task: ReviewTask) -> bool:
    issue_codes = _task_issue_codes(task)
    return bool(issue_codes) and _issue_codes_supported(issue_codes)

def _apply_issue_codes(
    config: AppConfig,
    *,
    task: ReviewTask,
    issue_codes: list[str],
    note: str | None,
    ) -> int:
    current_photo_ids = [task.entity_id]
    created_photo_count = 0
    split_issue = next((issue for issue in issue_codes if issue in SPLIT_ISSUES), None)
    if split_issue is not None:
        current_photo_ids = _apply_split_fix(
            config,
            task=task,
            note=note,
        )
        created_photo_count = max(0, len(current_photo_ids) - 1)

    for issue in issue_codes:
        if issue in SPLIT_ISSUES:
            continue
        if issue in ROTATION_BY_ISSUE:
            for photo_id in current_photo_ids:
                _apply_rotation_fix_to_photo(
                    config,
                    photo_id=photo_id,
                    rotation_degrees=ROTATION_BY_ISSUE[issue],
                )
            continue
        if issue == "SKEW":
            for photo_id in current_photo_ids:
                _apply_skew_fix_to_photo(
                    config,
                    photo_id=photo_id,
                    note=note,
                )

    for photo_id in current_photo_ids:
        resolve_orientation_review_for_photo(
            config,
            photo_id=photo_id,
            action="fixed_via_export_audit",
        )

    export_action = "fix_crop" if split_issue is not None else "fix_skew" if "SKEW" in issue_codes else "fix_rotation"
    resolve_export_audit_review(
        config,
        task_id=task.id,
        export_action=export_action,
        note=note,
        dry_run=False,
    )
    return created_photo_count


def _apply_rotation_fix_to_photo(
    config: AppConfig,
    *,
    photo_id: int,
    rotation_degrees: int,
) -> None:
    staged_context = _get_staged_photo_context(config, photo_id=photo_id)
    _apply_direct_transform(
        config,
        context=staged_context,
        transform=_rotate_image,
        transform_value=rotation_degrees,
    )


def _apply_skew_fix_to_photo(
    config: AppConfig,
    *,
    photo_id: int,
    note: str | None,
    ) -> None:
    forced_angle = _parse_forced_skew_angle(note)
    staged_context = _get_staged_photo_context(config, photo_id=photo_id)
    _apply_direct_transform(
        config,
        context=staged_context,
        transform=_deskew_image,
        transform_value=forced_angle,
    )


def _apply_split_fix(
    config: AppConfig,
    *,
    task: ReviewTask,
    note: str | None,
) -> list[int]:
    del note
    export_width, export_height, export_profile = resolve_frame_export_request(
        preset_name="auto",
        width_px=None,
        height_px=None,
        profile_name=None,
    )
    context = _get_photo_repair_context(config, photo_id=task.entity_id)
    source_path = config.photos_root.parent / context.raw_crop_path
    image = cv2.imread(str(source_path))
    if image is None:
        raise ValueError(f"Unable to load merged raw crop: {source_path}")

    regions = _detect_split_regions(image)
    if len(regions) < 2:
        raise ValueError(f"Unable to split merged raw crop for photo_id={context.photo_id}.")

    crops = [image[y1:y2, x1:x2] for (x1, y1, x2, y2) in regions]
    new_photo_ids = _rewrite_split_photo_arrays(
        config,
        context=context,
        crops=crops,
        pipeline_version="auto_split_v1",
    )
    all_photo_ids = [context.photo_id, *new_photo_ids]
    _reprocess_split_photo_ids(
        config,
        photo_ids=all_photo_ids,
        export_width=export_width,
        export_height=export_height,
        export_profile=export_profile,
    )
    return all_photo_ids


def _ensure_photo_has_staging_export(
    config: AppConfig,
    *,
    photo_id: int,
) -> Path:
    artifact_path = _get_existing_staging_export_path(config, photo_id=photo_id)
    if artifact_path is not None and artifact_path.exists():
        return artifact_path

    stage_photo_exports(
        config,
        photo_ids=[photo_id],
        dry_run=False,
    )
    artifact_path = _get_existing_staging_export_path(config, photo_id=photo_id)
    if artifact_path is None or not artifact_path.exists():
        raise ValueError(f"Split photo {photo_id} did not produce a staging export.")
    return artifact_path


def _get_existing_staging_export_path(
    config: AppConfig,
    *,
    photo_id: int,
) -> Path | None:
    with connect(config) as conn:
        return get_photo_artifact_path(
            conn,
            photo_id=photo_id,
            artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
        )


def _resolve_manual_split_inputs(input_paths: list[Path]) -> list[Path]:
    if len(input_paths) < 2:
        raise ValueError("Manual split requires at least two --input files.")
    resolved: list[Path] = []
    for input_path in input_paths:
        resolved_path = input_path.expanduser().resolve()
        if not resolved_path.exists():
            raise ValueError(f"Manual split input was not found: {resolved_path}")
        if not resolved_path.is_file():
            raise ValueError(f"Manual split input is not a file: {resolved_path}")
        resolved.append(resolved_path)
    return resolved


def _load_manual_split_image(path: Path):
    image = cv2.imread(str(path))
    if image is None:
        raise ValueError(f"Unable to load manual split image: {path}")
    if image.size == 0:
        raise ValueError(f"Manual split image is empty: {path}")
    return image


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


def _get_staged_photo_context(config: AppConfig, *, photo_id: int) -> StagedPhotoContext:
    with connect(config) as conn:
        photo = get_photo_record(conn, photo_id=photo_id)
        artifact_path = get_photo_artifact_path(
            conn,
            photo_id=photo_id,
            artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
        )

    if artifact_path is None:
        raise ValueError(f"Photo {photo_id} does not have a staged export to repair.")

    return StagedPhotoContext(
        photo_id=photo_id,
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


def _reprocess_split_photo_ids(
    config: AppConfig,
    *,
    photo_ids: list[int],
    export_width: int | None = None,
    export_height: int | None = None,
    export_profile: str | None = None,
) -> None:
    resolved_width = export_width
    resolved_height = export_height
    resolved_profile = export_profile
    if resolved_width is None or resolved_height is None or resolved_profile is None:
        resolved_width, resolved_height, resolved_profile = resolve_frame_export_request(
            preset_name="auto",
            width_px=None,
            height_px=None,
            profile_name=None,
        )

    for photo_id in photo_ids:
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
            width_px=resolved_width,
            height_px=resolved_height,
            profile_name=resolved_profile,
            dry_run=False,
        )
        _ensure_photo_has_staging_export(config, photo_id=photo_id)


def _enforce_staging_profiles(
    config: AppConfig,
    *,
    photo_ids: list[int],
    expected_profiles: list[str],
) -> None:
    if len(photo_ids) != len(expected_profiles):
        raise ValueError("Photo ids and expected profiles must have the same length.")

    with connect(config) as conn:
        for photo_id, expected_profile in zip(photo_ids, expected_profiles, strict=True):
            current_path = get_photo_artifact_path(
                conn,
                photo_id=photo_id,
                artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
            )
            if current_path is None:
                raise ValueError(f"Photo {photo_id} does not have a staged export to correct.")
            target_path = config.photos_root / "exports" / expected_profile / current_path.name
            if current_path == target_path:
                continue
            target_path.parent.mkdir(parents=True, exist_ok=True)
            if target_path.exists():
                target_path.unlink()
            current_path.replace(target_path)
            delete_photo_artifact(
                conn,
                photo_id=photo_id,
                artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
                path=current_path,
            )
            insert_photo_artifact(
                conn,
                photo_id=photo_id,
                artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
                path=target_path,
                pipeline_stage="frame_export",
                pipeline_version=FRAME_EXPORT_PIPELINE_VERSION,
            )
        conn.commit()


def _task_issue_codes(task: ReviewTask) -> list[str]:
    payload_codes = task.payload_json.get("issue_codes")
    if isinstance(payload_codes, list):
        codes: list[str] = []
        for value in payload_codes:
            code = str(value).strip().upper()
            if not code or code in codes:
                continue
            codes.append(code)
        if codes:
            return codes

    raw_issue = str(task.payload_json.get("issue", "")).strip()
    if not raw_issue:
        return []
    codes = []
    for value in re.split(r"[,;\n]+", raw_issue):
        code = value.strip().upper()
        if not code or code in codes:
            continue
        codes.append(code)
    return codes


def _issue_codes_supported(issue_codes: list[str]) -> bool:
    return all(issue in SUPPORTED_ISSUES for issue in issue_codes)


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
    merged_regions = _merge_overlapping_regions(regions)
    if len(merged_regions) >= 2:
        return merged_regions
    seam_regions = _detect_regions_from_vertical_seam(gray)
    if seam_regions:
        return seam_regions
    return merged_regions


def _detect_regions_from_vertical_seam(gray_image) -> list[tuple[int, int, int, int]]:
    height, width = gray_image.shape[:2]
    minimum_side_width = max(1, int(width * 0.2))
    if width < 2 * minimum_side_width:
        return []

    column_means = gray_image.mean(axis=0)
    smoothed_means = np.convolve(column_means, np.ones(15, dtype=float) / 15.0, mode="same")
    edge_strength = np.abs(np.diff(smoothed_means))
    search_start = minimum_side_width
    search_end = width - minimum_side_width
    if search_end - search_start < 2:
        return []

    local_edges = edge_strength[search_start : search_end - 1]
    if local_edges.size == 0:
        return []

    split_x = int(np.argmax(local_edges) + search_start + 1)
    if split_x <= minimum_side_width or width - split_x <= minimum_side_width:
        return []
    if float(edge_strength[split_x - 1]) < 2.0:
        return []
    return [
        (0, 0, split_x, height),
        (split_x, 0, width, height),
    ]


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
    crops = [image[y1:y2, x1:x2] for (x1, y1, x2, y2) in regions]
    return _rewrite_split_photo_arrays(
        config,
        context=context,
        crops=crops,
        pipeline_version="auto_split_v1",
    )


def _rewrite_split_photo_arrays(
    config: AppConfig,
    *,
    context: PhotoRepairContext,
    crops: list,
    pipeline_version: str,
    reuse_original_photo: bool = True,
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

            for region_index, crop in enumerate(crops, start=1):
                if crop.size == 0:
                    continue
                if reuse_original_photo and region_index == 1:
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
                    pipeline_version=pipeline_version,
                )
        conn.commit()
    return new_photo_ids


def _find_open_export_audit_task_id(config: AppConfig, *, photo_id: int) -> int | None:
    with connect(config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM review_tasks
                WHERE entity_type = 'photo'
                  AND entity_id = %s
                  AND task_type = 'review_export_audit'
                  AND status = 'open'
                ORDER BY id DESC
                LIMIT 1
                """,
                (photo_id,),
            )
            row = cur.fetchone()
    if row is None:
        return None
    return int(row[0])
