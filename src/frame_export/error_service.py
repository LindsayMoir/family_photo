"""Utilities for restaging final export errors back into the audit workflow."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import shutil

import cv2

from config import AppConfig
from db.connection import connect
from frame_export.service import (
    DEFAULT_FRAME_PROFILE,
    DEFAULT_FRAME_WIDTH,
    DEFAULT_FRAME_HEIGHT,
    FRAME_EXPORT_ARTIFACT_TYPE,
    FRAME_EXPORT_PIPELINE_VERSION,
    PORTRAIT_FRAME_PROFILE,
    PORTRAIT_FRAME_WIDTH,
    PORTRAIT_FRAME_HEIGHT,
    STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
    STAGING_LANDSCAPE_PROFILE,
    STAGING_PORTRAIT_PROFILE,
    stage_photo_exports,
)
from disposition.service import set_photo_export_disposition
from photo_repository import (
    delete_photo_artifact,
    get_photo_record,
    insert_photo_artifact,
    list_export_ready_photo_ids,
    list_photo_artifact_paths,
    list_photo_ids,
    update_photo_stage,
)
from review.service import resolve_export_audit_review, resolve_orientation_review_for_photo


_PHOTO_ID_RE = re.compile(r"(?:^|[/\\])photo_(?P<photo_id>\d+)\.jpg$", re.IGNORECASE)
_PHOTO_CHILD_RE = re.compile(
    r"(?:^|[/\\])photo_(?P<photo_id>\d+)_(?P<child_index>\d+)\.jpg$",
    re.IGNORECASE,
)
_BARE_ID_RE = re.compile(r"^(?P<photo_id>\d+)$")


@dataclass(frozen=True, slots=True)
class RestageExportErrorsSummary:
    """Summary of restaging selected final exports back into staging review."""

    errors_path: Path
    csv_path: Path
    requested_count: int
    staged_count: int
    audited_count: int
    missing_entries: tuple[str, ...]
    dry_run: bool


@dataclass(frozen=True, slots=True)
class RequeueFinalExportsSummary:
    """Summary of moving final exports back into staging for audit."""

    source_dir: Path
    csv_path: Path
    queued_count: int
    audited_count: int
    dry_run: bool


@dataclass(frozen=True, slots=True)
class StageNextExportsSummary:
    """Summary of staging the next export-ready batch for operator review."""

    target: str
    csv_path: Path
    selected_count: int
    staged_count: int
    audited_count: int
    dry_run: bool


@dataclass(frozen=True, slots=True)
class ApplyManualStagingEditsSummary:
    """Summary of applying operator edits from staging/temp back into staged exports."""

    temp_dir: Path
    edited_count: int
    staged_count: int
    missing_entries: tuple[str, ...]
    dry_run: bool


def restage_export_errors(
    config: AppConfig,
    *,
    errors_path: Path,
    csv_path: Path | None,
    dry_run: bool,
) -> RestageExportErrorsSummary:
    """Restage listed final exports and rebuild an audit CSV for just those photos."""
    if not errors_path.exists():
        raise ValueError(f"Errors file was not found: {errors_path}")

    requested_entries = _read_error_entries(errors_path)
    photo_ids, missing_entries = _resolve_error_photo_ids(config, requested_entries)
    output_csv_path = (
        csv_path
        if csv_path is not None
        else config.photos_root / "exports" / "staging" / "export_audit.csv"
    )

    if dry_run:
        return RestageExportErrorsSummary(
            errors_path=errors_path,
            csv_path=output_csv_path,
            requested_count=len(requested_entries),
            staged_count=len(photo_ids),
            audited_count=len(photo_ids),
            missing_entries=tuple(missing_entries),
            dry_run=True,
        )

    if not photo_ids:
        raise ValueError("No valid photo ids were found in the errors file.")

    stage_photo_exports(config, photo_ids=photo_ids, dry_run=False)

    from audit.service import _write_audit_csv, run_export_audit

    findings = []
    for photo_id in photo_ids:
        summary = run_export_audit(
            config,
            batch_name=None,
            sheet_id=None,
            photo_id=photo_id,
            limit=None,
            category=None,
            csv_path=None,
            dry_run=True,
        )
        findings.extend(summary.findings)

    _write_audit_csv(output_csv_path, findings, config=config)
    return RestageExportErrorsSummary(
        errors_path=errors_path,
        csv_path=output_csv_path,
        requested_count=len(requested_entries),
        staged_count=len(photo_ids),
        audited_count=len(findings),
        missing_entries=tuple(missing_entries),
        dry_run=False,
    )


def _read_error_entries(errors_path: Path) -> list[str]:
    entries: list[str] = []
    for raw_line in errors_path.read_text(encoding="utf-8").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        entries.append(stripped)
    return entries


def _resolve_error_photo_ids(
    config: AppConfig,
    entries: list[str],
) -> tuple[list[int], list[str]]:
    photo_ids: list[int] = []
    missing_entries: list[str] = []
    seen_photo_ids: set[int] = set()
    for entry in entries:
        photo_id = _photo_id_from_error_entry(entry)
        if photo_id is None or not _final_export_exists(config, photo_id):
            missing_entries.append(entry)
            continue
        if photo_id in seen_photo_ids:
            continue
        seen_photo_ids.add(photo_id)
        photo_ids.append(photo_id)
    return photo_ids, missing_entries


def _photo_id_from_error_entry(entry: str) -> int | None:
    match = _PHOTO_ID_RE.search(entry)
    if match is not None:
        return int(match.group("photo_id"))
    bare_match = _BARE_ID_RE.match(entry)
    if bare_match is not None:
        return int(bare_match.group("photo_id"))
    return None


def _final_export_exists(config: AppConfig, photo_id: int) -> bool:
    filename = f"photo_{photo_id}.jpg"
    final_root = config.photos_root / "exports"
    return any(
        (final_root / folder_name / filename).exists()
        for folder_name in ("frame_1920x1080", "frame_1080x1920")
    )


def requeue_final_exports_for_audit(
    config: AppConfig,
    *,
    source_profile: str,
    csv_path: Path | None,
    dry_run: bool,
) -> RequeueFinalExportsSummary:
    """Move final exports back into staging and rebuild an audit CSV for them."""
    source_dir = config.photos_root / "exports" / source_profile
    if not source_dir.exists():
        raise ValueError(f"Final export folder was not found: {source_dir}")

    final_paths = sorted(
        path for path in source_dir.iterdir()
        if path.is_file() and _photo_id_from_error_entry(path.name) is not None
    )
    if not final_paths:
        raise ValueError(f"No photo exports were found in {source_dir}.")

    photo_ids = [_photo_id_from_error_entry(path.name) for path in final_paths]
    output_csv_path = (
        csv_path
        if csv_path is not None
        else config.photos_root / "exports" / "staging" / "export_audit.csv"
    )
    staging_profile = _staging_profile_for_final_profile(source_profile)

    if dry_run:
        return RequeueFinalExportsSummary(
            source_dir=source_dir,
            csv_path=output_csv_path,
            queued_count=len(final_paths),
            audited_count=len(final_paths),
            dry_run=True,
        )

    with connect(config) as conn:
        for final_path, photo_id in zip(final_paths, photo_ids, strict=True):
            if photo_id is None:
                continue
            staging_path = config.photos_root / "exports" / staging_profile / final_path.name
            staging_path.parent.mkdir(parents=True, exist_ok=True)
            _clear_existing_artifacts(
                conn,
                photo_id=photo_id,
                artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
            )
            _clear_existing_artifacts(
                conn,
                photo_id=photo_id,
                artifact_type=FRAME_EXPORT_ARTIFACT_TYPE,
                preserve_path=final_path,
            )
            if staging_path.exists():
                staging_path.unlink()
            final_path.replace(staging_path)
            delete_photo_artifact(
                conn,
                photo_id=photo_id,
                artifact_type=FRAME_EXPORT_ARTIFACT_TYPE,
                path=final_path,
            )
            insert_photo_artifact(
                conn,
                photo_id=photo_id,
                artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
                path=staging_path,
                pipeline_stage="frame_export",
                pipeline_version=FRAME_EXPORT_PIPELINE_VERSION,
            )
        conn.commit()

    from audit.service import _classify_records, _write_audit_csv
    from photo_repository import list_export_audit_records

    with connect(config) as conn:
        records = list_export_audit_records(
            conn,
            artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
            batch_name=None,
            sheet_id=None,
            photo_id=None,
            limit=None,
        )
    target_photo_ids = {photo_id for photo_id in photo_ids if photo_id is not None}
    findings = _classify_records(
        [record for record in records if record.photo_id in target_photo_ids],
        config=config,
    )
    _write_audit_csv(output_csv_path, findings, config=config)
    return RequeueFinalExportsSummary(
        source_dir=source_dir,
        csv_path=output_csv_path,
        queued_count=len(final_paths),
        audited_count=len(findings),
        dry_run=False,
    )


def stage_next_exports_for_audit(
    config: AppConfig,
    *,
    batch_name: str | None,
    sheet_id: int | None,
    limit: int,
    csv_path: Path | None,
    dry_run: bool,
) -> StageNextExportsSummary:
    """Stage the next export-ready batch and rebuild a focused audit CSV."""
    if limit <= 0:
        raise ValueError("Limit must be a positive integer.")

    output_csv_path = (
        csv_path
        if csv_path is not None
        else config.photos_root / "exports" / "staging" / "export_audit.csv"
    )
    if not dry_run:
        from audit.service import _reconcile_staging_export_artifacts

        with connect(config) as conn:
            _reconcile_staging_export_artifacts(
                conn,
                config,
                batch_name=batch_name,
                sheet_id=sheet_id,
                photo_id=None,
                limit=None,
            )
            conn.commit()

    with connect(config) as conn:
        photo_ids = list_export_ready_photo_ids(
            conn,
            batch_name=batch_name,
            sheet_id=sheet_id,
            photo_id=None,
            exclude_final_exported=True,
            limit=limit,
        )

    target = batch_name if batch_name is not None else f"sheet_id={sheet_id}" if sheet_id is not None else "all_export_ready"
    if not photo_ids:
        raise ValueError(f"No export-ready photos were found for target '{target}'.")

    if dry_run:
        return StageNextExportsSummary(
            target=target,
            csv_path=output_csv_path,
            selected_count=len(photo_ids),
            staged_count=len(photo_ids),
            audited_count=len(photo_ids),
            dry_run=True,
        )

    _clear_unselected_staging_exports(
        config,
        batch_name=batch_name,
        sheet_id=sheet_id,
        keep_photo_ids=set(photo_ids),
    )
    stage_photo_exports(config, photo_ids=photo_ids, dry_run=False)

    from audit.service import _classify_records, _write_audit_csv
    from photo_repository import list_export_audit_records

    with connect(config) as conn:
        records = list_export_audit_records(
            conn,
            artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
            batch_name=None,
            sheet_id=None,
            photo_id=None,
            limit=None,
        )
    target_photo_ids = set(photo_ids)
    findings = _classify_records(
        [record for record in records if record.photo_id in target_photo_ids],
        config=config,
    )
    _write_audit_csv(output_csv_path, findings, config=config)
    return StageNextExportsSummary(
        target=target,
        csv_path=output_csv_path,
        selected_count=len(photo_ids),
        staged_count=len(photo_ids),
        audited_count=len(findings),
        dry_run=False,
    )


def apply_manual_staging_edits(
    config: AppConfig,
    *,
    temp_dir: Path | None,
    dry_run: bool,
) -> ApplyManualStagingEditsSummary:
    """Apply operator-edited files from staging/temp and regenerate staging exports for them."""
    resolved_temp_dir = (
        temp_dir
        if temp_dir is not None
        else config.photos_root / "exports" / "staging" / "temp"
    )
    if not resolved_temp_dir.exists():
        raise ValueError(f"Manual edit temp folder was not found: {resolved_temp_dir}")
    if not resolved_temp_dir.is_dir():
        raise ValueError(f"Manual edit temp path is not a directory: {resolved_temp_dir}")

    entries = sorted(path for path in resolved_temp_dir.iterdir() if path.is_file())
    if not entries:
        raise ValueError(f"No edited files were found in {resolved_temp_dir}.")

    direct_edits, split_groups, missing_entries = _classify_manual_edit_entries(entries)
    edited_count = len(direct_edits) + len(split_groups)
    staged_count = len(direct_edits) + sum(len(input_paths) for input_paths in split_groups.values())

    if dry_run:
        return ApplyManualStagingEditsSummary(
            temp_dir=resolved_temp_dir,
            edited_count=edited_count,
            staged_count=staged_count,
            missing_entries=tuple(missing_entries),
            dry_run=True,
        )

    with connect(config) as conn:
        for photo_id, path in direct_edits.items():
            photo = get_photo_record(conn, photo_id=photo_id)
            working_path = config.photos_root.parent / photo.working_path
            working_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, working_path)
            update_photo_stage(
                conn,
                photo_id=photo_id,
                working_path=photo.working_path,
                status=photo.status,
            )
            path.unlink()
        conn.commit()

    if direct_edits:
        _stage_exact_manual_edits(config, photo_ids=sorted(direct_edits))

    if split_groups:
        for photo_id, input_paths in split_groups.items():
            _apply_exact_manual_split(
                config,
                photo_id=photo_id,
                input_paths=list(input_paths),
                note="manual split from staging/temp",
            )
            for path in input_paths:
                path.unlink()

    return ApplyManualStagingEditsSummary(
        temp_dir=resolved_temp_dir,
        edited_count=edited_count,
        staged_count=staged_count,
        missing_entries=tuple(missing_entries),
        dry_run=False,
    )


def _stage_exact_manual_edits(
    config: AppConfig,
    *,
    photo_ids: list[int],
) -> None:
    with connect(config) as conn:
        source_paths = {
            photo_id: config.photos_root.parent / get_photo_record(conn, photo_id=photo_id).working_path
            for photo_id in photo_ids
        }
    _stage_exact_photo_sources(config, photo_sources=source_paths)


def _stage_exact_photo_sources(
    config: AppConfig,
    *,
    photo_sources: dict[int, Path],
) -> None:
    with connect(config) as conn:
        for photo_id, source_path in photo_sources.items():
            image = cv2.imread(str(source_path))
            if image is None:
                raise ValueError(f"Unable to load operator-edited image: {source_path}")
            profile = _staging_profile_for_manual_image(image)
            output_path = config.photos_root / 'exports' / profile / f'photo_{photo_id}.jpg'
            output_path.parent.mkdir(parents=True, exist_ok=True)
            _clear_existing_artifacts(
                conn,
                photo_id=photo_id,
                artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
            )
            shutil.copy2(source_path, output_path)
            insert_photo_artifact(
                conn,
                photo_id=photo_id,
                artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
                path=output_path,
                pipeline_stage='frame_export',
                pipeline_version=FRAME_EXPORT_PIPELINE_VERSION,
            )
        conn.commit()


def _staging_profile_for_manual_image(image) -> str:
    height, width = image.shape[:2]
    return _closest_profile_for_dimensions(
        width=width,
        height=height,
        landscape_profile=STAGING_LANDSCAPE_PROFILE,
        portrait_profile=STAGING_PORTRAIT_PROFILE,
    )


def _closest_profile_for_dimensions(
    *,
    width: int,
    height: int,
    landscape_profile: str,
    portrait_profile: str,
) -> str:
    landscape_mismatch = abs((width / DEFAULT_FRAME_WIDTH) - (height / DEFAULT_FRAME_HEIGHT))
    portrait_mismatch = abs((width / PORTRAIT_FRAME_WIDTH) - (height / PORTRAIT_FRAME_HEIGHT))
    if landscape_mismatch == portrait_mismatch:
        return landscape_profile if width >= height else portrait_profile
    return landscape_profile if landscape_mismatch < portrait_mismatch else portrait_profile


def _apply_exact_manual_split(
    config: AppConfig,
    *,
    photo_id: int,
    input_paths: list[Path],
    note: str | None,
) -> list[int]:
    from audit.fix_service import (
        _find_open_export_audit_task_id,
        _get_photo_repair_context,
        _load_manual_split_image,
        _resolve_manual_split_inputs,
        _rewrite_split_photo_arrays,
    )

    resolved_input_paths = _resolve_manual_split_inputs(input_paths)
    context = _get_photo_repair_context(config, photo_id=photo_id)
    crops = [_load_manual_split_image(path) for path in resolved_input_paths]
    new_photo_ids = _rewrite_split_photo_arrays(
        config,
        context=context,
        crops=crops,
        pipeline_version='manual_split_v1',
        reuse_original_photo=False,
    )
    if not new_photo_ids:
        raise ValueError(f"Manual split did not create any child photos for photo_id={photo_id}.")

    set_photo_export_disposition(
        config,
        photo_id=context.photo_id,
        disposition='exclude_reject',
        note='replaced by manual split children',
        dry_run=False,
    )

    with connect(config) as conn:
        source_paths = {
            current_photo_id: config.photos_root.parent / get_photo_record(conn, photo_id=current_photo_id).raw_crop_path
            for current_photo_id in new_photo_ids
        }
    _stage_exact_photo_sources(config, photo_sources=source_paths)

    resolved_task_id = _find_open_export_audit_task_id(config, photo_id=photo_id)
    if resolved_task_id is not None:
        resolve_export_audit_review(
            config,
            task_id=resolved_task_id,
            export_action='fix_crop',
            note=note or 'manual split applied',
            dry_run=False,
        )
    resolve_orientation_review_for_photo(
        config,
        photo_id=context.photo_id,
        action='excluded_via_manual_split',
    )
    for current_photo_id in new_photo_ids:
        resolve_orientation_review_for_photo(
            config,
            photo_id=current_photo_id,
            action='fixed_via_manual_split',
        )

    return new_photo_ids


def _classify_manual_edit_entries(
    entries: list[Path],
) -> tuple[dict[int, Path], dict[int, tuple[Path, ...]], list[str]]:
    direct_edits: dict[int, Path] = {}
    split_groups_by_id: dict[int, dict[int, Path]] = {}
    missing_entries: list[str] = []

    for path in entries:
        split_match = _PHOTO_CHILD_RE.search(path.name)
        if split_match is not None:
            photo_id = int(split_match.group("photo_id"))
            child_index = int(split_match.group("child_index"))
            child_paths = split_groups_by_id.setdefault(photo_id, {})
            if child_index in child_paths:
                raise ValueError(
                    f"Duplicate manual split child index for photo_{photo_id}: {path.name}"
                )
            child_paths[child_index] = path
            continue

        photo_id = _photo_id_from_error_entry(path.name)
        if photo_id is not None:
            direct_edits[photo_id] = path
            continue

        missing_entries.append(path.name)

    overlapping_photo_ids = sorted(set(direct_edits) & set(split_groups_by_id))
    if overlapping_photo_ids:
        overlap_text = ",".join(str(photo_id) for photo_id in overlapping_photo_ids)
        raise ValueError(
            "Manual edit temp folder contains both direct and split files for photo ids: "
            f"{overlap_text}"
        )

    split_groups: dict[int, tuple[Path, ...]] = {}
    for photo_id, indexed_paths in split_groups_by_id.items():
        if len(indexed_paths) < 2:
            raise ValueError(
                f"Manual split requires at least two files for photo_{photo_id}."
            )
        ordered_paths = tuple(path for _, path in sorted(indexed_paths.items()))
        split_groups[photo_id] = ordered_paths

    return direct_edits, split_groups, missing_entries


def _staging_profile_for_final_profile(source_profile: str) -> str:
    if source_profile == PORTRAIT_FRAME_PROFILE:
        return STAGING_PORTRAIT_PROFILE
    if source_profile == DEFAULT_FRAME_PROFILE:
        return STAGING_LANDSCAPE_PROFILE
    raise ValueError(f"Unsupported final export profile: {source_profile}")


def _clear_existing_artifacts(
    conn,
    *,
    photo_id: int,
    artifact_type: str,
    preserve_path: Path | None = None,
) -> None:
    for existing_path in list_photo_artifact_paths(conn, photo_id=photo_id, artifact_type=artifact_type):
        if preserve_path is not None and existing_path == preserve_path:
            continue
        delete_photo_artifact(
            conn,
            photo_id=photo_id,
            artifact_type=artifact_type,
            path=existing_path,
        )
        if existing_path.exists():
            existing_path.unlink()


def _clear_unselected_staging_exports(
    config: AppConfig,
    *,
    batch_name: str | None,
    sheet_id: int | None,
    keep_photo_ids: set[int],
) -> None:
    with connect(config) as conn:
        candidate_photo_ids = list_photo_ids(
            conn,
            batch_name=batch_name,
            sheet_id=sheet_id,
            photo_id=None,
            limit=None,
        )
        for photo_id in candidate_photo_ids:
            if photo_id in keep_photo_ids:
                continue
            for existing_path in list_photo_artifact_paths(
                conn,
                photo_id=photo_id,
                artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
            ):
                delete_photo_artifact(
                    conn,
                    photo_id=photo_id,
                    artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
                    path=existing_path,
                )
                if existing_path.exists():
                    existing_path.unlink()
        conn.commit()
