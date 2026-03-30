"""Application service for photo export disposition."""

from __future__ import annotations

from dataclasses import dataclass

from config import AppConfig
from db.connection import connect
from photo_repository import (
    delete_photo_artifact,
    list_photo_artifact_paths,
    update_photo_export_disposition,
)


FRAME_EXPORT_ARTIFACT_TYPE = "frame_export"


@dataclass(frozen=True, slots=True)
class PhotoDispositionSummary:
    """Summary of a photo export-disposition change."""

    photo_id: int
    disposition: str
    note: str | None
    removed_export_count: int
    dry_run: bool


def set_photo_export_disposition(
    config: AppConfig,
    *,
    photo_id: int,
    disposition: str,
    note: str | None,
    dry_run: bool,
) -> PhotoDispositionSummary:
    """Mark a photo as exportable or excluded from frame delivery."""
    if disposition not in {"include", "exclude_low_value", "exclude_reject"}:
        raise ValueError(f"Unsupported disposition '{disposition}'.")

    removed_export_count = 0
    with connect(config) as conn:
        existing_exports = list_photo_artifact_paths(
            conn,
            photo_id=photo_id,
            artifact_type=FRAME_EXPORT_ARTIFACT_TYPE,
        )

    if dry_run:
        return PhotoDispositionSummary(
            photo_id=photo_id,
            disposition=disposition,
            note=note,
            removed_export_count=len(existing_exports) if disposition != "include" else 0,
            dry_run=True,
        )

    with connect(config) as conn:
        update_photo_export_disposition(
            conn,
            photo_id=photo_id,
            disposition=disposition,
            note=note,
        )
        if disposition != "include":
            for export_path in list_photo_artifact_paths(
                conn,
                photo_id=photo_id,
                artifact_type=FRAME_EXPORT_ARTIFACT_TYPE,
            ):
                export_path.unlink(missing_ok=True)
                delete_photo_artifact(
                    conn,
                    photo_id=photo_id,
                    artifact_type=FRAME_EXPORT_ARTIFACT_TYPE,
                    path=export_path,
                )
                removed_export_count += 1
        conn.commit()

    return PhotoDispositionSummary(
        photo_id=photo_id,
        disposition=disposition,
        note=note,
        removed_export_count=removed_export_count,
        dry_run=False,
    )
