"""Simple split-review CSV workflow for staged exports."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from audit.fix_service import auto_split_photo
from config import AppConfig
from db.connection import connect


STAGING_ARTIFACT_TYPE = "frame_export_staging"
TRUTHY_SPLIT_VALUES = {"y", "yes", "true", "t", "1", "x"}


@dataclass(frozen=True, slots=True)
class SplitReviewCsvSummary:
    """Summary of writing a split-review CSV for current staging exports."""

    csv_path: Path
    row_count: int
    dry_run: bool


@dataclass(frozen=True, slots=True)
class SplitReviewImportSummary:
    """Summary of importing split-review decisions from CSV."""

    csv_path: Path
    processed_rows: int
    requested_split_count: int
    applied_split_count: int
    unresolved_count: int
    created_photo_count: int
    dry_run: bool


def write_split_review_csv(
    config: AppConfig,
    *,
    csv_path: Path | None,
    dry_run: bool,
) -> SplitReviewCsvSummary:
    """Write a simple image_name/Split CSV for all currently staged exports."""
    resolved_csv_path = (
        csv_path
        if csv_path is not None
        else config.photos_root / "exports" / "staging" / "split_review.csv"
    )
    staged_entries = _list_current_staging_entries(config)

    if dry_run:
        return SplitReviewCsvSummary(
            csv_path=resolved_csv_path,
            row_count=len(staged_entries),
            dry_run=True,
        )

    resolved_csv_path.parent.mkdir(parents=True, exist_ok=True)
    with resolved_csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["image_name", "Split"])
        for _, image_name in sorted(staged_entries, key=lambda item: item[1]):
            writer.writerow([image_name, "N"])

    return SplitReviewCsvSummary(
        csv_path=resolved_csv_path,
        row_count=len(staged_entries),
        dry_run=False,
    )


def import_split_review_csv(
    config: AppConfig,
    *,
    csv_path: Path | None,
    dry_run: bool,
) -> SplitReviewImportSummary:
    """Apply requested split decisions from a simple split-review CSV."""
    resolved_csv_path = (
        csv_path
        if csv_path is not None
        else config.photos_root / "exports" / "staging" / "split_review.csv"
    )
    if not resolved_csv_path.exists():
        raise ValueError(f"Split review CSV was not found: {resolved_csv_path}")

    rows = _read_split_review_rows(resolved_csv_path)
    image_name_to_photo_id = {
        image_name: photo_id
        for photo_id, image_name in _list_current_staging_entries(config)
    }
    processed_rows = len(rows)
    requested_photo_ids: list[int] = []
    unresolved_count = 0

    for row in rows:
        if not _row_requests_split(row):
            continue
        image_name = row["image_name"]
        photo_id = image_name_to_photo_id.get(image_name)
        if photo_id is None:
            unresolved_count += 1
            continue
        requested_photo_ids.append(photo_id)

    if dry_run:
        return SplitReviewImportSummary(
            csv_path=resolved_csv_path,
            processed_rows=processed_rows,
            requested_split_count=len(requested_photo_ids),
            applied_split_count=0,
            unresolved_count=unresolved_count,
            created_photo_count=0,
            dry_run=True,
        )

    applied_split_count = 0
    created_photo_count = 0
    for photo_id in requested_photo_ids:
        try:
            summary = auto_split_photo(
                config,
                photo_id=photo_id,
                note="split review csv applied",
                dry_run=False,
            )
        except ValueError:
            unresolved_count += 1
            continue
        applied_split_count += 1
        created_photo_count += max(0, len(summary.staged_photo_ids) - 1)

    return SplitReviewImportSummary(
        csv_path=resolved_csv_path,
        processed_rows=processed_rows,
        requested_split_count=len(requested_photo_ids),
        applied_split_count=applied_split_count,
        unresolved_count=unresolved_count,
        created_photo_count=created_photo_count,
        dry_run=False,
    )


def _list_current_staging_entries(config: AppConfig) -> list[tuple[int, str]]:
    with connect(config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT photo_id, path
                FROM photo_artifacts
                WHERE artifact_type = %s
                ORDER BY path, photo_id
                """,
                (STAGING_ARTIFACT_TYPE,),
            )
            return [
                (int(photo_id), Path(str(path)).name)
                for photo_id, path in cur.fetchall()
            ]


def _read_split_review_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or "image_name" not in reader.fieldnames or "Split" not in reader.fieldnames:
            raise ValueError("Split review CSV must include image_name and Split columns.")
        return [{key: value or "" for key, value in row.items()} for row in reader]


def _row_requests_split(row: dict[str, str]) -> bool:
    return row.get("Split", "").strip().lower() in TRUTHY_SPLIT_VALUES
