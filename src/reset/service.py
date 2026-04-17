"""Scoped cleanup for rescanned source directories."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from psycopg2.extensions import connection as PgConnection

from config import AppConfig
from db.connection import connect


@dataclass(frozen=True, slots=True)
class ResetSourceScansSummary:
    """Summary of a scoped downstream reset for rescanned source directories."""

    source_dirs: tuple[Path, ...]
    sheet_count: int
    detection_count: int
    photo_count: int
    artifact_count: int
    face_count: int
    photo_people_count: int
    review_task_count: int
    ocr_request_count: int
    processing_job_count: int
    file_count: int
    deleted_file_count: int
    dry_run: bool


@dataclass(frozen=True, slots=True)
class _ResetInventory:
    source_dirs: tuple[Path, ...]
    sheet_ids: tuple[int, ...]
    detection_ids: tuple[int, ...]
    photo_ids: tuple[int, ...]
    file_paths: tuple[Path, ...]
    sheet_count: int
    detection_count: int
    photo_count: int
    artifact_count: int
    face_count: int
    photo_people_count: int
    review_task_count: int
    ocr_request_count: int
    processing_job_count: int


def reset_source_scans(
    config: AppConfig,
    *,
    source_dirs: list[Path],
    dry_run: bool,
) -> ResetSourceScansSummary:
    """Delete downstream rows and files for sheet scans rooted under selected source dirs."""
    resolved_source_dirs = _resolve_source_dirs(config, source_dirs)
    with connect(config) as conn:
        inventory = _load_reset_inventory(conn, config=config, source_dirs=resolved_source_dirs)

        if dry_run:
            return ResetSourceScansSummary(
                source_dirs=inventory.source_dirs,
                sheet_count=inventory.sheet_count,
                detection_count=inventory.detection_count,
                photo_count=inventory.photo_count,
                artifact_count=inventory.artifact_count,
                face_count=inventory.face_count,
                photo_people_count=inventory.photo_people_count,
                review_task_count=inventory.review_task_count,
                ocr_request_count=inventory.ocr_request_count,
                processing_job_count=inventory.processing_job_count,
                file_count=len(inventory.file_paths),
                deleted_file_count=0,
                dry_run=True,
            )

        deleted_file_count = _delete_file_paths(inventory.file_paths)
        _delete_downstream_rows(conn, inventory=inventory)
        conn.commit()

    return ResetSourceScansSummary(
        source_dirs=inventory.source_dirs,
        sheet_count=inventory.sheet_count,
        detection_count=inventory.detection_count,
        photo_count=inventory.photo_count,
        artifact_count=inventory.artifact_count,
        face_count=inventory.face_count,
        photo_people_count=inventory.photo_people_count,
        review_task_count=inventory.review_task_count,
        ocr_request_count=inventory.ocr_request_count,
        processing_job_count=inventory.processing_job_count,
        file_count=len(inventory.file_paths),
        deleted_file_count=deleted_file_count,
        dry_run=False,
    )


def _resolve_source_dirs(config: AppConfig, source_dirs: list[Path]) -> tuple[Path, ...]:
    if not source_dirs:
        raise ValueError("At least one --source-dir must be provided.")

    resolved: list[Path] = []
    seen: set[Path] = set()
    for source_dir in source_dirs:
        candidate = source_dir.expanduser()
        if not candidate.is_absolute():
            candidate = config.photos_root / candidate
        candidate = candidate.resolve()
        if not candidate.exists():
            raise ValueError(f"Source directory does not exist: {candidate}")
        if not candidate.is_dir():
            raise ValueError(f"Source path is not a directory: {candidate}")
        if candidate in seen:
            continue
        seen.add(candidate)
        resolved.append(candidate)
    return tuple(sorted(resolved))


def _load_reset_inventory(
    conn: PgConnection,
    *,
    config: AppConfig,
    source_dirs: tuple[Path, ...],
) -> _ResetInventory:
    where_clause, params = _sheet_scan_where_clause(source_dirs)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT ss.id, sb.name
            FROM sheet_scans ss
            JOIN scan_batches sb ON sb.id = ss.scan_batch_id
            WHERE {where_clause}
            ORDER BY ss.id
            """,
            params,
        )
        sheet_rows = cur.fetchall()

    if not sheet_rows:
        joined_dirs = ", ".join(str(path) for path in source_dirs)
        raise ValueError(f"No sheet scans were found under source dirs: {joined_dirs}")

    sheet_ids = [int(row[0]) for row in sheet_rows]
    batch_names_by_sheet_id = {int(row[0]): str(row[1]) for row in sheet_rows}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, crop_path
            FROM photo_detections
            WHERE sheet_scan_id = ANY(%s)
            ORDER BY id
            """,
            (sheet_ids,),
        )
        detection_rows = cur.fetchall()
        cur.execute(
            """
            SELECT id, raw_crop_path, working_path, final_path
            FROM photos
            WHERE sheet_scan_id = ANY(%s)
            ORDER BY id
            """,
            (sheet_ids,),
        )
        photo_rows = cur.fetchall()

    detection_ids = [int(row[0]) for row in detection_rows]
    photo_ids = [int(row[0]) for row in photo_rows]

    artifact_rows: list[tuple[str]] = []
    face_count = 0
    photo_people_count = 0
    review_task_count = 0
    processing_job_count = 0

    with conn.cursor() as cur:
        if photo_ids:
            cur.execute(
                """
                SELECT path
                FROM photo_artifacts
                WHERE photo_id = ANY(%s)
                ORDER BY id
                """,
                (photo_ids,),
            )
            artifact_rows = cur.fetchall()
            cur.execute(
                "SELECT count(*) FROM faces WHERE photo_id = ANY(%s)",
                (photo_ids,),
            )
            face_count = int(cur.fetchone()[0])
            cur.execute(
                "SELECT count(*) FROM photo_people WHERE photo_id = ANY(%s)",
                (photo_ids,),
            )
            photo_people_count = int(cur.fetchone()[0])

        review_params: list[object] = []
        review_clauses: list[str] = []
        if sheet_ids:
            review_clauses.append("(entity_type = 'sheet_scan' AND entity_id = ANY(%s))")
            review_params.append(sheet_ids)
        if photo_ids:
            review_clauses.append("(entity_type = 'photo' AND entity_id = ANY(%s))")
            review_params.append(photo_ids)
        if detection_ids:
            review_clauses.append("(entity_type = 'photo_detection' AND entity_id = ANY(%s))")
            review_params.append(detection_ids)
        if review_clauses:
            cur.execute(
                f"SELECT count(*) FROM review_tasks WHERE {' OR '.join(review_clauses)}",
                tuple(review_params),
            )
            review_task_count = int(cur.fetchone()[0])

            cur.execute(
                f"SELECT count(*) FROM processing_jobs WHERE {' OR '.join(review_clauses)}",
                tuple(review_params),
            )
            processing_job_count = int(cur.fetchone()[0])

        cur.execute(
            "SELECT count(*) FROM ocr_requests WHERE sheet_scan_id = ANY(%s)",
            (sheet_ids,),
        )
        ocr_request_count = int(cur.fetchone()[0])

    file_paths = _list_candidate_file_paths(
        config=config,
        batch_names_by_sheet_id=batch_names_by_sheet_id,
        sheet_ids=sheet_ids,
        detection_rows=detection_rows,
        photo_rows=photo_rows,
        artifact_rows=artifact_rows,
    )

    return _ResetInventory(
        source_dirs=source_dirs,
        sheet_ids=tuple(sheet_ids),
        detection_ids=tuple(detection_ids),
        photo_ids=tuple(photo_ids),
        file_paths=file_paths,
        sheet_count=len(sheet_ids),
        detection_count=len(detection_ids),
        photo_count=len(photo_ids),
        artifact_count=len(artifact_rows),
        face_count=face_count,
        photo_people_count=photo_people_count,
        review_task_count=review_task_count,
        ocr_request_count=ocr_request_count,
        processing_job_count=processing_job_count,
    )


def _sheet_scan_where_clause(source_dirs: tuple[Path, ...]) -> tuple[str, tuple[str, ...]]:
    clauses: list[str] = []
    params: list[str] = []
    for source_dir in source_dirs:
        prefix = str(source_dir)
        trailing_prefix = prefix.rstrip("/\\") + "/%"
        clauses.append("(ss.original_path = %s OR ss.original_path LIKE %s)")
        params.extend((prefix, trailing_prefix))
    return " OR ".join(clauses), tuple(params)


def _list_candidate_file_paths(
    *,
    config: AppConfig,
    batch_names_by_sheet_id: dict[int, str],
    sheet_ids: list[int],
    detection_rows: list[tuple[object, object]],
    photo_rows: list[tuple[object, object, object, object]],
    artifact_rows: list[tuple[object]],
) -> tuple[Path, ...]:
    file_paths: set[Path] = set()

    for sheet_id in sheet_ids:
        batch_name = batch_names_by_sheet_id[sheet_id]
        file_paths.add(config.photos_root / "derivatives" / "review" / "detections" / batch_name / f"sheet_{sheet_id}.jpg")

    for _detection_id, crop_path in detection_rows:
        _add_candidate_path(file_paths, config, crop_path)

    photo_ids: list[int] = []
    for photo_id, raw_crop_path, working_path, final_path in photo_rows:
        resolved_photo_id = int(photo_id)
        photo_ids.append(resolved_photo_id)
        _add_candidate_path(file_paths, config, raw_crop_path)
        _add_candidate_path(file_paths, config, working_path)
        _add_candidate_path(file_paths, config, final_path)

    for (artifact_path,) in artifact_rows:
        _add_candidate_path(file_paths, config, artifact_path)

    for sheet_id in sheet_ids:
        crop_dir = config.photos_root / "crops" / f"sheet_{sheet_id}"
        if crop_dir.exists():
            for path in crop_dir.rglob("*"):
                if path.is_file():
                    file_paths.add(path)

    for sheet_id, batch_name in batch_names_by_sheet_id.items():
        region_dir = config.photos_root / "derivatives" / "review" / "regions" / batch_name
        if region_dir.exists():
            for path in region_dir.glob(f"sheet_{sheet_id}_*"):
                if path.is_file():
                    file_paths.add(path)

    for photo_id in photo_ids:
        filename = f"photo_{photo_id}.jpg"
        for folder_name in (
            "exports/staging/landscape",
            "exports/staging/portrait",
            "exports/frame_1920x1080",
            "exports/frame_1080x1920",
        ):
            file_paths.add(config.photos_root / folder_name / filename)

    return tuple(sorted(file_paths))


def _add_candidate_path(file_paths: set[Path], config: AppConfig, value: object) -> None:
    if value is None:
        return
    path = Path(str(value)).expanduser()
    if not path.is_absolute():
        path = config.photos_root / path
    file_paths.add(path)


def _delete_file_paths(file_paths: tuple[Path, ...]) -> int:
    deleted_count = 0
    for path in file_paths:
        if not path.exists() or not path.is_file():
            continue
        path.unlink()
        deleted_count += 1

    parent_dirs = sorted({path.parent for path in file_paths}, key=lambda path: len(path.parts), reverse=True)
    for directory in parent_dirs:
        if not directory.exists() or not directory.is_dir():
            continue
        try:
            directory.rmdir()
        except OSError:
            continue

    return deleted_count


def _delete_downstream_rows(conn: PgConnection, *, inventory: _ResetInventory) -> None:
    with conn.cursor() as cur:
        if inventory.photo_ids:
            cur.execute("DELETE FROM photo_people WHERE photo_id = ANY(%s)", (list(inventory.photo_ids),))
            cur.execute("DELETE FROM faces WHERE photo_id = ANY(%s)", (list(inventory.photo_ids),))
            cur.execute("DELETE FROM photo_artifacts WHERE photo_id = ANY(%s)", (list(inventory.photo_ids),))

        review_params: list[object] = []
        review_clauses: list[str] = []
        if inventory.sheet_ids:
            review_clauses.append("(entity_type = 'sheet_scan' AND entity_id = ANY(%s))")
            review_params.append(list(inventory.sheet_ids))
        if inventory.photo_ids:
            review_clauses.append("(entity_type = 'photo' AND entity_id = ANY(%s))")
            review_params.append(list(inventory.photo_ids))
        if inventory.detection_ids:
            review_clauses.append("(entity_type = 'photo_detection' AND entity_id = ANY(%s))")
            review_params.append(list(inventory.detection_ids))
        if review_clauses:
            cur.execute(
                f"DELETE FROM review_tasks WHERE {' OR '.join(review_clauses)}",
                tuple(review_params),
            )
            cur.execute(
                f"DELETE FROM processing_jobs WHERE {' OR '.join(review_clauses)}",
                tuple(review_params),
            )

        if inventory.sheet_ids:
            sheet_id_list = list(inventory.sheet_ids)
            cur.execute("DELETE FROM ocr_requests WHERE sheet_scan_id = ANY(%s)", (sheet_id_list,))
            cur.execute("DELETE FROM photos WHERE sheet_scan_id = ANY(%s)", (sheet_id_list,))
            cur.execute("DELETE FROM photo_detections WHERE sheet_scan_id = ANY(%s)", (sheet_id_list,))
            cur.execute("DELETE FROM sheet_scans WHERE id = ANY(%s)", (sheet_id_list,))
