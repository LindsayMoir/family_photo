"""Shared PostgreSQL helpers for promoted photo stages."""

from __future__ import annotations

from pathlib import Path
import json

from psycopg2.extensions import connection as PgConnection

from audit.models import ExportAuditRecord
from photo_models import PhotoRecord


def get_photo_record(conn: PgConnection, *, photo_id: int) -> PhotoRecord:
    """Fetch a promoted photo record."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, raw_crop_path, working_path, status
            FROM photos
            WHERE id = %s
            """,
            (photo_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"Photo {photo_id} was not found.")
    return PhotoRecord(
        id=int(row[0]),
        raw_crop_path=Path(str(row[1])),
        working_path=Path(str(row[2])),
        status=str(row[3]),
    )


def list_photo_ids_for_sheet(conn: PgConnection, *, sheet_id: int) -> list[int]:
    """Return photo ids associated with a sheet scan."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id
            FROM photos
            WHERE sheet_scan_id = %s
            ORDER BY crop_index ASC, id ASC
            """,
            (sheet_id,),
        )
        rows = cur.fetchall()
    return [int(row[0]) for row in rows]


def list_photo_ids(
    conn: PgConnection,
    *,
    batch_name: str | None = None,
    sheet_id: int | None = None,
    photo_id: int | None = None,
    limit: int | None = None,
) -> list[int]:
    """Return photo ids filtered by batch, sheet, or explicit photo id."""
    clauses: list[str] = []
    params: list[object] = []

    query = """
        SELECT p.id
        FROM photos p
        JOIN sheet_scans ss ON ss.id = p.sheet_scan_id
        JOIN scan_batches sb ON sb.id = ss.scan_batch_id
        WHERE 1 = 1
    """
    if batch_name is not None:
        clauses.append("sb.name = %s")
        params.append(batch_name)
    if sheet_id is not None:
        clauses.append("p.sheet_scan_id = %s")
        params.append(sheet_id)
    if photo_id is not None:
        clauses.append("p.id = %s")
        params.append(photo_id)
    if clauses:
        query += " AND " + " AND ".join(clauses)
    query += " ORDER BY p.id"
    if limit is not None:
        query += " LIMIT %s"
        params.append(limit)

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
    return [int(row[0]) for row in rows]


def list_export_ready_photo_ids(
    conn: PgConnection,
    *,
    batch_name: str | None = None,
    sheet_id: int | None = None,
    photo_id: int | None = None,
    exclude_final_exported: bool = False,
    limit: int | None = None,
) -> list[int]:
    """Return enhanced photo ids that are ready for frame export."""
    clauses: list[str] = [
        "p.status = 'enhancement_complete'",
        "p.export_disposition = 'include'",
    ]
    params: list[object] = []

    query = """
        SELECT p.id
        FROM photos p
        JOIN sheet_scans ss ON ss.id = p.sheet_scan_id
        JOIN scan_batches sb ON sb.id = ss.scan_batch_id
        WHERE 1 = 1
    """
    if batch_name is not None:
        clauses.append("sb.name = %s")
        params.append(batch_name)
    if sheet_id is not None:
        clauses.append("p.sheet_scan_id = %s")
        params.append(sheet_id)
    if photo_id is not None:
        clauses.append("p.id = %s")
        params.append(photo_id)
    if exclude_final_exported:
        clauses.append(
            """
            NOT EXISTS (
                SELECT 1
                FROM photo_artifacts pa
                WHERE pa.photo_id = p.id
                  AND pa.artifact_type = 'frame_export'
            )
            """.strip()
        )
    query += " AND " + " AND ".join(clauses)
    query += " ORDER BY p.id"
    if limit is not None:
        query += " LIMIT %s"
        params.append(limit)

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
    return [int(row[0]) for row in rows]


def list_export_audit_records(
    conn: PgConnection,
    *,
    artifact_type: str,
    batch_name: str | None = None,
    sheet_id: int | None = None,
    photo_id: int | None = None,
    limit: int | None = None,
) -> list[ExportAuditRecord]:
    """Return exported photo records with the metadata needed for audit classification."""
    clauses: list[str] = ["pa.artifact_type = %s"]
    params: list[object] = []
    params.append(artifact_type)

    query = """
        SELECT
            p.id,
            sb.name,
            p.sheet_scan_id,
            p.crop_index,
            p.raw_crop_path,
            p.working_path,
            pa.path,
            p.status,
            p.rotation_degrees,
            p.accepted_detection_id,
            pd.confidence,
            pd.reviewed_by_human,
            COALESCE(pd.bbox_json->>'width', NULL),
            COALESCE(pd.bbox_json->>'height', NULL),
            CASE WHEN rt.id IS NULL THEN FALSE ELSE TRUE END,
            rt.payload_json
        FROM photos p
        JOIN sheet_scans ss ON ss.id = p.sheet_scan_id
        JOIN scan_batches sb ON sb.id = ss.scan_batch_id
        JOIN photo_artifacts pa ON pa.photo_id = p.id
        LEFT JOIN photo_detections pd ON pd.id = p.accepted_detection_id
        LEFT JOIN review_tasks rt
            ON rt.entity_type = 'photo'
           AND rt.entity_id = p.id
           AND rt.task_type = 'review_orientation'
           AND rt.status = 'open'
        WHERE 1 = 1
    """
    if batch_name is not None:
        clauses.append("sb.name = %s")
        params.append(batch_name)
    if sheet_id is not None:
        clauses.append("p.sheet_scan_id = %s")
        params.append(sheet_id)
    if photo_id is not None:
        clauses.append("p.id = %s")
        params.append(photo_id)
    query += " AND " + " AND ".join(clauses)
    query += " ORDER BY p.id"
    if limit is not None:
        query += " LIMIT %s"
        params.append(limit)

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        rows = cur.fetchall()

    records: list[ExportAuditRecord] = []
    for row in rows:
        payload = row[15]
        if isinstance(payload, str):
            payload = json.loads(payload)
        if payload is None:
            payload = {}
        suggested_rotation = payload.get("suggested_rotation")
        confidence = payload.get("confidence")
        orientation_review_reason = None
        if suggested_rotation is not None:
            confidence_suffix = ""
            if confidence is not None:
                confidence_suffix = f" at confidence {float(confidence):.2f}"
            orientation_review_reason = (
                f"open orientation review suggests {int(suggested_rotation)} degree correction"
                f"{confidence_suffix}"
            )
        records.append(
            ExportAuditRecord(
                photo_id=int(row[0]),
                batch_name=str(row[1]),
                sheet_scan_id=int(row[2]),
                crop_index=int(row[3]),
                raw_crop_path=Path(str(row[4])),
                working_path=Path(str(row[5])),
                export_path=Path(str(row[6])),
                status=str(row[7]),
                rotation_degrees=int(row[8]) if row[8] is not None else None,
                accepted_detection_id=int(row[9]) if row[9] is not None else None,
                detection_confidence=float(row[10]) if row[10] is not None else None,
                detection_reviewed_by_human=bool(row[11]) if row[11] is not None else False,
                detection_width=float(row[12]) if row[12] is not None else None,
                detection_height=float(row[13]) if row[13] is not None else None,
                has_open_orientation_review=bool(row[14]),
                orientation_review_reason=orientation_review_reason,
            )
        )
    return records


def list_open_orientation_review_photo_ids(
    conn: PgConnection,
    *,
    batch_name: str | None = None,
    sheet_id: int | None = None,
    photo_id: int | None = None,
    limit: int | None = None,
) -> list[int]:
    """Return photo ids with open orientation review tasks."""
    clauses: list[str] = [
        "rt.task_type = 'review_orientation'",
        "rt.status = 'open'",
    ]
    params: list[object] = []
    query = """
        SELECT p.id
        FROM review_tasks rt
        JOIN photos p ON p.id = rt.entity_id
        JOIN sheet_scans ss ON ss.id = p.sheet_scan_id
        JOIN scan_batches sb ON sb.id = ss.scan_batch_id
        WHERE rt.entity_type = 'photo'
    """
    if batch_name is not None:
        clauses.append("sb.name = %s")
        params.append(batch_name)
    if sheet_id is not None:
        clauses.append("p.sheet_scan_id = %s")
        params.append(sheet_id)
    if photo_id is not None:
        clauses.append("p.id = %s")
        params.append(photo_id)
    query += " AND " + " AND ".join(clauses)
    query += " ORDER BY p.id"
    if limit is not None:
        query += " LIMIT %s"
        params.append(limit)

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
    return [int(row[0]) for row in rows]


def update_photo_stage(
    conn: PgConnection,
    *,
    photo_id: int,
    working_path: Path,
    status: str,
    deskew_angle: float | None = None,
    deskew_confidence: float | None = None,
    rotation_degrees: int | None = None,
    enhancement_version: str | None = None,
) -> None:
    """Update photo stage metadata."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE photos
            SET working_path = %s,
                status = %s,
                deskew_angle = COALESCE(%s, deskew_angle),
                deskew_confidence = COALESCE(%s, deskew_confidence),
                rotation_degrees = COALESCE(%s, rotation_degrees),
                enhancement_version = COALESCE(%s, enhancement_version),
                updated_at = NOW()
            WHERE id = %s
            """,
            (
                str(working_path),
                status,
                deskew_angle,
                deskew_confidence,
                rotation_degrees,
                enhancement_version,
                photo_id,
            ),
        )


def insert_photo_artifact(
    conn: PgConnection,
    *,
    photo_id: int,
    artifact_type: str,
    path: Path,
    pipeline_stage: str,
    pipeline_version: str,
) -> None:
    """Insert or replace a stage artifact row for a photo."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO photo_artifacts (
                photo_id,
                artifact_type,
                path,
                pipeline_stage,
                pipeline_version
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (path)
            DO UPDATE SET
                photo_id = EXCLUDED.photo_id,
                artifact_type = EXCLUDED.artifact_type,
                pipeline_stage = EXCLUDED.pipeline_stage,
                pipeline_version = EXCLUDED.pipeline_version
            """,
            (
                photo_id,
                artifact_type,
                str(path),
                pipeline_stage,
                pipeline_version,
            ),
        )


def list_photo_artifact_paths(
    conn: PgConnection,
    *,
    photo_id: int,
    artifact_type: str,
) -> list[Path]:
    """List artifact paths for a photo and artifact type."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT path
            FROM photo_artifacts
            WHERE photo_id = %s
              AND artifact_type = %s
            ORDER BY path
            """,
            (photo_id, artifact_type),
        )
        rows = cur.fetchall()
    return [Path(str(row[0])) for row in rows]


def get_photo_artifact_path(
    conn: PgConnection,
    *,
    photo_id: int,
    artifact_type: str,
) -> Path | None:
    """Return the most recent artifact path for a photo and artifact type."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT path
            FROM photo_artifacts
            WHERE photo_id = %s
              AND artifact_type = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (photo_id, artifact_type),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return Path(str(row[0]))


def delete_photo_artifact(
    conn: PgConnection,
    *,
    photo_id: int,
    artifact_type: str,
    path: Path,
) -> None:
    """Delete a single artifact row for a photo."""
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM photo_artifacts
            WHERE photo_id = %s
              AND artifact_type = %s
              AND path = %s
            """,
            (photo_id, artifact_type, str(path)),
        )


def update_photo_export_disposition(
    conn: PgConnection,
    *,
    photo_id: int,
    disposition: str,
    note: str | None,
) -> None:
    """Persist whether a photo should be included in frame exports."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE photos
            SET export_disposition = %s,
                export_disposition_note = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (disposition, note, photo_id),
        )
        if cur.rowcount != 1:
            raise ValueError(f"Photo {photo_id} was not found.")
