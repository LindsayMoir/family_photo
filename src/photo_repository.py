"""Shared PostgreSQL helpers for promoted photo stages."""

from __future__ import annotations

from pathlib import Path

from psycopg2.extensions import connection as PgConnection

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
