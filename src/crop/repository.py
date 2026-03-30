"""Database persistence for crop promotion."""

from __future__ import annotations

from pathlib import Path

from psycopg2.extensions import connection as PgConnection

from crop.models import AcceptedPhotoDetection
from photo_repository import insert_photo_artifact


PHOTO_STATUS_CROP_COMPLETE = "crop_complete"


def get_accepted_photo_detections(
    conn: PgConnection,
    *,
    sheet_id: int,
) -> list[AcceptedPhotoDetection]:
    """Fetch accepted photo detections for a sheet."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                pd.id,
                pd.sheet_scan_id,
                pd.crop_path,
                COALESCE(pd.bbox_json->>'width', '0') AS width_px,
                COALESCE(pd.bbox_json->>'height', '0') AS height_px
            FROM photo_detections pd
            WHERE pd.sheet_scan_id = %s
              AND pd.region_type = 'photo'
              AND pd.accepted = TRUE
            ORDER BY pd.id
            """,
            (sheet_id,),
        )
        rows = cur.fetchall()

    return [
        AcceptedPhotoDetection(
            detection_id=int(row[0]),
            sheet_scan_id=int(row[1]),
            crop_path=Path(str(row[2])),
            width_px=max(int(round(float(row[3]))), 0),
            height_px=max(int(round(float(row[4]))), 0),
        )
        for row in rows
    ]


def get_existing_photo_indices(conn: PgConnection, *, sheet_id: int) -> set[int]:
    """Return crop indices that already have photo rows."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT crop_index
            FROM photos
            WHERE sheet_scan_id = %s
            """,
            (sheet_id,),
        )
        rows = cur.fetchall()
    return {int(row[0]) for row in rows}


def upsert_photo_from_detection(
    conn: PgConnection,
    *,
    detection: AcceptedPhotoDetection,
    crop_index: int,
    raw_crop_path: Path,
) -> tuple[int, bool]:
    """Insert or update a photo row and its raw crop artifact."""
    with conn.cursor() as cur:
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
                status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sheet_scan_id, crop_index)
            DO UPDATE SET
                accepted_detection_id = EXCLUDED.accepted_detection_id,
                raw_crop_path = EXCLUDED.raw_crop_path,
                working_path = EXCLUDED.working_path,
                width_px = EXCLUDED.width_px,
                height_px = EXCLUDED.height_px,
                updated_at = NOW()
            RETURNING id, (xmax = 0) AS inserted
            """,
            (
                detection.sheet_scan_id,
                detection.detection_id,
                crop_index,
                str(raw_crop_path),
                str(raw_crop_path),
                detection.width_px or None,
                detection.height_px or None,
                PHOTO_STATUS_CROP_COMPLETE,
            ),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(
                f"Failed to insert photo row for detection_id={detection.detection_id}."
            )
        photo_id = int(row[0])
        inserted = bool(row[1])

        insert_photo_artifact(
            conn,
            photo_id=photo_id,
            artifact_type="raw_crop",
            path=raw_crop_path,
            pipeline_stage="crop",
            pipeline_version="crop_v1",
        )

    return photo_id, inserted
