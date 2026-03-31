"""Database persistence for detection."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from psycopg2.extensions import connection as PgConnection

from detection.models import DetectionCandidate, SheetScanRecord


SHEET_STATUS_DETECTION_COMPLETE = "detection_complete"
SHEET_STATUS_DETECTION_REVIEW_REQUIRED = "detection_review_required"
SHEET_STATUS_INGESTED = "ingested"


def get_sheet_scans(
    conn: PgConnection,
    *,
    batch_name: str | None = None,
    sheet_id: int | None = None,
    limit: int | None = None,
    pending_only: bool = False,
) -> list[SheetScanRecord]:
    """Fetch sheet scans by batch or single id."""
    if batch_name is None and sheet_id is None:
        raise ValueError("Either batch_name or sheet_id must be provided.")
    if pending_only and sheet_id is not None:
        raise ValueError("pending_only cannot be used with an explicit sheet_id.")

    clauses: list[str] = []
    params: list[object] = []

    if batch_name is not None:
        clauses.append("sb.name = %s")
        params.append(batch_name)
    if sheet_id is not None:
        clauses.append("ss.id = %s")
        params.append(sheet_id)
    if pending_only:
        clauses.append("ss.status = %s")
        params.append(SHEET_STATUS_INGESTED)

    query = """
        SELECT ss.id, sb.name, ss.original_path, ss.width_px, ss.height_px
        FROM sheet_scans ss
        JOIN scan_batches sb ON sb.id = ss.scan_batch_id
    """
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY ss.id"
    if limit is not None:
        query += " LIMIT %s"
        params.append(limit)

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        rows = cur.fetchall()

    return [
        SheetScanRecord(
            id=int(row[0]),
            batch_name=str(row[1]),
            original_path=Path(str(row[2])),
            width_px=int(row[3]),
            height_px=int(row[4]),
        )
        for row in rows
    ]


def count_sheet_scans_by_status(
    conn: PgConnection,
    *,
    batch_name: str,
    status: str,
) -> int:
    """Count sheet scans in one batch with a specific status."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM sheet_scans ss
            JOIN scan_batches sb ON sb.id = ss.scan_batch_id
            WHERE sb.name = %s
              AND ss.status = %s
            """,
            (batch_name, status),
        )
        row = cur.fetchone()
    return int(row[0]) if row is not None else 0


def replace_detections(
    conn: PgConnection,
    *,
    sheet_scan_id: int,
    candidates: Iterable[DetectionCandidate],
    detection_method: str,
    pipeline_version: str,
    review_required: bool,
    review_reason: str | None,
    preview_path: str | None,
    ocr_request_reason: str | None = None,
) -> int:
    """Replace the detection rows for a sheet scan."""
    candidate_list = list(candidates)
    status = (
        SHEET_STATUS_DETECTION_REVIEW_REQUIRED
        if review_required
        else SHEET_STATUS_DETECTION_COMPLETE
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM ocr_requests
            WHERE sheet_scan_id = %s
            """,
            (sheet_scan_id,),
        )
        cur.execute(
            """
            DELETE FROM review_tasks
            WHERE entity_type = 'photo_detection'
              AND entity_id IN (
                  SELECT id
                  FROM photo_detections
                  WHERE sheet_scan_id = %s
              )
              AND task_type = 'review_ocr'
              AND status IN ('open', 'in_progress')
            """,
            (sheet_scan_id,),
        )
        cur.execute("DELETE FROM photo_detections WHERE sheet_scan_id = %s", (sheet_scan_id,))
        cur.execute(
            """
            DELETE FROM review_tasks
            WHERE entity_type = 'sheet_scan'
              AND entity_id = %s
              AND task_type = 'review_detection'
              AND status IN ('open', 'in_progress')
            """,
            (sheet_scan_id,),
        )

        for candidate in candidate_list:
            cur.execute(
                """
                INSERT INTO photo_detections (
                    sheet_scan_id,
                    detection_method,
                    pipeline_version,
                    region_type,
                    contour_json,
                    bbox_json,
                    confidence,
                    crop_path,
                    ocr_text,
                    ocr_engine,
                    ocr_confidence,
                    accepted,
                    reviewed_by_human
                )
                VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s, FALSE)
                RETURNING id
                """,
                (
                    sheet_scan_id,
                    detection_method,
                    pipeline_version,
                    candidate.region_type,
                    json.dumps({"points": candidate.contour_points}),
                    json.dumps(
                        {
                            "region_type": candidate.region_type,
                            "center_x": candidate.center_x,
                            "center_y": candidate.center_y,
                            "width": candidate.width,
                            "height": candidate.height,
                            "angle": candidate.angle,
                            "points": candidate.box_points,
                            "area_ratio": candidate.area_ratio,
                            "rectangularity": candidate.rectangularity,
                        }
                    ),
                    candidate.confidence,
                    str(candidate.crop_path) if candidate.crop_path is not None else None,
                    candidate.ocr_text,
                    candidate.ocr_engine if candidate.region_type == "text" else None,
                    candidate.ocr_confidence,
                    not review_required,
                ),
            )
            detection_row = cur.fetchone()
            if detection_row is None:
                raise RuntimeError(f"Failed to persist detection for sheet_scan_id={sheet_scan_id}.")
            detection_id = int(detection_row[0])

            if candidate.region_type == "text":
                cur.execute(
                    """
                    INSERT INTO review_tasks (
                        entity_type,
                        entity_id,
                        task_type,
                        status,
                        priority,
                        payload_json
                    )
                    VALUES ('photo_detection', %s, 'review_ocr', 'open', %s, %s::jsonb)
                    """,
                    (
                        detection_id,
                        20 if (candidate.ocr_confidence or 0.0) >= 0.70 else 5,
                        json.dumps(
                            {
                                "sheet_scan_id": sheet_scan_id,
                                "crop_path": str(candidate.crop_path) if candidate.crop_path is not None else None,
                                "ocr_preview": candidate.ocr_text,
                                "ocr_confidence": candidate.ocr_confidence,
                            }
                        ),
                    ),
                )

        cur.execute(
            """
            UPDATE sheet_scans
            SET status = %s,
                error_message = NULL,
                updated_at = NOW()
            WHERE id = %s
            """,
            (status, sheet_scan_id),
        )

        if review_required:
            cur.execute(
                """
                INSERT INTO review_tasks (
                    entity_type,
                    entity_id,
                    task_type,
                    status,
                    priority,
                    payload_json
                )
                VALUES ('sheet_scan', %s, 'review_detection', 'open', %s, %s::jsonb)
                """,
                (
                    sheet_scan_id,
                    10,
                    json.dumps(
                        {
                            "review_reason": review_reason,
                            "preview_path": preview_path,
                            "candidate_count": len(candidate_list),
                        }
                    ),
                ),
            )

        if ocr_request_reason is not None:
            cur.execute(
                """
                INSERT INTO ocr_requests (
                    sheet_scan_id,
                    status,
                    request_reason
                )
                VALUES (%s, 'pending', %s)
                """,
                (sheet_scan_id, ocr_request_reason),
            )

    return len(candidate_list)
