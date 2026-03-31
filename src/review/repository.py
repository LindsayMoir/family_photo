"""Database access for review workflows."""

from __future__ import annotations

import json
from typing import Any

from psycopg2.extensions import connection as PgConnection

from detection.models import DetectionCandidate
from review.models import DetectionReviewCandidate, ReviewTask


def list_review_tasks(
    conn: PgConnection,
    *,
    task_type: str | None = None,
    status: str | None = None,
    limit: int = 25,
) -> list[ReviewTask]:
    """List review tasks ordered for operator work."""
    query = """
        SELECT id, entity_type, entity_id, task_type, status, priority, payload_json
        FROM review_tasks
        WHERE 1 = 1
    """
    params: list[object] = []
    if task_type is not None:
        query += " AND task_type = %s"
        params.append(task_type)
    if status is not None:
        query += " AND status = %s"
        params.append(status)
    query += " ORDER BY status ASC, priority ASC, created_at ASC LIMIT %s"
    params.append(limit)

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        rows = cur.fetchall()

    tasks: list[ReviewTask] = []
    for row in rows:
        payload = row[6]
        if isinstance(payload, str):
            payload = json.loads(payload)
        tasks.append(
            ReviewTask(
                id=int(row[0]),
                entity_type=str(row[1]),
                entity_id=int(row[2]),
                task_type=str(row[3]),
                status=str(row[4]),
                priority=int(row[5]),
                payload_json=dict(payload),
            )
        )
    return tasks


def list_sheet_review_tasks(
    conn: PgConnection,
    *,
    batch_name: str | None = None,
    status: str | None = "open",
    limit: int = 25,
) -> list[ReviewTask]:
    """List unresolved sheet-level review tasks with preview paths."""
    query = """
        SELECT rt.id, rt.entity_type, rt.entity_id, rt.task_type, rt.status, rt.priority, rt.payload_json
        FROM review_tasks rt
        JOIN sheet_scans ss ON ss.id = rt.entity_id
        JOIN scan_batches sb ON sb.id = ss.scan_batch_id
        WHERE rt.task_type = 'review_detection'
          AND rt.entity_type = 'sheet_scan'
    """
    params: list[object] = []
    if status is not None:
        query += " AND rt.status = %s"
        params.append(status)
    if batch_name is not None:
        query += " AND sb.name = %s"
        params.append(batch_name)
    query += " ORDER BY rt.status ASC, rt.priority ASC, rt.created_at ASC LIMIT %s"
    params.append(limit)

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        rows = cur.fetchall()

    tasks: list[ReviewTask] = []
    for row in rows:
        payload = row[6]
        if isinstance(payload, str):
            payload = json.loads(payload)
        tasks.append(
            ReviewTask(
                id=int(row[0]),
                entity_type=str(row[1]),
                entity_id=int(row[2]),
                task_type=str(row[3]),
                status=str(row[4]),
                priority=int(row[5]),
                payload_json=dict(payload),
            )
        )
    return tasks


def get_sheet_status_counts(
    conn: PgConnection,
    *,
    batch_name: str | None = None,
) -> dict[str, int]:
    """Return sheet counts grouped by status, optionally filtered by batch."""
    query = """
        SELECT COALESCE(ss.status, 'unknown') AS sheet_status, COUNT(*)
        FROM sheet_scans ss
        JOIN scan_batches sb ON sb.id = ss.scan_batch_id
        WHERE 1 = 1
    """
    params: list[object] = []
    if batch_name is not None:
        query += " AND sb.name = %s"
        params.append(batch_name)
    query += " GROUP BY COALESCE(ss.status, 'unknown') ORDER BY sheet_status"

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
    return {str(row[0]): int(row[1]) for row in rows}


def get_open_review_task_counts(
    conn: PgConnection,
    *,
    batch_name: str | None = None,
    sheet_id: int | None = None,
) -> dict[str, int]:
    """Return counts of open review tasks grouped by task type."""
    query = """
        SELECT rt.task_type, COUNT(*)
        FROM review_tasks rt
        LEFT JOIN sheet_scans ss
            ON rt.entity_type = 'sheet_scan'
           AND ss.id = rt.entity_id
        LEFT JOIN photos p
            ON rt.entity_type = 'photo'
           AND p.id = rt.entity_id
        LEFT JOIN sheet_scans photo_ss
            ON p.sheet_scan_id = photo_ss.id
        LEFT JOIN scan_batches ssb
            ON ss.scan_batch_id = ssb.id
        LEFT JOIN scan_batches psb
            ON photo_ss.scan_batch_id = psb.id
        WHERE rt.status = 'open'
    """
    params: list[object] = []
    if batch_name is not None:
        query += " AND COALESCE(ssb.name, psb.name) = %s"
        params.append(batch_name)
    if sheet_id is not None:
        query += " AND (rt.entity_id = %s OR photo_ss.id = %s)"
        params.extend((sheet_id, sheet_id))
    query += " GROUP BY rt.task_type ORDER BY rt.task_type"

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
    return {str(row[0]): int(row[1]) for row in rows}


def get_next_review_task(conn: PgConnection, task_type: str | None = None) -> ReviewTask | None:
    """Fetch the next open review task."""
    query = """
        SELECT id, entity_type, entity_id, task_type, status, priority, payload_json
        FROM review_tasks
        WHERE status = 'open'
    """
    params: list[object] = []
    if task_type is not None:
        query += " AND task_type = %s"
        params.append(task_type)
    query += " ORDER BY priority ASC, created_at ASC LIMIT 1"

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        row = cur.fetchone()

    if row is None:
        return None

    payload = row[6]
    if isinstance(payload, str):
        payload = json.loads(payload)

    return ReviewTask(
        id=int(row[0]),
        entity_type=str(row[1]),
        entity_id=int(row[2]),
        task_type=str(row[3]),
        status=str(row[4]),
        priority=int(row[5]),
        payload_json=dict(payload),
    )


def get_next_sheet_review_task(
    conn: PgConnection,
    *,
    batch_name: str | None = None,
    sheet_id: int | None = None,
) -> ReviewTask | None:
    """Fetch the next open sheet-level review task, optionally scoped."""
    query = """
        SELECT rt.id, rt.entity_type, rt.entity_id, rt.task_type, rt.status, rt.priority, rt.payload_json
        FROM review_tasks rt
        JOIN sheet_scans ss ON ss.id = rt.entity_id
        JOIN scan_batches sb ON sb.id = ss.scan_batch_id
        WHERE rt.status = 'open'
          AND rt.task_type = 'review_detection'
          AND rt.entity_type = 'sheet_scan'
    """
    params: list[object] = []
    if batch_name is not None:
        query += " AND sb.name = %s"
        params.append(batch_name)
    if sheet_id is not None:
        query += " AND rt.entity_id = %s"
        params.append(sheet_id)
    query += " ORDER BY rt.priority ASC, rt.created_at ASC LIMIT 1"

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        row = cur.fetchone()

    if row is None:
        return None

    payload = row[6]
    if isinstance(payload, str):
        payload = json.loads(payload)

    return ReviewTask(
        id=int(row[0]),
        entity_type=str(row[1]),
        entity_id=int(row[2]),
        task_type=str(row[3]),
        status=str(row[4]),
        priority=int(row[5]),
        payload_json=dict(payload),
    )


def get_review_task(conn: PgConnection, task_id: int) -> ReviewTask | None:
    """Fetch a review task and enrich it with current linked fields."""
    task = _get_review_task(conn, task_id)
    if task is None:
        return task

    if task.task_type == "review_detection" and task.entity_type == "sheet_scan":
        payload_json = dict(task.payload_json)
        payload_json["detections"] = [
            {
                "id": candidate.id,
                "region_type": candidate.region_type,
                "confidence": candidate.confidence,
                "accepted": candidate.accepted,
                "crop_path": candidate.crop_path,
            }
            for candidate in _list_detection_candidates(conn, sheet_scan_id=task.entity_id)
        ]
        return ReviewTask(
            id=task.id,
            entity_type=task.entity_type,
            entity_id=task.entity_id,
            task_type=task.task_type,
            status=task.status,
            priority=task.priority,
            payload_json=payload_json,
        )

    if task.task_type == "review_orientation" and task.entity_type == "photo":
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT working_path, rotation_degrees, status
                FROM photos
                WHERE id = %s
                """,
                (task.entity_id,),
            )
            row = cur.fetchone()
        if row is None:
            return task
        payload_json = dict(task.payload_json)
        payload_json["working_path"] = row[0]
        payload_json["rotation_degrees"] = row[1]
        payload_json["photo_status"] = row[2]
        return ReviewTask(
            id=task.id,
            entity_type=task.entity_type,
            entity_id=task.entity_id,
            task_type=task.task_type,
            status=task.status,
            priority=task.priority,
            payload_json=payload_json,
        )

    if task.task_type == "review_export_audit" and task.entity_type == "photo":
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT working_path, rotation_degrees, status
                FROM photos
                WHERE id = %s
                """,
                (task.entity_id,),
            )
            row = cur.fetchone()
        if row is None:
            return task
        payload_json = dict(task.payload_json)
        payload_json["working_path"] = row[0]
        payload_json["rotation_degrees"] = row[1]
        payload_json["photo_status"] = row[2]
        return ReviewTask(
            id=task.id,
            entity_type=task.entity_type,
            entity_id=task.entity_id,
            task_type=task.task_type,
            status=task.status,
            priority=task.priority,
            payload_json=payload_json,
        )

    if task.task_type != "review_ocr":
        return task

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT crop_path, ocr_text, ocr_confidence, reviewed_by_human
            FROM photo_detections
            WHERE id = %s
            """,
            (task.entity_id,),
        )
        row = cur.fetchone()

    if row is None:
        return task

    payload_json = dict(task.payload_json)
    payload_json["crop_path"] = row[0]
    payload_json["ocr_text"] = row[1]
    payload_json["ocr_confidence"] = row[2]
    payload_json["reviewed_by_human"] = row[3]
    return ReviewTask(
        id=task.id,
        entity_type=task.entity_type,
        entity_id=task.entity_id,
        task_type=task.task_type,
        status=task.status,
        priority=task.priority,
        payload_json=payload_json,
    )


def accept_detection_review(
    conn: PgConnection,
    *,
    task_id: int,
    detection_ids: list[int],
    note: str | None,
) -> ReviewTask:
    """Accept selected detections for a sheet-level review task and resolve it."""
    task = _get_review_task(conn, task_id)
    if task is None:
        raise ValueError(f"Review task {task_id} was not found.")
    if task.task_type != "review_detection" or task.entity_type != "sheet_scan":
        raise ValueError(f"Review task {task_id} is not a sheet detection review task.")
    if task.status not in {"open", "in_progress"}:
        raise ValueError(f"Review task {task_id} is already {task.status}.")

    candidates = _list_detection_candidates(conn, sheet_scan_id=task.entity_id)
    candidate_ids = {candidate.id for candidate in candidates}
    requested_ids = set(detection_ids)
    if not requested_ids:
        raise ValueError("At least one detection id must be provided.")
    unknown_ids = sorted(requested_ids.difference(candidate_ids))
    if unknown_ids:
        raise ValueError(
            f"Detection ids are not attached to sheet_scan_id={task.entity_id}: {unknown_ids}."
        )

    resolution_payload: dict[str, Any] = {
        "action": "accepted_detections",
        "accepted_detection_ids": sorted(requested_ids),
    }
    if note:
        resolution_payload["note"] = note

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE photo_detections
            SET accepted = FALSE,
                reviewed_by_human = TRUE
            WHERE sheet_scan_id = %s
            """,
            (task.entity_id,),
        )
        cur.execute(
            """
            UPDATE photo_detections
            SET accepted = TRUE,
                reviewed_by_human = TRUE
            WHERE id = ANY(%s)
            """,
            (list(requested_ids),),
        )
        cur.execute(
            """
            UPDATE sheet_scans
            SET status = 'detection_complete',
                error_message = NULL,
                updated_at = NOW()
            WHERE id = %s
            """,
            (task.entity_id,),
        )
        cur.execute(
            """
            UPDATE review_tasks
            SET status = 'resolved',
                resolution_json = %s::jsonb,
                resolved_at = NOW()
            WHERE id = %s
            RETURNING id, entity_type, entity_id, task_type, status, priority, payload_json
            """,
            (json.dumps(resolution_payload), task_id),
        )
        row = cur.fetchone()

    if row is None:
        raise RuntimeError(f"Failed to resolve review task {task_id}.")

    payload = row[6]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return ReviewTask(
        id=int(row[0]),
        entity_type=str(row[1]),
        entity_id=int(row[2]),
        task_type=str(row[3]),
        status=str(row[4]),
        priority=int(row[5]),
        payload_json=dict(payload),
    )


def create_manual_detection(
    conn: PgConnection,
    *,
    sheet_scan_id: int,
    candidate: DetectionCandidate,
) -> int:
    """Insert one manually reviewed detection row."""
    with conn.cursor() as cur:
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
            VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, FALSE, TRUE)
            RETURNING id
            """,
            (
                sheet_scan_id,
                "manual_review_v1",
                "review_manual_v1",
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
                candidate.ocr_engine,
                candidate.ocr_confidence,
            ),
        )
        row = cur.fetchone()

    if row is None:
        raise RuntimeError(f"Failed to insert manual detection for sheet_scan_id={sheet_scan_id}.")
    return int(row[0])


def resolve_review_task(
    conn: PgConnection,
    *,
    task_id: int,
    dismiss: bool,
    note: str | None,
    ocr_text: str | None,
    export_action: str | None = None,
) -> ReviewTask:
    """Resolve a review task and apply OCR edits when requested."""
    task = _get_review_task(conn, task_id)
    if task is None:
        raise ValueError(f"Review task {task_id} was not found.")
    if task.status not in {"open", "in_progress"}:
        raise ValueError(f"Review task {task_id} is already {task.status}.")

    resolution_payload: dict[str, Any] = {}
    if note:
        resolution_payload["note"] = note

    if task.task_type == "review_ocr" and not dismiss:
        if ocr_text is not None:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE photo_detections
                    SET ocr_text = %s,
                        ocr_confidence = 1.0,
                        reviewed_by_human = TRUE
                    WHERE id = %s
                    """,
                    (ocr_text, task.entity_id),
                )
            resolution_payload["ocr_text"] = ocr_text
            resolution_payload["action"] = "corrected_ocr"
        else:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE photo_detections
                    SET reviewed_by_human = TRUE
                    WHERE id = %s
                    """,
                    (task.entity_id,),
                )
            resolution_payload["action"] = "accepted_ocr"

    if task.task_type == "review_export_audit" and not dismiss:
        if export_action is None:
            raise ValueError("An export_action is required for review_export_audit tasks.")
        resolution_payload["action"] = export_action

    new_status = "dismissed" if dismiss else "resolved"
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE review_tasks
            SET status = %s,
                resolution_json = %s::jsonb,
                resolved_at = NOW()
            WHERE id = %s
            RETURNING id, entity_type, entity_id, task_type, status, priority, payload_json
            """,
            (new_status, json.dumps(resolution_payload), task_id),
        )
        row = cur.fetchone()

    if row is None:
        raise RuntimeError(f"Failed to update review task {task_id}.")

    payload = row[6]
    if isinstance(payload, str):
        payload = json.loads(payload)

    return ReviewTask(
        id=int(row[0]),
        entity_type=str(row[1]),
        entity_id=int(row[2]),
        task_type=str(row[3]),
        status=str(row[4]),
        priority=int(row[5]),
        payload_json=dict(payload),
    )


def _get_review_task(conn: PgConnection, task_id: int) -> ReviewTask | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, entity_type, entity_id, task_type, status, priority, payload_json
            FROM review_tasks
            WHERE id = %s
            """,
            (task_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    payload = row[6]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return ReviewTask(
        id=int(row[0]),
        entity_type=str(row[1]),
        entity_id=int(row[2]),
        task_type=str(row[3]),
        status=str(row[4]),
        priority=int(row[5]),
        payload_json=dict(payload),
    )


def upsert_export_audit_review_task(
    conn: PgConnection,
    *,
    photo_id: int,
    payload_json: dict[str, Any],
    priority: int,
) -> None:
    """Create or refresh an export-audit review task for a photo."""
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM review_tasks
            WHERE entity_type = 'photo'
              AND entity_id = %s
              AND task_type = 'review_export_audit'
              AND status IN ('open', 'in_progress')
            """,
            (photo_id,),
        )
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
            VALUES ('photo', %s, 'review_export_audit', 'open', %s, %s::jsonb)
            """,
            (photo_id, priority, json.dumps(payload_json)),
        )


def dismiss_export_audit_review_task(
    conn: PgConnection,
    *,
    photo_id: int,
) -> bool:
    """Dismiss any open export-audit review task for a photo."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE review_tasks
            SET status = 'dismissed',
                resolution_json = jsonb_build_object('action', 'cleared_from_audit_csv'),
                resolved_at = NOW()
            WHERE entity_type = 'photo'
              AND entity_id = %s
              AND task_type = 'review_export_audit'
              AND status IN ('open', 'in_progress')
            """,
            (photo_id,),
        )
        return cur.rowcount > 0


def resolve_open_orientation_review_task(
    conn: PgConnection,
    *,
    photo_id: int,
    action: str,
) -> bool:
    """Resolve any open orientation review task for a photo."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE review_tasks
            SET status = 'resolved',
                resolution_json = jsonb_build_object('action', %s),
                resolved_at = NOW()
            WHERE entity_type = 'photo'
              AND entity_id = %s
              AND task_type = 'review_orientation'
              AND status IN ('open', 'in_progress')
            """,
            (action, photo_id),
        )
        return cur.rowcount > 0


def dismiss_open_ocr_review_tasks(
    conn: PgConnection,
    *,
    batch_name: str | None = None,
) -> int:
    """Dismiss open OCR review tasks, optionally scoped to a batch."""
    params: list[object] = []
    query = """
        UPDATE review_tasks rt
        SET status = 'dismissed',
            resolution_json = jsonb_build_object('action', 'bulk_dismissed_ocr'),
            resolved_at = NOW()
        WHERE rt.task_type = 'review_ocr'
          AND rt.status IN ('open', 'in_progress')
    """
    if batch_name is not None:
        query += """
          AND EXISTS (
              SELECT 1
              FROM photo_detections pd
              JOIN sheet_scans ss ON ss.id = pd.sheet_scan_id
              JOIN scan_batches sb ON sb.id = ss.scan_batch_id
              WHERE pd.id = rt.entity_id
                AND sb.name = %s
          )
        """
        params.append(batch_name)

    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        return cur.rowcount


def _list_detection_candidates(
    conn: PgConnection,
    *,
    sheet_scan_id: int,
) -> list[DetectionReviewCandidate]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, region_type, confidence, accepted, crop_path
            FROM photo_detections
            WHERE sheet_scan_id = %s
            ORDER BY id
            """,
            (sheet_scan_id,),
        )
        rows = cur.fetchall()

    return [
        DetectionReviewCandidate(
            id=int(row[0]),
            region_type=str(row[1]),
            confidence=float(row[2]),
            accepted=bool(row[3]),
            crop_path=str(row[4]) if row[4] is not None else None,
        )
        for row in rows
    ]
