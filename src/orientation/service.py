"""Orientation stage for promoted photos."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

import cv2

from config import AppConfig
from db.connection import connect
from photo_repository import (
    get_photo_artifact_path,
    get_photo_record,
    insert_photo_artifact,
    update_photo_stage,
)


ORIENTATION_STATUS = "orientation_complete"
ORIENTATION_REVIEW_STATUS = "orientation_review_required"
ORIENTATION_ARTIFACT_TYPE = "oriented"
ORIENTATION_PIPELINE_VERSION = "orientation_v2"
ORIENTATION_MANUAL_PIPELINE_VERSION = "orientation_manual_v1"
ORIENTATION_REVIEW_THRESHOLD = 0.15


@dataclass(frozen=True, slots=True)
class OrientationDecision:
    rotation_degrees: int
    confidence: float
    score_by_rotation: dict[int, float]
    review_required: bool


@dataclass(frozen=True, slots=True)
class OrientationSummary:
    photo_id: int
    rotation_degrees: int
    confidence: float
    review_required: bool
    output_path: Path
    dry_run: bool


def run_orientation(
    config: AppConfig,
    *,
    photo_id: int,
    forced_rotation: int | None = None,
    dry_run: bool,
) -> OrientationSummary:
    """Select a cardinal rotation and require review when confidence is too low."""
    with connect(config) as conn:
        photo = get_photo_record(conn, photo_id=photo_id)
        input_path = _resolve_orientation_input_path(
            conn,
            photo_id=photo_id,
            fallback_path=photo.working_path,
            forced_rotation=forced_rotation,
        )

    decision = (
        OrientationDecision(
            rotation_degrees=forced_rotation,
            confidence=1.0,
            score_by_rotation={forced_rotation: 1.0},
            review_required=False,
        )
        if forced_rotation is not None
        else _select_rotation(input_path)
    )
    output_path = _orientation_output_path(config.photos_root, photo_id)

    if dry_run:
        return OrientationSummary(
            photo_id=photo_id,
            rotation_degrees=decision.rotation_degrees,
            confidence=decision.confidence,
            review_required=decision.review_required,
            output_path=output_path,
            dry_run=True,
        )

    if decision.review_required and forced_rotation is None:
        with connect(config) as conn:
            _upsert_orientation_review_task(
                conn,
                photo_id=photo_id,
                preview_path=input_path,
                suggested_rotation=decision.rotation_degrees,
                confidence=decision.confidence,
                score_by_rotation=decision.score_by_rotation,
            )
            update_photo_stage(
                conn,
                photo_id=photo_id,
                working_path=photo.working_path,
                status=ORIENTATION_REVIEW_STATUS,
            )
            conn.commit()
        return OrientationSummary(
            photo_id=photo_id,
            rotation_degrees=decision.rotation_degrees,
            confidence=decision.confidence,
            review_required=True,
            output_path=output_path,
            dry_run=False,
        )

    _write_oriented_image(input_path, output_path, decision.rotation_degrees)

    with connect(config) as conn:
        update_photo_stage(
            conn,
            photo_id=photo_id,
            working_path=output_path,
            status=ORIENTATION_STATUS,
            rotation_degrees=decision.rotation_degrees,
        )
        insert_photo_artifact(
            conn,
            photo_id=photo_id,
            artifact_type=ORIENTATION_ARTIFACT_TYPE,
            path=output_path,
            pipeline_stage="orientation",
            pipeline_version=(
                ORIENTATION_MANUAL_PIPELINE_VERSION
                if forced_rotation is not None
                else ORIENTATION_PIPELINE_VERSION
            ),
        )
        _resolve_open_orientation_review_task(conn, photo_id=photo_id)
        conn.commit()

    return OrientationSummary(
        photo_id=photo_id,
        rotation_degrees=decision.rotation_degrees,
        confidence=decision.confidence,
        review_required=False,
        output_path=output_path,
        dry_run=False,
    )


def _select_rotation(image_path: Path) -> OrientationDecision:
    image = cv2.imread(str(image_path))
    if image is None:
        raise ValueError(f"Unable to load photo for orientation: {image_path}")

    cascade_path = Path(cv2.data.haarcascades) / "haarcascade_frontalface_default.xml"
    classifier = cv2.CascadeClassifier(str(cascade_path))
    if classifier.empty():
        return OrientationDecision(
            rotation_degrees=0,
            confidence=0.0,
            score_by_rotation={0: 0.0, 90: 0.0, 180: 0.0, 270: 0.0},
            review_required=True,
        )

    score_by_rotation: dict[int, float] = {}

    for rotation in (0, 90, 180, 270):
        rotated = _rotate_image(image, rotation)
        gray = cv2.cvtColor(rotated, cv2.COLOR_BGR2GRAY)
        faces = classifier.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5, minSize=(40, 40))
        score = float(len(faces))
        if len(faces) > 0:
            score += sum((w * h) for (_, _, w, h) in faces) / float(rotated.shape[0] * rotated.shape[1])
        score_by_rotation[rotation] = score

    ordered_scores = sorted(score_by_rotation.items(), key=lambda item: item[1], reverse=True)
    best_rotation, best_score = ordered_scores[0]
    second_score = ordered_scores[1][1] if len(ordered_scores) > 1 else 0.0
    if best_score <= 0.0:
        confidence = 0.0
    else:
        confidence = max(0.0, min(1.0, (best_score - second_score) / best_score))

    return OrientationDecision(
        rotation_degrees=best_rotation,
        confidence=confidence,
        score_by_rotation=score_by_rotation,
        review_required=(
            confidence < ORIENTATION_REVIEW_THRESHOLD
            or best_rotation in {90, 270}
        ),
    )


def _write_oriented_image(input_path: Path, output_path: Path, rotation: int) -> None:
    image = cv2.imread(str(input_path))
    if image is None:
        raise ValueError(f"Unable to load photo for orientation write: {input_path}")
    transformed = _rotate_image(image, rotation)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not cv2.imwrite(str(output_path), transformed):
        raise ValueError(f"Failed to write oriented image: {output_path}")


def _rotate_image(image, rotation: int):
    if rotation == 0:
        return image
    if rotation == 90:
        return cv2.rotate(image, cv2.ROTATE_90_CLOCKWISE)
    if rotation == 180:
        return cv2.rotate(image, cv2.ROTATE_180)
    if rotation == 270:
        return cv2.rotate(image, cv2.ROTATE_90_COUNTERCLOCKWISE)
    raise ValueError(f"Unsupported rotation: {rotation}")


def _orientation_output_path(photos_root: Path, photo_id: int) -> Path:
    return photos_root / "derivatives" / "orient" / f"photo_{photo_id}.jpg"


def _resolve_orientation_input_path(
    conn,
    *,
    photo_id: int,
    fallback_path: Path,
    forced_rotation: int | None,
) -> Path:
    if forced_rotation is None:
        return fallback_path
    deskew_path = get_photo_artifact_path(conn, photo_id=photo_id, artifact_type="deskewed")
    if deskew_path is not None:
        return deskew_path
    return fallback_path


def _upsert_orientation_review_task(
    conn,
    *,
    photo_id: int,
    preview_path: Path,
    suggested_rotation: int,
    confidence: float,
    score_by_rotation: dict[int, float],
) -> None:
    payload_json = json.dumps(
        {
            "preview_path": str(preview_path),
            "suggested_rotation": suggested_rotation,
            "confidence": confidence,
            "score_by_rotation": score_by_rotation,
        }
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM review_tasks
            WHERE entity_type = 'photo'
              AND entity_id = %s
              AND task_type = 'review_orientation'
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
            VALUES ('photo', %s, 'review_orientation', 'open', %s, %s::jsonb)
            """,
            (photo_id, 15, payload_json),
        )


def _resolve_open_orientation_review_task(conn, *, photo_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE review_tasks
            SET status = 'resolved',
                resolution_json = %s::jsonb,
                resolved_at = NOW()
            WHERE entity_type = 'photo'
              AND entity_id = %s
              AND task_type = 'review_orientation'
              AND status IN ('open', 'in_progress')
            """,
            (json.dumps({"action": "orientation_applied"}), photo_id),
        )
