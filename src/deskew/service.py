"""Deskew stage for promoted photos."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import cv2
import numpy as np

from config import AppConfig
from db.connection import connect
from photo_repository import get_photo_record, insert_photo_artifact, update_photo_stage


DESKEW_STATUS = "deskew_complete"
DESKEW_ARTIFACT_TYPE = "deskewed"
DESKEW_PIPELINE_VERSION = "deskew_v1"
MAX_DESKEW_ABS_ANGLE = 8.0


@dataclass(frozen=True, slots=True)
class DeskewSummary:
    photo_id: int
    angle_degrees: float
    confidence: float
    output_path: Path
    dry_run: bool


def run_deskew(config: AppConfig, *, photo_id: int, dry_run: bool) -> DeskewSummary:
    """Estimate and apply a small-angle deskew correction."""
    with connect(config) as conn:
        photo = get_photo_record(conn, photo_id=photo_id)

    angle, confidence = _estimate_deskew(photo.working_path)
    output_path = _deskew_output_path(config.photos_root, photo_id)

    if dry_run:
        return DeskewSummary(photo_id=photo_id, angle_degrees=angle, confidence=confidence, output_path=output_path, dry_run=True)

    _write_deskewed_image(photo.working_path, output_path, angle)

    with connect(config) as conn:
        update_photo_stage(
            conn,
            photo_id=photo_id,
            working_path=output_path,
            status=DESKEW_STATUS,
            deskew_angle=angle,
            deskew_confidence=confidence,
        )
        insert_photo_artifact(
            conn,
            photo_id=photo_id,
            artifact_type=DESKEW_ARTIFACT_TYPE,
            path=output_path,
            pipeline_stage="deskew",
            pipeline_version=DESKEW_PIPELINE_VERSION,
        )
        conn.commit()

    return DeskewSummary(photo_id=photo_id, angle_degrees=angle, confidence=confidence, output_path=output_path, dry_run=False)


def _estimate_deskew(image_path: Path) -> tuple[float, float]:
    image = cv2.imread(str(image_path))
    if image is None:
        raise ValueError(f"Unable to load photo for deskew: {image_path}")

    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    edges = cv2.Canny(gray, 50, 150)
    lines = cv2.HoughLinesP(edges, 1, np.pi / 180.0, threshold=100, minLineLength=200, maxLineGap=20)
    if lines is None:
        return 0.0, 0.0

    angles: list[float] = []
    for line in lines[:, 0]:
        x1, y1, x2, y2 = line.tolist()
        angle = np.degrees(np.arctan2(y2 - y1, x2 - x1))
        normalized = ((angle + 90.0) % 180.0) - 90.0
        if abs(normalized) <= MAX_DESKEW_ABS_ANGLE:
            angles.append(float(normalized))

    if not angles:
        return 0.0, 0.0

    median_angle = float(np.median(np.array(angles)))
    confidence = min(len(angles) / 20.0, 1.0)
    if abs(median_angle) < 0.15:
        return 0.0, confidence
    return round(median_angle, 4), round(confidence, 4)


def _write_deskewed_image(input_path: Path, output_path: Path, angle: float) -> None:
    image = cv2.imread(str(input_path))
    if image is None:
        raise ValueError(f"Unable to load photo for deskew write: {input_path}")

    if abs(angle) < 0.01:
        transformed = image
    else:
        height, width = image.shape[:2]
        center = (width / 2.0, height / 2.0)
        matrix = cv2.getRotationMatrix2D(center, angle, 1.0)
        transformed = cv2.warpAffine(
            image,
            matrix,
            (width, height),
            flags=cv2.INTER_CUBIC,
            borderMode=cv2.BORDER_REPLICATE,
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not cv2.imwrite(str(output_path), transformed):
        raise ValueError(f"Failed to write deskewed image: {output_path}")


def _deskew_output_path(photos_root: Path, photo_id: int) -> Path:
    return photos_root / "derivatives" / "deskew" / f"photo_{photo_id}.jpg"

