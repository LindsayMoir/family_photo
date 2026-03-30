"""Enhancement stage for promoted photos."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import cv2

from config import AppConfig
from db.connection import connect
from photo_repository import get_photo_record, insert_photo_artifact, update_photo_stage


ENHANCE_STATUS = "enhancement_complete"
ENHANCE_ARTIFACT_TYPE = "enhanced"
ENHANCE_PIPELINE_VERSION = "enhance_v1"


@dataclass(frozen=True, slots=True)
class EnhancementSummary:
    photo_id: int
    output_path: Path
    enhancement_version: str
    dry_run: bool


def run_enhancement(config: AppConfig, *, photo_id: int, dry_run: bool) -> EnhancementSummary:
    """Apply conservative enhancement to a promoted photo."""
    with connect(config) as conn:
        photo = get_photo_record(conn, photo_id=photo_id)

    output_path = _enhancement_output_path(config.photos_root, photo_id)

    if dry_run:
        return EnhancementSummary(
            photo_id=photo_id,
            output_path=output_path,
            enhancement_version=ENHANCE_PIPELINE_VERSION,
            dry_run=True,
        )

    _write_enhanced_image(photo.working_path, output_path)

    with connect(config) as conn:
        update_photo_stage(
            conn,
            photo_id=photo_id,
            working_path=output_path,
            status=ENHANCE_STATUS,
            enhancement_version=ENHANCE_PIPELINE_VERSION,
        )
        insert_photo_artifact(
            conn,
            photo_id=photo_id,
            artifact_type=ENHANCE_ARTIFACT_TYPE,
            path=output_path,
            pipeline_stage="enhance",
            pipeline_version=ENHANCE_PIPELINE_VERSION,
        )
        conn.commit()

    return EnhancementSummary(
        photo_id=photo_id,
        output_path=output_path,
        enhancement_version=ENHANCE_PIPELINE_VERSION,
        dry_run=False,
    )


def _write_enhanced_image(input_path: Path, output_path: Path) -> None:
    image = cv2.imread(str(input_path))
    if image is None:
        raise ValueError(f"Unable to load photo for enhancement: {input_path}")

    denoised = cv2.fastNlMeansDenoisingColored(image, None, 4, 4, 7, 21)

    lab = cv2.cvtColor(denoised, cv2.COLOR_BGR2LAB)
    l_channel, a_channel, b_channel = cv2.split(lab)
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    enhanced_l = clahe.apply(l_channel)
    merged_lab = cv2.merge((enhanced_l, a_channel, b_channel))
    contrast_adjusted = cv2.cvtColor(merged_lab, cv2.COLOR_LAB2BGR)

    blurred = cv2.GaussianBlur(contrast_adjusted, (0, 0), sigmaX=1.0)
    sharpened = cv2.addWeighted(contrast_adjusted, 1.15, blurred, -0.15, 0)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not cv2.imwrite(str(output_path), sharpened):
        raise ValueError(f"Failed to write enhanced image: {output_path}")


def _enhancement_output_path(photos_root: Path, photo_id: int) -> Path:
    return photos_root / "derivatives" / "enhance" / f"photo_{photo_id}.jpg"
