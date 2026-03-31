from __future__ import annotations

from detection.models import DetectionCandidate
from detection.service import (
    _intersection_over_smaller_area,
    _preview_path,
    _region_crop_path,
)


def _candidate(
    *,
    x1: int,
    y1: int,
    x2: int,
    y2: int,
    confidence: float = 0.90,
    area_ratio: float = 0.10,
    region_type: str = "photo",
) -> DetectionCandidate:
    return DetectionCandidate(
        region_type=region_type,
        contour_points=((x1, y1), (x2, y1), (x2, y2), (x1, y2)),
        box_points=((x1, y1), (x2, y1), (x2, y2), (x1, y2)),
        center_x=float(x1 + x2) / 2.0,
        center_y=float(y1 + y2) / 2.0,
        width=float(x2 - x1),
        height=float(y2 - y1),
        angle=0.0,
        area_ratio=area_ratio,
        rectangularity=0.95,
        confidence=confidence,
    )


def test_intersection_over_smaller_area_uses_smaller_box_as_denominator() -> None:
    overlap_ratio = _intersection_over_smaller_area(
        _candidate(x1=0, y1=0, x2=100, y2=100),
        _candidate(x1=50, y1=50, x2=150, y2=150),
    )

    assert overlap_ratio == 0.25


def test_detection_output_paths_follow_repo_convention(app_config, sample_sheet_scan) -> None:
    preview_path = _preview_path(app_config.photos_root, sample_sheet_scan)
    crop_path = _region_crop_path(app_config.photos_root, sample_sheet_scan, "photo", 2)

    assert preview_path == (
        app_config.photos_root / "derivatives" / "review" / "detections" / "batch-a" / "sheet_101.jpg"
    )
    assert crop_path == (
        app_config.photos_root
        / "derivatives"
        / "review"
        / "regions"
        / "batch-a"
        / "sheet_101_2_photo.jpg"
    )
