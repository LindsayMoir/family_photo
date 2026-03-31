from __future__ import annotations

from pathlib import Path

import cv2
import numpy as np

from detection.analysis import detect_sheet_regions


def test_detect_sheet_regions_uses_full_image_photo_fallback_when_ocr_disabled(tmp_path: Path) -> None:
    image_path = tmp_path / "text_like.jpg"
    image = np.full((200, 300, 3), 255, dtype=np.uint8)
    assert cv2.imwrite(str(image_path), image)

    result = detect_sheet_regions(image_path, enable_ocr=False)

    assert result.ocr_request_reason == "ocr_disabled_requires_followup"
    assert len(result.candidates) == 1
    candidate = result.candidates[0]
    assert candidate.region_type == "photo"
    assert candidate.confidence >= 0.8
    assert candidate.area_ratio == 1.0
