"""Hybrid photo and text region detection for sheet scans."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import cv2
import numpy as np
import pytesseract
from pytesseract import Output

from detection.models import DetectionCandidate
from detection.ocr import extract_text


MAX_OUTPUT_CANDIDATES = 8
MIN_AREA_RATIO = 0.015
MAX_AREA_RATIO = 0.95
MIN_RECTANGULARITY = 0.72
MIN_ASPECT_RATIO = 0.35
MAX_ASPECT_RATIO = 3.2
MIN_PHOTO_CONFIDENCE = 0.45
MIN_TEXT_WORDS = 3
MIN_TEXT_CONFIDENCE = 0.40
MAX_IOU = 0.35
MAX_TEXT_AREA_RATIO = 0.45
MAX_TEXT_WIDTH_RATIO = 0.92
MAX_TEXT_HEIGHT_RATIO = 0.92
SUBDIVISION_MIN_COMPONENT_AREA_RATIO = 0.025
INTENSITY_MASK_THRESHOLD = 205
MIN_TEXT_BOX_COVERAGE = 0.01
MIN_TEXT_HEIGHT_RATIO_FOR_LOW_CONFIDENCE = 0.18
WIDE_DUPLICATE_WIDTH_RATIO = 0.75
WIDE_DUPLICATE_HALF_SIMILARITY = 0.83
WIDE_DUPLICATE_MIN_OTHER_AREA_RATIO = 0.05


@dataclass(frozen=True, slots=True)
class DetectionAnalysisResult:
    candidates: list[DetectionCandidate]
    ocr_request_reason: str | None = None


def detect_sheet_regions(
    image_path: Path,
    *,
    fast_mode: bool = False,
    enable_ocr: bool = False,
) -> DetectionAnalysisResult:
    """Detect photo and text regions in a sheet scan."""
    image = cv2.imread(str(image_path))
    if image is None:
        raise ValueError(f"Unable to load image for detection: {image_path}")

    photo_candidates = _detect_photo_candidates(image)
    photo_candidates = _subdivide_large_photo_candidates(image, photo_candidates)
    if not enable_ocr:
        if photo_candidates:
            return DetectionAnalysisResult(candidates=photo_candidates[:MAX_OUTPUT_CANDIDATES])
        return DetectionAnalysisResult(
            candidates=[_full_image_photo_candidate(image)],
            ocr_request_reason="ocr_disabled_requires_followup",
        )

    text_candidates = _detect_text_candidates(image, photo_candidates, fast_mode=fast_mode)

    combined = list(photo_candidates)
    for text_candidate in text_candidates:
        if not _overlaps_existing(text_candidate, combined):
            combined.append(text_candidate)

    combined.sort(key=lambda candidate: candidate.confidence, reverse=True)
    return DetectionAnalysisResult(candidates=combined[:MAX_OUTPUT_CANDIDATES])


def render_detection_preview(image_path: Path, candidates: list[DetectionCandidate], output_path: Path) -> Path:
    """Render a preview image with detected boxes and types."""
    image = cv2.imread(str(image_path))
    if image is None:
        raise ValueError(f"Unable to load image for preview rendering: {image_path}")

    for index, candidate in enumerate(candidates, start=1):
        points = np.array(candidate.box_points, dtype=np.int32)
        color = (0, 255, 0) if candidate.region_type == "photo" else (0, 165, 255)
        cv2.polylines(image, [points], isClosed=True, color=color, thickness=5)
        label_point = tuple(points[0])
        cv2.putText(
            image,
            f"{index}:{candidate.region_type}:{candidate.confidence:.2f}",
            label_point,
            cv2.FONT_HERSHEY_SIMPLEX,
            0.8,
            (255, 0, 0),
            2,
            cv2.LINE_AA,
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not cv2.imwrite(str(output_path), image):
        raise ValueError(f"Failed to write detection preview: {output_path}")
    return output_path


def write_candidate_crop(image_path: Path, candidate: DetectionCandidate, output_path: Path) -> Path:
    """Write an axis-aligned crop for a detected region."""
    image = cv2.imread(str(image_path))
    if image is None:
        raise ValueError(f"Unable to load image for crop writing: {image_path}")

    x1, y1, x2, y2 = _bounds(candidate.box_points)
    x1 = max(0, x1)
    y1 = max(0, y1)
    x2 = min(image.shape[1], x2)
    y2 = min(image.shape[0], y2)
    crop = image[y1:y2, x1:x2]
    if crop.size == 0:
        raise ValueError(f"Detected empty crop for {image_path}.")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not cv2.imwrite(str(output_path), crop):
        raise ValueError(f"Failed to write crop image: {output_path}")
    return output_path


def has_duplicate_wide_photo_candidate(
    image_path: Path,
    photo_candidates: list[DetectionCandidate],
) -> bool:
    """Return True when a wide candidate appears to duplicate another photo on the sheet."""
    if len(photo_candidates) < 2:
        return False

    image = cv2.imread(str(image_path))
    if image is None:
        raise ValueError(f"Unable to load image for duplicate-wide audit: {image_path}")

    image_height, image_width = image.shape[:2]
    for candidate in photo_candidates:
        x1, y1, x2, y2 = _bounds(candidate.box_points)
        width = x2 - x1
        height = y2 - y1
        if width <= 0 or height <= 0:
            continue
        if (width / float(image_width)) < WIDE_DUPLICATE_WIDTH_RATIO:
            continue

        half_midpoint = x1 + (width // 2)
        left_half = image[y1:y2, x1:half_midpoint]
        right_half = image[y1:y2, half_midpoint:x2]
        if left_half.size == 0 or right_half.size == 0:
            continue

        for other in photo_candidates:
            if other is candidate or other.area_ratio < WIDE_DUPLICATE_MIN_OTHER_AREA_RATIO:
                continue
            other_x1, other_y1, other_x2, other_y2 = _bounds(other.box_points)
            other_crop = image[other_y1:other_y2, other_x1:other_x2]
            if other_crop.size == 0:
                continue

            if _crop_similarity(left_half, other_crop) >= WIDE_DUPLICATE_HALF_SIMILARITY:
                return True
            if _crop_similarity(right_half, other_crop) >= WIDE_DUPLICATE_HALF_SIMILARITY:
                return True
    return False


def _full_image_photo_candidate(image: np.ndarray) -> DetectionCandidate:
    height, width = image.shape[:2]
    return DetectionCandidate(
        region_type="photo",
        contour_points=((0, 0), (width, 0), (width, height), (0, height)),
        box_points=((0, 0), (width, 0), (width, height), (0, height)),
        center_x=float(width / 2.0),
        center_y=float(height / 2.0),
        width=float(width),
        height=float(height),
        angle=0.0,
        area_ratio=1.0,
        rectangularity=1.0,
        confidence=0.85,
    )


def _detect_photo_candidates(image: np.ndarray) -> list[DetectionCandidate]:
    image_height, image_width = image.shape[:2]
    image_area = float(image_width * image_height)

    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    blurred = cv2.GaussianBlur(gray, (5, 5), 0)
    edges = cv2.Canny(blurred, 40, 120)
    kernel = np.ones((5, 5), dtype=np.uint8)
    merged = cv2.dilate(edges, kernel, iterations=2)
    merged = cv2.morphologyEx(merged, cv2.MORPH_CLOSE, kernel, iterations=2)

    contours, _ = cv2.findContours(merged, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    candidates: list[DetectionCandidate] = []

    for contour in contours:
        contour_area = float(cv2.contourArea(contour))
        if contour_area <= 0:
            continue

        area_ratio = contour_area / image_area
        if area_ratio < MIN_AREA_RATIO or area_ratio > MAX_AREA_RATIO:
            continue

        rect = cv2.minAreaRect(contour)
        (center_x, center_y), (width, height), angle = rect
        if width <= 1 or height <= 1:
            continue

        rect_area = float(width * height)
        rectangularity = contour_area / rect_area if rect_area > 0 else 0.0
        if rectangularity < MIN_RECTANGULARITY:
            continue

        aspect_ratio = max(width, height) / min(width, height)
        if aspect_ratio < MIN_ASPECT_RATIO or aspect_ratio > MAX_ASPECT_RATIO:
            continue

        confidence = _score_photo_candidate(area_ratio=area_ratio, rectangularity=rectangularity)
        if confidence < MIN_PHOTO_CONFIDENCE:
            continue

        box_points = cv2.boxPoints(rect)
        candidates.append(
            DetectionCandidate(
                region_type="photo",
                contour_points=tuple((int(point[0][0]), int(point[0][1])) for point in contour),
                box_points=tuple((int(point[0]), int(point[1])) for point in box_points),
                center_x=float(center_x),
                center_y=float(center_y),
                width=float(width),
                height=float(height),
                angle=float(angle),
                area_ratio=area_ratio,
                rectangularity=rectangularity,
                confidence=confidence,
            )
        )

    if not candidates:
        candidates = _detect_photo_candidates_by_intensity(image)

    candidates.sort(key=lambda candidate: candidate.confidence, reverse=True)
    return _suppress_overlaps(candidates)


def _detect_text_candidates(
    image: np.ndarray,
    photo_candidates: list[DetectionCandidate],
    *,
    fast_mode: bool,
) -> list[DetectionCandidate]:
    rois = _text_search_rois(image.shape[1], image.shape[0], photo_candidates)
    candidates: list[DetectionCandidate] = []

    for roi in rois:
        candidate = _ocr_roi_to_candidate(image, roi, fast_mode=fast_mode)
        if candidate is not None:
            candidates.append(candidate)

    if not candidates and not photo_candidates:
        fallback_result = extract_text(image, fast_mode=fast_mode)
        if _looks_like_fallback_text(fallback_result.text) and _can_use_full_sheet_text_fallback(image):
            height, width = image.shape[:2]
            candidates.append(
                DetectionCandidate(
                    region_type="text",
                    contour_points=((0, 0), (width, 0), (width, height), (0, height)),
                    box_points=((0, 0), (width, 0), (width, height), (0, height)),
                    center_x=float(width / 2.0),
                    center_y=float(height / 2.0),
                    width=float(width),
                    height=float(height),
                    angle=0.0,
                    area_ratio=1.0,
                    rectangularity=1.0,
                    confidence=0.35,
                    ocr_text=fallback_result.text.strip(),
                    ocr_confidence=max(0.35, fallback_result.confidence),
                    ocr_engine=fallback_result.engine,
                )
            )

    candidates.sort(key=lambda candidate: candidate.confidence, reverse=True)
    return _suppress_overlaps(candidates)


def _ocr_roi_to_candidate(
    image: np.ndarray,
    roi: tuple[int, int, int, int],
    *,
    fast_mode: bool,
) -> DetectionCandidate | None:
    rx1, ry1, rx2, ry2 = roi
    crop = image[ry1:ry2, rx1:rx2]
    if crop.size == 0:
        return None

    gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
    scaled = cv2.resize(gray, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
    normalized = cv2.GaussianBlur(scaled, (3, 3), 0)
    _, thresholded = cv2.threshold(normalized, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

    data = pytesseract.image_to_data(
        thresholded,
        output_type=Output.DICT,
        config="--oem 3 --psm 11",
    )

    boxes: list[tuple[int, int, int, int]] = []
    words: list[str] = []
    confidences: list[float] = []
    scale = 2.0

    for index, text in enumerate(data["text"]):
        cleaned = text.strip()
        if not cleaned:
            continue
        try:
            confidence = float(data["conf"][index])
        except (TypeError, ValueError):
            continue
        if confidence < 20:
            continue

        left = int(data["left"][index] / scale) + rx1
        top = int(data["top"][index] / scale) + ry1
        width = int(data["width"][index] / scale)
        height = int(data["height"][index] / scale)
        if width <= 0 or height <= 0:
            continue

        boxes.append((left, top, left + width, top + height))
        words.append(cleaned)
        confidences.append(confidence / 100.0)

    if len(words) < MIN_TEXT_WORDS:
        return None

    x1 = min(box[0] for box in boxes)
    y1 = min(box[1] for box in boxes)
    x2 = max(box[2] for box in boxes)
    y2 = max(box[3] for box in boxes)

    padding_x = max(20, int((x2 - x1) * 0.08))
    padding_y = max(20, int((y2 - y1) * 0.12))
    x1 = max(0, x1 - padding_x)
    y1 = max(0, y1 - padding_y)
    x2 = min(image.shape[1], x2 + padding_x)
    y2 = min(image.shape[0], y2 + padding_y)

    roi_area = float((rx2 - rx1) * (ry2 - ry1))
    text_box_area = float(sum((box[2] - box[0]) * (box[3] - box[1]) for box in boxes))
    if roi_area <= 0 or (text_box_area / roi_area) < MIN_TEXT_BOX_COVERAGE:
        return None

    final_crop = image[y1:y2, x1:x2]
    ocr_result = extract_text(final_crop, fast_mode=fast_mode)
    text = ocr_result.text
    if not text.strip():
        return None

    area_ratio = ((x2 - x1) * (y2 - y1)) / float(image.shape[0] * image.shape[1])
    width_ratio = (x2 - x1) / float(image.shape[1])
    height_ratio = (y2 - y1) / float(image.shape[0])
    confidence = round(sum(confidences) / len(confidences), 4)
    if confidence < MIN_TEXT_CONFIDENCE:
        return None
    if area_ratio > MAX_TEXT_AREA_RATIO:
        return None
    if width_ratio > MAX_TEXT_WIDTH_RATIO and height_ratio > MAX_TEXT_HEIGHT_RATIO:
        return None
    if height_ratio < MIN_TEXT_HEIGHT_RATIO_FOR_LOW_CONFIDENCE and confidence < 0.60:
        return None
    if not _looks_like_fallback_text(text) and confidence < 0.55:
        return None

    return DetectionCandidate(
        region_type="text",
        contour_points=((x1, y1), (x2, y1), (x2, y2), (x1, y2)),
        box_points=((x1, y1), (x2, y1), (x2, y2), (x1, y2)),
        center_x=float((x1 + x2) / 2),
        center_y=float((y1 + y2) / 2),
        width=float(x2 - x1),
        height=float(y2 - y1),
        angle=0.0,
        area_ratio=area_ratio,
        rectangularity=1.0,
        confidence=confidence,
        ocr_text=text.strip(),
        ocr_confidence=max(confidence, ocr_result.confidence),
        ocr_engine=ocr_result.engine,
    )


def _text_search_rois(
    image_width: int,
    image_height: int,
    photo_candidates: list[DetectionCandidate],
) -> list[tuple[int, int, int, int]]:
    if not photo_candidates:
        return [(0, 0, image_width, image_height)]

    min_x = min(_bounds(candidate.box_points)[0] for candidate in photo_candidates)
    min_y = min(_bounds(candidate.box_points)[1] for candidate in photo_candidates)
    max_x = max(_bounds(candidate.box_points)[2] for candidate in photo_candidates)
    max_y = max(_bounds(candidate.box_points)[3] for candidate in photo_candidates)

    rois: list[tuple[int, int, int, int]] = []
    if min_y > image_height * 0.08:
        rois.append((0, 0, image_width, min_y))
    if max_y < image_height * 0.95:
        rois.append((0, max_y, image_width, image_height))
    if min_x > image_width * 0.08:
        rois.append((0, min_y, min_x, max_y))
    if max_x < image_width * 0.95:
        rois.append((max_x, min_y, image_width, max_y))
    return [roi for roi in rois if (roi[2] - roi[0]) > 100 and (roi[3] - roi[1]) > 100]


def _detect_photo_candidates_by_intensity(image: np.ndarray) -> list[DetectionCandidate]:
    image_height, image_width = image.shape[:2]
    image_area = float(image_width * image_height)

    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    blurred = cv2.GaussianBlur(gray, (5, 5), 0)
    _, mask = cv2.threshold(blurred, INTENSITY_MASK_THRESHOLD, 255, cv2.THRESH_BINARY_INV)
    kernel = np.ones((9, 9), dtype=np.uint8)
    cleaned = cv2.morphologyEx(mask, cv2.MORPH_OPEN, kernel, iterations=1)
    cleaned = cv2.morphologyEx(cleaned, cv2.MORPH_CLOSE, kernel, iterations=3)

    contours, _ = cv2.findContours(cleaned, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    candidates: list[DetectionCandidate] = []
    for contour in contours:
        contour_area = float(cv2.contourArea(contour))
        if contour_area <= 0:
            continue
        area_ratio = contour_area / image_area
        if area_ratio < MIN_AREA_RATIO or area_ratio > MAX_AREA_RATIO:
            continue

        x, y, width, height = cv2.boundingRect(contour)
        if width <= 1 or height <= 1:
            continue
        rect_area = float(width * height)
        rectangularity = contour_area / rect_area if rect_area > 0 else 0.0
        if rectangularity < 0.60:
            continue

        aspect_ratio = max(width, height) / min(width, height)
        if aspect_ratio < MIN_ASPECT_RATIO or aspect_ratio > MAX_ASPECT_RATIO:
            continue

        confidence = _score_photo_candidate(area_ratio=area_ratio, rectangularity=max(rectangularity, 0.72))
        if confidence < MIN_PHOTO_CONFIDENCE:
            continue

        candidates.append(
            DetectionCandidate(
                region_type="photo",
                contour_points=((x, y), (x + width, y), (x + width, y + height), (x, y + height)),
                box_points=((x, y), (x + width, y), (x + width, y + height), (x, y + height)),
                center_x=float(x + (width / 2.0)),
                center_y=float(y + (height / 2.0)),
                width=float(width),
                height=float(height),
                angle=0.0,
                area_ratio=area_ratio,
                rectangularity=rectangularity,
                confidence=confidence,
            )
        )

    candidates.sort(key=lambda item: item.confidence, reverse=True)
    return candidates


def _subdivide_large_photo_candidates(
    image: np.ndarray,
    candidates: list[DetectionCandidate],
) -> list[DetectionCandidate]:
    refined: list[DetectionCandidate] = []
    for candidate in candidates:
        components = _find_sub_photo_components(image, candidate)
        if len(components) >= 2:
            refined.extend(components)
            continue
        refined.append(candidate)

    refined.sort(key=lambda item: item.confidence, reverse=True)
    return _suppress_overlaps(refined)


def _find_sub_photo_components(
    image: np.ndarray,
    candidate: DetectionCandidate,
) -> list[DetectionCandidate]:
    x1, y1, x2, y2 = _bounds(candidate.box_points)
    crop = image[max(0, y1):min(image.shape[0], y2), max(0, x1):min(image.shape[1], x2)]
    if crop.size == 0:
        return []

    gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
    blurred = cv2.GaussianBlur(gray, (5, 5), 0)
    edges = cv2.Canny(blurred, 40, 120)
    kernel = np.ones((5, 5), dtype=np.uint8)
    merged = cv2.dilate(edges, kernel, iterations=2)
    merged = cv2.morphologyEx(merged, cv2.MORPH_CLOSE, kernel, iterations=2)
    contours, _ = cv2.findContours(merged, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    crop_area = float(crop.shape[0] * crop.shape[1])
    components: list[DetectionCandidate] = []
    for contour in contours:
        contour_area = float(cv2.contourArea(contour))
        if contour_area / crop_area < SUBDIVISION_MIN_COMPONENT_AREA_RATIO:
            continue

        rect = cv2.minAreaRect(contour)
        (center_x, center_y), (width, height), angle = rect
        if width <= 1 or height <= 1:
            continue
        rect_area = float(width * height)
        rectangularity = contour_area / rect_area if rect_area > 0 else 0.0
        if rectangularity < MIN_RECTANGULARITY:
            continue

        aspect_ratio = max(width, height) / min(width, height)
        if aspect_ratio < MIN_ASPECT_RATIO or aspect_ratio > MAX_ASPECT_RATIO:
            continue

        box_points = cv2.boxPoints(rect)
        shifted_box = tuple((int(point[0]) + x1, int(point[1]) + y1) for point in box_points)
        shifted_contour = tuple((int(point[0][0]) + x1, int(point[0][1]) + y1) for point in contour)
        area_ratio = contour_area / float(image.shape[0] * image.shape[1])
        confidence = _score_photo_candidate(area_ratio=area_ratio, rectangularity=rectangularity)
        if confidence < MIN_PHOTO_CONFIDENCE:
            continue

        components.append(
            DetectionCandidate(
                region_type="photo",
                contour_points=shifted_contour,
                box_points=shifted_box,
                center_x=float(center_x + x1),
                center_y=float(center_y + y1),
                width=float(width),
                height=float(height),
                angle=float(angle),
                area_ratio=area_ratio,
                rectangularity=rectangularity,
                confidence=confidence,
            )
        )

    components.sort(key=lambda item: item.confidence, reverse=True)
    kept = _suppress_overlaps(components)
    if len(kept) <= 1:
        return []
    return kept


def _can_use_full_sheet_text_fallback(image: np.ndarray) -> bool:
    photo_like_components = _detect_photo_candidates(image)
    return len(photo_like_components) == 0

def _looks_like_useful_text(text: str) -> bool:
    cleaned = " ".join(text.split())
    if len(cleaned) < 12:
        return False
    alpha_chars = sum(character.isalpha() for character in cleaned)
    return alpha_chars >= max(8, int(len(cleaned) * 0.35))


def _looks_like_fallback_text(text: str) -> bool:
    cleaned = " ".join(text.split())
    if not cleaned:
        return False

    alnum_chars = sum(character.isalnum() for character in cleaned)
    alpha_chars = sum(character.isalpha() for character in cleaned)
    digit_chars = sum(character.isdigit() for character in cleaned)

    if _looks_like_useful_text(cleaned):
        return True
    if len(cleaned) >= 8 and alnum_chars >= max(5, int(len(cleaned) * 0.45)):
        return True
    if len(cleaned) >= 6 and alpha_chars >= 4:
        return True
    return digit_chars >= 4 and alpha_chars >= 2


def _score_photo_candidate(area_ratio: float, rectangularity: float) -> float:
    area_score = min(area_ratio / 0.10, 1.0)
    rectangularity_score = min(max((rectangularity - 0.70) / 0.30, 0.0), 1.0)
    return round((0.45 * area_score) + (0.55 * rectangularity_score), 4)


def _crop_similarity(left: np.ndarray, right: np.ndarray) -> float:
    left_gray = cv2.cvtColor(left, cv2.COLOR_BGR2GRAY)
    right_gray = cv2.cvtColor(right, cv2.COLOR_BGR2GRAY)
    target_size = (64, 64)
    left_resized = cv2.resize(left_gray, target_size, interpolation=cv2.INTER_AREA)
    right_resized = cv2.resize(right_gray, target_size, interpolation=cv2.INTER_AREA)
    left_normalized = cv2.equalizeHist(left_resized)
    right_normalized = cv2.equalizeHist(right_resized)
    difference = cv2.absdiff(left_normalized, right_normalized)
    mean_difference = float(np.mean(difference))
    return max(0.0, 1.0 - (mean_difference / 255.0))


def _suppress_overlaps(candidates: list[DetectionCandidate]) -> list[DetectionCandidate]:
    kept: list[DetectionCandidate] = []
    for candidate in candidates:
        if any(_box_iou(candidate, existing) > MAX_IOU for existing in kept):
            continue
        kept.append(candidate)
    return kept


def _overlaps_existing(candidate: DetectionCandidate, others: list[DetectionCandidate]) -> bool:
    return any(_box_iou(candidate, other) > 0.20 for other in others)


def _box_iou(left: DetectionCandidate, right: DetectionCandidate) -> float:
    left_bounds = _bounds(left.box_points)
    right_bounds = _bounds(right.box_points)

    inter_x1 = max(left_bounds[0], right_bounds[0])
    inter_y1 = max(left_bounds[1], right_bounds[1])
    inter_x2 = min(left_bounds[2], right_bounds[2])
    inter_y2 = min(left_bounds[3], right_bounds[3])

    if inter_x2 <= inter_x1 or inter_y2 <= inter_y1:
        return 0.0

    intersection = float((inter_x2 - inter_x1) * (inter_y2 - inter_y1))
    left_area = float((left_bounds[2] - left_bounds[0]) * (left_bounds[3] - left_bounds[1]))
    right_area = float((right_bounds[2] - right_bounds[0]) * (right_bounds[3] - right_bounds[1]))
    union = left_area + right_area - intersection
    if union <= 0:
        return 0.0
    return intersection / union


def _bounds(points: tuple[tuple[int, int], ...]) -> tuple[int, int, int, int]:
    xs = [point[0] for point in points]
    ys = [point[1] for point in points]
    return min(xs), min(ys), max(xs), max(ys)
