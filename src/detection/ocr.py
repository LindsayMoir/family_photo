"""OCR helpers with a handwriting-aware strategy seam."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import os
from typing import Any

import cv2
import numpy as np
from PIL import Image
import pytesseract


PRINTED_ENGINE = "tesseract_printed_v1"
HANDWRITING_ENGINE = "tesseract_handwriting_stub_v1"
MIXED_ENGINE = "tesseract_mixed_v1"
TROCR_ENGINE = "trocr_handwritten_v1"
DEFAULT_TROCR_MODEL = "microsoft/trocr-base-handwritten"
MAX_TROCR_DIMENSION = 1200
MIN_LINE_HEIGHT = 24
MIN_LINE_WIDTH_RATIO = 0.15
MAX_LINE_COUNT = 24

LOGGER = logging.getLogger(__name__)
_TROCR_BACKEND: "_TrOcrBackend | None | bool" = None


@dataclass(frozen=True, slots=True)
class OcrResult:
    """OCR output for a text crop."""

    text: str
    confidence: float
    engine: str
    text_style: str


@dataclass(slots=True)
class _TrOcrBackend:
    """Lazy-loaded TrOCR backend."""

    processor: Any
    model: Any
    device: Any


def extract_text(crop: np.ndarray, *, fast_mode: bool = False) -> OcrResult:
    """Run OCR using a text-style aware strategy."""
    text_style = classify_text_style(crop)
    if fast_mode:
        result = _run_printed_ocr(crop)
        return OcrResult(
            text=result.text,
            confidence=result.confidence,
            engine=f"{result.engine}_fast",
            text_style=text_style,
        )
    if text_style == "handwritten":
        return _best_result(
            text_style=text_style,
            results=(
                _run_trocr_handwriting_ocr(crop),
                _run_handwriting_ocr(crop),
                _run_printed_ocr(crop),
            ),
        )
    if text_style == "mixed":
        return _best_result(
            text_style=text_style,
            results=(
                _run_trocr_handwriting_ocr(crop),
                _run_handwriting_ocr(crop),
                _run_printed_ocr(crop),
            ),
        )
    return _best_result(
        text_style=text_style,
        results=(
            _run_printed_ocr(crop),
            _run_handwriting_ocr(crop),
        ),
    )


def classify_text_style(crop: np.ndarray) -> str:
    """Classify text as printed, handwritten, mixed, or unknown using image heuristics."""
    gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
    blurred = cv2.GaussianBlur(gray, (3, 3), 0)
    _, inverted = cv2.threshold(blurred, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)

    component_count, _, stats, _ = cv2.connectedComponentsWithStats(inverted, connectivity=8)
    component_widths: list[float] = []
    component_heights: list[float] = []
    wide_component_count = 0

    for index in range(1, component_count):
        width = int(stats[index, cv2.CC_STAT_WIDTH])
        height = int(stats[index, cv2.CC_STAT_HEIGHT])
        area = int(stats[index, cv2.CC_STAT_AREA])
        if width <= 1 or height <= 1 or area < 20:
            continue
        component_widths.append(float(width))
        component_heights.append(float(height))
        if width >= max(24, int(height * 2.3)):
            wide_component_count += 1

    if not component_widths or not component_heights:
        return "unknown"

    avg_width = sum(component_widths) / len(component_widths)
    avg_height = sum(component_heights) / len(component_heights)
    wide_ratio = wide_component_count / len(component_widths)

    if wide_ratio >= 0.20 and avg_width >= avg_height * 1.4:
        return "handwritten"
    if wide_ratio >= 0.10:
        return "mixed"
    return "printed"


def _run_printed_ocr(crop: np.ndarray) -> OcrResult:
    gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
    scaled = cv2.resize(gray, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
    normalized = cv2.GaussianBlur(scaled, (3, 3), 0)
    _, thresholded = cv2.threshold(normalized, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    text = pytesseract.image_to_string(thresholded, config="--oem 3 --psm 6").strip()
    return OcrResult(
        text=text,
        confidence=_score_text_quality(text),
        engine=PRINTED_ENGINE,
        text_style="printed",
    )


def _run_handwriting_ocr(crop: np.ndarray) -> OcrResult:
    gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
    clahe = cv2.createCLAHE(clipLimit=4.0, tileGridSize=(8, 8)).apply(gray)
    handwriting_scaled = cv2.resize(clahe, None, fx=2.5, fy=2.5, interpolation=cv2.INTER_CUBIC)
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (25, 25))
    blackhat = cv2.morphologyEx(handwriting_scaled, cv2.MORPH_BLACKHAT, kernel)
    handwriting_norm = cv2.normalize(blackhat, None, 0, 255, cv2.NORM_MINMAX)
    _, thresholded = cv2.threshold(
        handwriting_norm,
        0,
        255,
        cv2.THRESH_BINARY + cv2.THRESH_OTSU,
    )
    text = pytesseract.image_to_string(thresholded, config="--oem 3 --psm 6").strip()
    return OcrResult(
        text=text,
        confidence=_score_text_quality(text),
        engine=HANDWRITING_ENGINE,
        text_style="handwritten",
    )


def _run_trocr_handwriting_ocr(crop: np.ndarray) -> OcrResult:
    backend = _get_trocr_backend()
    if backend is None:
        return OcrResult(
            text="",
            confidence=0.0,
            engine=TROCR_ENGINE,
            text_style="handwritten",
        )

    line_crops = _segment_handwritten_lines(crop)
    if not line_crops:
        line_crops = [_resize_for_trocr(crop)]

    text_parts: list[str] = []
    confidence_parts: list[float] = []
    for line_crop in line_crops:
        text = _run_trocr_line(backend, line_crop)
        if not text:
            continue
        text_parts.append(text)
        confidence_parts.append(_score_text_quality(text))

    combined_text = "\n".join(text_parts).strip()
    combined_confidence = (
        round(sum(confidence_parts) / len(confidence_parts), 4)
        if confidence_parts
        else 0.0
    )
    return OcrResult(
        text=combined_text,
        confidence=combined_confidence,
        engine=TROCR_ENGINE,
        text_style="handwritten",
    )


def _best_result(text_style: str, results: tuple[OcrResult, ...]) -> OcrResult:
    best = max(results, key=lambda result: result.confidence)
    engine = best.engine if text_style in {"printed", "unknown"} else (
        MIXED_ENGINE if text_style == "mixed" and best.engine == PRINTED_ENGINE else best.engine
    )
    return OcrResult(
        text=best.text,
        confidence=best.confidence,
        engine=engine,
        text_style=text_style,
    )


def _score_text_quality(text: str) -> float:
    cleaned = " ".join(text.split())
    if not cleaned:
        return 0.0

    alpha_chars = sum(character.isalpha() for character in cleaned)
    alnum_chars = sum(character.isalnum() for character in cleaned)
    punctuation_chars = sum(not character.isalnum() and not character.isspace() for character in cleaned)

    length_score = min(len(cleaned) / 60.0, 1.0)
    alpha_score = alpha_chars / len(cleaned)
    alnum_score = alnum_chars / len(cleaned)
    punctuation_penalty = min(punctuation_chars / max(len(cleaned), 1), 0.35)
    return max(0.0, round((0.30 * length_score) + (0.35 * alpha_score) + (0.35 * alnum_score) - punctuation_penalty, 4))


def _get_trocr_backend() -> _TrOcrBackend | None:
    global _TROCR_BACKEND
    if _TROCR_BACKEND is False:
        return None
    if isinstance(_TROCR_BACKEND, _TrOcrBackend):
        return _TROCR_BACKEND

    model_name = os.getenv("FAMILY_PHOTO_TROCR_MODEL", DEFAULT_TROCR_MODEL).strip() or DEFAULT_TROCR_MODEL
    local_only = os.getenv("FAMILY_PHOTO_TROCR_LOCAL_ONLY", "").strip().lower() in {"1", "true", "yes"}

    try:
        processor, model, device = _load_trocr_backend(model_name=model_name, local_only=local_only)
    except Exception as exc:
        if local_only:
            LOGGER.warning("TrOCR backend unavailable for model '%s': %s", model_name, exc)
            _TROCR_BACKEND = False
            return None
        try:
            processor, model, device = _load_trocr_backend(model_name=model_name, local_only=True)
        except Exception as local_exc:
            LOGGER.warning("TrOCR backend unavailable for model '%s': %s", model_name, local_exc)
            _TROCR_BACKEND = False
            return None

    _TROCR_BACKEND = _TrOcrBackend(
        processor=processor,
        model=model,
        device=device,
    )
    return _TROCR_BACKEND


def _load_trocr_backend(model_name: str, *, local_only: bool) -> tuple[Any, Any, Any]:
    import torch
    from transformers import TrOCRProcessor, VisionEncoderDecoderModel

    processor = TrOCRProcessor.from_pretrained(
        model_name,
        local_files_only=local_only,
        use_fast=False,
    )
    model = VisionEncoderDecoderModel.from_pretrained(model_name, local_files_only=local_only)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)
    model.eval()
    return processor, model, device


def _resize_for_trocr(crop: np.ndarray) -> np.ndarray:
    height, width = crop.shape[:2]
    largest_dimension = max(height, width)
    if largest_dimension <= MAX_TROCR_DIMENSION:
        return crop

    scale = MAX_TROCR_DIMENSION / float(largest_dimension)
    return cv2.resize(crop, None, fx=scale, fy=scale, interpolation=cv2.INTER_AREA)


def _segment_handwritten_lines(crop: np.ndarray) -> list[np.ndarray]:
    gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
    clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8, 8)).apply(gray)
    blurred = cv2.GaussianBlur(clahe, (3, 3), 0)
    _, inverted = cv2.threshold(blurred, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)

    width = inverted.shape[1]
    horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (max(15, width // 20), 3))
    connected = cv2.morphologyEx(inverted, cv2.MORPH_CLOSE, horizontal_kernel, iterations=1)

    row_density = np.count_nonzero(connected, axis=1)
    threshold = max(12, int(width * 0.03))
    dense_rows = row_density >= threshold

    lines: list[tuple[int, int]] = []
    start: int | None = None
    for index, is_dense in enumerate(dense_rows):
        if is_dense and start is None:
            start = index
        elif not is_dense and start is not None:
            if index - start >= MIN_LINE_HEIGHT:
                lines.append((start, index))
            start = None
    if start is not None and len(dense_rows) - start >= MIN_LINE_HEIGHT:
        lines.append((start, len(dense_rows)))

    line_crops: list[np.ndarray] = []
    for top, bottom in lines[:MAX_LINE_COUNT]:
        line_mask = connected[top:bottom, :]
        coords = cv2.findNonZero(line_mask)
        if coords is None:
            continue
        x, y, w, h = cv2.boundingRect(coords)
        if w < int(width * MIN_LINE_WIDTH_RATIO) or h < MIN_LINE_HEIGHT:
            continue
        padding_x = max(12, int(w * 0.03))
        padding_y = max(10, int(h * 0.35))
        x1 = max(0, x - padding_x)
        x2 = min(crop.shape[1], x + w + padding_x)
        y1 = max(0, top + y - padding_y)
        y2 = min(crop.shape[0], top + y + h + padding_y)
        line_crop = crop[y1:y2, x1:x2]
        if line_crop.size == 0:
            continue
        line_crops.append(_resize_for_trocr(line_crop))
    return line_crops


def _run_trocr_line(backend: _TrOcrBackend, crop: np.ndarray) -> str:
    rgb_crop = cv2.cvtColor(crop, cv2.COLOR_BGR2RGB)
    pil_image = Image.fromarray(rgb_crop)
    pixel_values = backend.processor(images=pil_image, return_tensors="pt").pixel_values.to(backend.device)

    import torch

    with torch.inference_mode():
        generated_ids = backend.model.generate(pixel_values)
    return backend.processor.batch_decode(generated_ids, skip_special_tokens=True)[0].strip()
