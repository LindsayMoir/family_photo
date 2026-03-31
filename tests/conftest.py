from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from config import AppConfig
from detection.models import DetectionCandidate, SheetScanRecord


@pytest.fixture
def app_config(tmp_path: Path) -> AppConfig:
    photos_root = tmp_path / "photos"
    photos_root.mkdir()
    return AppConfig(
        environment="test",
        database_url="postgresql://localhost:5432/photo_db_test",
        photos_root=photos_root,
        log_level="INFO",
    )


@pytest.fixture
def sample_same_person_embeddings() -> np.ndarray:
    return np.array(
        [
            [0.10, 0.20, 0.30],
            [0.11, 0.19, 0.31],
        ],
        dtype=np.float32,
    )


@pytest.fixture
def sample_different_person_embeddings() -> np.ndarray:
    return np.array(
        [
            [0.10, 0.20, 0.30],
            [0.90, 0.80, 0.70],
        ],
        dtype=np.float32,
    )


@pytest.fixture
def sample_sheet_scan(tmp_path: Path) -> SheetScanRecord:
    image_path = tmp_path / "sheet.jpg"
    image_path.write_bytes(b"fixture")
    return SheetScanRecord(
        id=101,
        batch_name="batch-a",
        original_path=image_path,
        width_px=1000,
        height_px=800,
    )


@pytest.fixture
def photo_detection_candidate() -> DetectionCandidate:
    return DetectionCandidate(
        region_type="photo",
        contour_points=((0, 0), (120, 0), (120, 80), (0, 80)),
        box_points=((0, 0), (120, 0), (120, 80), (0, 80)),
        center_x=60.0,
        center_y=40.0,
        width=120.0,
        height=80.0,
        angle=0.0,
        area_ratio=0.10,
        rectangularity=0.95,
        confidence=0.90,
    )
