from __future__ import annotations

from pathlib import Path

import pytest

from crop.service import _canonical_raw_crop_path, _copy_crop_file


def test_canonical_raw_crop_path_builds_expected_location(tmp_path: Path) -> None:
    output_path = _canonical_raw_crop_path(tmp_path, sheet_id=12, crop_index=3)

    assert output_path == tmp_path / "crops" / "sheet_12" / "crop_3.jpg"


def test_copy_crop_file_raises_for_missing_source(tmp_path: Path) -> None:
    source_path = tmp_path / "missing.jpg"
    destination_path = tmp_path / "out" / "crop.jpg"

    with pytest.raises(ValueError, match="Crop source file does not exist"):
        _copy_crop_file(source_path, destination_path)


def test_copy_crop_file_creates_parent_and_copies_bytes(tmp_path: Path) -> None:
    source_path = tmp_path / "source.jpg"
    destination_path = tmp_path / "nested" / "crop.jpg"
    source_path.write_bytes(b"image-bytes")

    _copy_crop_file(source_path, destination_path)

    assert destination_path.read_bytes() == b"image-bytes"
