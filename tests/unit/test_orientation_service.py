from __future__ import annotations

import numpy as np
import pytest

from orientation.service import _orientation_output_path, _rotate_image


def test_rotate_image_applies_cardinal_rotation() -> None:
    image = np.arange(18, dtype=np.uint8).reshape(2, 3, 3)

    rotated = _rotate_image(image, 90)

    assert rotated.shape == (3, 2, 3)


def test_rotate_image_rejects_unsupported_rotation() -> None:
    image = np.zeros((2, 2, 3), dtype=np.uint8)

    with pytest.raises(ValueError, match="Unsupported rotation"):
        _rotate_image(image, 45)


def test_orientation_output_path_uses_photo_id(app_config) -> None:
    output_path = _orientation_output_path(app_config.photos_root, photo_id=7)

    assert output_path == app_config.photos_root / "derivatives" / "orient" / "photo_7.jpg"
