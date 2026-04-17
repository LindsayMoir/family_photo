from __future__ import annotations

from deskew.service import _normalize_line_angle, _select_dominant_deskew_angle


def test_normalize_line_angle_maps_into_expected_range() -> None:
    assert _normalize_line_angle(0.0) == 0.0
    assert _normalize_line_angle(91.0) == -89.0
    assert _normalize_line_angle(-95.0) == 85.0


def test_select_dominant_deskew_angle_prefers_strong_nonzero_cluster_over_zero() -> None:
    angles = [
        0.0,
        0.1,
        -0.1,
        -5.2,
        -5.1,
        -4.9,
        -5.0,
        1.2,
    ]
    weights = [
        100.0,
        90.0,
        80.0,
        130.0,
        140.0,
        120.0,
        110.0,
        40.0,
    ]

    angle = _select_dominant_deskew_angle(angles, weights)

    assert round(angle, 1) == -5.1


def test_select_dominant_deskew_angle_returns_zero_when_zero_cluster_is_clearly_strongest() -> None:
    angles = [0.0, 0.1, -0.1, 0.2, -0.2, 4.8, 5.0]
    weights = [200.0, 180.0, 170.0, 150.0, 140.0, 90.0, 80.0]

    angle = _select_dominant_deskew_angle(angles, weights)

    assert round(angle, 2) == 0.0
