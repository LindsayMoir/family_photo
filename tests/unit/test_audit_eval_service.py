from __future__ import annotations

from audit.eval_service import _select_trial_rows


def test_select_trial_rows_keeps_only_supported_single_issue_rows() -> None:
    rows = [
        {"export_filename": "photo_003.jpg", "issue": "DELETE"},
        {"export_filename": "photo_002.jpg", "issue": "MERGE"},
        {"export_filename": "photo_001.jpg", "issue": "R180"},
        {"export_filename": "photo_004.jpg", "issue": "R180, CROP"},
        {"export_filename": "photo_005.jpg", "issue": "RR90"},
        {"export_filename": "photo_006.jpg", "issue": "CROP"},
        {"export_filename": "photo_007.jpg", "issue": "SKEW, R180"},
    ]

    selected = _select_trial_rows(rows, limit=10)

    assert [row["export_filename"] for row in selected] == [
        "photo_001.jpg",
        "photo_002.jpg",
        "photo_003.jpg",
        "photo_004.jpg",
        "photo_005.jpg",
        "photo_006.jpg",
        "photo_007.jpg",
    ]
