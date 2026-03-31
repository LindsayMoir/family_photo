from __future__ import annotations

import csv

from audit.models import ExportAuditFinding
from audit.service import _write_audit_csv


def test_write_audit_csv_sorts_photo_rows_by_export_filename(tmp_path) -> None:
    csv_path = tmp_path / "export_audit.csv"
    findings = [
        ExportAuditFinding(
            photo_id=2,
            batch_name="batch-a",
            sheet_scan_id=11,
            crop_index=2,
            category="ok",
            reason="second",
            export_path=tmp_path / "staging" / "photo_200.jpg",
            auto_rotation_suggestion=0,
            auto_rotation_confidence=0.25,
            review_priority="low",
        ),
        ExportAuditFinding(
            photo_id=1,
            batch_name="batch-a",
            sheet_scan_id=10,
            crop_index=1,
            category="ok",
            reason="first",
            export_path=tmp_path / "staging" / "photo_100.jpg",
            auto_rotation_suggestion=0,
            auto_rotation_confidence=0.50,
            review_priority="low",
        ),
    ]

    _write_audit_csv(csv_path, findings)

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    photo_rows = [row for row in rows if row["row_type"] == "photo"]
    assert [row["export_filename"] for row in photo_rows] == ["photo_100.jpg", "photo_200.jpg"]
