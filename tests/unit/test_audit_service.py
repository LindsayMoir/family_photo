from __future__ import annotations

import csv
from pathlib import Path

from audit.models import ExportAuditFinding
from audit.service import (
    _classify_record,
    _default_manual_fields_for_finding,
    _photo_id_from_staging_filename,
    _reconcile_staging_export_artifacts,
    _resolve_orientation_decisions,
    _should_query_model_for_record,
    _should_auto_prefill_issue,
    _write_audit_csv,
)
from config import AppConfig


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
        suggested_issue=None,
        suggested_issue_confidence=None,
        suggested_issue_reason=None,
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
        suggested_issue=None,
        suggested_issue_confidence=None,
        suggested_issue_reason=None,
    ),
    ]

    _write_audit_csv(csv_path, findings)

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    photo_rows = [row for row in rows if row["row_type"] == "photo"]
    assert [row["export_filename"] for row in photo_rows] == ["photo_100.jpg", "photo_200.jpg"]


def test_photo_id_from_staging_filename_parses_expected_pattern() -> None:
    assert _photo_id_from_staging_filename("photo_123.jpg") == 123
    assert _photo_id_from_staging_filename("photo_123.jpeg") is None
    assert _photo_id_from_staging_filename("notes.txt") is None


def test_reconcile_staging_export_artifacts_inserts_missing_disk_exports(app_config, monkeypatch) -> None:
    staging_path = app_config.photos_root / "exports" / "staging" / "landscape" / "photo_123.jpg"
    staging_path.parent.mkdir(parents=True, exist_ok=True)
    staging_path.write_bytes(b"jpg")

    inserted: list[tuple[int, str]] = []
    deleted: list[str] = []

    monkeypatch.setattr("audit.service.list_photo_ids", lambda *args, **kwargs: [123])
    monkeypatch.setattr("audit.service.list_photo_artifact_paths", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        "audit.service.insert_photo_artifact",
        lambda *args, **kwargs: inserted.append((kwargs["photo_id"], str(kwargs["path"]))),
    )
    monkeypatch.setattr(
        "audit.service.delete_photo_artifact",
        lambda *args, **kwargs: deleted.append(str(kwargs["path"])),
    )

    _reconcile_staging_export_artifacts(
        object(),
        app_config,
        batch_name=None,
        sheet_id=None,
        photo_id=None,
        limit=None,
    )

    assert inserted == [(123, str(staging_path))]
    assert deleted == []


def test_reconcile_staging_export_artifacts_replaces_stale_paths(app_config, monkeypatch) -> None:
    current_path = app_config.photos_root / "exports" / "staging" / "portrait" / "photo_456.jpg"
    current_path.parent.mkdir(parents=True, exist_ok=True)
    current_path.write_bytes(b"jpg")
    stale_path = app_config.photos_root / "exports" / "staging" / "landscape" / "photo_456.jpg"

    inserted: list[str] = []
    deleted: list[str] = []

    monkeypatch.setattr("audit.service.list_photo_ids", lambda *args, **kwargs: [456])
    monkeypatch.setattr("audit.service.list_photo_artifact_paths", lambda *args, **kwargs: [stale_path, current_path])
    monkeypatch.setattr(
        "audit.service.insert_photo_artifact",
        lambda *args, **kwargs: inserted.append(str(kwargs["path"])),
    )
    monkeypatch.setattr(
        "audit.service.delete_photo_artifact",
        lambda *args, **kwargs: deleted.append(str(kwargs["path"])),
    )

    _reconcile_staging_export_artifacts(
        object(),
        app_config,
        batch_name=None,
        sheet_id=None,
        photo_id=None,
        limit=None,
    )

    assert inserted == [str(current_path)]
    assert deleted == [str(stale_path)]


def test_reconcile_staging_export_artifacts_removes_missing_file_artifacts(app_config, monkeypatch) -> None:
    missing_path = app_config.photos_root / "exports" / "staging" / "landscape" / "photo_789.jpg"

    inserted: list[str] = []
    deleted: list[str] = []
    updated: list[tuple[int, str, str]] = []

    monkeypatch.setattr("audit.service.list_photo_ids", lambda *args, **kwargs: [789])
    monkeypatch.setattr("audit.service.list_photo_artifact_paths", lambda *args, **kwargs: [missing_path])
    monkeypatch.setattr(
        "audit.service.insert_photo_artifact",
        lambda *args, **kwargs: inserted.append(str(kwargs["path"])),
    )
    monkeypatch.setattr(
        "audit.service.delete_photo_artifact",
        lambda *args, **kwargs: deleted.append(str(kwargs["path"])),
    )
    monkeypatch.setattr(
        "audit.service.update_photo_export_disposition",
        lambda conn, photo_id, disposition, note: updated.append((photo_id, disposition, note)),
    )

    _reconcile_staging_export_artifacts(
        object(),
        app_config,
        batch_name=None,
        sheet_id=None,
        photo_id=None,
        limit=None,
    )

    assert inserted == []
    assert updated == [
        (789, "exclude_reject", "Manually deleted from staging before audit reconciliation")
    ]
    assert deleted == [str(missing_path)]


def test_default_manual_fields_prefill_only_for_high_confidence_r180() -> None:
    finding = ExportAuditFinding(
        photo_id=10,
        batch_name="batch-a",
        sheet_scan_id=20,
        crop_index=1,
        category="rotation",
        reason="orientation audit suggestion",
        export_path=Path("/tmp/photo_10.jpg"),
        auto_rotation_suggestion=180,
        auto_rotation_confidence=0.95,
        review_priority="high",
        suggested_issue="R180",
        suggested_issue_confidence=0.95,
        suggested_issue_reason="orientation audit suggestion",
    )

    assert _default_manual_fields_for_finding(finding) == {
        "needs_help": "x",
        "issue": "R180",
        "notes": "",
    }


def test_write_audit_csv_resets_operator_fields_on_refresh(tmp_path) -> None:
    csv_path = tmp_path / "export_audit.csv"
    csv_path.write_text(
        "\n".join(
            [
                "row_type,export_folder,export_filename,needs_help,issue,notes,review_priority,auto_rotation_suggestion,auto_rotation_confidence,audit_category,audit_reason,export_path,photo_id,batch_name,sheet_scan_id,crop_index",
                "photo,landscape,photo_10.jpg,yes,MERGE,old note,high,0,0.00,ok,,/tmp/photo_10.jpg,10,batch-a,20,1",
            ]
        ),
        encoding="utf-8",
    )
    findings = [
        ExportAuditFinding(
            photo_id=10,
            batch_name="batch-a",
            sheet_scan_id=20,
            crop_index=1,
            category="rotation",
            reason="orientation audit suggestion",
            export_path=tmp_path / "staging" / "photo_10.jpg",
            auto_rotation_suggestion=180,
            auto_rotation_confidence=0.95,
            review_priority="high",
            suggested_issue="R180",
            suggested_issue_confidence=0.95,
            suggested_issue_reason="orientation audit suggestion",
        )
    ]

    _write_audit_csv(csv_path, findings)

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        row = next(row for row in csv.DictReader(handle) if row["row_type"] == "photo")

    assert row["needs_help"] == "x"
    assert row["issue"] == "R180"
    assert row["notes"] == ""


def test_should_auto_prefill_issue_rejects_merge_suggestions() -> None:
    finding = ExportAuditFinding(
        photo_id=10,
        batch_name="batch-a",
        sheet_scan_id=20,
        crop_index=1,
        category="merged_detection",
        reason="vision audit suggests MERGE",
        export_path=Path("/tmp/photo_10.jpg"),
        auto_rotation_suggestion=0,
        auto_rotation_confidence=0.0,
        review_priority="high",
        suggested_issue="MERGE",
        suggested_issue_confidence=0.99,
        suggested_issue_reason="vision audit suggests MERGE",
    )

    assert _should_auto_prefill_issue(finding) is False
    assert _default_manual_fields_for_finding(finding) == {
        "needs_help": "",
        "issue": "",
        "notes": "",
    }


def test_write_audit_csv_clears_stale_manual_fields_after_refresh(tmp_path) -> None:
    csv_path = tmp_path / "export_audit.csv"
    csv_path.write_text(
        "\n".join(
            [
                "row_type,export_folder,export_filename,needs_help,issue,notes,review_priority,auto_rotation_suggestion,auto_rotation_confidence,audit_category,audit_reason,export_path,photo_id,batch_name,sheet_scan_id,crop_index",
                "photo,landscape,photo_10.jpg,x,MERGE,stale flag,high,0,0.00,ok,,/tmp/photo_10.jpg,10,batch-a,20,1",
            ]
        ),
        encoding="utf-8",
    )
    findings = [
        ExportAuditFinding(
            photo_id=10,
            batch_name="batch-a",
            sheet_scan_id=20,
            crop_index=1,
            category="ok",
            reason="no audit issue detected",
            export_path=tmp_path / "staging" / "photo_10.jpg",
            auto_rotation_suggestion=0,
            auto_rotation_confidence=0.10,
            review_priority="low",
            suggested_issue=None,
            suggested_issue_confidence=None,
            suggested_issue_reason=None,
        )
    ]

    _write_audit_csv(csv_path, findings)

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        row = next(row for row in csv.DictReader(handle) if row["row_type"] == "photo")

    assert row["needs_help"] == ""
    assert row["issue"] == ""
    assert row["notes"] == ""


def test_write_audit_csv_preserves_open_review_task_manual_fields(
    tmp_path,
    app_config,
    monkeypatch,
) -> None:
    csv_path = tmp_path / "export_audit.csv"
    findings = [
        ExportAuditFinding(
            photo_id=951,
            batch_name="batch-a",
            sheet_scan_id=545,
            crop_index=1,
            category="ok",
            reason="no audit issue detected",
            export_path=tmp_path / "staging" / "photo_951.jpg",
            auto_rotation_suggestion=0,
            auto_rotation_confidence=0.25,
            review_priority="low",
            suggested_issue=None,
            suggested_issue_confidence=None,
            suggested_issue_reason=None,
        )
    ]
    monkeypatch.setattr(
        "audit.service._open_export_audit_manual_fields",
        lambda config: {
            "951": {
                "needs_help": "yes",
                "issue": "MERGE",
                "notes": "manual judgment wins",
            }
        },
    )

    _write_audit_csv(csv_path, findings, config=app_config)

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        row = next(row for row in csv.DictReader(handle) if row["row_type"] == "photo")

    assert row["needs_help"] == "yes"
    assert row["issue"] == "MERGE"
    assert row["notes"] == "manual judgment wins"


def test_classify_record_uses_model_merge_decision(monkeypatch, tmp_path) -> None:
    raw_crop_path = tmp_path / "crop.jpg"
    raw_crop_path.write_bytes(b"placeholder")
    export_path = tmp_path / "photo_1.jpg"
    export_path.write_bytes(b"placeholder")
    record = type(
        "Record",
        (),
        {
            "photo_id": 1,
            "sheet_scan_id": 2,
            "crop_index": 1,
            "raw_crop_path": raw_crop_path,
            "working_path": raw_crop_path,
            "export_path": export_path,
            "has_open_orientation_review": False,
            "orientation_review_reason": None,
            "accepted_detection_id": 10,
            "detection_reviewed_by_human": False,
            "detection_confidence": 0.95,
            "detection_width": 200.0,
            "detection_height": 100.0,
        },
    )()
    app_config = AppConfig(
        environment="test",
        database_url="postgresql://localhost:5432/photo_db_test",
        photos_root=tmp_path,
        log_level="INFO",
    )

    monkeypatch.setattr("audit.service.cv2.imread", lambda path: __import__("numpy").zeros((100, 200, 3), dtype="uint8"))

    category, reason = _classify_record(
        record,
        [],
        type("Decision", (), {"rotation_degrees": 0, "confidence": 0.0})(),
    )

    assert category == "ok"
    assert reason == "no audit issue detected"


def test_should_query_model_for_record_only_targets_strong_r180() -> None:
    assert _should_query_model_for_record(
        category="rotation",
        orientation_decision=type("Decision", (), {"rotation_degrees": 180, "confidence": 0.95})(),
    ) is True
    assert _should_query_model_for_record(
        category="rotation",
        orientation_decision=type("Decision", (), {"rotation_degrees": 90, "confidence": 0.95})(),
    ) is False
    assert _should_query_model_for_record(
        category="ok",
        orientation_decision=type("Decision", (), {"rotation_degrees": 180, "confidence": 0.95})(),
    ) is False


def test_resolve_orientation_decisions_maps_photo_ids(monkeypatch, tmp_path) -> None:
    record_a = type("Record", (), {"photo_id": 10, "working_path": tmp_path / "a.jpg"})()
    record_b = type("Record", (), {"photo_id": 20, "working_path": tmp_path / "b.jpg"})()

    monkeypatch.setattr(
        "audit.service.audit_orientation_image",
        lambda path: f"decision:{path.name}",
    )

    decisions = _resolve_orientation_decisions([record_a, record_b])

    assert decisions == {
        10: "decision:a.jpg",
        20: "decision:b.jpg",
    }
