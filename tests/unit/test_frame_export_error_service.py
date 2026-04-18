from __future__ import annotations

import csv
from contextlib import contextmanager
from pathlib import Path

import pytest

from audit.models import ExportAuditFinding, ExportAuditSummary
from frame_export.error_service import (
    _closest_profile_for_dimensions,
    _photo_id_from_error_entry,
    _read_error_entries,
    _resolve_error_photo_ids,
    _stage_exact_manual_edits,
    apply_manual_staging_edits,
    requeue_final_exports_for_audit,
    restage_export_errors,
    stage_next_exports_for_audit,
)


def test_photo_id_from_error_entry_accepts_filenames_paths_and_bare_ids() -> None:
    assert _photo_id_from_error_entry("photo_951.jpg") == 951
    assert _photo_id_from_error_entry("frame_1920x1080/photo_951.jpg") == 951
    assert _photo_id_from_error_entry(r"frame_1080x1920\photo_951.jpg") == 951
    assert _photo_id_from_error_entry("951") == 951
    assert _photo_id_from_error_entry("bad_name.jpg") is None


def test_read_error_entries_ignores_comments_and_blank_lines(tmp_path) -> None:
    errors_path = tmp_path / "errors.txt"
    errors_path.write_text("\n# header\nphoto_951.jpg\n\n952\n", encoding="utf-8")

    assert _read_error_entries(errors_path) == ["photo_951.jpg", "952"]


def test_resolve_error_photo_ids_keeps_order_and_reports_missing(app_config) -> None:
    exports_root = app_config.photos_root / "exports"
    (exports_root / "frame_1920x1080").mkdir(parents=True, exist_ok=True)
    (exports_root / "frame_1080x1920").mkdir(parents=True, exist_ok=True)
    (exports_root / "frame_1920x1080" / "photo_951.jpg").write_bytes(b"jpg")
    (exports_root / "frame_1080x1920" / "photo_952.jpg").write_bytes(b"jpg")

    photo_ids, missing = _resolve_error_photo_ids(
        app_config,
        ["photo_951.jpg", "952", "photo_951.jpg", "photo_999.jpg", "bad"],
    )

    assert photo_ids == [951, 952]
    assert missing == ["photo_999.jpg", "bad"]


def test_restage_export_errors_stages_and_writes_audit_csv(app_config, monkeypatch, tmp_path) -> None:
    errors_path = app_config.photos_root / "exports" / "errors.txt"
    errors_path.parent.mkdir(parents=True, exist_ok=True)
    errors_path.write_text("photo_951.jpg\nphoto_952.jpg\n", encoding="utf-8")
    final_landscape = app_config.photos_root / "exports" / "frame_1920x1080" / "photo_951.jpg"
    final_portrait = app_config.photos_root / "exports" / "frame_1080x1920" / "photo_952.jpg"
    final_landscape.parent.mkdir(parents=True, exist_ok=True)
    final_portrait.parent.mkdir(parents=True, exist_ok=True)
    final_landscape.write_bytes(b"jpg")
    final_portrait.write_bytes(b"jpg")

    staged: list[int] = []

    monkeypatch.setattr(
        "frame_export.error_service.stage_photo_exports",
        lambda config, photo_ids, dry_run: staged.extend(photo_ids),
    )
    monkeypatch.setattr(
        "audit.service.run_export_audit",
        lambda config, **kwargs: ExportAuditSummary(
            target=f"photo_id={kwargs['photo_id']}",
            audited_count=1,
            category_counts={"ok": 1},
            findings=[
                ExportAuditFinding(
                    photo_id=kwargs["photo_id"],
                    batch_name="batch-a",
                    sheet_scan_id=10,
                    crop_index=1,
                    category="ok",
                    reason="no audit issue detected",
                    export_path=tmp_path / "staging" / f"photo_{kwargs['photo_id']}.jpg",
                    auto_rotation_suggestion=0,
                    auto_rotation_confidence=0.2,
                    review_priority="low",
                    suggested_issue=None,
                    suggested_issue_confidence=None,
                    suggested_issue_reason=None,
                )
            ],
            csv_path=None,
            dry_run=True,
        ),
    )
    monkeypatch.setattr(
        "audit.service._open_export_audit_manual_fields",
        lambda config: {},
    )

    summary = restage_export_errors(
        app_config,
        errors_path=errors_path,
        csv_path=None,
        dry_run=False,
    )

    assert staged == [951, 952]
    assert summary.requested_count == 2
    assert summary.staged_count == 2
    assert summary.audited_count == 2
    assert summary.missing_entries == ()

    with summary.csv_path.open("r", encoding="utf-8", newline="") as handle:
        rows = [row for row in csv.DictReader(handle) if row["row_type"] == "photo"]
    assert [row["photo_id"] for row in rows] == ["951", "952"]


@contextmanager
def _fake_connect(_config):
    class _FakeConn:
        def commit(self) -> None:
            return None

    yield _FakeConn()


def test_requeue_final_exports_for_audit_moves_portrait_exports_to_staging(
    app_config,
    monkeypatch,
    tmp_path,
) -> None:
    final_dir = app_config.photos_root / "exports" / "frame_1080x1920"
    final_dir.mkdir(parents=True, exist_ok=True)
    (final_dir / "photo_951.jpg").write_bytes(b"one")
    (final_dir / "photo_952.jpg").write_bytes(b"two")
    deleted: list[tuple[int, str, str]] = []
    inserted: list[tuple[int, str, str]] = []

    monkeypatch.setattr("frame_export.error_service.connect", _fake_connect)
    monkeypatch.setattr(
        "frame_export.error_service.list_photo_artifact_paths",
        lambda conn, photo_id, artifact_type: [],
    )
    monkeypatch.setattr(
        "frame_export.error_service.delete_photo_artifact",
        lambda conn, photo_id, artifact_type, path: deleted.append((photo_id, artifact_type, str(path))),
    )
    monkeypatch.setattr(
        "frame_export.error_service.insert_photo_artifact",
        lambda conn, photo_id, artifact_type, path, pipeline_stage, pipeline_version: inserted.append((photo_id, artifact_type, str(path))),
    )
    monkeypatch.setattr(
        "photo_repository.list_export_audit_records",
        lambda conn, **kwargs: [
            type(
                "Record",
                (),
                {
                    "photo_id": 951,
                    "batch_name": "batch-a",
                    "sheet_scan_id": 10,
                    "crop_index": 1,
                    "raw_crop_path": tmp_path / "raw_951.jpg",
                    "working_path": tmp_path / "work_951.jpg",
                    "export_path": tmp_path / "staging" / "photo_951.jpg",
                    "status": "enhancement_complete",
                    "rotation_degrees": 0,
                    "accepted_detection_id": None,
                    "detection_confidence": None,
                    "detection_reviewed_by_human": False,
                    "detection_width": None,
                    "detection_height": None,
                    "has_open_orientation_review": False,
                    "orientation_review_reason": None,
                },
            )(),
            type(
                "Record",
                (),
                {
                    "photo_id": 952,
                    "batch_name": "batch-a",
                    "sheet_scan_id": 10,
                    "crop_index": 2,
                    "raw_crop_path": tmp_path / "raw_952.jpg",
                    "working_path": tmp_path / "work_952.jpg",
                    "export_path": tmp_path / "staging" / "photo_952.jpg",
                    "status": "enhancement_complete",
                    "rotation_degrees": 0,
                    "accepted_detection_id": None,
                    "detection_confidence": None,
                    "detection_reviewed_by_human": False,
                    "detection_width": None,
                    "detection_height": None,
                    "has_open_orientation_review": False,
                    "orientation_review_reason": None,
                },
            )(),
        ],
    )
    monkeypatch.setattr(
        "audit.service._classify_records",
        lambda records, config: [
            ExportAuditFinding(
                photo_id=record.photo_id,
                batch_name="batch-a",
                sheet_scan_id=10,
                crop_index=index,
                category="ok",
                reason="no audit issue detected",
                export_path=record.export_path,
                auto_rotation_suggestion=0,
                auto_rotation_confidence=0.2,
                review_priority="low",
                suggested_issue=None,
                suggested_issue_confidence=None,
                suggested_issue_reason=None,
            )
            for index, record in enumerate(records, start=1)
        ],
    )
    monkeypatch.setattr(
        "audit.service._open_export_audit_manual_fields",
        lambda config: {},
    )

    summary = requeue_final_exports_for_audit(
        app_config,
        source_profile="frame_1080x1920",
        csv_path=None,
        dry_run=False,
    )

    assert summary.queued_count == 2
    assert summary.audited_count == 2
    assert not (final_dir / "photo_951.jpg").exists()
    assert not (final_dir / "photo_952.jpg").exists()
    assert (app_config.photos_root / "exports" / "staging" / "portrait" / "photo_951.jpg").exists()
    assert (app_config.photos_root / "exports" / "staging" / "portrait" / "photo_952.jpg").exists()
    assert deleted == [
        (951, "frame_export", str(final_dir / "photo_951.jpg")),
        (952, "frame_export", str(final_dir / "photo_952.jpg")),
    ]
    assert inserted == [
        (951, "frame_export_staging", str(app_config.photos_root / "exports" / "staging" / "portrait" / "photo_951.jpg")),
        (952, "frame_export_staging", str(app_config.photos_root / "exports" / "staging" / "portrait" / "photo_952.jpg")),
    ]


def test_stage_next_exports_for_audit_reconciles_missing_staging_files_before_selection(
    app_config,
    monkeypatch,
) -> None:
    reconciled: list[tuple[str | None, int | None, int | None, int | None]] = []
    selection_calls: list[dict[str, object]] = []

    monkeypatch.setattr("frame_export.error_service.connect", _fake_connect)
    monkeypatch.setattr(
        "audit.service._reconcile_staging_export_artifacts",
        lambda conn, config, batch_name, sheet_id, photo_id, limit: reconciled.append(
            (batch_name, sheet_id, photo_id, limit)
        ),
    )
    monkeypatch.setattr(
        "frame_export.error_service.list_export_ready_photo_ids",
        lambda conn, **kwargs: selection_calls.append(kwargs) or [951],
    )
    monkeypatch.setattr(
        "frame_export.error_service._clear_unselected_staging_exports",
        lambda config, batch_name, sheet_id, keep_photo_ids: None,
    )
    monkeypatch.setattr(
        "frame_export.error_service.stage_photo_exports",
        lambda config, photo_ids, dry_run: None,
    )
    monkeypatch.setattr(
        "photo_repository.list_export_audit_records",
        lambda conn, **kwargs: [],
    )
    monkeypatch.setattr(
        "audit.service._classify_records",
        lambda records, config: [],
    )
    monkeypatch.setattr(
        "audit.service._open_export_audit_manual_fields",
        lambda config: {},
    )

    summary = stage_next_exports_for_audit(
        app_config,
        batch_name="batch-a",
        sheet_id=None,
        limit=20,
        csv_path=None,
        dry_run=False,
    )

    assert summary.selected_count == 1
    assert reconciled == [("batch-a", None, None, None)]
    assert selection_calls == [
        {
            "batch_name": "batch-a",
            "sheet_id": None,
            "photo_id": None,
            "exclude_final_exported": True,
            "limit": 20,
        }
    ]


def test_stage_next_exports_for_audit_clears_unselected_staging_exports(
    app_config,
    monkeypatch,
) -> None:
    selection_calls: list[dict[str, object]] = []
    cleared: list[tuple[str | None, int | None, set[int]]] = []
    staged: list[int] = []

    monkeypatch.setattr("frame_export.error_service.connect", _fake_connect)
    monkeypatch.setattr(
        "audit.service._reconcile_staging_export_artifacts",
        lambda conn, config, batch_name, sheet_id, photo_id, limit: None,
    )
    monkeypatch.setattr(
        "frame_export.error_service.list_export_ready_photo_ids",
        lambda conn, **kwargs: selection_calls.append(kwargs) or [951, 952],
    )
    monkeypatch.setattr(
        "frame_export.error_service._clear_unselected_staging_exports",
        lambda config, batch_name, sheet_id, keep_photo_ids: cleared.append((batch_name, sheet_id, set(keep_photo_ids))),
    )
    monkeypatch.setattr(
        "frame_export.error_service.stage_photo_exports",
        lambda config, photo_ids, dry_run: staged.extend(photo_ids),
    )
    monkeypatch.setattr(
        "photo_repository.list_export_audit_records",
        lambda conn, **kwargs: [],
    )
    monkeypatch.setattr(
        "audit.service._classify_records",
        lambda records, config: [],
    )
    monkeypatch.setattr(
        "audit.service._open_export_audit_manual_fields",
        lambda config: {},
    )

    summary = stage_next_exports_for_audit(
        app_config,
        batch_name="batch-a",
        sheet_id=None,
        limit=20,
        csv_path=None,
        dry_run=False,
    )

    assert summary.selected_count == 2
    assert cleared == [("batch-a", None, {951, 952})]
    assert staged == [951, 952]
    assert selection_calls == [
        {
            "batch_name": "batch-a",
            "sheet_id": None,
            "photo_id": None,
            "exclude_final_exported": True,
            "limit": 20,
        }
    ]


def test_stage_next_exports_for_audit_stages_selected_export_ready_ids(
    app_config,
    monkeypatch,
    tmp_path,
) -> None:
    selection_calls: list[dict[str, object]] = []
    monkeypatch.setattr("frame_export.error_service.connect", _fake_connect)
    monkeypatch.setattr(
        "audit.service._reconcile_staging_export_artifacts",
        lambda conn, config, batch_name, sheet_id, photo_id, limit: None,
    )
    monkeypatch.setattr(
        "frame_export.error_service.list_export_ready_photo_ids",
        lambda conn, **kwargs: selection_calls.append(kwargs) or [951, 952],
    )
    monkeypatch.setattr(
        "frame_export.error_service._clear_unselected_staging_exports",
        lambda config, batch_name, sheet_id, keep_photo_ids: None,
    )
    staged: list[int] = []
    monkeypatch.setattr(
        "frame_export.error_service.stage_photo_exports",
        lambda config, photo_ids, dry_run: staged.extend(photo_ids),
    )
    monkeypatch.setattr(
        "photo_repository.list_export_audit_records",
        lambda conn, **kwargs: [
            type(
                "Record",
                (),
                {
                    "photo_id": 951,
                    "batch_name": "batch-a",
                    "sheet_scan_id": 10,
                    "crop_index": 1,
                    "raw_crop_path": tmp_path / "raw_951.jpg",
                    "working_path": tmp_path / "work_951.jpg",
                    "export_path": tmp_path / "staging" / "photo_951.jpg",
                    "status": "enhancement_complete",
                    "rotation_degrees": 0,
                    "accepted_detection_id": None,
                    "detection_confidence": None,
                    "detection_reviewed_by_human": False,
                    "detection_width": None,
                    "detection_height": None,
                    "has_open_orientation_review": False,
                    "orientation_review_reason": None,
                },
            )(),
            type(
                "Record",
                (),
                {
                    "photo_id": 952,
                    "batch_name": "batch-a",
                    "sheet_scan_id": 10,
                    "crop_index": 2,
                    "raw_crop_path": tmp_path / "raw_952.jpg",
                    "working_path": tmp_path / "work_952.jpg",
                    "export_path": tmp_path / "staging" / "photo_952.jpg",
                    "status": "enhancement_complete",
                    "rotation_degrees": 0,
                    "accepted_detection_id": None,
                    "detection_confidence": None,
                    "detection_reviewed_by_human": False,
                    "detection_width": None,
                    "detection_height": None,
                    "has_open_orientation_review": False,
                    "orientation_review_reason": None,
                },
            )(),
            type(
                "Record",
                (),
                {
                    "photo_id": 953,
                    "batch_name": "batch-a",
                    "sheet_scan_id": 10,
                    "crop_index": 3,
                    "raw_crop_path": tmp_path / "raw_953.jpg",
                    "working_path": tmp_path / "work_953.jpg",
                    "export_path": tmp_path / "staging" / "photo_953.jpg",
                    "status": "enhancement_complete",
                    "rotation_degrees": 0,
                    "accepted_detection_id": None,
                    "detection_confidence": None,
                    "detection_reviewed_by_human": False,
                    "detection_width": None,
                    "detection_height": None,
                    "has_open_orientation_review": False,
                    "orientation_review_reason": None,
                },
            )(),
        ],
    )
    monkeypatch.setattr(
        "audit.service._classify_records",
        lambda records, config: [
            ExportAuditFinding(
                photo_id=record.photo_id,
                batch_name="batch-a",
                sheet_scan_id=10,
                crop_index=index,
                category="ok",
                reason="no audit issue detected",
                export_path=record.export_path,
                auto_rotation_suggestion=0,
                auto_rotation_confidence=0.2,
                review_priority="low",
                suggested_issue=None,
                suggested_issue_confidence=None,
                suggested_issue_reason=None,
            )
            for index, record in enumerate(records, start=1)
        ],
    )
    monkeypatch.setattr(
        "audit.service._open_export_audit_manual_fields",
        lambda config: {},
    )

    summary = stage_next_exports_for_audit(
        app_config,
        batch_name="batch-a",
        sheet_id=None,
        limit=20,
        csv_path=None,
        dry_run=False,
    )

    assert summary.target == "batch-a"
    assert summary.selected_count == 2
    assert summary.staged_count == 2
    assert summary.audited_count == 2
    assert staged == [951, 952]
    assert selection_calls == [
        {
            "batch_name": "batch-a",
            "sheet_id": None,
            "photo_id": None,
            "exclude_final_exported": True,
            "limit": 20,
        }
    ]

    with summary.csv_path.open("r", encoding="utf-8", newline="") as handle:
        rows = [row for row in csv.DictReader(handle) if row["row_type"] == "photo"]
    assert [row["photo_id"] for row in rows] == ["951", "952"]


def test_apply_manual_staging_edits_updates_working_files_and_restages(
    app_config,
    monkeypatch,
    tmp_path,
) -> None:
    temp_dir = app_config.photos_root / "exports" / "staging" / "temp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    edited_path = temp_dir / "photo_951.jpg"
    edited_path.write_bytes(b"edited")
    working_path = app_config.photos_root / "derivatives" / "enhance" / "photo_951.jpg"
    working_path.parent.mkdir(parents=True, exist_ok=True)
    working_path.write_bytes(b"old")
    updated: list[tuple[int, str, str]] = []
    staged: list[int] = []

    class _FakeCursor:
        def execute(self, query, params=None) -> None:
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeConn:
        def cursor(self):
            return _FakeCursor()

        def commit(self) -> None:
            return None

    @contextmanager
    def _fake_connect(_config):
        yield _FakeConn()

    monkeypatch.setattr("frame_export.error_service.connect", _fake_connect)
    monkeypatch.setattr(
        "frame_export.error_service.get_photo_record",
        lambda conn, photo_id: type("Photo", (), {"working_path": working_path.relative_to(app_config.photos_root.parent), "status": "enhancement_complete"})(),
    )
    monkeypatch.setattr(
        "frame_export.error_service.update_photo_stage",
        lambda conn, photo_id, working_path, status: updated.append((photo_id, str(working_path), status)),
    )
    monkeypatch.setattr(
        "frame_export.error_service._stage_exact_manual_edits",
        lambda config, photo_ids: staged.extend(photo_ids),
    )

    summary = apply_manual_staging_edits(
        app_config,
        temp_dir=None,
        dry_run=False,
    )

    assert summary.edited_count == 1
    assert summary.staged_count == 1
    assert summary.missing_entries == ()
    assert working_path.read_bytes() == b"edited"
    assert not edited_path.exists()
    assert updated == [(951, str(working_path.relative_to(app_config.photos_root.parent)), "enhancement_complete")]
    assert staged == [951]


def test_stage_exact_manual_edits_copies_working_image_without_reframing(
    app_config,
    monkeypatch,
) -> None:
    working_path = app_config.photos_root / "derivatives" / "enhance" / "photo_951.jpg"
    working_path.parent.mkdir(parents=True, exist_ok=True)
    image = __import__("numpy").full((120, 60, 3), 180, dtype=__import__("numpy").uint8)
    __import__("cv2").imwrite(str(working_path), image)
    inserted: list[tuple[int, str, str]] = []

    class _FakeCursor:
        def execute(self, query, params=None) -> None:
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeConn:
        def cursor(self):
            return _FakeCursor()

        def commit(self) -> None:
            return None

    @contextmanager
    def _fake_connect(_config):
        yield _FakeConn()

    monkeypatch.setattr("frame_export.error_service.connect", _fake_connect)
    monkeypatch.setattr(
        "frame_export.error_service.get_photo_record",
        lambda conn, photo_id: type("Photo", (), {"working_path": working_path.relative_to(app_config.photos_root.parent)})(),
    )
    monkeypatch.setattr(
        "frame_export.error_service.list_photo_artifact_paths",
        lambda conn, photo_id, artifact_type: [],
    )
    monkeypatch.setattr(
        "frame_export.error_service.insert_photo_artifact",
        lambda conn, photo_id, artifact_type, path, pipeline_stage, pipeline_version: inserted.append((photo_id, artifact_type, str(path))),
    )

    _stage_exact_manual_edits(
        app_config,
        photo_ids=[951],
    )

    staging_path = app_config.photos_root / "exports" / "staging" / "portrait" / "photo_951.jpg"
    assert staging_path.exists()
    assert staging_path.read_bytes() == working_path.read_bytes()
    assert inserted == [(951, "frame_export_staging", str(staging_path))]



def test_apply_manual_staging_edits_processes_manual_split_groups(
    app_config,
    monkeypatch,
) -> None:
    temp_dir = app_config.photos_root / "exports" / "staging" / "temp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    split_left = temp_dir / "photo_1146_1.jpg"
    split_right = temp_dir / "photo_1146_2.jpg"
    split_left.write_bytes(b"left")
    split_right.write_bytes(b"right")
    split_calls: list[tuple[int, list[str], str | None, bool]] = []

    @contextmanager
    def _fake_connect(_config):
        class _FakeConn:
            def commit(self) -> None:
                return None

        yield _FakeConn()

    monkeypatch.setattr("frame_export.error_service.connect", _fake_connect)
    monkeypatch.setattr(
        "frame_export.error_service._apply_exact_manual_split",
        lambda config, photo_id, input_paths, note: split_calls.append(
            (photo_id, [str(path) for path in input_paths], note)
        )
        or [1201, 1202],
    )

    summary = apply_manual_staging_edits(
        app_config,
        temp_dir=None,
        dry_run=False,
    )

    assert summary.edited_count == 1
    assert summary.staged_count == 2
    assert summary.missing_entries == ()
    assert split_calls == [
        (
            1146,
            [str(split_left), str(split_right)],
            "manual split from staging/temp",
        )
    ]
    assert not split_left.exists()
    assert not split_right.exists()


def test_apply_manual_staging_edits_rejects_mixed_direct_and_split_entries_for_same_photo(
    app_config,
) -> None:
    temp_dir = app_config.photos_root / "exports" / "staging" / "temp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    (temp_dir / "photo_1146.jpg").write_bytes(b"direct")
    (temp_dir / "photo_1146_1.jpg").write_bytes(b"left")
    (temp_dir / "photo_1146_2.jpg").write_bytes(b"right")

    with pytest.raises(
        ValueError,
        match="both direct and split files for photo ids: 1146",
    ):
        apply_manual_staging_edits(
            app_config,
            temp_dir=None,
            dry_run=False,
        )


def test_closest_profile_for_dimensions_prefers_nearest_frame_shape() -> None:
    assert _closest_profile_for_dimensions(
        width=875,
        height=918,
        landscape_profile="landscape",
        portrait_profile="portrait",
    ) == "portrait"
    assert _closest_profile_for_dimensions(
        width=1080,
        height=922,
        landscape_profile="landscape",
        portrait_profile="portrait",
    ) == "landscape"
