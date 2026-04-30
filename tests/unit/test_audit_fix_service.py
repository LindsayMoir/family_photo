from __future__ import annotations

import cv2
import numpy as np
from pathlib import Path

from audit.fix_service import (
    _apply_split_fix,
    _detect_split_regions,
    _is_supported_issue,
    _task_issue_codes,
    PhotoRepairContext,
    auto_split_photo,
    apply_export_audit_fixes,
    manual_split_photo,
)
from review.models import ReviewTask


def test_task_issue_codes_uses_payload_list_before_raw_issue() -> None:
    task = ReviewTask(
        id=1,
        entity_type="photo",
        entity_id=42,
        task_type="review_export_audit",
        status="open",
        priority=10,
        payload_json={"issue": "OTHER", "issue_codes": ["R180", "CROP"]},
    )

    assert _task_issue_codes(task) == ["R180", "CROP"]


def test_is_supported_issue_accepts_multiple_supported_codes() -> None:
    task = ReviewTask(
        id=1,
        entity_type="photo",
        entity_id=42,
        task_type="review_export_audit",
        status="open",
        priority=10,
        payload_json={"issue_codes": ["R180", "CROP"]},
    )

    assert _is_supported_issue(task) is True


def test_apply_export_audit_fixes_runs_split_before_rotation(app_config, monkeypatch) -> None:
    task = ReviewTask(
        id=1,
        entity_type="photo",
        entity_id=42,
        task_type="review_export_audit",
        status="open",
        priority=10,
        payload_json={"issue": "R180, CROP", "issue_codes": ["R180", "CROP"], "notes": ""},
    )
    calls: list[tuple[str, object]] = []

    monkeypatch.setattr("audit.fix_service.list_tasks", lambda *args, **kwargs: [task])
    monkeypatch.setattr(
        "audit.fix_service._apply_split_fix",
        lambda *args, **kwargs: calls.append(("split", task.entity_id)) or [42, 99],
    )
    monkeypatch.setattr(
        "audit.fix_service._apply_rotation_fix_to_photo",
        lambda *args, **kwargs: calls.append(("rotate", kwargs["photo_id"], kwargs["rotation_degrees"])),
    )
    monkeypatch.setattr(
        "audit.fix_service._apply_skew_fix_to_photo",
        lambda *args, **kwargs: calls.append(("skew", kwargs["photo_id"])),
    )
    monkeypatch.setattr(
        "audit.fix_service.resolve_export_audit_review",
        lambda *args, **kwargs: calls.append(("resolve", kwargs["export_action"])),
    )
    monkeypatch.setattr(
        "audit.fix_service.resolve_orientation_review_for_photo",
        lambda *args, **kwargs: calls.append(("orientation", kwargs["photo_id"])),
    )

    summary = apply_export_audit_fixes(app_config, dry_run=False)

    assert summary.fixed_count == 1
    assert summary.unresolved_count == 0
    assert summary.created_photo_count == 1
    assert calls == [
        ("split", 42),
        ("rotate", 42, 180),
        ("rotate", 99, 180),
        ("orientation", 42),
        ("orientation", 99),
        ("resolve", "fix_crop"),
    ]


def test_detect_split_regions_falls_back_to_center_seam() -> None:
    image = np.full((120, 200, 3), 220, dtype=np.uint8)
    image[:, :96] = 40
    image[:, 104:] = 120
    image[:, 96:104] = 245

    regions = _detect_split_regions(image)

    assert len(regions) == 2
    assert regions[0] == (0, 0, 95, 120)
    assert regions[1] == (95, 0, 200, 120)


def test_apply_split_fix_restages_split_photos_through_staging_exports(
    app_config,
    monkeypatch,
    tmp_path,
) -> None:
    source_dir = app_config.photos_root.parent / "photos" / "crops" / "sheet_10"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_path = source_dir / "crop_1.jpg"
    source_path.write_bytes(b"fake")

    task = ReviewTask(
        id=1,
        entity_type="photo",
        entity_id=42,
        task_type="review_export_audit",
        status="open",
        priority=10,
        payload_json={"issue": "MERGE", "issue_codes": ["MERGE"], "notes": ""},
    )
    export_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        "audit.fix_service._get_photo_repair_context",
        lambda config, photo_id: PhotoRepairContext(
            photo_id=42,
            sheet_scan_id=10,
            crop_index=1,
            raw_crop_path=source_path.relative_to(app_config.photos_root.parent),
        ),
    )
    monkeypatch.setattr(
        "audit.fix_service.cv2.imread",
        lambda path: np.zeros((100, 200, 3), dtype=np.uint8),
    )
    monkeypatch.setattr(
        "audit.fix_service._detect_split_regions",
        lambda image: [(0, 0, 100, 100), (100, 0, 200, 100)],
    )
    monkeypatch.setattr(
        "audit.fix_service._rewrite_split_photo_arrays",
        lambda config, context, crops, pipeline_version, reuse_original_photo=True: [99],
    )
    monkeypatch.setattr("audit.fix_service.run_deskew", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "audit.fix_service.run_orientation",
        lambda *args, **kwargs: type(
            "OrientationResult",
            (),
            {"review_required": False, "rotation_degrees": 0},
        )(),
    )
    monkeypatch.setattr("audit.fix_service.run_enhancement", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "audit.fix_service.resolve_frame_export_request",
        lambda **kwargs: (1920, 1080, "frame_auto"),
    )
    monkeypatch.setattr(
        "audit.fix_service.run_frame_export",
        lambda config, **kwargs: export_calls.append(kwargs),
    )
    monkeypatch.setattr(
        "audit.fix_service._get_existing_staging_export_path",
        lambda config, *, photo_id: tmp_path / "staging" / f"photo_{photo_id}.jpg",
    )
    for photo_id in (42, 99):
        staged_path = tmp_path / "staging" / f"photo_{photo_id}.jpg"
        staged_path.parent.mkdir(parents=True, exist_ok=True)
        staged_path.write_bytes(b"jpg")

    photo_ids = _apply_split_fix(
        app_config,
        task=task,
        note=None,
    )

    assert photo_ids == [42, 99]
    assert export_calls == [
        {
            "batch_name": None,
            "sheet_id": None,
            "photo_id": 42,
            "limit": None,
            "width_px": 1920,
            "height_px": 1080,
            "profile_name": "frame_auto",
            "dry_run": False,
        },
        {
            "batch_name": None,
            "sheet_id": None,
            "photo_id": 99,
            "limit": None,
            "width_px": 1920,
            "height_px": 1080,
            "profile_name": "frame_auto",
            "dry_run": False,
        },
    ] 


def test_manual_split_photo_restages_operator_supplied_children(
    app_config,
    monkeypatch,
    tmp_path,
) -> None:
    left_path = tmp_path / "left.jpg"
    right_path = tmp_path / "right.jpg"
    cv2.imwrite(str(left_path), np.full((80, 50, 3), 120, dtype=np.uint8))
    cv2.imwrite(str(right_path), np.full((60, 120, 3), 180, dtype=np.uint8))

    monkeypatch.setattr(
        "audit.fix_service._get_photo_repair_context",
        lambda config, photo_id: PhotoRepairContext(
            photo_id=1105,
            sheet_scan_id=1035,
            crop_index=3,
            raw_crop_path=(Path("photos") / "crops" / "sheet_1035" / "crop_3.jpg"),
        ),
    )
    monkeypatch.setattr(
        "audit.fix_service._rewrite_split_photo_arrays",
        lambda config, context, crops, pipeline_version, reuse_original_photo: [1200, 1201],
    )
    reprocessed: list[int] = []
    monkeypatch.setattr(
        "audit.fix_service._reprocess_split_photo_ids",
        lambda config, photo_ids, **kwargs: reprocessed.extend(photo_ids),
    )
    enforced_profiles: list[tuple[list[int], list[str]]] = []
    monkeypatch.setattr(
        "audit.fix_service._enforce_staging_profiles",
        lambda config, photo_ids, expected_profiles: enforced_profiles.append((photo_ids, expected_profiles)),
    )
    monkeypatch.setattr("audit.fix_service._find_open_export_audit_task_id", lambda config, photo_id: 1871)
    disposition_calls: list[tuple[int, str, str | None, bool]] = []
    monkeypatch.setattr(
        "audit.fix_service.set_photo_export_disposition",
        lambda config, photo_id, disposition, note, dry_run: disposition_calls.append(
            (photo_id, disposition, note, dry_run)
        ),
    )
    resolved: list[tuple[int, str, str | None]] = []
    monkeypatch.setattr(
        "audit.fix_service.resolve_export_audit_review",
        lambda config, task_id, export_action, note, dry_run: resolved.append((task_id, export_action, note)),
    )
    orientation_calls: list[tuple[int, str]] = []
    monkeypatch.setattr(
        "audit.fix_service.resolve_orientation_review_for_photo",
        lambda config, photo_id, action: orientation_calls.append((photo_id, action)),
    )

    summary = manual_split_photo(
        app_config,
        photo_id=1105,
        input_paths=[left_path, right_path],
        note="manual split for two photos",
        dry_run=False,
    )

    assert summary.photo_id == 1105
    assert summary.staged_photo_ids == (1200, 1201)
    assert summary.resolved_task_id == 1871
    assert disposition_calls == [
        (1105, "exclude_reject", "replaced by manual split children", False),
    ]
    assert reprocessed == [1200, 1201]
    assert enforced_profiles == [([1200, 1201], ["staging/portrait", "staging/landscape"])]
    assert resolved == [(1871, "fix_crop", "manual split for two photos")]
    assert orientation_calls == [
        (1105, "excluded_via_manual_split"),
        (1200, "fixed_via_manual_split"),
        (1201, "fixed_via_manual_split"),
    ]


def test_auto_split_photo_resolves_open_export_task_when_present(
    app_config,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "audit.fix_service._auto_split_photo_ids",
        lambda config, photo_id: [photo_id, 1200],
    )
    monkeypatch.setattr(
        "audit.fix_service._find_open_export_audit_task_id",
        lambda config, photo_id: 1871,
    )
    resolved: list[tuple[int, str, str | None]] = []
    monkeypatch.setattr(
        "audit.fix_service.resolve_export_audit_review",
        lambda config, task_id, export_action, note, dry_run: resolved.append((task_id, export_action, note)),
    )
    orientation_calls: list[tuple[int, str]] = []
    monkeypatch.setattr(
        "audit.fix_service.resolve_orientation_review_for_photo",
        lambda config, photo_id, action: orientation_calls.append((photo_id, action)),
    )

    summary = auto_split_photo(
        app_config,
        photo_id=1105,
        note="split review csv applied",
        dry_run=False,
    )

    assert summary.photo_id == 1105
    assert summary.staged_photo_ids == (1105, 1200)
    assert summary.resolved_task_id == 1871
    assert resolved == [(1871, "fix_crop", "split review csv applied")]
    assert orientation_calls == [
        (1105, "fixed_via_export_audit"),
        (1200, "fixed_via_export_audit"),
    ]


def test_enforce_staging_profiles_moves_exports_to_expected_folder(
    app_config,
    monkeypatch,
) -> None:
    landscape_path = app_config.photos_root / "exports" / "staging" / "landscape" / "photo_1105.jpg"
    portrait_path = app_config.photos_root / "exports" / "staging" / "portrait" / "photo_1105.jpg"
    landscape_path.parent.mkdir(parents=True, exist_ok=True)
    landscape_path.write_bytes(b"jpg")
    deleted: list[tuple[int, str]] = []
    inserted: list[tuple[int, str]] = []

    class _FakeCursor:
        def __init__(self) -> None:
            self._row = None

        def execute(self, query, params=None) -> None:
            if "SELECT path" in query:
                self._row = (str(landscape_path),)
            else:
                self._row = None

        def fetchone(self):
            return self._row

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeConn:
        def cursor(self):
            return _FakeCursor()

        def commit(self) -> None:
            return None

    from contextlib import contextmanager

    @contextmanager
    def _fake_connect(_config):
        yield _FakeConn()

    monkeypatch.setattr("audit.fix_service.connect", _fake_connect)
    monkeypatch.setattr(
        "audit.fix_service.delete_photo_artifact",
        lambda conn, photo_id, artifact_type, path: deleted.append((photo_id, str(path))),
    )
    monkeypatch.setattr(
        "audit.fix_service.insert_photo_artifact",
        lambda conn, photo_id, artifact_type, path, pipeline_stage, pipeline_version: inserted.append((photo_id, str(path))),
    )

    from audit.fix_service import _enforce_staging_profiles

    _enforce_staging_profiles(
        app_config,
        photo_ids=[1105],
        expected_profiles=["staging/portrait"],
    )

    assert not landscape_path.exists()
    assert portrait_path.exists()
    assert deleted == [(1105, str(landscape_path))]
    assert inserted == [(1105, str(portrait_path))]


def test_apply_split_fix_recovers_missing_staging_export_with_forced_restaging(
    app_config,
    monkeypatch,
    tmp_path,
) -> None:
    source_dir = app_config.photos_root.parent / "photos" / "crops" / "sheet_10"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_path = source_dir / "crop_1.jpg"
    source_path.write_bytes(b"fake")
    staged_dir = app_config.photos_root / "exports" / "staging" / "portrait"
    staged_dir.mkdir(parents=True, exist_ok=True)
    staged_paths = {
        42: staged_dir / "photo_42.jpg",
        99: staged_dir / "photo_99.jpg",
    }

    task = ReviewTask(
        id=1,
        entity_type="photo",
        entity_id=42,
        task_type="review_export_audit",
        status="open",
        priority=10,
        payload_json={"issue": "MERGE", "issue_codes": ["MERGE"], "notes": ""},
    )
    stage_calls: list[int] = []

    monkeypatch.setattr(
        "audit.fix_service._get_photo_repair_context",
        lambda config, photo_id: PhotoRepairContext(
            photo_id=42,
            sheet_scan_id=10,
            crop_index=1,
            raw_crop_path=source_path.relative_to(app_config.photos_root.parent),
        ),
    )
    monkeypatch.setattr(
        "audit.fix_service.cv2.imread",
        lambda path: np.zeros((100, 200, 3), dtype=np.uint8),
    )
    monkeypatch.setattr(
        "audit.fix_service._detect_split_regions",
        lambda image: [(0, 0, 100, 100), (100, 0, 200, 100)],
    )
    monkeypatch.setattr(
        "audit.fix_service._rewrite_split_photo_arrays",
        lambda config, context, crops, pipeline_version, reuse_original_photo=True: [99],
    )
    monkeypatch.setattr("audit.fix_service.run_deskew", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "audit.fix_service.run_orientation",
        lambda *args, **kwargs: type(
            "OrientationResult",
            (),
            {"review_required": False, "rotation_degrees": 0},
        )(),
    )
    monkeypatch.setattr("audit.fix_service.run_enhancement", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "audit.fix_service.resolve_frame_export_request",
        lambda **kwargs: (1920, 1080, "frame_auto"),
    )
    monkeypatch.setattr(
        "audit.fix_service.run_frame_export",
        lambda config, **kwargs: None,
    )

    artifact_lookup: dict[int, Path | None] = {42: None, 99: None}

    def _fake_get_existing_staging_export_path(config, *, photo_id):
        return artifact_lookup[photo_id]

    def _fake_stage_photo_exports(config, *, photo_ids, dry_run):
        for photo_id in photo_ids:
            stage_calls.append(photo_id)
            staged_paths[photo_id].write_bytes(b"jpg")
            artifact_lookup[photo_id] = staged_paths[photo_id]

    monkeypatch.setattr(
        "audit.fix_service._get_existing_staging_export_path",
        _fake_get_existing_staging_export_path,
    )
    monkeypatch.setattr(
        "audit.fix_service.stage_photo_exports",
        _fake_stage_photo_exports,
    )

    photo_ids = _apply_split_fix(
        app_config,
        task=task,
        note=None,
    )

    assert photo_ids == [42, 99]
    assert stage_calls == [42, 99]
    assert staged_paths[42].exists()
    assert staged_paths[99].exists()


def test_apply_split_fix_fails_when_restaging_cannot_create_staging_export(
    app_config,
    monkeypatch,
    tmp_path,
) -> None:
    source_dir = app_config.photos_root.parent / "photos" / "crops" / "sheet_10"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_path = source_dir / "crop_1.jpg"
    source_path.write_bytes(b"fake")

    task = ReviewTask(
        id=1,
        entity_type="photo",
        entity_id=42,
        task_type="review_export_audit",
        status="open",
        priority=10,
        payload_json={"issue": "MERGE", "issue_codes": ["MERGE"], "notes": ""},
    )

    monkeypatch.setattr(
        "audit.fix_service._get_photo_repair_context",
        lambda config, photo_id: PhotoRepairContext(
            photo_id=42,
            sheet_scan_id=10,
            crop_index=1,
            raw_crop_path=source_path.relative_to(app_config.photos_root.parent),
        ),
    )
    monkeypatch.setattr(
        "audit.fix_service.cv2.imread",
        lambda path: np.zeros((100, 200, 3), dtype=np.uint8),
    )
    monkeypatch.setattr(
        "audit.fix_service._detect_split_regions",
        lambda image: [(0, 0, 100, 100), (100, 0, 200, 100)],
    )
    monkeypatch.setattr(
        "audit.fix_service._rewrite_split_photo_arrays",
        lambda config, context, crops, pipeline_version, reuse_original_photo=True: [99],
    )
    monkeypatch.setattr("audit.fix_service.run_deskew", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "audit.fix_service.run_orientation",
        lambda *args, **kwargs: type(
            "OrientationResult",
            (),
            {"review_required": False, "rotation_degrees": 0},
        )(),
    )
    monkeypatch.setattr("audit.fix_service.run_enhancement", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "audit.fix_service.resolve_frame_export_request",
        lambda **kwargs: (1920, 1080, "frame_auto"),
    )
    monkeypatch.setattr(
        "audit.fix_service.run_frame_export",
        lambda config, **kwargs: None,
    )
    monkeypatch.setattr(
        "audit.fix_service._get_existing_staging_export_path",
        lambda config, *, photo_id: None,
    )
    monkeypatch.setattr(
        "audit.fix_service.stage_photo_exports",
        lambda config, *, photo_ids, dry_run: None,
    )

    try:
        _apply_split_fix(
            app_config,
            task=task,
            note=None,
        )
    except ValueError as exc:
        assert str(exc) == "Split photo 42 did not produce a staging export."
    else:
        raise AssertionError("Expected split fix to fail when restaging cannot produce an export.")
