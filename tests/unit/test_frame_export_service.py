from __future__ import annotations

from contextlib import contextmanager

import numpy as np

from frame_export.service import (
    _clamp_crop_offset,
    _compute_face_aware_crop_offsets,
    _detect_face_boxes,
    promote_staging_exports,
    run_frame_export,
)


def test_promote_staging_exports_uses_staging_artifacts_when_csv_is_omitted(
    app_config,
    monkeypatch,
) -> None:
    staging_path = app_config.photos_root / "exports" / "staging" / "portrait" / "photo_951.jpg"
    staging_path.parent.mkdir(parents=True, exist_ok=True)
    staging_path.write_bytes(b"jpg")
    deleted: list[tuple[int, str, str]] = []
    inserted: list[tuple[int, str, str]] = []

    class _FakeCursor:
        def __init__(self) -> None:
            self._rows: list[tuple[object, ...]] = []

        def execute(self, query, params=None) -> None:
            normalized = " ".join(str(query).split())
            if "SELECT photo_id, path FROM photo_artifacts" in normalized:
                self._rows = [(951, str(staging_path))]
            else:
                self._rows = []

        def fetchall(self):
            return self._rows

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

    monkeypatch.setattr("frame_export.service.connect", _fake_connect)
    monkeypatch.setattr(
        "frame_export.service.list_photo_artifact_paths",
        lambda conn, photo_id, artifact_type: [],
    )
    monkeypatch.setattr(
        "frame_export.service.insert_photo_artifact",
        lambda conn, photo_id, artifact_type, path, pipeline_stage, pipeline_version: inserted.append(
            (photo_id, artifact_type, str(path))
        ),
    )
    monkeypatch.setattr(
        "frame_export.service.delete_photo_artifact",
        lambda conn, photo_id, artifact_type, path: deleted.append((photo_id, artifact_type, str(path))),
    )

    summary = promote_staging_exports(
        app_config,
        csv_path=None,
        dry_run=False,
    )

    final_path = app_config.photos_root / "exports" / "frame_1080x1920" / "photo_951.jpg"
    assert summary.csv_path is None
    assert summary.promoted_count == 1
    assert summary.skipped_count == 0
    assert not staging_path.exists()
    assert final_path.exists()
    assert inserted == [(951, "frame_export", str(final_path))]
    assert deleted == [(951, "frame_export_staging", str(staging_path))]


def test_promote_staging_exports_applies_pending_manual_temp_files(
    app_config,
    monkeypatch,
) -> None:
    temp_dir = app_config.photos_root / "exports" / "staging" / "temp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    (temp_dir / "photo_951.jpg").write_bytes(b"edited")
    applied: list[tuple[object, ...]] = []

    monkeypatch.setattr(
        "frame_export.error_service.apply_manual_staging_edits",
        lambda config, temp_dir, dry_run: applied.append((config, temp_dir, dry_run)),
    )
    monkeypatch.setattr(
        "frame_export.service._read_staging_promotable_rows",
        lambda config: [],
    )
    class _FakeCursor:
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

    monkeypatch.setattr("frame_export.service.connect", _fake_connect)

    summary = promote_staging_exports(
        app_config,
        csv_path=None,
        dry_run=False,
    )

    assert summary.promoted_count == 0
    assert summary.skipped_count == 0
    assert applied == [
        (
            app_config,
            None,
            False,
        )
    ]


def test_promote_staging_exports_rejects_pending_manual_temp_files_with_csv_path(
    app_config,
    tmp_path,
) -> None:
    temp_dir = app_config.photos_root / "exports" / "staging" / "temp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    (temp_dir / "photo_951.jpg").write_bytes(b"edited")
    csv_path = tmp_path / "export_audit.csv"
    csv_path.write_text("row_type,photo_id,export_path,needs_help\n", encoding="utf-8")

    try:
        promote_staging_exports(
            app_config,
            csv_path=csv_path,
            dry_run=False,
        )
    except ValueError as exc:
        assert str(exc) == (
            "Manual edit files are still present in staging/temp. "
            "Run apply-manual-staging-edits before promote-exports when using --csv-path."
        )
    else:
        raise AssertionError("Expected pending temp files to block CSV-gated promote_staging_exports.")


def test_compute_face_aware_crop_offsets_returns_centered_crop_without_faces(monkeypatch) -> None:
    image = np.zeros((200, 100, 3), dtype=np.uint8)
    monkeypatch.setattr("frame_export.service._detect_face_boxes", lambda image: ())

    x_offset, y_offset = _compute_face_aware_crop_offsets(
        image=image,
        crop_width=100,
        crop_height=100,
    )

    assert x_offset == 0
    assert y_offset == 50


def test_compute_face_aware_crop_offsets_shifts_up_for_top_face(monkeypatch) -> None:
    image = np.zeros((200, 100, 3), dtype=np.uint8)
    monkeypatch.setattr(
        "frame_export.service._detect_face_boxes",
        lambda image: ((20, 10, 40, 40),),
    )

    x_offset, y_offset = _compute_face_aware_crop_offsets(
        image=image,
        crop_width=100,
        crop_height=100,
    )

    assert x_offset == 0
    assert y_offset == 0


def test_detect_face_boxes_returns_empty_when_classifier_is_unavailable(monkeypatch) -> None:
    image = np.zeros((100, 100, 3), dtype=np.uint8)

    class _EmptyClassifier:
        def empty(self) -> bool:
            return True

    monkeypatch.setattr("frame_export.service.cv2.CascadeClassifier", lambda path: _EmptyClassifier())

    assert _detect_face_boxes(image) == ()


def test_clamp_crop_offset_centers_on_face_when_margin_cannot_fit() -> None:
    offset = _clamp_crop_offset(
        default_offset=50,
        face_min=10,
        face_max=90,
        crop_size=60,
        image_size=120,
        margin=12,
    )

    assert offset == 20


def test_promote_staging_exports_deletes_stale_missing_staging_rows(
    app_config,
    monkeypatch,
) -> None:
    missing_staging_path = app_config.photos_root / "exports" / "staging" / "portrait" / "photo_952.jpg"
    deleted: list[tuple[int, str, str]] = []
    inserted: list[tuple[int, str, str]] = []
    updated: list[tuple[int, str, str]] = []

    class _FakeCursor:
        def __init__(self) -> None:
            self._rows: list[tuple[object, ...]] = []

        def execute(self, query, params=None) -> None:
            normalized = " ".join(str(query).split())
            if "SELECT photo_id, path FROM photo_artifacts" in normalized:
                self._rows = [(952, str(missing_staging_path))]
            else:
                self._rows = []

        def fetchall(self):
            return self._rows

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

    monkeypatch.setattr("frame_export.service.connect", _fake_connect)
    monkeypatch.setattr(
        "frame_export.service.list_photo_artifact_paths",
        lambda conn, photo_id, artifact_type: [],
    )
    monkeypatch.setattr(
        "frame_export.service.insert_photo_artifact",
        lambda conn, photo_id, artifact_type, path, pipeline_stage, pipeline_version: inserted.append(
            (photo_id, artifact_type, str(path))
        ),
    )
    monkeypatch.setattr(
        "frame_export.service.delete_photo_artifact",
        lambda conn, photo_id, artifact_type, path: deleted.append((photo_id, artifact_type, str(path))),
    )
    monkeypatch.setattr(
        "frame_export.service.update_photo_export_disposition",
        lambda conn, photo_id, disposition, note: updated.append((photo_id, disposition, note)),
    )

    summary = promote_staging_exports(
        app_config,
        csv_path=None,
        dry_run=False,
    )

    assert summary.promoted_count == 0
    assert summary.skipped_count == 1
    assert inserted == []
    assert updated == [(952, "exclude_reject", "Manually deleted from staging before promotion")]
    assert deleted == [(952, "frame_export_staging", str(missing_staging_path))]


def test_run_frame_export_can_exclude_already_promoted_photos(
    app_config,
    monkeypatch,
) -> None:
    captured_calls: list[dict[str, object]] = []

    class _FakeConn:
        def commit(self) -> None:
            return None

    @contextmanager
    def _fake_connect(_config):
        yield _FakeConn()

    def _fake_list_export_ready_photo_ids(conn, **kwargs):
        captured_calls.append(kwargs)
        return []

    monkeypatch.setattr("frame_export.service.connect", _fake_connect)
    monkeypatch.setattr("frame_export.service.list_export_ready_photo_ids", _fake_list_export_ready_photo_ids)

    try:
        run_frame_export(
            app_config,
            batch_name="batch-a",
            sheet_id=None,
            photo_id=None,
            exclude_final_exported=True,
            limit=10,
            width_px=1920,
            height_px=1080,
            profile_name="frame_auto",
            dry_run=False,
        )
    except ValueError as exc:
        assert str(exc) == "No photos found for target 'batch-a'."
    else:
        raise AssertionError("Expected run_frame_export to raise when no photos are returned.")

    assert captured_calls == [
        {
            "batch_name": "batch-a",
            "sheet_id": None,
            "photo_id": None,
            "exclude_final_exported": True,
            "limit": 10,
        }
    ]
