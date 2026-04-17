from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest

from reset.service import _list_candidate_file_paths, _resolve_source_dirs, reset_source_scans


def test_resolve_source_dirs_supports_relative_paths_and_deduplicates(app_config) -> None:
    (app_config.photos_root / "book_1").mkdir(parents=True)

    resolved = _resolve_source_dirs(
        app_config,
        [Path("book_1"), app_config.photos_root / "book_1"],
    )

    assert resolved == ((app_config.photos_root / "book_1").resolve(),)


def test_list_candidate_file_paths_collects_db_and_inferred_export_paths(app_config) -> None:
    detection_crop = app_config.photos_root / "derivatives" / "review" / "regions" / "batch-a" / "sheet_5_1_photo.jpg"
    raw_crop = app_config.photos_root / "crops" / "sheet_5" / "crop_1.jpg"
    working = app_config.photos_root / "derivatives" / "enhance" / "photo_10.jpg"
    artifact = app_config.photos_root / "exports" / "staging" / "portrait" / "photo_10.jpg"
    final_export = app_config.photos_root / "exports" / "frame_1080x1920" / "photo_10.jpg"
    preview = app_config.photos_root / "derivatives" / "review" / "detections" / "batch-a" / "sheet_5.jpg"

    for path in (detection_crop, raw_crop, working, artifact, final_export, preview):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x")

    paths = _list_candidate_file_paths(
        config=app_config,
        batch_names_by_sheet_id={5: "batch-a"},
        sheet_ids=[5],
        detection_rows=[(21, detection_crop)],
        photo_rows=[(10, raw_crop, working, None)],
        artifact_rows=[(artifact,)],
    )

    assert detection_crop in paths
    assert raw_crop in paths
    assert working in paths
    assert artifact in paths
    assert final_export in paths
    assert preview in paths


@contextmanager
def _fake_connect(_config):
    class _FakeConn:
        def __init__(self) -> None:
            self.committed = False

        def commit(self) -> None:
            self.committed = True

    yield _FakeConn()


def test_reset_source_scans_reports_inventory_in_dry_run(app_config, monkeypatch) -> None:
    inventory = type(
        "Inventory",
        (),
        {
            "source_dirs": ((app_config.photos_root / "book_1").resolve(),),
            "sheet_ids": (1,),
            "detection_ids": (10, 11),
            "photo_ids": (100, 101),
            "file_paths": (app_config.photos_root / "exports" / "frame_1080x1920" / "photo_100.jpg",),
            "sheet_count": 1,
            "detection_count": 2,
            "photo_count": 2,
            "artifact_count": 3,
            "face_count": 4,
            "photo_people_count": 5,
            "review_task_count": 6,
            "ocr_request_count": 7,
            "processing_job_count": 8,
        },
    )()
    (app_config.photos_root / "book_1").mkdir(parents=True)

    monkeypatch.setattr("reset.service.connect", _fake_connect)
    monkeypatch.setattr("reset.service._load_reset_inventory", lambda conn, config, source_dirs: inventory)

    summary = reset_source_scans(
        app_config,
        source_dirs=[Path("book_1")],
        dry_run=True,
    )

    assert summary.dry_run is True
    assert summary.sheet_count == 1
    assert summary.detection_count == 2
    assert summary.photo_count == 2
    assert summary.file_count == 1
    assert summary.deleted_file_count == 0


def test_reset_source_scans_deletes_files_and_rows(app_config, monkeypatch) -> None:
    source_dir = app_config.photos_root / "book_1"
    source_dir.mkdir(parents=True)
    file_path = app_config.photos_root / "exports" / "frame_1080x1920" / "photo_100.jpg"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(b"jpg")

    inventory = type(
        "Inventory",
        (),
        {
            "source_dirs": (source_dir.resolve(),),
            "sheet_ids": (1,),
            "detection_ids": (10,),
            "photo_ids": (100,),
            "file_paths": (file_path,),
            "sheet_count": 1,
            "detection_count": 1,
            "photo_count": 1,
            "artifact_count": 1,
            "face_count": 0,
            "photo_people_count": 0,
            "review_task_count": 1,
            "ocr_request_count": 1,
            "processing_job_count": 1,
        },
    )()
    deleted_rows: list[object] = []

    monkeypatch.setattr("reset.service.connect", _fake_connect)
    monkeypatch.setattr("reset.service._load_reset_inventory", lambda conn, config, source_dirs: inventory)
    monkeypatch.setattr("reset.service._delete_downstream_rows", lambda conn, inventory: deleted_rows.append(inventory))

    summary = reset_source_scans(
        app_config,
        source_dirs=[Path("book_1")],
        dry_run=False,
    )

    assert summary.dry_run is False
    assert summary.deleted_file_count == 1
    assert not file_path.exists()
    assert deleted_rows == [inventory]


def test_resolve_source_dirs_rejects_missing_directory(app_config) -> None:
    with pytest.raises(ValueError, match="Source directory does not exist"):
        _resolve_source_dirs(app_config, [Path("missing_book")])
