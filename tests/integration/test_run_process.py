from __future__ import annotations

from contextlib import contextmanager

from detection.models import DetectionRunSummary, SheetScanRecord
from pipeline.service import run_process


@contextmanager
def _unused_connect(_config):
    yield object()


def test_run_process_advances_detected_sheet_without_touching_database(
    app_config,
    monkeypatch,
) -> None:
    sheet = SheetScanRecord(
        id=11,
        batch_name="batch-a",
        original_path=app_config.photos_root / "sheet_11.jpg",
        width_px=1200,
        height_px=900,
    )
    sheet.original_path.write_bytes(b"sheet")
    stage_calls: list[tuple[str, int]] = []

    monkeypatch.setattr("pipeline.service.connect", _unused_connect)
    monkeypatch.setattr("pipeline.service.get_sheet_scans", lambda *args, **kwargs: [sheet])
    monkeypatch.setattr(
        "pipeline.service.run_detection",
        lambda *args, **kwargs: DetectionRunSummary(
            target="sheet_id=11",
            processed_count=1,
            detected_count=2,
            dry_run=True,
        ),
    )
    monkeypatch.setattr(
        "pipeline.service.run_crop",
        lambda *args, **kwargs: stage_calls.append(("crop", kwargs["sheet_id"])),
    )
    monkeypatch.setattr("pipeline.service.list_photo_ids_for_sheet", lambda *args, **kwargs: [101, 102])
    monkeypatch.setattr(
        "pipeline.service.run_deskew",
        lambda *args, **kwargs: stage_calls.append(("deskew", kwargs["photo_id"])),
    )
    monkeypatch.setattr(
        "pipeline.service.run_orientation",
        lambda *args, **kwargs: stage_calls.append(("orientation", kwargs["photo_id"])),
    )
    monkeypatch.setattr(
        "pipeline.service.run_enhancement",
        lambda *args, **kwargs: stage_calls.append(("enhance", kwargs["photo_id"])),
    )

    summary = run_process(
        app_config,
        batch_name=None,
        sheet_id=11,
        limit=None,
        fast_mode=False,
        enable_ocr=False,
        dry_run=True,
    )

    assert summary.target == "sheet_id=11"
    assert summary.sheets_processed == 1
    assert summary.photos_processed == 2
    assert summary.dry_run is True
    assert stage_calls == [
        ("crop", 11),
        ("deskew", 101),
        ("orientation", 101),
        ("enhance", 101),
        ("deskew", 102),
        ("orientation", 102),
        ("enhance", 102),
    ]


def test_run_process_continues_even_when_detection_finds_no_candidates(app_config, monkeypatch) -> None:
    sheet = SheetScanRecord(
        id=22,
        batch_name="batch-b",
        original_path=app_config.photos_root / "sheet_22.jpg",
        width_px=1200,
        height_px=900,
    )
    sheet.original_path.write_bytes(b"sheet")

    monkeypatch.setattr("pipeline.service.connect", _unused_connect)
    monkeypatch.setattr("pipeline.service.get_sheet_scans", lambda *args, **kwargs: [sheet])
    monkeypatch.setattr(
        "pipeline.service.run_detection",
        lambda *args, **kwargs: DetectionRunSummary(
            target="sheet_id=22",
            processed_count=1,
            detected_count=0,
            dry_run=True,
        ),
    )
    stage_calls: list[tuple[str, int]] = []
    monkeypatch.setattr(
        "pipeline.service.run_crop",
        lambda *args, **kwargs: stage_calls.append(("crop", kwargs["sheet_id"])),
    )
    monkeypatch.setattr("pipeline.service.list_photo_ids_for_sheet", lambda *args, **kwargs: [])

    summary = run_process(
        app_config,
        batch_name=None,
        sheet_id=22,
        limit=None,
        fast_mode=False,
        enable_ocr=False,
        dry_run=True,
    )

    assert summary.photos_processed == 0
    assert stage_calls == [("crop", 22)]


def test_run_process_marks_sheet_complete_after_finishing_photos(
    app_config,
    monkeypatch,
) -> None:
    sheet = SheetScanRecord(
        id=33,
        batch_name="batch-c",
        original_path=app_config.photos_root / "sheet_33.jpg",
        width_px=1200,
        height_px=900,
    )
    sheet.original_path.write_bytes(b"sheet")
    stage_calls: list[tuple[str, int]] = []
    completed_sheet_ids: list[int] = []

    class _FakeConn:
        def commit(self) -> None:
            return None

    @contextmanager
    def _fake_connect(_config):
        yield _FakeConn()

    monkeypatch.setattr("pipeline.service.connect", _fake_connect)
    monkeypatch.setattr("pipeline.service.get_sheet_scans", lambda *args, **kwargs: [sheet])
    monkeypatch.setattr(
        "pipeline.service.run_detection",
        lambda *args, **kwargs: DetectionRunSummary(
            target="sheet_id=33",
            processed_count=1,
            detected_count=2,
            dry_run=False,
        ),
    )
    monkeypatch.setattr(
        "pipeline.service.run_crop",
        lambda *args, **kwargs: stage_calls.append(("crop", kwargs["sheet_id"])),
    )
    monkeypatch.setattr("pipeline.service.list_photo_ids_for_sheet", lambda *args, **kwargs: [301, 302])
    monkeypatch.setattr(
        "pipeline.service.run_deskew",
        lambda *args, **kwargs: stage_calls.append(("deskew", kwargs["photo_id"])),
    )
    monkeypatch.setattr(
        "pipeline.service.run_orientation",
        lambda *args, **kwargs: stage_calls.append(("orientation", kwargs["photo_id"])),
    )
    monkeypatch.setattr(
        "pipeline.service.run_enhancement",
        lambda *args, **kwargs: stage_calls.append(("enhance", kwargs["photo_id"])),
    )
    monkeypatch.setattr(
        "pipeline.service.mark_sheet_scan_processing_complete_if_finished",
        lambda conn, sheet_scan_id: completed_sheet_ids.append(sheet_scan_id) or True,
    )

    summary = run_process(
        app_config,
        batch_name=None,
        sheet_id=33,
        limit=None,
        fast_mode=False,
        enable_ocr=False,
        dry_run=False,
    )

    assert summary.target == "sheet_id=33"
    assert summary.sheets_processed == 1
    assert summary.photos_processed == 2
    assert completed_sheet_ids == [33]
    assert stage_calls == [
        ("crop", 33),
        ("deskew", 301),
        ("orientation", 301),
        ("enhance", 301),
        ("deskew", 302),
        ("orientation", 302),
        ("enhance", 302),
    ]
