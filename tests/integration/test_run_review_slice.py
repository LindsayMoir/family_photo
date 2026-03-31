from __future__ import annotations

from contextlib import contextmanager

from frame_export.service import FrameExportSummary
from pipeline.service import run_review_slice
from review.models import ReviewTask


@contextmanager
def _unused_connect(_config):
    yield object()


def _task(task_id: int, entity_id: int) -> ReviewTask:
    return ReviewTask(
        id=task_id,
        entity_type="sheet_scan",
        entity_id=entity_id,
        task_type="review_detection",
        status="open",
        priority=10,
        payload_json={},
    )


def test_run_review_slice_dry_run_scans_past_non_actionable_tasks(app_config, monkeypatch) -> None:
    open_tasks = [_task(1, 101), _task(2, 102), _task(3, 103)]
    detailed_tasks = {
        1: ReviewTask(
            id=1,
            entity_type="sheet_scan",
            entity_id=101,
            task_type="review_detection",
            status="open",
            priority=10,
            payload_json={"detections": []},
        ),
        2: ReviewTask(
            id=2,
            entity_type="sheet_scan",
            entity_id=102,
            task_type="review_detection",
            status="open",
            priority=10,
            payload_json={"detections": [{"id": 21, "region_type": "photo"}]},
        ),
        3: ReviewTask(
            id=3,
            entity_type="sheet_scan",
            entity_id=103,
            task_type="review_detection",
            status="open",
            priority=10,
            payload_json={"detections": [{"id": 31, "region_type": "photo"}]},
        ),
    }

    monkeypatch.setattr("pipeline.service.list_sheet_tasks", lambda *args, **kwargs: open_tasks)
    monkeypatch.setattr("pipeline.service.get_task", lambda _config, task_id: detailed_tasks[task_id])

    summary = run_review_slice(
        app_config,
        batch_name="batch-a",
        limit=2,
        dry_run=True,
    )

    assert summary.requested_tasks == 2
    assert summary.actionable_tasks == 2
    assert summary.skipped_tasks_without_photo_detections == 1
    assert summary.photos_processed == 0
    assert summary.dry_run is True


def test_run_review_slice_executes_only_selected_actionable_tasks(app_config, monkeypatch) -> None:
    open_tasks = [_task(1, 201), _task(2, 202), _task(3, 203)]
    detailed_tasks = {
        1: ReviewTask(
            id=1,
            entity_type="sheet_scan",
            entity_id=201,
            task_type="review_detection",
            status="open",
            priority=10,
            payload_json={"detections": []},
        ),
        2: ReviewTask(
            id=2,
            entity_type="sheet_scan",
            entity_id=202,
            task_type="review_detection",
            status="open",
            priority=10,
            payload_json={"detections": [{"id": 22, "region_type": "photo"}]},
        ),
        3: ReviewTask(
            id=3,
            entity_type="sheet_scan",
            entity_id=203,
            task_type="review_detection",
            status="open",
            priority=10,
            payload_json={"detections": [{"id": 33, "region_type": "photo"}]},
        ),
    }
    accepted: list[tuple[int, list[int]]] = []
    stage_calls: list[tuple[str, int]] = []

    monkeypatch.setattr("pipeline.service.list_sheet_tasks", lambda *args, **kwargs: open_tasks)
    monkeypatch.setattr("pipeline.service.get_task", lambda _config, task_id: detailed_tasks[task_id])
    monkeypatch.setattr(
        "pipeline.service.accept_detections",
        lambda _config, task_id, detection_ids, note: accepted.append((task_id, detection_ids)),
    )
    monkeypatch.setattr(
        "pipeline.service.run_crop",
        lambda _config, sheet_id, dry_run: stage_calls.append(("crop", sheet_id)),
    )
    monkeypatch.setattr("pipeline.service.connect", _unused_connect)
    monkeypatch.setattr("pipeline.service.list_photo_ids_for_sheet", lambda *args, **kwargs: [900 + kwargs["sheet_id"]])
    monkeypatch.setattr(
        "pipeline.service.run_deskew",
        lambda _config, photo_id, dry_run: stage_calls.append(("deskew", photo_id)),
    )
    monkeypatch.setattr(
        "pipeline.service.run_orientation",
        lambda _config, photo_id, dry_run: stage_calls.append(("orientation", photo_id)),
    )
    monkeypatch.setattr(
        "pipeline.service.run_enhancement",
        lambda _config, photo_id, dry_run: stage_calls.append(("enhance", photo_id)),
    )
    monkeypatch.setattr(
        "pipeline.service.resolve_frame_export_request",
        lambda **kwargs: (1600, 1200, "archive"),
    )
    monkeypatch.setattr(
        "pipeline.service.run_frame_export",
        lambda _config, **kwargs: stage_calls.append(("export", kwargs["photo_id"])) or FrameExportSummary(
            target=f"photo_id={kwargs['photo_id']}",
            exported_count=1,
            output_dir=app_config.photos_root / "exports" / "staging",
            width_px=1600,
            height_px=1200,
            dry_run=False,
        ),
    )
    monkeypatch.setattr(
        "pipeline.service.run_export_audit",
        lambda *args, **kwargs: __import__("audit.models", fromlist=["ExportAuditSummary"]).ExportAuditSummary(
            target="all_staging_exports",
            audited_count=2,
            category_counts={"ok": 2},
            findings=[],
            csv_path=app_config.photos_root / "exports" / "staging" / "export_audit.csv",
            dry_run=False,
        ),
    )

    summary = run_review_slice(
        app_config,
        batch_name="batch-b",
        limit=2,
        dry_run=False,
    )

    assert accepted == [
        (2, [22]),
        (3, [33]),
    ]
    assert stage_calls == [
        ("crop", 202),
        ("deskew", 1102),
        ("orientation", 1102),
        ("enhance", 1102),
        ("export", 1102),
        ("crop", 203),
        ("deskew", 1103),
        ("orientation", 1103),
        ("enhance", 1103),
        ("export", 1103),
    ]
    assert summary.requested_tasks == 2
    assert summary.actionable_tasks == 2
    assert summary.skipped_tasks_without_photo_detections == 1
    assert summary.photos_processed == 2
    assert summary.staged_photo_count == 2
    assert summary.dry_run is False
