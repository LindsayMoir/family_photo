from __future__ import annotations

from contextlib import contextmanager

from frame_export.service import FrameExportSummary
from pipeline.service import ProcessSummary, RunBatchSummary, run_batch
from review.models import ReviewTask, ReviewTaskSummary


@contextmanager
def _unused_connect(_config):
    yield object()


def test_run_batch_smoke_reports_exports_and_review_state(app_config, monkeypatch) -> None:
    export_ready_calls: list[dict[str, object]] = []
    frame_export_calls: list[dict[str, object]] = []
    split_review_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        "pipeline.service.run_process",
        lambda *args, **kwargs: ProcessSummary(
            target="sheet_id=31",
            sheets_processed=1,
            photos_processed=2,
            dry_run=False,
        ),
    )
    monkeypatch.setattr("pipeline.service.connect", _unused_connect)

    def _fake_list_export_ready_photo_ids(*args, **kwargs):
        export_ready_calls.append(kwargs)
        return [301, 302]

    monkeypatch.setattr("pipeline.service.list_export_ready_photo_ids", _fake_list_export_ready_photo_ids)
    monkeypatch.setattr(
        "pipeline.service.resolve_frame_export_request",
        lambda **kwargs: (1600, 1200, "archive"),
    )

    def _fake_run_frame_export(*args, **kwargs):
        frame_export_calls.append(kwargs)
        return FrameExportSummary(
            target="sheet_id=31",
            exported_count=2,
            output_dir=app_config.photos_root / "exports" / "staging",
            width_px=1600,
            height_px=1200,
            dry_run=False,
        )

    monkeypatch.setattr("pipeline.service.run_frame_export", _fake_run_frame_export)
    monkeypatch.setattr(
        "pipeline.service.write_split_review_csv",
        lambda config, csv_path, dry_run: split_review_calls.append(
            {"config": config, "csv_path": csv_path, "dry_run": dry_run}
        ),
    )
    monkeypatch.setattr(
        "pipeline.service.get_task_summary",
        lambda *args, **kwargs: ReviewTaskSummary(task_counts={"review_orientation": 1}),
    )
    monkeypatch.setattr(
        "pipeline.service.get_next_task",
        lambda *args, **kwargs: ReviewTask(
            id=9,
            entity_type="sheet",
            entity_id=31,
            task_type="review_orientation",
            status="open",
            priority=10,
            payload_json={},
        ),
    )

    summary = run_batch(
        app_config,
        batch_name=None,
        sheet_id=31,
        limit=None,
        fast_mode=False,
        enable_ocr=False,
        dry_run=False,
    )

    assert isinstance(summary, RunBatchSummary)
    assert summary.exported_count == 2
    assert summary.review_task_counts == {"review_orientation": 1}
    assert summary.blocking_task is not None
    assert summary.blocking_task.entity_id == 31
    assert export_ready_calls == [{"batch_name": None, "sheet_id": 31}]
    assert frame_export_calls == [
        {
            "batch_name": None,
            "sheet_id": 31,
            "photo_id": None,
            "exclude_final_exported": True,
            "limit": None,
            "width_px": 1600,
            "height_px": 1200,
            "profile_name": "archive",
            "dry_run": False,
        }
    ]
    assert split_review_calls == [
        {
            "config": app_config,
            "csv_path": None,
            "dry_run": False,
        }
    ]
