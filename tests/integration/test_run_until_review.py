from __future__ import annotations

from contextlib import contextmanager
import logging

from audit.models import ExportAuditSummary
from pipeline.service import RunBatchSummary, run_until_review


@contextmanager
def _unused_connect(_config):
    yield object()


def test_run_until_review_logs_audit_stage_when_processing_finishes(
    app_config,
    monkeypatch,
    caplog,
) -> None:
    pending_counts = iter([1, 0])

    monkeypatch.setattr("pipeline.service.connect", _unused_connect)
    monkeypatch.setattr(
        "pipeline.service.count_sheet_scans_by_status",
        lambda *args, **kwargs: next(pending_counts),
    )
    monkeypatch.setattr(
        "pipeline.service.run_batch",
        lambda *args, **kwargs: RunBatchSummary(
            target="batch-a",
            sheets_processed=1,
            photos_processed=1,
            exported_count=1,
            review_task_counts={},
            blocking_task=None,
            dry_run=False,
        ),
    )
    monkeypatch.setattr(
        "pipeline.service.run_export_audit",
        lambda *args, **kwargs: ExportAuditSummary(
            target="batch-a",
            audited_count=12,
            category_counts={"ok": 12},
            findings=[],
            csv_path=app_config.photos_root / "exports" / "staging" / "export_audit.csv",
            dry_run=False,
        ),
    )

    with caplog.at_level(logging.INFO):
        summary = run_until_review(
            app_config,
            batch_name="batch-a",
            fast_mode=False,
            enable_ocr=False,
            dry_run=False,
        )

    assert summary.batch_runs == 1
    assert summary.pending_sheets == 0
    assert summary.staged_photo_count == 12
    assert "pipeline_batch_stage_start batch=batch-a stage=audit_exports" in caplog.text
    assert "pipeline_batch_stage_complete batch=batch-a stage=audit_exports staged_photo_count=12" in caplog.text
