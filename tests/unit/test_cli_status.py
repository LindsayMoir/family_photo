from __future__ import annotations

import argparse
from pathlib import Path

import pytest

import cli
from audit.models import ExportAuditFinding, ExportAuditSummary
from config import AppConfig


def test_command_target_prefers_batch_then_entity_ids() -> None:
    args = argparse.Namespace(command="run-batch", batch="batch-a", sheet_id=12, photo_id=99, task_id=None, input=None)

    assert cli._command_target(args) == "batch-a"


def test_command_target_handles_review_subcommands() -> None:
    args = argparse.Namespace(
        command="review",
        review_command="show",
        batch=None,
        sheet_id=None,
        photo_id=None,
        task_id=None,
        input=None,
    )

    assert cli._command_target(args) == "review:show"


def test_command_target_handles_reset_source_dirs() -> None:
    args = argparse.Namespace(
        command="reset-source-scans",
        source_dir=[Path("book_1"), Path("book_2")],
        batch=None,
        sheet_id=None,
        photo_id=None,
        task_id=None,
        input=None,
    )

    assert cli._command_target(args) == "book_1,book_2"


def test_print_status_line_emits_terminal_friendly_fields(capsys: pytest.CaptureFixture[str]) -> None:
    cli._print_status_line(
        "failed",
        command_name="run-batch",
        target="batch-a",
        elapsed_seconds="12.5",
        failure_kind="runtime_error",
        error_type="RuntimeError",
        error_message="boom",
    )

    captured = capsys.readouterr()

    assert "status=failed" in captured.out
    assert "command_name=run-batch" in captured.out
    assert "command_target=batch-a" in captured.out
    assert "failure_kind=runtime_error" in captured.out
    assert "error_type=RuntimeError" in captured.out
    assert "error_message=boom" in captured.out


def test_main_reports_failed_status_for_runtime_errors(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cli,
        "load_config",
        lambda: AppConfig(
            environment="test",
            database_url="postgresql://localhost:5432/photo_db_test",
            photos_root=tmp_path / "photos",
            log_level="INFO",
        ),
    )
    monkeypatch.setattr(cli, "dispatch_command", lambda args, config: (_ for _ in ()).throw(RuntimeError("boom")))

    with pytest.raises(SystemExit) as exc_info:
        cli.main(["run-batch", "--batch", "batch-a"])

    captured = capsys.readouterr()

    assert exc_info.value.code == 1
    assert "status=started" in captured.out
    assert "status=failed" in captured.out
    assert "failure_kind=runtime_error" in captured.out
    assert "error_message=boom" in captured.out


def test_main_prints_log_path_for_tracked_commands(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cli,
        "load_config",
        lambda: AppConfig(
            environment="test",
            database_url="postgresql://localhost:5432/photo_db_test",
            photos_root=tmp_path / "photos",
            log_level="INFO",
        ),
    )
    monkeypatch.setattr(cli, "dispatch_command", lambda args, config: 0)

    exit_code = cli.main(["run-batch", "--batch", "batch-a"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "log_path=logs/run-" in captured.out


def test_main_does_not_print_log_path_for_untracked_commands(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cli,
        "load_config",
        lambda: AppConfig(
            environment="test",
            database_url="postgresql://localhost:5432/photo_db_test",
            photos_root=tmp_path / "photos",
            log_level="INFO",
        ),
    )
    monkeypatch.setattr(cli, "dispatch_command", lambda args, config: 0)

    exit_code = cli.main(["show-config"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "log_path=" not in captured.out


def test_detect_parser_disables_ocr_by_default() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(["detect", "--sheet-id", "12", "--dry-run"])

    assert args.ocr is False


def test_detect_parser_enables_ocr_when_requested() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(["detect", "--sheet-id", "12", "--ocr", "--dry-run"])

    assert args.ocr is True


def test_audit_exports_parser_accepts_debug_flag() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(["audit-exports", "--debug-audit", "--dry-run"])

    assert args.debug_audit is True


def test_audit_exports_parser_accepts_show_findings_flag() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(["audit-exports", "--show-findings", "--dry-run"])

    assert args.show_findings is True


def test_requeue_final_exports_parser_accepts_source_profile() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(["requeue-final-exports", "--source-profile", "frame_1920x1080", "--dry-run"])

    assert args.source_profile == "frame_1920x1080"


def test_stage_next_exports_parser_accepts_limit_and_batch() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(["stage-next-exports", "--batch", "batch-a", "--limit", "20", "--dry-run"])

    assert args.batch == "batch-a"
    assert args.limit == 20


def test_reset_source_scans_parser_accepts_repeated_source_dirs() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(
        ["reset-source-scans", "--source-dir", "book_1", "--source-dir", "book_2", "--dry-run"]
    )

    assert args.source_dir == [Path("book_1"), Path("book_2")]


def test_manual_split_photo_parser_accepts_repeated_inputs() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(
        [
            "manual-split-photo",
            "--photo-id",
            "1105",
            "--input",
            "left.jpg",
            "--input",
            "right.jpg",
            "--dry-run",
        ]
    )

    assert args.photo_id == 1105
    assert args.input == [Path("left.jpg"), Path("right.jpg")]


def test_apply_manual_staging_edits_parser_accepts_temp_dir() -> None:
    parser = cli.build_parser()

    args = parser.parse_args(
        ["apply-manual-staging-edits", "--temp-dir", "photos/exports/staging/temp", "--dry-run"]
    )

    assert args.temp_dir == Path("photos/exports/staging/temp")


def test_main_reports_completed_status_for_audit_exports(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        cli,
        "load_config",
        lambda: AppConfig(
            environment="test",
            database_url="postgresql://localhost:5432/photo_db_test",
            photos_root=tmp_path / "photos",
            log_level="INFO",
        ),
    )
    monkeypatch.setattr(
        cli,
        "run_export_audit",
        lambda *args, **kwargs: ExportAuditSummary(
            target="all_staging_exports",
            audited_count=2,
            category_counts={"ok": 1, "rotation": 1},
            findings=[
                ExportAuditFinding(
                    photo_id=1,
                    batch_name="batch-a",
                    sheet_scan_id=10,
                    crop_index=0,
                    category="rotation",
                    reason="needs R180",
                    export_path=tmp_path / "photos" / "exports" / "staging" / "photo_1.jpg",
                    auto_rotation_suggestion=180,
                    auto_rotation_confidence=0.95,
                    review_priority="high",
                    suggested_issue="R180",
                    suggested_issue_confidence=0.95,
                    suggested_issue_reason="upside down",
                )
            ],
            csv_path=tmp_path / "photos" / "exports" / "staging" / "export_audit.csv",
            dry_run=False,
        ),
    )

    exit_code = cli.main(["audit-exports"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "command=audit-exports" in captured.out
    assert "finding_count=1" in captured.out
    assert "audit_findings=omitted" in captured.out
    assert "status=completed" in captured.out


def test_handle_audit_exports_prints_findings_only_when_requested(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path,
) -> None:
    summary = ExportAuditSummary(
        target="all_staging_exports",
        audited_count=1,
        category_counts={"rotation": 1},
        findings=[
            ExportAuditFinding(
                photo_id=1,
                batch_name="batch-a",
                sheet_scan_id=10,
                crop_index=0,
                category="rotation",
                reason="needs R180",
                export_path=tmp_path / "photos" / "exports" / "staging" / "photo_1.jpg",
                auto_rotation_suggestion=180,
                auto_rotation_confidence=0.95,
                review_priority="high",
                suggested_issue="R180",
                suggested_issue_confidence=0.95,
                suggested_issue_reason="upside down",
            )
        ],
        csv_path=tmp_path / "photos" / "exports" / "staging" / "export_audit.csv",
        dry_run=False,
    )
    monkeypatch.setattr(cli, "run_export_audit", lambda *args, **kwargs: summary)
    config = AppConfig(
        environment="test",
        database_url="postgresql://localhost:5432/photo_db_test",
        photos_root=tmp_path / "photos",
        log_level="INFO",
    )

    cli._handle_audit_exports(
        config,
        batch_name=None,
        sheet_id=None,
        photo_id=None,
        limit=None,
        category=None,
        csv_path=None,
        debug_audit=False,
        show_findings=False,
        dry_run=False,
    )
    captured = capsys.readouterr()
    assert "audit_findings=omitted" in captured.out
    assert "category\tphoto_id" not in captured.out

    cli._handle_audit_exports(
        config,
        batch_name=None,
        sheet_id=None,
        photo_id=None,
        limit=None,
        category=None,
        csv_path=None,
        debug_audit=False,
        show_findings=True,
        dry_run=False,
    )
    captured = capsys.readouterr()
    assert "category\tphoto_id\tsheet_id\tcrop_index\treason\texport_path" in captured.out


def test_handle_requeue_final_exports_prints_summary(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path,
) -> None:
    monkeypatch.setattr(
        cli,
        "requeue_final_exports_for_audit",
        lambda *args, **kwargs: type(
            "Summary",
            (),
            {
                "source_dir": tmp_path / "photos" / "exports" / "frame_1080x1920",
                "csv_path": tmp_path / "photos" / "exports" / "staging" / "export_audit.csv",
                "queued_count": 10,
                "audited_count": 10,
                "dry_run": False,
            },
        )(),
    )
    config = AppConfig(
        environment="test",
        database_url="postgresql://localhost:5432/photo_db_test",
        photos_root=tmp_path / "photos",
        log_level="INFO",
    )

    cli._handle_requeue_final_exports(
        config,
        source_profile="frame_1080x1920",
        csv_path=None,
        dry_run=False,
    )
    captured = capsys.readouterr()

    assert "command=requeue-final-exports" in captured.out
    assert "queued_count=10" in captured.out
    assert "audited_count=10" in captured.out


def test_handle_stage_next_exports_prints_summary(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path,
) -> None:
    monkeypatch.setattr(
        cli,
        "stage_next_exports_for_audit",
        lambda *args, **kwargs: type(
            "Summary",
            (),
            {
                "target": "batch-a",
                "csv_path": tmp_path / "photos" / "exports" / "staging" / "export_audit.csv",
                "selected_count": 20,
                "staged_count": 20,
                "audited_count": 20,
                "dry_run": False,
            },
        )(),
    )
    config = AppConfig(
        environment="test",
        database_url="postgresql://localhost:5432/photo_db_test",
        photos_root=tmp_path / "photos",
        log_level="INFO",
    )

    cli._handle_stage_next_exports(
        config,
        batch_name="batch-a",
        sheet_id=None,
        limit=20,
        csv_path=None,
        dry_run=False,
    )
    captured = capsys.readouterr()

    assert "command=stage-next-exports" in captured.out
    assert "target=batch-a" in captured.out
    assert "selected_count=20" in captured.out
    assert "staged_count=20" in captured.out
    assert "audited_count=20" in captured.out


def test_handle_promote_exports_defaults_to_all_staging_exports(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path,
) -> None:
    promote_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        cli,
        "promote_staging_exports",
        lambda *args, **kwargs: promote_calls.append(kwargs)
        or type(
            "Summary",
            (),
            {
                "csv_path": None,
                "promoted_count": 19,
                "skipped_count": 0,
                "dry_run": False,
            },
        )(),
    )
    config = AppConfig(
        environment="test",
        database_url="postgresql://localhost:5432/photo_db_test",
        photos_root=tmp_path / "photos",
        log_level="INFO",
    )

    cli._handle_promote_exports(
        config,
        csv_path=None,
        dry_run=False,
    )
    captured = capsys.readouterr()

    assert promote_calls == [{"csv_path": None, "dry_run": False}]
    assert "command=promote-exports" in captured.out
    assert "csv_path=" in captured.out
    assert "promoted_count=19" in captured.out
    assert "skipped_count=0" in captured.out


def test_handle_run_batch_prints_stage_next_exports_hint(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path,
) -> None:
    monkeypatch.setattr(
        cli,
        "run_batch",
        lambda *args, **kwargs: type(
            "Summary",
            (),
            {
                "target": "book_1_600dpi",
                "sheets_processed": 14,
                "photos_processed": 21,
                "exported_count": 188,
                "review_task_counts": {},
                "blocking_task": None,
                "dry_run": False,
            },
        )(),
    )
    config = AppConfig(
        environment="test",
        database_url="postgresql://localhost:5432/photo_db_test",
        photos_root=tmp_path / "photos",
        log_level="INFO",
    )

    cli._handle_run_batch(
        config,
        batch_name="book_1_600dpi",
        sheet_id=None,
        limit=20,
        fast_mode=False,
        enable_ocr=False,
        dry_run=False,
    )
    captured = capsys.readouterr()

    assert "command=run-batch" in captured.out
    assert "exported_count=188" in captured.out
    assert "next_staging_review_command=PYTHONPATH=src python3 -m cli stage-next-exports --batch <batch_name> --limit 20" in captured.out
    assert "next_review_task=none" in captured.out



def test_handle_reset_source_scans_prints_summary(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path,
) -> None:
    monkeypatch.setattr(
        cli,
        "reset_source_scans",
        lambda *args, **kwargs: type(
            "Summary",
            (),
            {
                "source_dirs": (tmp_path / "photos" / "book_1", tmp_path / "photos" / "book_2"),
                "sheet_count": 10,
                "detection_count": 20,
                "photo_count": 30,
                "artifact_count": 40,
                "face_count": 5,
                "photo_people_count": 6,
                "review_task_count": 7,
                "ocr_request_count": 8,
                "processing_job_count": 9,
                "file_count": 50,
                "deleted_file_count": 45,
                "dry_run": False,
            },
        )(),
    )
    config = AppConfig(
        environment="test",
        database_url="postgresql://localhost:5432/photo_db_test",
        photos_root=tmp_path / "photos",
        log_level="INFO",
    )

    cli._handle_reset_source_scans(
        config,
        source_dirs=[Path("book_1"), Path("book_2")],
        dry_run=False,
    )
    captured = capsys.readouterr()

    assert "command=reset-source-scans" in captured.out
    assert "sheet_count=10" in captured.out
    assert "photo_count=30" in captured.out
    assert "deleted_file_count=45" in captured.out


def test_handle_manual_split_photo_prints_summary(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path,
) -> None:
    monkeypatch.setattr(
        cli,
        "manual_split_photo",
        lambda *args, **kwargs: type(
            "Summary",
            (),
            {
                "photo_id": 1105,
                "input_paths": (tmp_path / "left.jpg", tmp_path / "right.jpg"),
                "staged_photo_ids": (1105, 1200),
                "resolved_task_id": 1871,
                "dry_run": False,
            },
        )(),
    )
    config = AppConfig(
        environment="test",
        database_url="postgresql://localhost:5432/photo_db_test",
        photos_root=tmp_path / "photos",
        log_level="INFO",
    )

    cli._handle_manual_split_photo(
        config,
        photo_id=1105,
        input_paths=[Path("left.jpg"), Path("right.jpg")],
        note=None,
        dry_run=False,
    )
    captured = capsys.readouterr()

    assert "command=manual-split-photo" in captured.out
    assert "photo_id=1105" in captured.out
    assert "staged_photo_ids=1105,1200" in captured.out
    assert "resolved_task_id=1871" in captured.out


def test_handle_apply_manual_staging_edits_prints_summary(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path,
) -> None:
    monkeypatch.setattr(
        cli,
        "apply_manual_staging_edits",
        lambda *args, **kwargs: type(
            "Summary",
            (),
            {
                "temp_dir": tmp_path / "photos" / "exports" / "staging" / "temp",
                "edited_count": 2,
                "staged_count": 2,
                "missing_entries": (),
                "dry_run": False,
            },
        )(),
    )
    config = AppConfig(
        environment="test",
        database_url="postgresql://localhost:5432/photo_db_test",
        photos_root=tmp_path / "photos",
        log_level="INFO",
    )

    cli._handle_apply_manual_staging_edits(
        config,
        temp_dir=None,
        dry_run=False,
    )
    captured = capsys.readouterr()

    assert "command=apply-manual-staging-edits" in captured.out
    assert "edited_count=2" in captured.out
    assert "staged_count=2" in captured.out
