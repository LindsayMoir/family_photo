from __future__ import annotations

import argparse

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
