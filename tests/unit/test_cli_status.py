from __future__ import annotations

import argparse

import pytest

import cli
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
