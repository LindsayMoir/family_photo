from __future__ import annotations

from pathlib import Path
import subprocess
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]


def _run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    env = {
        "PHOTO_DB_URL": "postgresql://localhost:5432/photo_db_test",
        "PHOTOS_ROOT": "photos",
        "LOG_LEVEL": "INFO",
    }
    return subprocess.run(
        [sys.executable, *args],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )


def test_module_entrypoint_runs_help() -> None:
    result = _run_cli("-m", "src.cli", "--help")

    assert result.returncode == 0
    assert "family-photo" in result.stdout
    assert "show-config" in result.stdout


def test_script_entrypoint_prints_schema() -> None:
    result = _run_cli("src/cli.py", "init-db", "--print-sql")

    assert result.returncode == 0
    assert "CREATE TABLE IF NOT EXISTS scan_batches" in result.stdout
