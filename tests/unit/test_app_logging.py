from __future__ import annotations

import logging
from pathlib import Path

from app_logging import configure_logging


def test_configure_logging_creates_timestamped_run_log(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    log_path = configure_logging("INFO", write_run_log=True)
    logging.getLogger("test.logger").info("hello log file")

    assert log_path.parent == Path("logs")
    assert log_path.name.startswith("run-")
    assert log_path.suffix == ".log"
    assert log_path.exists()
    assert "hello log file" in log_path.read_text(encoding="utf-8")


def test_configure_logging_skips_run_log_when_disabled(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    log_path = configure_logging("INFO", write_run_log=False)
    logging.getLogger("test.logger").info("stream only")

    assert log_path is None
    assert not (tmp_path / "logs").exists()
