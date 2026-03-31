"""Logging helpers for CLI commands."""

from __future__ import annotations

from datetime import datetime
import logging
import os
from pathlib import Path


def configure_logging(log_level: str, *, write_run_log: bool) -> Path | None:
    """Configure process-wide logging once at startup and return the log file path when enabled."""
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    handlers: list[logging.Handler] = [stream_handler]
    log_path: Path | None = None
    if write_run_log:
        logs_dir = Path("logs")
        logs_dir.mkdir(parents=True, exist_ok=True)
        log_path = logs_dir / (
            f"run-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{os.getpid()}.log"
        )
        file_handler = logging.FileHandler(log_path, encoding="utf-8", delay=True)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        handlers=handlers,
        force=True,
    )
    return log_path
