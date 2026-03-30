"""Logging helpers for CLI commands."""

from __future__ import annotations

import logging


def configure_logging(log_level: str) -> None:
    """Configure process-wide logging once at startup."""
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
