"""Schema-related constants and helpers."""

from __future__ import annotations

from pathlib import Path


SCHEMA_PATH = Path(__file__).resolve().parents[2] / "sql" / "migrations" / "001_initial_schema.sql"


def read_initial_schema() -> str:
    """Return the initial schema SQL."""
    return SCHEMA_PATH.read_text(encoding="utf-8")
