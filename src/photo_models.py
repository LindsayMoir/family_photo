"""Shared models for promoted photo stages."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class PhotoRecord:
    """A promoted photo record loaded from PostgreSQL."""

    id: int
    raw_crop_path: Path
    working_path: Path
    status: str

