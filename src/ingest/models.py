"""Models for ingest discovery and persistence."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DiscoveredScan:
    """A scanned sheet discovered on disk."""

    absolute_path: Path
    original_filename: str
    width_px: int
    height_px: int
    dpi_x: int | None
    dpi_y: int | None
    content_hash: str


@dataclass(frozen=True, slots=True)
class IngestResult:
    """Summary of an ingest run."""

    batch_name: str
    input_path: Path
    discovered_count: int
    inserted_count: int
    updated_count: int
    dry_run: bool
