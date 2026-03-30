"""Shared pipeline types."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class CommandPlan:
    """A minimal description of the work a CLI command would perform."""

    command_name: str
    target: str
    dry_run: bool
    notes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class IngestRequest:
    """Parameters for a future ingest command implementation."""

    input_path: Path
    batch_name: str


@dataclass(frozen=True, slots=True)
class EntityStageRequest:
    """Parameters for a future stage-specific command implementation."""

    entity_id: int
    stage_name: str
