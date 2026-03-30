"""Application service for ingesting sheet scans."""

from __future__ import annotations

from pathlib import Path

from config import AppConfig
from db.connection import connect
from ingest.discovery import discover_scans
from ingest.models import IngestResult
from ingest.repository import ensure_scan_batch, upsert_sheet_scans


def run_ingest(config: AppConfig, input_path: Path, batch_name: str, dry_run: bool) -> IngestResult:
    """Discover scan files and optionally persist them to PostgreSQL."""
    if not batch_name.strip():
        raise ValueError("Batch name must not be empty.")

    scans = discover_scans(input_path)

    if dry_run:
        return IngestResult(
            batch_name=batch_name,
            input_path=input_path.expanduser().resolve(),
            discovered_count=len(scans),
            inserted_count=0,
            updated_count=0,
            dry_run=True,
        )

    try:
        with connect(config) as conn:
            scan_batch_id = ensure_scan_batch(conn, batch_name)
            inserted_count, updated_count = upsert_sheet_scans(conn, scan_batch_id, scans)
            conn.commit()
    except Exception as exc:
        detail = str(exc).strip()
        if not detail and getattr(exc, "args", None):
            detail = ", ".join(str(arg) for arg in exc.args if str(arg).strip())
        if not detail:
            detail = repr(exc)
        raise RuntimeError(f"Ingest failed for batch '{batch_name}': {detail}") from exc

    return IngestResult(
        batch_name=batch_name.strip(),
        input_path=input_path.expanduser().resolve(),
        discovered_count=len(scans),
        inserted_count=inserted_count,
        updated_count=updated_count,
        dry_run=False,
    )
