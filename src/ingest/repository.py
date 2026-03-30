"""Database persistence for ingest."""

from __future__ import annotations

from typing import Iterable

from psycopg2.extensions import connection as PgConnection

from ingest.models import DiscoveredScan


SHEET_STATUS_INGESTED = "ingested"


def ensure_scan_batch(conn: PgConnection, batch_name: str) -> int:
    """Create or fetch a scan batch id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO scan_batches (name)
            VALUES (%s)
            ON CONFLICT (name)
            DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            (batch_name,),
        )
        row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"Failed to create or fetch scan batch '{batch_name}'.")
    return int(row[0])


def upsert_sheet_scans(
    conn: PgConnection,
    scan_batch_id: int,
    scans: Iterable[DiscoveredScan],
) -> tuple[int, int]:
    """Insert or update sheet scan rows."""
    inserted_count = 0
    updated_count = 0

    with conn.cursor() as cur:
        for scan in scans:
            cur.execute(
                """
                INSERT INTO sheet_scans (
                    scan_batch_id,
                    original_path,
                    original_filename,
                    content_hash,
                    width_px,
                    height_px,
                    dpi_x,
                    dpi_y,
                    status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (original_path)
                DO UPDATE SET
                    scan_batch_id = EXCLUDED.scan_batch_id,
                    original_filename = EXCLUDED.original_filename,
                    content_hash = EXCLUDED.content_hash,
                    width_px = EXCLUDED.width_px,
                    height_px = EXCLUDED.height_px,
                    dpi_x = EXCLUDED.dpi_x,
                    dpi_y = EXCLUDED.dpi_y,
                    status = EXCLUDED.status,
                    error_message = NULL,
                    updated_at = NOW()
                RETURNING (xmax = 0) AS inserted
                """,
                (
                    scan_batch_id,
                    str(scan.absolute_path),
                    scan.original_filename,
                    scan.content_hash,
                    scan.width_px,
                    scan.height_px,
                    scan.dpi_x,
                    scan.dpi_y,
                    SHEET_STATUS_INGESTED,
                ),
            )
            row = cur.fetchone()
            if row is None:
                raise RuntimeError(f"Failed to persist scan '{scan.absolute_path}'.")
            if bool(row[0]):
                inserted_count += 1
            else:
                updated_count += 1

    return inserted_count, updated_count
