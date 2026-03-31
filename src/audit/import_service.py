"""Import operator flags from the export audit CSV into review tasks."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
import re

from audit.fix_service import apply_export_audit_fixes
from audit.service import run_export_audit
from config import AppConfig
from db.connection import connect
from disposition.service import set_photo_export_disposition
from frame_export.service import delete_staging_exports, promote_staging_exports
from review.repository import (
    dismiss_export_audit_review_task,
    resolve_open_orientation_review_task,
    upsert_export_audit_review_task,
)


TRUTHY_VALUES = {"1", "true", "t", "yes", "y", "x"}
AUTO_FIX_ISSUES = {"RR90", "RL90", "R180", "FLIP", "MERGE", "CROP", "DUP", "SKEW"}
DELETE_ISSUE = "DELETE"


@dataclass(frozen=True, slots=True)
class AuditImportSummary:
    """Summary of importing operator flags from the audit CSV."""

    csv_path: Path
    processed_rows: int
    flagged_rows: int
    created_or_updated_count: int
    dismissed_count: int
    deleted_count: int
    promoted_count: int
    auto_fixed_count: int
    auto_fix_unresolved_count: int
    created_photo_count: int
    dry_run: bool


def import_audit_csv(
    config: AppConfig,
    *,
    csv_path: Path,
    dry_run: bool,
) -> AuditImportSummary:
    """Read the audit CSV and sync flagged rows into review tasks."""
    if not csv_path.exists():
        raise ValueError(f"Audit CSV was not found: {csv_path}")

    rows = _read_audit_rows(csv_path)
    processed_rows = len(rows)
    flagged_rows = 0
    created_or_updated_count = 0
    dismissed_count = 0
    deleted_count = 0
    promoted_count = 0
    auto_fixed_count = 0
    auto_fix_unresolved_count = 0
    created_photo_count = 0

    if dry_run:
        auto_fixed_count = sum(
            1
            for row in rows
            if _row_needs_help(row) and _row_can_auto_fix(row)
        )
        auto_fix_unresolved_count = sum(
            1
            for row in rows
            if _row_needs_help(row) and _row_has_unresolved_issues(row)
        )
        for row in rows:
            if _row_needs_help(row):
                flagged_rows += 1
        return AuditImportSummary(
            csv_path=csv_path,
            processed_rows=processed_rows,
            flagged_rows=flagged_rows,
            created_or_updated_count=flagged_rows,
            dismissed_count=0,
            deleted_count=sum(1 for row in rows if DELETE_ISSUE in _row_issue_codes(row)),
            promoted_count=sum(1 for row in rows if not _row_needs_help(row)),
            auto_fixed_count=auto_fixed_count,
            auto_fix_unresolved_count=auto_fix_unresolved_count,
            created_photo_count=0,
            dry_run=True,
        )

    delete_photo_ids: list[int] = []
    with connect(config) as conn:
        for row in rows:
            photo_id = int(row["photo_id"])
            issue_codes = _row_issue_codes(row)
            if _row_needs_help(row):
                flagged_rows += 1
                if DELETE_ISSUE in issue_codes:
                    delete_photo_ids.append(photo_id)
                    if dismiss_export_audit_review_task(conn, photo_id=photo_id):
                        dismissed_count += 1
                    resolve_open_orientation_review_task(
                        conn,
                        photo_id=photo_id,
                        action="excluded_via_export_audit",
                    )
                    continue
                upsert_export_audit_review_task(
                    conn,
                    photo_id=photo_id,
                    payload_json={
                        "audit_category": row.get("audit_category", row.get("category", "")),
                        "audit_reason": row.get("audit_reason", row.get("reason", "")),
                        "export_path": row.get("export_path", ""),
                        "export_folder": row.get("export_folder", ""),
                        "export_filename": row.get("export_filename", ""),
                        "issue": row.get("issue", ""),
                        "issue_codes": issue_codes,
                        "notes": row.get("notes", ""),
                    },
                    priority=_priority_for_row(row),
                )
                created_or_updated_count += 1
                continue
            if dismiss_export_audit_review_task(conn, photo_id=photo_id):
                dismissed_count += 1
            resolve_open_orientation_review_task(
                conn,
                photo_id=photo_id,
                action="accepted_current_orientation_via_export_audit",
            )
        conn.commit()

    for photo_id in delete_photo_ids:
        set_photo_export_disposition(
            config,
            photo_id=photo_id,
            disposition="exclude_reject",
            note="Deleted from staging via export audit CSV",
            dry_run=False,
        )
    deleted_count = len(delete_photo_ids)

    promote_summary = promote_staging_exports(
        config,
        csv_path=csv_path,
        dry_run=False,
    )
    promoted_count = promote_summary.promoted_count
    delete_staging_exports(
        config,
        csv_path=csv_path,
        dry_run=False,
    )
    fix_summary = apply_export_audit_fixes(config, dry_run=False)
    auto_fixed_count = fix_summary.fixed_count
    auto_fix_unresolved_count = fix_summary.unresolved_count
    created_photo_count = fix_summary.created_photo_count
    if csv_path.exists():
        csv_path.unlink()
    run_export_audit(
        config,
        batch_name=None,
        sheet_id=None,
        photo_id=None,
        limit=None,
        category=None,
        csv_path=csv_path,
        dry_run=False,
    )

    return AuditImportSummary(
        csv_path=csv_path,
        processed_rows=processed_rows,
        flagged_rows=flagged_rows,
        created_or_updated_count=created_or_updated_count,
        dismissed_count=dismissed_count,
        deleted_count=deleted_count,
        promoted_count=promoted_count,
        auto_fixed_count=auto_fixed_count,
        auto_fix_unresolved_count=auto_fix_unresolved_count,
        created_photo_count=created_photo_count,
        dry_run=False,
    )


def _read_audit_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or "photo_id" not in reader.fieldnames:
            raise ValueError("Audit CSV must include a photo_id column.")
        rows = []
        for row in reader:
            normalized = {key: value or "" for key, value in row.items()}
            row_type = normalized.get("row_type", "photo").strip().lower()
            if row_type != "photo":
                continue
            rows.append(normalized)
        return rows


def _row_needs_help(row: dict[str, str]) -> bool:
    return row.get("needs_help", "").strip().lower() in TRUTHY_VALUES


def _priority_for_row(row: dict[str, str]) -> int:
    issue_codes = _row_issue_codes(row)
    audit_category = row.get("audit_category", row.get("category", "")).strip().lower()
    priorities: list[int] = []
    for issue in issue_codes:
        if issue in {"RR90", "RL90", "R180", "FLIP"}:
            priorities.append(5)
        elif issue in {"CROP", "MERGE", "DUP"}:
            priorities.append(10)
        elif issue == "SKEW":
            priorities.append(12)
        elif issue in {"DARK", "BLUR", "EXCL", "DELETE"}:
            priorities.append(15)
        elif issue in {"AMBIG", "TEXT", "OTHER"}:
            priorities.append(20)
    if priorities:
        return min(priorities)
    if audit_category == "rotation":
        return 5
    if audit_category == "merged_detection":
        return 10
    if audit_category == "source_ambiguous":
        return 20
    return 15


def _row_issue(row: dict[str, str]) -> str:
    issue_codes = _row_issue_codes(row)
    return issue_codes[0] if issue_codes else ""


def _row_issue_codes(row: dict[str, str]) -> list[str]:
    raw_issue = row.get("issue", "")
    if not raw_issue:
        return []
    codes: list[str] = []
    for value in re.split(r"[,;\n]+", raw_issue):
        code = value.strip().upper()
        if not code or code in codes:
            continue
        codes.append(code)
    return codes


def _row_can_auto_fix(row: dict[str, str]) -> bool:
    issue_codes = _row_issue_codes(row)
    if not issue_codes:
        return False
    return all(issue in AUTO_FIX_ISSUES for issue in issue_codes)


def _row_has_unresolved_issues(row: dict[str, str]) -> bool:
    issue_codes = _row_issue_codes(row)
    if not issue_codes:
        return False
    return any(issue not in AUTO_FIX_ISSUES and issue != DELETE_ISSUE for issue in issue_codes)
