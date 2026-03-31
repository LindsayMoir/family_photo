"""Audit exported photos into actionable issue categories."""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import cv2

from audit.models import ExportAuditFinding, ExportAuditRecord, ExportAuditSummary
from config import AppConfig
from db.connection import connect
from frame_export.service import STAGING_FRAME_EXPORT_ARTIFACT_TYPE, stage_photo_exports
from orientation.service import audit_orientation_image
from photo_repository import list_export_audit_records, list_open_orientation_review_photo_ids


MERGED_SIMILARITY_THRESHOLD = 0.83
ROTATION_180_CONFIDENCE_THRESHOLD = 0.80
ROTATION_RIGHT_ANGLE_THRESHOLD = 0.90
AMBIGUOUS_DETECTION_CONFIDENCE = 0.75
ISSUE_CODEBOOK: tuple[tuple[str, str], ...] = (
    ("RR90", "Rotate right 90 degrees."),
    ("RL90", "Rotate left 90 degrees."),
    ("R180", "Rotate 180 degrees."),
    ("FLIP", "Flip or mirror the image."),
    ("CROP", "Adjust the crop."),
    ("MERGE", "Split a merged multi-photo crop."),
    ("DUP", "Duplicate photo or duplicated content."),
    ("SKEW", "Correct the tilt or small-angle skew."),
    ("TEXT", "Text or OCR issue."),
    ("DARK", "Too dark for frame use."),
    ("BLUR", "Too blurry or low-quality."),
    ("DELETE", "Delete the image from staging and exclude it from exports."),
    ("EXCL", "Exclude from frame exports."),
    ("AMBIG", "Source ambiguous, manual judgment required."),
    ("OTHER", "Custom issue. Explain in notes."),
)


def run_export_audit(
    config: AppConfig,
    *,
    batch_name: str | None,
    sheet_id: int | None,
    photo_id: int | None,
    limit: int | None,
    category: str | None,
    csv_path: Path | None,
    dry_run: bool,
) -> ExportAuditSummary:
    """Classify exported photos into operator-facing categories."""
    if not dry_run:
        _stage_open_orientation_reviews(
            config,
            batch_name=batch_name,
            sheet_id=sheet_id,
            photo_id=photo_id,
            limit=limit,
        )
    with connect(config) as conn:
        records = list_export_audit_records(
            conn,
            artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
            batch_name=batch_name,
            sheet_id=sheet_id,
            photo_id=photo_id,
            limit=limit,
        )

    findings = _classify_records(records)
    if category is not None:
        findings = [finding for finding in findings if finding.category == category]

    category_counts = dict(Counter(finding.category for finding in findings))
    output_csv_path = (
        csv_path
        if csv_path is not None
        else config.photos_root / "exports" / "staging" / "export_audit.csv"
    )
    if not dry_run:
        _write_audit_csv(output_csv_path, findings)
    return ExportAuditSummary(
        target=_target_name(batch_name=batch_name, sheet_id=sheet_id, photo_id=photo_id),
        audited_count=len(findings),
        category_counts=category_counts,
        findings=findings,
        csv_path=output_csv_path,
        dry_run=dry_run,
    )


def _classify_records(records: list[ExportAuditRecord]) -> list[ExportAuditFinding]:
    by_sheet: dict[int, list[ExportAuditRecord]] = defaultdict(list)
    for record in records:
        by_sheet[record.sheet_scan_id].append(record)

    findings: list[ExportAuditFinding] = []
    for record in records:
        siblings = [candidate for candidate in by_sheet[record.sheet_scan_id] if candidate.photo_id != record.photo_id]
        orientation_decision = audit_orientation_image(record.working_path)
        category, reason = _classify_record(record, siblings, orientation_decision)
        findings.append(
            ExportAuditFinding(
                photo_id=record.photo_id,
                batch_name=record.batch_name,
                sheet_scan_id=record.sheet_scan_id,
                crop_index=record.crop_index,
                category=category,
                reason=reason,
                export_path=record.export_path,
                auto_rotation_suggestion=orientation_decision.rotation_degrees,
                auto_rotation_confidence=orientation_decision.confidence,
                review_priority=_review_priority_for_category(category, orientation_decision.confidence),
            )
        )

    findings.sort(key=lambda item: (item.category, item.batch_name, item.sheet_scan_id, item.crop_index, item.photo_id))
    return findings


def _classify_record(
    record: ExportAuditRecord,
    siblings: list[ExportAuditRecord],
    orientation_decision,
) -> tuple[str, str]:
    if record.has_open_orientation_review and record.orientation_review_reason is not None:
        return "rotation", record.orientation_review_reason

    merged_reason = _merged_detection_reason(record, siblings)
    if merged_reason is not None:
        return "merged_detection", merged_reason

    rotation_reason = _rotation_reason(record, orientation_decision)
    if rotation_reason is not None:
        return "rotation", rotation_reason

    source_reason = _source_ambiguous_reason(record)
    if source_reason is not None:
        return "source_ambiguous", source_reason

    return "ok", "no audit issue detected"


def _stage_open_orientation_reviews(
    config: AppConfig,
    *,
    batch_name: str | None,
    sheet_id: int | None,
    photo_id: int | None,
    limit: int | None,
) -> None:
    with connect(config) as conn:
        photo_ids = list_open_orientation_review_photo_ids(
            conn,
            batch_name=batch_name,
            sheet_id=sheet_id,
            photo_id=photo_id,
            limit=limit,
        )
    if not photo_ids:
        return
    stage_photo_exports(config, photo_ids=photo_ids, dry_run=False)


def _merged_detection_reason(
    record: ExportAuditRecord,
    siblings: list[ExportAuditRecord],
) -> str | None:
    crop = cv2.imread(str(record.raw_crop_path))
    if crop is None:
        return None

    crop_height, crop_width = crop.shape[:2]
    if crop_width <= crop_height or not siblings:
        return None

    split_index = crop_width // 2
    left_half = crop[:, :split_index]
    right_half = crop[:, split_index:]
    if left_half.size == 0 or right_half.size == 0:
        return None

    if record.detection_width and record.detection_height:
        aspect_ratio = max(record.detection_width, record.detection_height) / max(
            min(record.detection_width, record.detection_height),
            1.0,
        )
        if aspect_ratio < 1.4:
            return None

    for sibling in siblings:
        sibling_crop = cv2.imread(str(sibling.raw_crop_path))
        if sibling_crop is None:
            continue
        left_similarity = _crop_similarity(left_half, sibling_crop)
        right_similarity = _crop_similarity(right_half, sibling_crop)
        if max(left_similarity, right_similarity) >= MERGED_SIMILARITY_THRESHOLD:
            return f"wide crop duplicates sibling photo_id={sibling.photo_id}"
    return None


def _rotation_reason(record: ExportAuditRecord, decision) -> str | None:
    if decision.rotation_degrees == 180 and decision.confidence >= ROTATION_180_CONFIDENCE_THRESHOLD:
        return f"audit suggests 180 degree correction at confidence {decision.confidence:.2f}"
    if decision.rotation_degrees in {90, 270} and decision.confidence >= ROTATION_RIGHT_ANGLE_THRESHOLD:
        return f"audit suggests {decision.rotation_degrees} degree correction at confidence {decision.confidence:.2f}"
    return None


def _review_priority_for_category(category: str, confidence: float) -> str:
    if category == "rotation":
        if confidence >= 0.8:
            return "high"
        return "medium"
    if category == "merged_detection":
        return "high"
    if category == "source_ambiguous":
        return "medium"
    return "low"


def _source_ambiguous_reason(record: ExportAuditRecord) -> str | None:
    if record.accepted_detection_id is None:
        return "photo was created without a linked accepted detection"
    if record.detection_reviewed_by_human:
        return "photo depends on a manually reviewed detection"
    if record.detection_confidence is not None and record.detection_confidence < AMBIGUOUS_DETECTION_CONFIDENCE:
        return f"accepted detection confidence is only {record.detection_confidence:.2f}"
    return None


def _crop_similarity(left: Any, right: Any) -> float:
    left_gray = cv2.cvtColor(left, cv2.COLOR_BGR2GRAY)
    right_gray = cv2.cvtColor(right, cv2.COLOR_BGR2GRAY)
    target_size = (64, 64)
    left_resized = cv2.resize(left_gray, target_size, interpolation=cv2.INTER_AREA)
    right_resized = cv2.resize(right_gray, target_size, interpolation=cv2.INTER_AREA)
    left_normalized = cv2.equalizeHist(left_resized)
    right_normalized = cv2.equalizeHist(right_resized)
    difference = cv2.absdiff(left_normalized, right_normalized)
    mean_difference = float(difference.mean())
    return max(0.0, 1.0 - (mean_difference / 255.0))


def _target_name(
    *,
    batch_name: str | None,
    sheet_id: int | None,
    photo_id: int | None,
) -> str:
    if batch_name is not None:
        return batch_name
    if sheet_id is not None:
        return f"sheet_id={sheet_id}"
    if photo_id is not None:
        return f"photo_id={photo_id}"
    return "all_staging_exports"


def _write_audit_csv(csv_path: Path, findings: list[ExportAuditFinding]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    preserved_manual_fields = _load_existing_manual_fields(csv_path)
    fieldnames = [
        "row_type",
        "export_folder",
        "export_filename",
        "needs_help",
        "issue",
        "notes",
        "review_priority",
        "auto_rotation_suggestion",
        "auto_rotation_confidence",
        "audit_category",
        "audit_reason",
        "export_path",
        "photo_id",
        "batch_name",
        "sheet_scan_id",
        "crop_index",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for finding in findings:
            preserved = preserved_manual_fields.get(str(finding.photo_id), {})
            writer.writerow(
                {
                    "row_type": "photo",
                    "export_folder": finding.export_path.parent.name,
                    "export_filename": finding.export_path.name,
                    "needs_help": preserved.get("needs_help", ""),
                    "issue": preserved.get("issue", ""),
                    "notes": preserved.get("notes", ""),
                    "review_priority": finding.review_priority,
                    "auto_rotation_suggestion": finding.auto_rotation_suggestion,
                    "auto_rotation_confidence": f"{finding.auto_rotation_confidence:.2f}",
                    "audit_category": finding.category,
                    "audit_reason": finding.reason,
                    "export_path": str(finding.export_path),
                    "photo_id": finding.photo_id,
                    "batch_name": finding.batch_name,
                    "sheet_scan_id": finding.sheet_scan_id,
                    "crop_index": finding.crop_index,
                }
            )
        for code, description in ISSUE_CODEBOOK:
            writer.writerow(
                {
                    "row_type": "codebook",
                    "issue": code,
                    "notes": description,
                }
            )


def _load_existing_manual_fields(csv_path: Path) -> dict[str, dict[str, str]]:
    if not csv_path.exists():
        return {}

    manual_fields: dict[str, dict[str, str]] = {}
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            row_type = (row.get("row_type") or "photo").strip().lower()
            if row_type != "photo":
                continue
            photo_id = (row.get("photo_id") or "").strip()
            if not photo_id:
                continue
            manual_fields[photo_id] = {
                "needs_help": row.get("needs_help", ""),
                "issue": row.get("issue", row.get("operator_issue", "")),
                "notes": row.get("notes", row.get("operator_notes", "")),
            }
    return manual_fields
