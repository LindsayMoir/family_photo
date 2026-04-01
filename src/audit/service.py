"""Audit exported photos into actionable issue categories."""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import os
from pathlib import Path
import re
from typing import Any

import cv2

from audit.merge_detection import ExportIssueSuggestion, resolve_export_issues
from audit.models import ExportAuditFinding, ExportAuditRecord, ExportAuditSummary
from config import AppConfig
from db.connection import connect
from frame_export.service import (
    FRAME_EXPORT_PIPELINE_VERSION,
    STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
    stage_photo_exports,
)
from orientation.service import audit_orientation_image
from photo_repository import (
    delete_photo_artifact,
    insert_photo_artifact,
    list_export_audit_records,
    list_open_orientation_review_photo_ids,
    list_photo_artifact_paths,
    list_photo_ids,
)
import logging


MERGED_SIMILARITY_THRESHOLD = 0.83
ROTATION_180_CONFIDENCE_THRESHOLD = 0.80
ROTATION_RIGHT_ANGLE_THRESHOLD = 0.90
AMBIGUOUS_DETECTION_CONFIDENCE = 0.75
AUTO_PREFILL_ISSUE_THRESHOLDS: dict[str, float] = {
    "R180": 0.90,
}
DEFAULT_AUDIT_ORIENTATION_WORKERS = 6
RESOLVED_EXPORT_AUDIT_FIX_ACTIONS = ("fix_crop", "fix_rotation", "fix_skew")
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
_STAGING_EXPORT_FILENAME_RE = re.compile(r"^photo_(?P<photo_id>\d+)\.jpg$", re.IGNORECASE)
LOGGER = logging.getLogger(__name__)


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
    debug_enabled = os.getenv("FAMILY_PHOTO_AUDIT_DEBUG", "").strip().lower() in {"1", "true", "yes"}
    if not dry_run:
        LOGGER.info("audit_stage_start stage=stage_open_orientation_reviews")
        _stage_open_orientation_reviews(
            config,
            batch_name=batch_name,
            sheet_id=sheet_id,
            photo_id=photo_id,
            limit=limit,
        )
        LOGGER.info("audit_stage_complete stage=stage_open_orientation_reviews")
    with connect(config) as conn:
        if not dry_run:
            LOGGER.info("audit_stage_start stage=reconcile_staging_artifacts")
            _reconcile_staging_export_artifacts(
                conn,
                config,
                batch_name=batch_name,
                sheet_id=sheet_id,
                photo_id=photo_id,
                limit=limit,
            )
            conn.commit()
            LOGGER.info("audit_stage_complete stage=reconcile_staging_artifacts")
        LOGGER.info("audit_stage_start stage=load_records")
        records = list_export_audit_records(
            conn,
            artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
            batch_name=batch_name,
            sheet_id=sheet_id,
            photo_id=photo_id,
            limit=limit,
        )
        LOGGER.info("audit_stage_complete stage=load_records record_count=%s", len(records))

    LOGGER.info("audit_stage_start stage=classify_records debug=%s", str(debug_enabled).lower())
    findings = _classify_records(records, config=config)
    LOGGER.info("audit_stage_complete stage=classify_records finding_count=%s", len(findings))
    if category is not None:
        findings = [finding for finding in findings if finding.category == category]

    category_counts = dict(Counter(finding.category for finding in findings))
    output_csv_path = (
        csv_path
        if csv_path is not None
        else config.photos_root / "exports" / "staging" / "export_audit.csv"
    )
    if not dry_run:
        LOGGER.info("audit_stage_start stage=write_csv csv_path=%s", output_csv_path)
        _write_audit_csv(output_csv_path, findings, config=config)
        LOGGER.info("audit_stage_complete stage=write_csv csv_path=%s", output_csv_path)
    return ExportAuditSummary(
        target=_target_name(batch_name=batch_name, sheet_id=sheet_id, photo_id=photo_id),
        audited_count=len(findings),
        category_counts=category_counts,
        findings=findings,
        csv_path=output_csv_path,
        dry_run=dry_run,
    )


def _classify_records(
    records: list[ExportAuditRecord],
    *,
    config: AppConfig,
) -> list[ExportAuditFinding]:
    debug_enabled = os.getenv("FAMILY_PHOTO_AUDIT_DEBUG", "").strip().lower() in {"1", "true", "yes"}
    by_sheet: dict[int, list[ExportAuditRecord]] = defaultdict(list)
    for record in records:
        by_sheet[record.sheet_scan_id].append(record)
    classified_rows: list[tuple[ExportAuditRecord, object, str, str]] = []
    candidate_model_paths: set[Path] = set()
    LOGGER.info("audit_stage_start stage=resolve_orientation_decisions debug=%s", str(debug_enabled).lower())
    orientation_by_photo_id = _resolve_orientation_decisions(
        records,
        executor_factory=_orientation_executor_factory(),
    )
    LOGGER.info("audit_stage_complete stage=resolve_orientation_decisions decision_count=%s", len(orientation_by_photo_id))

    for record in records:
        if debug_enabled:
            LOGGER.info("audit_record_start photo_id=%s export_path=%s", record.photo_id, record.export_path)
        siblings = [candidate for candidate in by_sheet[record.sheet_scan_id] if candidate.photo_id != record.photo_id]
        orientation_decision = orientation_by_photo_id[record.photo_id]
        category, reason = _classify_record(record, siblings, orientation_decision)
        classified_rows.append((record, orientation_decision, category, reason))
        if _should_query_model_for_record(category=category, orientation_decision=orientation_decision):
            candidate_model_paths.add(record.export_path)
        if debug_enabled:
            LOGGER.info(
                "audit_record_complete photo_id=%s category=%s rotation=%s confidence=%.2f",
                record.photo_id,
                category,
                orientation_decision.rotation_degrees,
                orientation_decision.confidence,
            )

    LOGGER.info("audit_stage_start stage=resolve_export_issues candidate_count=%s", len(candidate_model_paths))
    suggestion_by_export_path = resolve_export_issues(
        config,
        image_paths=sorted(candidate_model_paths),
    )
    LOGGER.info("audit_stage_complete stage=resolve_export_issues resolved_count=%s", len(suggestion_by_export_path))

    findings: list[ExportAuditFinding] = []
    for record, orientation_decision, category, reason in classified_rows:
        suggestion = _suggested_issue_for_record(
            category=category,
            suggestion=suggestion_by_export_path.get(record.export_path),
        )
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
                suggested_issue=suggestion.issue_code if suggestion is not None and suggestion.issue_code != "OK" else None,
                suggested_issue_confidence=suggestion.confidence if suggestion is not None and suggestion.issue_code != "OK" else None,
                suggested_issue_reason=suggestion.reason if suggestion is not None and suggestion.issue_code != "OK" else None,
            )
        )

    findings.sort(key=lambda item: (item.category, item.batch_name, item.sheet_scan_id, item.crop_index, item.photo_id))
    return findings


def _resolve_orientation_decisions(records: list[ExportAuditRecord], executor_factory=ThreadPoolExecutor) -> dict[int, object]:
    if not records:
        return {}
    debug_enabled = os.getenv("FAMILY_PHOTO_AUDIT_DEBUG", "").strip().lower() in {"1", "true", "yes"}
    workers = max(
        1,
        int(
            os.getenv(
                "FAMILY_PHOTO_AUDIT_ORIENTATION_WORKERS",
                str(DEFAULT_AUDIT_ORIENTATION_WORKERS),
            ).strip()
            or DEFAULT_AUDIT_ORIENTATION_WORKERS
        ),
    )
    decisions: dict[int, object] = {}
    if debug_enabled:
        for record in records:
            LOGGER.info("audit_orientation_debug_start photo_id=%s working_path=%s", record.photo_id, record.working_path)
            decisions[record.photo_id] = audit_orientation_image(record.working_path)
            LOGGER.info("audit_orientation_debug_complete photo_id=%s", record.photo_id)
        return decisions
    executor_kwargs = {"max_workers": min(workers, len(records))}
    if executor_factory is ThreadPoolExecutor:
        executor_kwargs["thread_name_prefix"] = "audit-orient"
    with executor_factory(**executor_kwargs) as executor:
        future_to_photo_id = {
            executor.submit(audit_orientation_image, record.working_path): record.photo_id
            for record in records
        }
        for future in as_completed(future_to_photo_id):
            photo_id = future_to_photo_id[future]
            decisions[photo_id] = future.result()
    return decisions


def _orientation_executor_factory():
    if os.getenv("PYTEST_CURRENT_TEST"):
        return ThreadPoolExecutor
    return ProcessPoolExecutor


def _reconcile_staging_export_artifacts(
    conn,
    config: AppConfig,
    *,
    batch_name: str | None,
    sheet_id: int | None,
    photo_id: int | None,
    limit: int | None,
) -> None:
    allowed_photo_ids = set(
        list_photo_ids(
            conn,
            batch_name=batch_name,
            sheet_id=sheet_id,
            photo_id=photo_id,
            limit=limit,
        )
    )
    if not allowed_photo_ids:
        return
    removed_paths: set[tuple[int, Path]] = set()

    for staging_path in _iter_staging_export_paths(config):
        parsed_photo_id = _photo_id_from_staging_filename(staging_path.name)
        if parsed_photo_id is None or parsed_photo_id not in allowed_photo_ids:
            continue

        existing_paths = list_photo_artifact_paths(
            conn,
            photo_id=parsed_photo_id,
            artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
        )
        for existing_path in existing_paths:
            if existing_path == staging_path:
                continue
            removal_key = (parsed_photo_id, existing_path)
            if removal_key in removed_paths:
                continue
            delete_photo_artifact(
                conn,
                photo_id=parsed_photo_id,
                artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
                path=existing_path,
            )
            removed_paths.add(removal_key)

        insert_photo_artifact(
            conn,
            photo_id=parsed_photo_id,
            artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
            path=staging_path,
            pipeline_stage="frame_export",
            pipeline_version=FRAME_EXPORT_PIPELINE_VERSION,
        )

    for allowed_photo_id in allowed_photo_ids:
        existing_paths = list_photo_artifact_paths(
            conn,
            photo_id=allowed_photo_id,
            artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
        )
        for existing_path in existing_paths:
            if existing_path.exists():
                continue
            removal_key = (allowed_photo_id, existing_path)
            if removal_key in removed_paths:
                continue
            delete_photo_artifact(
                conn,
                photo_id=allowed_photo_id,
                artifact_type=STAGING_FRAME_EXPORT_ARTIFACT_TYPE,
                path=existing_path,
            )
            removed_paths.add(removal_key)


def _iter_staging_export_paths(config: AppConfig) -> list[Path]:
    staging_root = config.photos_root / "exports" / "staging"
    paths: list[Path] = []
    for folder_name in ("landscape", "portrait"):
        folder = staging_root / folder_name
        if not folder.exists():
            continue
        paths.extend(sorted(path for path in folder.iterdir() if path.is_file()))
    return paths


def _photo_id_from_staging_filename(filename: str) -> int | None:
    match = _STAGING_EXPORT_FILENAME_RE.match(filename)
    if match is None:
        return None
    return int(match.group("photo_id"))


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


def _suggested_issue_for_record(
    *,
    category: str,
    suggestion: ExportIssueSuggestion | None,
) -> ExportIssueSuggestion | None:
    if suggestion is None or suggestion.issue_code == "OK":
        return None
    return suggestion


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
    if crop_width <= crop_height:
        return None

    if record.detection_width and record.detection_height:
        aspect_ratio = max(record.detection_width, record.detection_height) / max(
            min(record.detection_width, record.detection_height),
            1.0,
        )
        if aspect_ratio < 1.4:
            return None

    if not siblings:
        return None

    split_index = crop_width // 2
    left_half = crop[:, :split_index]
    right_half = crop[:, split_index:]
    if left_half.size == 0 or right_half.size == 0:
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


def _rotation_issue_code(rotation_degrees: int) -> str | None:
    if rotation_degrees == 90:
        return "RR90"
    if rotation_degrees == 180:
        return "R180"
    if rotation_degrees == 270:
        return "RL90"
    return None


def _should_query_model_for_record(
    *,
    category: str,
    orientation_decision,
) -> bool:
    if category != "rotation":
        return False
    if _rotation_issue_code(orientation_decision.rotation_degrees) != "R180":
        return False
    if orientation_decision.confidence < ROTATION_180_CONFIDENCE_THRESHOLD:
        return False
    return True


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


def _write_audit_csv(
    csv_path: Path,
    findings: list[ExportAuditFinding],
    *,
    config: AppConfig | None = None,
) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    preserved_manual_fields = _load_existing_manual_fields(csv_path)
    fixed_photo_ids = _photo_ids_with_resolved_export_audit_fixes(config)
    sorted_findings = sorted(findings, key=lambda finding: finding.export_path.name)
    fieldnames = [
        "row_type",
        "export_folder",
        "export_filename",
        "needs_help",
        "issue",
        "notes",
        "review_priority",
        "suggested_issue",
        "suggested_issue_confidence",
        "suggested_issue_reason",
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
        for finding in sorted_findings:
            photo_id = str(finding.photo_id)
            preserved = preserved_manual_fields.get(photo_id, {})
            if photo_id in fixed_photo_ids:
                preserved = {}
            defaults = _default_manual_fields_for_finding(finding)
            writer.writerow(
                {
                    "row_type": "photo",
                    "export_folder": finding.export_path.parent.name,
                    "export_filename": finding.export_path.name,
                    "needs_help": preserved.get("needs_help", defaults["needs_help"]),
                    "issue": preserved.get("issue", defaults["issue"]),
                    "notes": preserved.get("notes", defaults["notes"]),
                    "review_priority": finding.review_priority,
                    "suggested_issue": finding.suggested_issue or "",
                    "suggested_issue_confidence": (
                        f"{finding.suggested_issue_confidence:.2f}"
                        if finding.suggested_issue_confidence is not None
                        else ""
                    ),
                    "suggested_issue_reason": finding.suggested_issue_reason or "",
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


def _photo_ids_with_resolved_export_audit_fixes(
    config: AppConfig | None,
) -> set[str]:
    if config is None:
        return set()

    with connect(config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT entity_id
                FROM review_tasks
                WHERE task_type = 'review_export_audit'
                  AND entity_type = 'photo'
                  AND status = 'resolved'
                  AND resolution_json ->> 'action' = ANY(%s)
                  AND resolved_at IS NOT NULL
                """,
                (list(RESOLVED_EXPORT_AUDIT_FIX_ACTIONS),),
            )
            rows = cur.fetchall()
    return {str(row[0]) for row in rows}


def _default_manual_fields_for_finding(finding: ExportAuditFinding) -> dict[str, str]:
    if _should_auto_prefill_issue(finding):
        return {
            "needs_help": "x",
            "issue": finding.suggested_issue or "",
            "notes": "",
        }
    return {
        "needs_help": "",
        "issue": "",
        "notes": "",
    }


def _should_auto_prefill_issue(finding: ExportAuditFinding) -> bool:
    if finding.suggested_issue is None or finding.suggested_issue_confidence is None:
        return False
    threshold = AUTO_PREFILL_ISSUE_THRESHOLDS.get(finding.suggested_issue)
    if threshold is None:
        return False
    return finding.suggested_issue_confidence >= threshold
