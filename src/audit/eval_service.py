"""Non-destructive evaluation harness for staged export audit suggestions."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import re
import shutil

import cv2

from audit.fix_service import _deskew_image, _detect_split_regions, _rotate_image
from audit.merge_detection import classify_export_issue
from config import AppConfig


DEMO_SUPPORTED_ISSUES = ("MERGE", "R180", "RR90", "RL90", "CROP", "SKEW", "DELETE")


@dataclass(frozen=True, slots=True)
class AuditModelTrialSummary:
    """Summary of a non-destructive audit model trial."""

    csv_path: Path
    output_dir: Path
    evaluated_count: int
    agreement_count: int
    dry_run: bool


def run_audit_model_trial(
    config: AppConfig,
    *,
    csv_path: Path,
    limit: int,
    output_dir: Path | None,
    dry_run: bool,
) -> AuditModelTrialSummary:
    """Compare model suggestions with manual audit labels on staged exports."""
    if not csv_path.exists():
        raise ValueError(f"Audit CSV was not found: {csv_path}")

    rows = _read_trial_rows(csv_path)
    selected_rows = _select_trial_rows(rows, limit=limit)
    resolved_output_dir = output_dir if output_dir is not None else _default_output_dir()

    if dry_run:
        return AuditModelTrialSummary(
            csv_path=csv_path,
            output_dir=resolved_output_dir,
            evaluated_count=len(selected_rows),
            agreement_count=0,
            dry_run=True,
        )

    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    report_rows: list[dict[str, str]] = []
    agreement_count = 0

    for index, row in enumerate(selected_rows, start=1):
        export_path = _resolve_export_path(config, row["export_path"])
        if not export_path.exists():
            raise ValueError(f"Trial image was not found: {export_path}")

        manual_issue = row["issue"].strip().upper()
        manual_issue_codes = _issue_codes(manual_issue)
        manual_primary_issue = manual_issue_codes[0] if manual_issue_codes else ""
        suggestion = classify_export_issue(config, image_path=export_path)
        suggested_issue = suggestion.issue_code if suggestion is not None else ""
        if suggested_issue == manual_primary_issue:
            agreement_count += 1

        case_dir = resolved_output_dir / f"{index:02d}_{export_path.stem}"
        before_dir = case_dir / "before"
        manual_dir = case_dir / "manual_expected"
        model_dir = case_dir / "model_predicted"
        before_dir.mkdir(parents=True, exist_ok=True)
        manual_dir.mkdir(parents=True, exist_ok=True)
        model_dir.mkdir(parents=True, exist_ok=True)

        before_path = before_dir / export_path.name
        shutil.copy2(export_path, before_path)
        _write_issue_preview(export_path, manual_issue_codes, manual_dir, notes=row.get("notes", ""))
        if suggested_issue:
            _write_issue_preview(export_path, [suggested_issue], model_dir, notes="")

        summary_payload = {
            "photo_id": row["photo_id"],
            "export_filename": row["export_filename"],
            "manual_issue": manual_issue,
            "manual_issue_codes": manual_issue_codes,
            "manual_primary_issue": manual_primary_issue,
            "manual_notes": row.get("notes", ""),
            "model_issue": suggested_issue,
            "model_confidence": suggestion.confidence if suggestion is not None else None,
            "model_reason": suggestion.reason if suggestion is not None else "",
            "agree": suggested_issue == manual_primary_issue,
        }
        (case_dir / "summary.json").write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")

        report_rows.append(
            {
                "photo_id": row["photo_id"],
                "export_filename": row["export_filename"],
                "manual_issue": manual_issue,
                "manual_primary_issue": manual_primary_issue,
                "model_issue": suggested_issue,
                "model_confidence": f"{suggestion.confidence:.2f}" if suggestion is not None else "",
                "agree": "yes" if suggested_issue == manual_primary_issue else "no",
                "case_dir": str(case_dir),
            }
        )

    _write_report_csv(resolved_output_dir / "report.csv", report_rows)
    (resolved_output_dir / "README.txt").write_text(
        "\n".join(
            [
                "Audit model trial output",
                "",
                "Each case folder contains:",
                "- before/: the current staged export",
                "- manual_expected/: a non-destructive preview based on your current CSV issue code",
                "- model_predicted/: a non-destructive preview based on the vision model suggestion",
                "- summary.json: manual label, model label, confidence, and agreement",
                "",
                f"Supported issue previews: {', '.join(DEMO_SUPPORTED_ISSUES)}",
                "CROP and SKEW previews use manual notes when available.",
                "Agreement is scored against the first manual issue code for multi-code rows.",
            ]
        ),
        encoding="utf-8",
    )

    return AuditModelTrialSummary(
        csv_path=csv_path,
        output_dir=resolved_output_dir,
        evaluated_count=len(selected_rows),
        agreement_count=agreement_count,
        dry_run=False,
    )


def _read_trial_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = []
        for row in reader:
            normalized = {key: value or "" for key, value in row.items()}
            if normalized.get("row_type", "photo").strip().lower() != "photo":
                continue
            rows.append(normalized)
    return rows


def _select_trial_rows(rows: list[dict[str, str]], *, limit: int) -> list[dict[str, str]]:
    selected = [
        row
        for row in rows
        if _primary_issue_code(row.get("issue", "")) in DEMO_SUPPORTED_ISSUES
    ]
    selected.sort(key=lambda row: row["export_filename"])
    return selected[:limit]


def _resolve_export_path(config: AppConfig, export_path_value: str) -> Path:
    export_path = Path(export_path_value)
    if export_path.is_absolute():
        return export_path
    return config.photos_root.parent / export_path


def _write_issue_preview(
    source_path: Path,
    issue_codes: list[str],
    output_dir: Path,
    *,
    notes: str,
) -> None:
    image = cv2.imread(str(source_path))
    if image is None:
        raise ValueError(f"Unable to load trial image: {source_path}")

    if not issue_codes:
        (output_dir / "preview_unavailable.txt").write_text(
            "No issue code available for preview.",
            encoding="utf-8",
        )
        return

    try:
        if issue_codes[0] == "MERGE":
            regions = _detect_split_regions(image)
            if len(regions) < 2:
                raise_preview_unavailable("Split preview unavailable: less than two regions detected.")
            for index, (x1, y1, x2, y2) in enumerate(regions, start=1):
                crop = image[y1:y2, x1:x2]
                if crop.size == 0:
                    continue
                crop = _apply_followup_transforms(crop, issue_codes[1:], notes=notes)
                _write_image(output_dir / f"split_{index:02d}.jpg", crop)
            return

        transformed = _apply_followup_transforms(image, issue_codes, notes=notes)
        _write_image(output_dir / source_path.name, transformed)
    except ValueError as exc:
        (output_dir / "preview_unavailable.txt").write_text(str(exc), encoding="utf-8")


def _rotation_for_issue(issue_code: str) -> int | None:
    if issue_code == "RR90":
        return 90
    if issue_code == "R180":
        return 180
    if issue_code == "RL90":
        return 270
    return None


def _apply_followup_transforms(image, issue_codes: list[str], *, notes: str):
    transformed = image
    for issue_code in issue_codes:
        if issue_code == "DELETE":
            continue
        rotation = _rotation_for_issue(issue_code)
        if rotation is not None:
            transformed = _rotate_image(transformed, rotation)
            continue
        if issue_code == "SKEW":
            angle = _parse_skew_angle_from_notes(notes)
            if angle is None:
                raise_preview_unavailable("No skew angle found in notes.")
            transformed = _deskew_image(transformed, angle)
            continue
        if issue_code == "CROP":
            transformed = _apply_crop_from_notes(transformed, notes)
            continue
        raise_preview_unavailable(f"No preview transform defined for issue {issue_code}.")
    return transformed


def _apply_crop_from_notes(image, notes: str):
    operations = _parse_crop_operations(notes)
    if not operations:
        raise_preview_unavailable("No crop instructions found in notes.")
    height, width = image.shape[:2]
    left = 0
    right = width
    top = 0
    bottom = height
    for side, percent in operations:
        trim_pixels = int(round((width if side in {"left", "right"} else height) * (percent / 100.0)))
        if side == "left":
            left = min(right - 1, left + trim_pixels)
        elif side == "right":
            right = max(left + 1, right - trim_pixels)
        elif side == "top":
            top = min(bottom - 1, top + trim_pixels)
        elif side == "bottom":
            bottom = max(top + 1, bottom - trim_pixels)
    cropped = image[top:bottom, left:right]
    if cropped.size == 0:
        raise_preview_unavailable("Crop instructions removed the full image.")
    return cropped


def _parse_crop_operations(notes: str) -> list[tuple[str, float]]:
    matches = re.findall(r"(left|right|top|bottom)\D*(\d+(?:\.\d+)?)\s*%", notes, flags=re.IGNORECASE)
    return [(side.lower(), float(percent)) for side, percent in matches]


def _parse_skew_angle_from_notes(notes: str) -> float | None:
    match = re.search(r"(\d+(?:\.\d+)?)", notes)
    if match is None:
        return None
    angle = float(match.group(1))
    lowered = notes.lower()
    if "left" in lowered:
        return -abs(angle)
    if "right" in lowered:
        return abs(angle)
    return angle


def _primary_issue_code(raw_issue: str) -> str:
    codes = _issue_codes(raw_issue)
    return codes[0] if codes else ""


def _issue_codes(raw_issue: str) -> list[str]:
    codes: list[str] = []
    for value in re.split(r"[,;\n]+", raw_issue):
        code = value.strip().upper()
        if not code or code in codes:
            continue
        codes.append(code)
    return codes


def raise_preview_unavailable(message: str) -> None:
    raise ValueError(message)


def _write_image(path: Path, image) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not cv2.imwrite(str(path), image, [int(cv2.IMWRITE_JPEG_QUALITY), 95]):
        raise ValueError(f"Failed to write evaluation image: {path}")


def _write_report_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = [
        "photo_id",
        "export_filename",
        "manual_issue",
        "manual_primary_issue",
        "model_issue",
        "model_confidence",
        "agree",
        "case_dir",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _default_output_dir() -> Path:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return Path("tmp") / "audit_model_trials" / timestamp
