from __future__ import annotations

from audit.fix_service import _is_supported_issue, _task_issue_codes, apply_export_audit_fixes
from review.models import ReviewTask


def test_task_issue_codes_uses_payload_list_before_raw_issue() -> None:
    task = ReviewTask(
        id=1,
        entity_type="photo",
        entity_id=42,
        task_type="review_export_audit",
        status="open",
        priority=10,
        payload_json={"issue": "OTHER", "issue_codes": ["R180", "CROP"]},
    )

    assert _task_issue_codes(task) == ["R180", "CROP"]


def test_is_supported_issue_accepts_multiple_supported_codes() -> None:
    task = ReviewTask(
        id=1,
        entity_type="photo",
        entity_id=42,
        task_type="review_export_audit",
        status="open",
        priority=10,
        payload_json={"issue_codes": ["R180", "CROP"]},
    )

    assert _is_supported_issue(task) is True


def test_apply_export_audit_fixes_runs_split_before_rotation(app_config, monkeypatch) -> None:
    task = ReviewTask(
        id=1,
        entity_type="photo",
        entity_id=42,
        task_type="review_export_audit",
        status="open",
        priority=10,
        payload_json={"issue": "R180, CROP", "issue_codes": ["R180", "CROP"], "notes": ""},
    )
    calls: list[tuple[str, object]] = []

    monkeypatch.setattr("audit.fix_service.list_tasks", lambda *args, **kwargs: [task])
    monkeypatch.setattr(
        "audit.fix_service._apply_split_fix",
        lambda *args, **kwargs: calls.append(("split", task.entity_id)) or [42, 99],
    )
    monkeypatch.setattr(
        "audit.fix_service._apply_rotation_fix_to_photo",
        lambda *args, **kwargs: calls.append(("rotate", kwargs["photo_id"], kwargs["rotation_degrees"])),
    )
    monkeypatch.setattr(
        "audit.fix_service._apply_skew_fix_to_photo",
        lambda *args, **kwargs: calls.append(("skew", kwargs["photo_id"])),
    )
    monkeypatch.setattr(
        "audit.fix_service.resolve_export_audit_review",
        lambda *args, **kwargs: calls.append(("resolve", kwargs["export_action"])),
    )
    monkeypatch.setattr(
        "audit.fix_service.resolve_orientation_review_for_photo",
        lambda *args, **kwargs: calls.append(("orientation", kwargs["photo_id"])),
    )

    summary = apply_export_audit_fixes(app_config, dry_run=False)

    assert summary.fixed_count == 1
    assert summary.unresolved_count == 0
    assert summary.created_photo_count == 1
    assert calls == [
        ("split", 42),
        ("rotate", 42, 180),
        ("rotate", 99, 180),
        ("orientation", 42),
        ("orientation", 99),
        ("resolve", "fix_crop"),
    ]
