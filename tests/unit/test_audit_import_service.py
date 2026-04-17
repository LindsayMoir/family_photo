from __future__ import annotations

from contextlib import contextmanager

from audit.import_service import (
    _priority_for_row,
    _row_can_auto_fix,
    _row_has_unresolved_issues,
    _row_issue_codes,
    _sync_export_audit_review_tasks,
)


@contextmanager
def _fake_connect(_config):
    class _FakeConn:
        def commit(self) -> None:
            return None

    yield _FakeConn()


def test_row_issue_codes_parses_comma_separated_values() -> None:
    row = {"issue": "R180, CROP, R180"}

    assert _row_issue_codes(row) == ["R180", "CROP"]


def test_priority_for_row_uses_highest_priority_issue_code() -> None:
    row = {"issue": "CROP, R180", "audit_category": ""}

    assert _priority_for_row(row) == 5


def test_row_can_auto_fix_requires_all_issue_codes_supported() -> None:
    assert _row_can_auto_fix({"issue": "R180, CROP"}) is True
    assert _row_can_auto_fix({"issue": "R180, OTHER"}) is False


def test_row_has_unresolved_issues_detects_unknown_codes() -> None:
    assert _row_has_unresolved_issues({"issue": "R180, OTHER"}) is True
    assert _row_has_unresolved_issues({"issue": "R180, CROP"}) is False


def test_sync_export_audit_review_tasks_dismisses_clean_rows(app_config, monkeypatch) -> None:
    calls: list[tuple[str, int]] = []

    monkeypatch.setattr("audit.import_service.connect", _fake_connect)
    monkeypatch.setattr(
        "audit.import_service.dismiss_export_audit_review_task",
        lambda conn, photo_id: calls.append(("dismiss", photo_id)) or True,
    )
    monkeypatch.setattr(
        "audit.import_service.resolve_open_orientation_review_task",
        lambda conn, photo_id, action: calls.append(("orientation", photo_id)),
    )
    monkeypatch.setattr(
        "audit.import_service.upsert_export_audit_review_task",
        lambda *args, **kwargs: calls.append(("upsert", kwargs["photo_id"])),
    )

    result = _sync_export_audit_review_tasks(
        app_config,
        [
            {
                "photo_id": "951",
                "needs_help": "",
                "issue": "",
                "notes": "",
                "audit_category": "ok",
                "audit_reason": "no audit issue detected",
            }
        ],
    )

    assert result.flagged_rows == 0
    assert result.created_or_updated_count == 0
    assert result.dismissed_count == 1
    assert result.delete_photo_ids == []
    assert calls == [
        ("dismiss", 951),
        ("orientation", 951),
    ]


def test_sync_export_audit_review_tasks_upserts_flagged_rows(app_config, monkeypatch) -> None:
    calls: list[tuple[str, int]] = []

    monkeypatch.setattr("audit.import_service.connect", _fake_connect)
    monkeypatch.setattr(
        "audit.import_service.dismiss_export_audit_review_task",
        lambda conn, photo_id: calls.append(("dismiss", photo_id)) or False,
    )
    monkeypatch.setattr(
        "audit.import_service.resolve_open_orientation_review_task",
        lambda conn, photo_id, action: calls.append(("orientation", photo_id)),
    )
    monkeypatch.setattr(
        "audit.import_service.upsert_export_audit_review_task",
        lambda *args, **kwargs: calls.append(("upsert", kwargs["photo_id"])),
    )

    result = _sync_export_audit_review_tasks(
        app_config,
        [
            {
                "photo_id": "539",
                "needs_help": "yes",
                "issue": "MERGE",
                "notes": "",
                "audit_category": "ok",
                "audit_reason": "no audit issue detected",
                "export_path": "photos/exports/staging/landscape/photo_539.jpg",
                "export_folder": "landscape",
                "export_filename": "photo_539.jpg",
            }
        ],
    )

    assert result.flagged_rows == 1
    assert result.created_or_updated_count == 1
    assert result.dismissed_count == 0
    assert result.delete_photo_ids == []
    assert calls == [
        ("upsert", 539),
    ]
