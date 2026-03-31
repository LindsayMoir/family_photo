from __future__ import annotations

from audit.import_service import _priority_for_row, _row_can_auto_fix, _row_has_unresolved_issues, _row_issue_codes


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
