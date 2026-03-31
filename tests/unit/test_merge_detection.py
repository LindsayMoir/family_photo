from __future__ import annotations

import json
from pathlib import Path

from audit.merge_detection import (
    ExportIssueSuggestion,
    _parse_merge_decision_response,
    classify_export_issue,
    classify_merge_candidate,
    resolve_export_issues,
)


def test_parse_merge_decision_response_extracts_json_from_output_text() -> None:
    payload = {
        "output": [
            {
                "content": [
                    {
                        "type": "output_text",
                        "text": '{"issue_code":"MERGE","confidence":0.83,"reason":"clear divider and two adjacent prints"}',
                    }
                ]
            }
        ]
    }

    decision = _parse_merge_decision_response(json.dumps(payload))

    assert decision == ExportIssueSuggestion(
        issue_code="MERGE",
        confidence=0.83,
        reason="clear divider and two adjacent prints",
    )


def test_classify_merge_candidate_returns_none_for_placeholder_api_key(app_config, monkeypatch, tmp_path) -> None:
    image_path = tmp_path / "photo_1.jpg"
    image_path.write_bytes(b"jpg")

    monkeypatch.setenv("OPENAI_API_KEY", "your_openai_key_here")

    assert classify_merge_candidate(app_config, image_path=image_path) is None


def test_classify_export_issue_returns_none_for_placeholder_api_key(app_config, monkeypatch, tmp_path) -> None:
    image_path = tmp_path / "photo_1.jpg"
    image_path.write_bytes(b"jpg")

    monkeypatch.setenv("OPENAI_API_KEY", "your_openai_key_here")

    assert classify_export_issue(app_config, image_path=image_path) is None


def test_resolve_export_issues_reuses_cache_and_missing_paths(app_config, monkeypatch, tmp_path) -> None:
    existing_path = tmp_path / "photo_1.jpg"
    existing_path.write_bytes(b"jpg")
    missing_path = tmp_path / "photo_missing.jpg"

    monkeypatch.setenv("OPENAI_API_KEY", "real_key")
    monkeypatch.setattr(
        "audit.merge_detection._load_cache",
        lambda config: {
            f"{existing_path.resolve()}::{existing_path.stat().st_size}::{existing_path.stat().st_mtime_ns}": {
                "issue_code": "R180",
                "confidence": 0.95,
                "reason": "cached",
            }
        },
    )
    monkeypatch.setattr("audit.merge_detection._write_cache", lambda *args, **kwargs: None)

    results = resolve_export_issues(app_config, image_paths=[existing_path, missing_path])

    assert results[existing_path] == ExportIssueSuggestion(
        issue_code="R180",
        confidence=0.95,
        reason="cached",
    )
    assert results[missing_path] is None
