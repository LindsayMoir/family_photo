"""Model-assisted staging issue suggestions for export audit."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
import os
from pathlib import Path
import re
from typing import Any
from urllib import error, request

from config import AppConfig


LOGGER = logging.getLogger(__name__)
DEFAULT_EXPORT_AUDIT_MODEL = "gpt-4.1-mini"
EXPORT_AUDIT_CACHE_NAME = ".export_audit_model_cache.json"
_JSON_OBJECT_RE = re.compile(r"\{.*\}", re.DOTALL)
SUPPORTED_ISSUE_CODES = {"OK", "MERGE", "CROP", "SKEW", "RR90", "RL90", "R180"}
DEFAULT_EXPORT_AUDIT_WORKERS = 6


@dataclass(frozen=True, slots=True)
class ExportIssueSuggestion:
    """One model-assisted suggestion about a staged export issue."""

    issue_code: str
    confidence: float
    reason: str

    @property
    def is_merge(self) -> bool:
        return self.issue_code == "MERGE"


def classify_export_issue(
    config: AppConfig,
    *,
    image_path: Path,
) -> ExportIssueSuggestion | None:
    """Return a cached or freshly inferred issue suggestion for one image."""
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key or api_key == "your_openai_key_here":
        return None

    resolved_image_path = _resolve_image_path(config, image_path)
    if not resolved_image_path.exists():
        return None

    cache = _load_cache(config)
    cache_key = _cache_key_for_path(resolved_image_path)
    cached_value = cache.get(cache_key)
    if isinstance(cached_value, dict):
        cached_suggestion = _suggestion_from_dict(cached_value)
        if cached_suggestion is not None:
            return cached_suggestion

    try:
        suggestion = _request_issue_suggestion(
            api_key=api_key,
            model=os.getenv("FAMILY_PHOTO_EXPORT_AUDIT_MODEL", DEFAULT_EXPORT_AUDIT_MODEL).strip() or DEFAULT_EXPORT_AUDIT_MODEL,
            image_path=resolved_image_path,
        )
    except (OSError, ValueError, error.URLError, TimeoutError) as exc:
        LOGGER.warning("export audit model unavailable for %s: %s", image_path, exc)
        return None

    cache[cache_key] = asdict(suggestion)
    _write_cache(config, cache)
    return suggestion


def resolve_export_issues(
    config: AppConfig,
    *,
    image_paths: list[Path],
) -> dict[Path, ExportIssueSuggestion | None]:
    """Resolve model suggestions for many images with shared cache and bounded concurrency."""
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    resolved_paths = {image_path: _resolve_image_path(config, image_path) for image_path in image_paths}
    if not api_key or api_key == "your_openai_key_here":
        return {image_path: None for image_path in image_paths}

    cache = _load_cache(config)
    results: dict[Path, ExportIssueSuggestion | None] = {}
    pending: dict[Path, tuple[Path, str]] = {}
    for original_path, resolved_path in resolved_paths.items():
        if not resolved_path.exists():
            results[original_path] = None
            continue
        cache_key = _cache_key_for_path(resolved_path)
        cached_value = cache.get(cache_key)
        if isinstance(cached_value, dict):
            cached_suggestion = _suggestion_from_dict(cached_value)
            if cached_suggestion is not None:
                results[original_path] = cached_suggestion
                continue
        pending[original_path] = (resolved_path, cache_key)

    if not pending:
        return results

    workers = max(1, int(os.getenv("FAMILY_PHOTO_EXPORT_AUDIT_WORKERS", str(DEFAULT_EXPORT_AUDIT_WORKERS)).strip() or DEFAULT_EXPORT_AUDIT_WORKERS))
    model = os.getenv("FAMILY_PHOTO_EXPORT_AUDIT_MODEL", DEFAULT_EXPORT_AUDIT_MODEL).strip() or DEFAULT_EXPORT_AUDIT_MODEL
    with ThreadPoolExecutor(max_workers=min(workers, len(pending)), thread_name_prefix="audit-model") as executor:
        future_to_original = {
            executor.submit(
                _safe_request_issue_suggestion,
                api_key=api_key,
                model=model,
                original_path=original_path,
                resolved_path=resolved_path,
            ): (original_path, cache_key)
            for original_path, (resolved_path, cache_key) in pending.items()
        }
        for future in as_completed(future_to_original):
            original_path, cache_key = future_to_original[future]
            suggestion = future.result()
            results[original_path] = suggestion
            if suggestion is not None:
                cache[cache_key] = asdict(suggestion)

    _write_cache(config, cache)
    return results


def classify_merge_candidate(
    config: AppConfig,
    *,
    image_path: Path,
) -> ExportIssueSuggestion | None:
    """Backward-compatible merge-only helper."""
    suggestion = classify_export_issue(config, image_path=image_path)
    if suggestion is None or suggestion.issue_code != "MERGE":
        return None
    return suggestion


def _safe_request_issue_suggestion(
    *,
    api_key: str,
    model: str,
    original_path: Path,
    resolved_path: Path,
) -> ExportIssueSuggestion | None:
    try:
        return _request_issue_suggestion(
            api_key=api_key,
            model=model,
            image_path=resolved_path,
        )
    except (OSError, ValueError, error.URLError, TimeoutError) as exc:
        LOGGER.warning("export audit model unavailable for %s: %s", original_path, exc)
        return None


def _request_issue_suggestion(
    *,
    api_key: str,
    model: str,
    image_path: Path,
) -> ExportIssueSuggestion:
    if not image_path.exists():
        raise ValueError(f"Export audit image was not found: {image_path}")

    payload = {
        "model": model,
        "input": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": (
                            "You are auditing family-photo staging exports for digital frame review. "
                            "Choose the single best issue code for this staged image from: "
                            "OK, MERGE, CROP, SKEW, RR90, RL90, R180. "
                            "Use MERGE when the image contains multiple adjacent printed photos that should be split. "
                            "Use CROP when the framing is wrong or content is clipped. "
                            "Use SKEW when the image is tilted by a small angle. "
                            "Use RR90, RL90, or R180 only when a cardinal rotation is clearly needed. "
                            "Return JSON only with keys issue_code, confidence, and reason. "
                            "issue_code must be one of the allowed codes. "
                            "confidence must be a number between 0 and 1. "
                            "Prefer OK if there is no clear issue."
                        ),
                    },
                    {
                        "type": "input_image",
                        "image_url": _data_url_for_image(image_path),
                        "detail": "low",
                    },
                ],
            }
        ],
        "max_output_tokens": 120,
    }
    req = request.Request(
        "https://api.openai.com/v1/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with request.urlopen(req, timeout=60) as response:
        body = response.read().decode("utf-8")
    return _parse_issue_suggestion_response(body)


def _parse_issue_suggestion_response(body: str) -> ExportIssueSuggestion:
    payload = json.loads(body)
    text = _extract_output_text(payload)
    if not text:
        raise ValueError("Export audit model returned no text output.")
    match = _JSON_OBJECT_RE.search(text)
    if match is None:
        raise ValueError(f"Export audit model returned non-JSON output: {text}")
    return _parse_issue_suggestion_json(match.group(0))


def _extract_output_text(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for item in payload.get("output", []):
        if not isinstance(item, dict):
            continue
        content = item.get("content")
        if isinstance(content, list):
            for content_item in content:
                if not isinstance(content_item, dict):
                    continue
                if content_item.get("type") == "output_text":
                    text = str(content_item.get("text", "")).strip()
                    if text:
                        parts.append(text)
        if item.get("type") == "output_text":
            text = str(item.get("text", "")).strip()
            if text:
                parts.append(text)
    return "\n".join(parts).strip()


def _parse_merge_decision_response(body: str) -> ExportIssueSuggestion:
    """Backward-compatible parser alias."""
    return _parse_issue_suggestion_response(body)


def _parse_issue_suggestion_json(raw_json: str) -> ExportIssueSuggestion:
    payload = json.loads(raw_json)
    return _suggestion_from_dict(payload, strict=True)


def _suggestion_from_dict(
    payload: dict[str, Any],
    *,
    strict: bool = False,
) -> ExportIssueSuggestion | None:
    issue_code = str(payload.get("issue_code", payload.get("classification", ""))).strip().upper()
    if issue_code == "SINGLE":
        issue_code = "OK"
    if issue_code not in SUPPORTED_ISSUE_CODES:
        if strict:
            raise ValueError(f"Invalid export audit issue code: {issue_code!r}")
        return None

    confidence_raw = payload.get("confidence", 0.0)
    try:
        confidence = float(confidence_raw)
    except (TypeError, ValueError):
        if strict:
            raise ValueError(f"Invalid export audit confidence: {confidence_raw!r}") from None
        return None
    confidence = max(0.0, min(1.0, confidence))

    reason = str(payload.get("reason", "")).strip()
    if not reason:
        reason = "model-assisted export audit"

    return ExportIssueSuggestion(
        issue_code=issue_code,
        confidence=confidence,
        reason=reason,
    )


def _data_url_for_image(image_path: Path) -> str:
    mime_type = "image/jpeg"
    if image_path.suffix.lower() == ".png":
        mime_type = "image/png"
    encoded = base64.b64encode(image_path.read_bytes()).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def _load_cache(config: AppConfig) -> dict[str, dict[str, Any]]:
    cache_path = _cache_path(config)
    if not cache_path.exists():
        return {}
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return {str(key): value for key, value in payload.items() if isinstance(value, dict)}


def _write_cache(config: AppConfig, cache: dict[str, dict[str, Any]]) -> None:
    cache_path = _cache_path(config)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(cache, indent=2, sort_keys=True), encoding="utf-8")


def _cache_path(config: AppConfig) -> Path:
    return config.photos_root / "exports" / "staging" / EXPORT_AUDIT_CACHE_NAME


def _cache_key_for_path(image_path: Path) -> str:
    stat_result = image_path.stat()
    return f"{image_path.resolve()}::{stat_result.st_size}::{stat_result.st_mtime_ns}"


def _resolve_image_path(config: AppConfig, image_path: Path) -> Path:
    if image_path.is_absolute():
        return image_path
    return config.photos_root.parent / image_path
