"""Filesystem discovery and metadata extraction for ingest."""

from __future__ import annotations

from hashlib import sha256
import re
from pathlib import Path

from PIL import Image

from ingest.models import DiscoveredScan


JPEG_SUFFIXES = {".jpg", ".jpeg"}


def discover_scans(input_path: Path) -> list[DiscoveredScan]:
    """Discover JPEG scans under a file or directory input."""
    resolved = input_path.expanduser().resolve()
    if resolved.is_file():
        scan_paths = [resolved]
    else:
        scan_paths = sorted(
            (
                path.resolve()
                for path in resolved.rglob("*")
                if path.is_file() and path.suffix.lower() in JPEG_SUFFIXES
            ),
            key=_natural_sort_key,
        )

    if not scan_paths:
        raise ValueError(f"No JPEG scans found under: {resolved}")

    return [_read_scan_metadata(path) for path in scan_paths]


def _read_scan_metadata(path: Path) -> DiscoveredScan:
    try:
        with Image.open(path) as image:
            width_px, height_px = image.size
            dpi_x, dpi_y = _normalize_dpi(image.info.get("dpi"))
    except OSError as exc:
        raise ValueError(f"Unable to read JPEG metadata from '{path}': {exc}") from exc

    return DiscoveredScan(
        absolute_path=path,
        original_filename=path.name,
        width_px=width_px,
        height_px=height_px,
        dpi_x=dpi_x,
        dpi_y=dpi_y,
        content_hash=_sha256_file(path),
    )


def _normalize_dpi(dpi: object) -> tuple[int | None, int | None]:
    if not isinstance(dpi, tuple) or len(dpi) != 2:
        return None, None

    normalized: list[int | None] = []
    for value in dpi:
        if isinstance(value, (int, float)) and value > 0:
            normalized.append(int(round(value)))
        else:
            normalized.append(None)
    return normalized[0], normalized[1]


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _natural_sort_key(path: Path) -> tuple[object, ...]:
    parts = re.split(r"(\d+)", path.name.lower())
    key: list[object] = []
    for part in parts:
        key.append(int(part) if part.isdigit() else part)
    return tuple(key)
