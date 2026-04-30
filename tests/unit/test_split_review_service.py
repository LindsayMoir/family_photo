from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

from audit.split_review_service import import_split_review_csv, write_split_review_csv


def test_write_split_review_csv_writes_current_staging_entries(app_config, monkeypatch) -> None:
    class _FakeCursor:
        def __init__(self) -> None:
            self._rows = [
                (952, "photos/exports/staging/portrait/photo_952.jpg"),
                (951, "photos/exports/staging/landscape/photo_951.jpg"),
            ]

        def execute(self, query, params=None) -> None:
            return None

        def fetchall(self):
            return self._rows

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeConn:
        def cursor(self):
            return _FakeCursor()

    @contextmanager
    def _fake_connect(_config):
        yield _FakeConn()

    monkeypatch.setattr("audit.split_review_service.connect", _fake_connect)

    summary = write_split_review_csv(
        app_config,
        csv_path=None,
        dry_run=False,
    )

    assert summary.row_count == 2
    assert summary.csv_path == app_config.photos_root / "exports" / "staging" / "split_review.csv"
    lines = summary.csv_path.read_text(encoding="utf-8").splitlines()
    assert lines == [
        "image_name,Split",
        "photo_951.jpg,N",
        "photo_952.jpg,N",
    ]


def test_import_split_review_csv_applies_only_y_rows(app_config, monkeypatch, tmp_path) -> None:
    csv_path = tmp_path / "split_review.csv"
    csv_path.write_text(
        "\n".join(
            [
                "image_name,Split",
                "photo_951.jpg,Y",
                "photo_952.jpg,N",
                "photo_953.jpg,Y",
            ]
        ),
        encoding="utf-8",
    )

    class _FakeCursor:
        def __init__(self) -> None:
            self._rows = [
                (951, "photos/exports/staging/landscape/photo_951.jpg"),
                (952, "photos/exports/staging/portrait/photo_952.jpg"),
            ]

        def execute(self, query, params=None) -> None:
            return None

        def fetchall(self):
            return self._rows

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class _FakeConn:
        def cursor(self):
            return _FakeCursor()

    @contextmanager
    def _fake_connect(_config):
        yield _FakeConn()

    split_calls: list[tuple[int, str | None, bool]] = []

    monkeypatch.setattr("audit.split_review_service.connect", _fake_connect)
    monkeypatch.setattr(
        "audit.split_review_service.auto_split_photo",
        lambda config, photo_id, note, dry_run: split_calls.append((photo_id, note, dry_run))
        or type(
            "Summary",
            (),
            {
                "staged_photo_ids": (photo_id, 1200),
            },
        )(),
    )

    summary = import_split_review_csv(
        app_config,
        csv_path=csv_path,
        dry_run=False,
    )

    assert split_calls == [(951, "split review csv applied", False)]
    assert summary.processed_rows == 3
    assert summary.requested_split_count == 1
    assert summary.applied_split_count == 1
    assert summary.unresolved_count == 1
    assert summary.created_photo_count == 1


def test_import_split_review_csv_requires_expected_columns(app_config, tmp_path) -> None:
    csv_path = tmp_path / "split_review.csv"
    csv_path.write_text("image_name,Flag\nphoto_951.jpg,Y\n", encoding="utf-8")

    try:
        import_split_review_csv(
            app_config,
            csv_path=csv_path,
            dry_run=True,
        )
    except ValueError as exc:
        assert str(exc) == "Split review CSV must include image_name and Split columns."
    else:
        raise AssertionError("Expected missing Split column to raise ValueError.")
