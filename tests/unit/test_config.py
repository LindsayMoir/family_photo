from __future__ import annotations

from pathlib import Path

import pytest

from config import ConfigError, DEFAULT_DATABASE_NAME, load_config


def test_load_config_uses_defaults_when_environment_is_empty(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.delenv("PHOTO_DB_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("PHOTOS_ROOT", raising=False)
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.chdir(tmp_path)

    config = load_config()

    assert config.environment == "development"
    assert config.database_url == f"postgresql://localhost:5432/{DEFAULT_DATABASE_NAME}"
    assert config.photos_root == Path("photos")
    assert config.log_level == "INFO"


def test_load_config_rejects_non_postgresql_database_urls(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PHOTO_DB_URL", "sqlite:///tmp/test.db")
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(ConfigError, match="must start with 'postgresql://'"):
        load_config()
