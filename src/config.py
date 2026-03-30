"""Configuration loading for the family photo CLI."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


DEFAULT_DATABASE_NAME = "photo_db"
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_PHOTOS_ROOT = "photos"


class ConfigError(ValueError):
    """Raised when required configuration is invalid."""


@dataclass(frozen=True, slots=True)
class AppConfig:
    """Application configuration loaded from the environment."""

    environment: str
    database_url: str
    photos_root: Path
    log_level: str

    @property
    def expected_database_name(self) -> str:
        """Return the expected PostgreSQL database name."""
        return DEFAULT_DATABASE_NAME


def load_config() -> AppConfig:
    """Load application configuration from environment variables."""
    _load_dotenv_file()

    environment = os.getenv("ENV", "development").strip() or "development"
    database_url = (
        os.getenv("PHOTO_DB_URL")
        or os.getenv("DATABASE_URL")
        or f"postgresql://localhost:5432/{DEFAULT_DATABASE_NAME}"
    ).strip()
    photos_root = Path(os.getenv("PHOTOS_ROOT", DEFAULT_PHOTOS_ROOT)).expanduser()
    log_level = os.getenv("LOG_LEVEL", DEFAULT_LOG_LEVEL).strip().upper() or DEFAULT_LOG_LEVEL

    _validate_database_url(database_url)
    _validate_photos_root(photos_root)
    _validate_log_level(log_level)

    return AppConfig(
        environment=environment,
        database_url=database_url,
        photos_root=photos_root,
        log_level=log_level,
    )


def _load_dotenv_file() -> None:
    dotenv_path = Path(".env")
    if not dotenv_path.exists():
        return

    for line in dotenv_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        if not key:
            continue
        os.environ.setdefault(key, value.strip())


def _validate_database_url(database_url: str) -> None:
    if not database_url:
        raise ConfigError("Database URL is empty. Set PHOTO_DB_URL or DATABASE_URL.")
    if not database_url.startswith("postgresql://"):
        raise ConfigError("Database URL must start with 'postgresql://'.")


def _validate_photos_root(photos_root: Path) -> None:
    if not str(photos_root):
        raise ConfigError("PHOTOS_ROOT must not be empty.")


def _validate_log_level(log_level: str) -> None:
    valid_levels = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
    if log_level not in valid_levels:
        raise ConfigError(
            f"Invalid LOG_LEVEL '{log_level}'. Expected one of: {', '.join(sorted(valid_levels))}."
        )
