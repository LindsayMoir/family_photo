"""Database connection helpers."""

from __future__ import annotations

from contextlib import contextmanager

from config import AppConfig

import psycopg2
from psycopg2.extensions import connection as PgConnection


def get_database_url(config: AppConfig) -> str:
    """Return the configured database URL."""
    return config.database_url


@contextmanager
def connect(config: AppConfig) -> PgConnection:
    """Open a PostgreSQL connection for the configured database."""
    conn = psycopg2.connect(config.database_url)
    try:
        yield conn
    finally:
        conn.close()
