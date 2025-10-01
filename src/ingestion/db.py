"""Database utilities for the ingestion pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import duckdb
from duckdb import DuckDBPyConnection


DEFAULT_DB_PATH = Path("data") / "ingestion.duckdb"

_DDL_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS shows (
        show_id TEXT PRIMARY KEY,
        title TEXT,
        canonical_rss_url TEXT UNIQUE,
        publisher TEXT,
        lang TEXT,
        last_crawl_at TIMESTAMP
    )
    """.strip(),
    """
    CREATE TABLE IF NOT EXISTS episodes (
        episode_id TEXT PRIMARY KEY,
        show_id TEXT REFERENCES shows(show_id),
        guid TEXT,
        title TEXT,
        pubdate TIMESTAMP,
        duration_s INTEGER,
        audio_url TEXT,
        transcript_url TEXT,
        enclosure_type TEXT,
        explicit BOOLEAN,
        episode_type TEXT,
        season_n INTEGER,
        episode_n INTEGER,
        first_seen_at TIMESTAMP,
        last_seen_at TIMESTAMP,
        tombstoned_bool BOOLEAN
    )
    """.strip(),
    """
    CREATE TABLE IF NOT EXISTS source_meta (
        resource_url TEXT PRIMARY KEY,
        etag TEXT,
        last_modified TEXT,
        last_status INTEGER,
        last_fetch_ts TIMESTAMP,
        content_sha256 TEXT,
        bytes INTEGER
    )
    """.strip(),
    """
    CREATE TABLE IF NOT EXISTS provenance (
        object_type TEXT,
        object_id TEXT,
        source_url TEXT,
        fetched_at TIMESTAMP,
        parser_version TEXT,
        notes TEXT,
        PRIMARY KEY (object_type, object_id, source_url, fetched_at)
    )
    """.strip(),
)


def open_database(db_path: Path = DEFAULT_DB_PATH) -> DuckDBPyConnection:
    """Open (and create if necessary) the ingestion DuckDB database."""

    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(db_path))
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def initialize_database(db_path: Path = DEFAULT_DB_PATH) -> DuckDBPyConnection:
    """Ensure the database exists and run all schema migrations."""

    connection = open_database(db_path)
    _run_schema_statements(connection, _DDL_STATEMENTS)
    return connection


def _run_schema_statements(
    connection: DuckDBPyConnection, statements: Iterable[str]
) -> None:
    connection.execute("BEGIN TRANSACTION")
    try:
        for statement in statements:
            connection.execute(statement)
    except Exception:  # pragma: no cover - defensive rollback
        connection.execute("ROLLBACK")
        raise
    else:
        connection.execute("COMMIT")

