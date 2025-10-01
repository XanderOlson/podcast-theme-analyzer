"""Database utilities for the ingestion pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import duckdb
from duckdb import DuckDBPyConnection


DEFAULT_DB_PATH = Path("data") / "ingestion.duckdb"


@dataclass(frozen=True)
class Migration:
    """Single schema migration represented by a set of SQL statements."""

    name: str
    statements: tuple[str, ...]


_MIGRATIONS: tuple[Migration, ...] = (
    Migration(
        name="0001_initial_schema",
        statements=(
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
        ),
    ),
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
    _ensure_schema_migrations_table(connection)
    _run_pending_migrations(connection, _MIGRATIONS)
    return connection


def _ensure_schema_migrations_table(connection: DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            migration_name TEXT PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """.strip()
    )


def _run_pending_migrations(
    connection: DuckDBPyConnection, migrations: Iterable[Migration]
) -> None:
    applied = {
        row[0]
        for row in connection.execute(
            "SELECT migration_name FROM schema_migrations"
        ).fetchall()
    }

    for migration in migrations:
        if migration.name in applied:
            continue
        _apply_migration(connection, migration)


def _apply_migration(connection: DuckDBPyConnection, migration: Migration) -> None:
    connection.execute("BEGIN TRANSACTION")
    try:
        for statement in migration.statements:
            connection.execute(statement)
        connection.execute(
            "INSERT INTO schema_migrations (migration_name) VALUES (?)",
            (migration.name,),
        )
    except Exception:  # pragma: no cover - defensive rollback
        connection.execute("ROLLBACK")
        raise
    else:
        connection.execute("COMMIT")

