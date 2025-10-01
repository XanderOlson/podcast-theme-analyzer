from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

import pytest

duckdb = pytest.importorskip("duckdb")

from ingestion.db import initialize_database, open_database


def test_open_database_creates_file(tmp_path: Path) -> None:
    db_path = tmp_path / "nested" / "ingestion.duckdb"

    connection = open_database(db_path)
    try:
        assert db_path.exists()
        assert connection.execute("SELECT 1").fetchone() == (1,)
    finally:
        connection.close()


@pytest.fixture()
def migrated_connection(tmp_path: Path):
    db_path = tmp_path / "ingestion.duckdb"
    connection = initialize_database(db_path)
    try:
        yield connection
    finally:
        connection.close()


def _column_names(connection, table: str) -> list[str]:
    info = connection.execute(f"PRAGMA table_info('{table}')").fetchall()
    return [row[1] for row in info]


def _column_types(connection, table: str) -> dict[str, tuple[str, bool]]:
    """Return column types and whether the column is part of the primary key."""

    info = connection.execute(f"PRAGMA table_info('{table}')").fetchall()
    return {row[1]: (row[2], bool(row[5])) for row in info}


def test_shows_table_schema(migrated_connection) -> None:
    assert _column_names(migrated_connection, "shows") == [
        "show_id",
        "title",
        "canonical_rss_url",
        "publisher",
        "lang",
        "last_crawl_at",
    ]


def test_shows_table_types_and_primary_key(migrated_connection) -> None:
    info = _column_types(migrated_connection, "shows")

    assert info == {
        "show_id": ("TEXT", True),
        "title": ("TEXT", False),
        "canonical_rss_url": ("TEXT", False),
        "publisher": ("TEXT", False),
        "lang": ("TEXT", False),
        "last_crawl_at": ("TIMESTAMP", False),
    }


def test_episodes_table_schema(migrated_connection) -> None:
    assert _column_names(migrated_connection, "episodes") == [
        "episode_id",
        "show_id",
        "guid",
        "title",
        "pubdate",
        "duration_s",
        "audio_url",
        "transcript_url",
        "enclosure_type",
        "explicit",
        "episode_type",
        "season_n",
        "episode_n",
        "first_seen_at",
        "last_seen_at",
        "tombstoned_bool",
    ]


def test_episodes_table_types_and_primary_key(migrated_connection) -> None:
    info = _column_types(migrated_connection, "episodes")

    assert info == {
        "episode_id": ("TEXT", True),
        "show_id": ("TEXT", False),
        "guid": ("TEXT", False),
        "title": ("TEXT", False),
        "pubdate": ("TIMESTAMP", False),
        "duration_s": ("INTEGER", False),
        "audio_url": ("TEXT", False),
        "transcript_url": ("TEXT", False),
        "enclosure_type": ("TEXT", False),
        "explicit": ("BOOLEAN", False),
        "episode_type": ("TEXT", False),
        "season_n": ("INTEGER", False),
        "episode_n": ("INTEGER", False),
        "first_seen_at": ("TIMESTAMP", False),
        "last_seen_at": ("TIMESTAMP", False),
        "tombstoned_bool": ("BOOLEAN", False),
    }


def test_source_meta_insert_select(migrated_connection) -> None:
    migrated_connection.execute(
        """
        INSERT INTO source_meta (
            resource_url, etag, last_modified, last_status, last_fetch_ts,
            content_sha256, bytes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "https://example.com/feed.xml",
            "etag-value",
            "Wed, 21 Oct 2015 07:28:00 GMT",
            200,
            "2024-01-01 00:00:00",
            "abc123",
            1024,
        ),
    )

    row = (
        migrated_connection.execute(
            "SELECT * FROM source_meta WHERE resource_url = ?",
            ("https://example.com/feed.xml",),
        ).fetchone()
    )

    assert row == (
        "https://example.com/feed.xml",
        "etag-value",
        "Wed, 21 Oct 2015 07:28:00 GMT",
        200,
        datetime(2024, 1, 1, 0, 0),
        "abc123",
        1024,
    )


def test_source_meta_table_schema(migrated_connection) -> None:
    assert _column_names(migrated_connection, "source_meta") == [
        "resource_url",
        "etag",
        "last_modified",
        "last_status",
        "last_fetch_ts",
        "content_sha256",
        "bytes",
    ]


def test_source_meta_table_types_and_primary_key(migrated_connection) -> None:
    info = _column_types(migrated_connection, "source_meta")

    assert info == {
        "resource_url": ("TEXT", True),
        "etag": ("TEXT", False),
        "last_modified": ("TEXT", False),
        "last_status": ("INTEGER", False),
        "last_fetch_ts": ("TIMESTAMP", False),
        "content_sha256": ("TEXT", False),
        "bytes": ("INTEGER", False),
    }


def test_provenance_table_schema(migrated_connection) -> None:
    assert _column_names(migrated_connection, "provenance") == [
        "object_type",
        "object_id",
        "source_url",
        "fetched_at",
        "parser_version",
        "notes",
    ]


def test_provenance_table_types_and_primary_key(migrated_connection) -> None:
    info = _column_types(migrated_connection, "provenance")

    assert info == {
        "object_type": ("TEXT", True),
        "object_id": ("TEXT", True),
        "source_url": ("TEXT", True),
        "fetched_at": ("TIMESTAMP", True),
        "parser_version": ("TEXT", False),
        "notes": ("TEXT", False),
    }


def test_provenance_insert_select(migrated_connection) -> None:
    migrated_connection.execute(
        """
        INSERT INTO provenance (
            object_type, object_id, source_url, fetched_at, parser_version, notes
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            "episode",
            "episode-123",
            "https://example.com/feed.xml",
            "2024-01-02 12:00:00",
            "1.0.0",
            "initial import",
        ),
    )

    row = (
        migrated_connection.execute(
            "SELECT * FROM provenance WHERE object_id = ?",
            ("episode-123",),
        ).fetchone()
    )

    assert row == (
        "episode",
        "episode-123",
        "https://example.com/feed.xml",
        datetime(2024, 1, 2, 12, 0),
        "1.0.0",
        "initial import",
    )


def test_shows_canonical_rss_url_unique(migrated_connection) -> None:
    migrated_connection.execute(
        "INSERT INTO shows (show_id, canonical_rss_url) VALUES (?, ?)",
        ("show-1", "https://example.com/feed.xml"),
    )

    with pytest.raises(duckdb.ConstraintException):
        migrated_connection.execute(
            "INSERT INTO shows (show_id, canonical_rss_url) VALUES (?, ?)",
            ("show-2", "https://example.com/feed.xml"),
        )


def test_episodes_show_id_foreign_key_enforced(migrated_connection) -> None:
    migrated_connection.execute(
        "INSERT INTO shows (show_id, title) VALUES (?, ?)",
        ("show-1", "Test Show"),
    )

    with pytest.raises(duckdb.ConstraintException):
        migrated_connection.execute(
            "INSERT INTO episodes (episode_id, show_id, title) VALUES (?, ?, ?)",
            ("episode-missing-show", "missing", "Bad Episode"),
        )

    migrated_connection.execute(
        "INSERT INTO episodes (episode_id, show_id, title) VALUES (?, ?, ?)",
        ("episode-1", "show-1", "Good Episode"),
    )

    rows = migrated_connection.execute(
        "SELECT episode_id, show_id, title FROM episodes WHERE episode_id = ?",
        ("episode-1",),
    ).fetchall()

    assert rows == [("episode-1", "show-1", "Good Episode")]


def test_schema_migrations_recorded(migrated_connection) -> None:
    rows = migrated_connection.execute(
        "SELECT migration_name FROM schema_migrations ORDER BY applied_at"
    ).fetchall()

    assert rows == [("0001_initial_schema",)]


def test_initialize_database_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "ingestion.duckdb"

    first = initialize_database(db_path)
    try:
        first.execute(
            "INSERT INTO shows (show_id, title) VALUES (?, ?)",
            ("show-1", "Test Show"),
        )
    finally:
        first.close()

    second = initialize_database(db_path)
    try:
        shows = second.execute("SELECT show_id, title FROM shows").fetchall()
        migrations = second.execute(
            "SELECT migration_name FROM schema_migrations"
        ).fetchall()
        assert shows == [("show-1", "Test Show")]
        assert migrations == [("0001_initial_schema",)]
    finally:
        second.close()

