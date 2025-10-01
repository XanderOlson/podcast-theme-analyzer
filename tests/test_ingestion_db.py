from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

import pytest

pytest.importorskip("duckdb")

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


def test_shows_table_schema(migrated_connection) -> None:
    assert _column_names(migrated_connection, "shows") == [
        "show_id",
        "title",
        "canonical_rss_url",
        "publisher",
        "lang",
        "last_crawl_at",
    ]


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
        rows = second.execute("SELECT show_id, title FROM shows").fetchall()
        assert rows == [("show-1", "Test Show")]
    finally:
        second.close()

