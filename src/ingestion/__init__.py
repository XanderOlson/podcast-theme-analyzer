"""Ingestion package exposing database utilities for the MVP build."""

from .db import initialize_database, open_database

__all__ = ["initialize_database", "open_database"]

