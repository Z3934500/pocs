"""Bronze layer: raw event landing.

Rebuilt from source on every pipeline run, so it belongs to the analytics set
that reset_tables() truncates.
"""

from __future__ import annotations

TABLES: list[str] = [
    "bronze_events",
]

DDL = """
CREATE TABLE IF NOT EXISTS bronze_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    source_system TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    ingested_at TEXT NOT NULL
);
"""
