"""Data quality layer: issues raised by the pipeline's DQ checks.

Regenerated on every run alongside the layers the checks inspect.
"""

from __future__ import annotations

TABLES: list[str] = [
    "dq_issues",
]

DDL = """
CREATE TABLE IF NOT EXISTS dq_issues (
    issue_id TEXT PRIMARY KEY,
    layer TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    severity TEXT NOT NULL,
    issue_type TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""
