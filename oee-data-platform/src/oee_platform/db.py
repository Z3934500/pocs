from __future__ import annotations

import sqlite3
from pathlib import Path

from .config import settings


SCHEMA = """
CREATE TABLE IF NOT EXISTS bronze_api_events (
    event_id TEXT PRIMARY KEY,
    site TEXT NOT NULL,
    interface_name TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    ingested_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_site (
    site_code TEXT PRIMARY KEY,
    site_name TEXT NOT NULL,
    country TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_machine (
    machine_number TEXT PRIMARY KEY,
    site_code TEXT NOT NULL,
    line_name TEXT NOT NULL,
    machine_type TEXT NOT NULL,
    ideal_cycle_seconds REAL NOT NULL,
    planned_minutes_per_shift INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_machine_event (
    event_id TEXT PRIMARY KEY,
    site_code TEXT NOT NULL,
    shift_date TEXT NOT NULL,
    shift_code TEXT NOT NULL,
    machine_number TEXT NOT NULL,
    produced_qty INTEGER NOT NULL,
    good_qty INTEGER NOT NULL,
    runtime_minutes REAL NOT NULL,
    downtime_minutes REAL NOT NULL,
    downtime_reason TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_oee_daily (
    site_code TEXT NOT NULL,
    shift_date TEXT NOT NULL,
    machine_number TEXT NOT NULL,
    availability REAL NOT NULL,
    performance REAL NOT NULL,
    quality REAL NOT NULL,
    oee REAL NOT NULL,
    produced_qty INTEGER NOT NULL,
    downtime_minutes REAL NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (site_code, shift_date, machine_number)
);

CREATE TABLE IF NOT EXISTS fact_downtime_reason (
    site_code TEXT NOT NULL,
    shift_date TEXT NOT NULL,
    downtime_reason TEXT NOT NULL,
    downtime_minutes REAL NOT NULL,
    PRIMARY KEY (site_code, shift_date, downtime_reason)
);

CREATE TABLE IF NOT EXISTS anomaly_alerts (
    alert_id TEXT PRIMARY KEY,
    site_code TEXT NOT NULL,
    machine_number TEXT NOT NULL,
    shift_date TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value REAL NOT NULL,
    severity TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TEXT NOT NULL
);

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


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    path = db_path or settings.sqlite_path
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


def reset_tables(conn: sqlite3.Connection) -> None:
    table_names = [
        "bronze_api_events",
        "dim_site",
        "dim_machine",
        "fact_machine_event",
        "fact_oee_daily",
        "fact_downtime_reason",
        "anomaly_alerts",
        "dq_issues",
    ]
    for table_name in table_names:
        conn.execute(f"DELETE FROM {table_name}")
    conn.commit()
