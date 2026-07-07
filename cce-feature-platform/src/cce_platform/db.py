from __future__ import annotations

import sqlite3
from pathlib import Path

from .config import settings


SCHEMA = """
CREATE TABLE IF NOT EXISTS bronze_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    source_system TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    ingested_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS identity_crosswalk (
    id_type TEXT NOT NULL,
    id_value TEXT NOT NULL,
    unified_customer_key TEXT NOT NULL,
    source_customer_ref TEXT,
    PRIMARY KEY (id_type, id_value)
);

CREATE TABLE IF NOT EXISTS dim_customer (
    unified_customer_key TEXT PRIMARY KEY,
    primary_name TEXT NOT NULL,
    customer_type TEXT NOT NULL,
    first_seen_date TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_transaction (
    txn_id TEXT PRIMARY KEY,
    unified_customer_key TEXT NOT NULL,
    txn_ts TEXT NOT NULL,
    product TEXT NOT NULL,
    amount REAL NOT NULL,
    channel TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_policy (
    policy_id TEXT PRIMARY KEY,
    unified_customer_key TEXT NOT NULL,
    policy_type TEXT NOT NULL,
    policy_status TEXT NOT NULL,
    effective_date TEXT NOT NULL,
    premium_amount REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS silver_identity_candidates (
    candidate_id TEXT PRIMARY KEY,
    left_ref TEXT NOT NULL,
    right_ref TEXT NOT NULL,
    left_identity TEXT NOT NULL,
    right_identity TEXT NOT NULL,
    left_unified_customer_key TEXT,
    right_unified_customer_key TEXT,
    match_score REAL NOT NULL,
    match_reason TEXT NOT NULL,
    resolution_action TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gold_customer_features (
    unified_customer_key TEXT PRIMARY KEY,
    recency_days INTEGER NOT NULL,
    tx_count_30d INTEGER NOT NULL,
    monetary_30d REAL NOT NULL,
    product_diversity INTEGER NOT NULL,
    velocity_7d INTEGER NOT NULL,
    cluster_id INTEGER NOT NULL,
    segment_name TEXT NOT NULL,
    risk_score REAL NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gold_policy_features (
    policy_id TEXT PRIMARY KEY,
    unified_customer_key TEXT NOT NULL,
    policy_type TEXT NOT NULL,
    policy_status TEXT NOT NULL,
    policy_tenure_days INTEGER NOT NULL,
    premium_amount REAL NOT NULL,
    claim_count_12m INTEGER NOT NULL,
    renewal_due_days INTEGER NOT NULL,
    lapse_risk_score REAL NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gold_campaign_eligibility (
    campaign_id TEXT NOT NULL,
    unified_customer_key TEXT NOT NULL,
    is_eligible INTEGER NOT NULL,
    reason TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (campaign_id, unified_customer_key)
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

CREATE TABLE IF NOT EXISTS gold_customer_model_scores (
    unified_customer_key TEXT NOT NULL,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    propensity_score REAL NOT NULL,
    risk_band TEXT NOT NULL,
    score_explanation TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (unified_customer_key, model_name, model_version)
);

CREATE TABLE IF NOT EXISTS ml_model_runs (
    model_run_id TEXT PRIMARY KEY,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    training_rows INTEGER NOT NULL,
    feature_table TEXT NOT NULL,
    target_definition TEXT NOT NULL,
    auc REAL NOT NULL,
    precision_at_20 REAL NOT NULL,
    status TEXT NOT NULL,
    artifact_uri TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ml_feature_drift (
    drift_id TEXT PRIMARY KEY,
    feature_name TEXT NOT NULL,
    baseline_mean REAL NOT NULL,
    current_mean REAL NOT NULL,
    drift_ratio REAL NOT NULL,
    severity TEXT NOT NULL,
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
        "bronze_events",
        "identity_crosswalk",
        "dim_customer",
        "fact_transaction",
        "dim_policy",
        "silver_identity_candidates",
        "gold_customer_features",
        "gold_policy_features",
        "gold_campaign_eligibility",
        "dq_issues",
        "gold_customer_model_scores",
        "ml_model_runs",
        "ml_feature_drift",
    ]
    for table_name in table_names:
        conn.execute(f"DELETE FROM {table_name}")
    conn.commit()
