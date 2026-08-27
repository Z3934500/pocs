"""MLOps layer: model scores, run registry and feature drift.

Rewritten on every pipeline run by build_mlops(), so it is part of the
analytics set that reset_tables() truncates.
"""

from __future__ import annotations

TABLES: list[str] = [
    "gold_customer_model_scores",
    "ml_model_runs",
    "ml_feature_drift",
]

DDL = """
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
