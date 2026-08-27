"""Silver layer: identity resolution and conformed dimensions/facts.

Derived from Bronze by the deterministic pipeline, so every table here is
reproducible and safe to truncate before a rebuild.
"""

from __future__ import annotations

TABLES: list[str] = [
    "identity_crosswalk",
    "dim_customer",
    "fact_transaction",
    "dim_policy",
    "silver_identity_candidates",
]

DDL = """
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
"""
