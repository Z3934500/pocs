"""Gold layer: serving-ready customer, policy and campaign features.

Fully derived from Silver by the deterministic pipeline, so these tables are
rebuilt from scratch on every run.
"""

from __future__ import annotations

TABLES: list[str] = [
    "gold_customer_features",
    "gold_policy_features",
    "gold_campaign_eligibility",
]

DDL = """
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
"""
