"""Operational layer: transactional outbox and T+N settlement schedule.

These tables are NOT derived from anything. They hold in-flight operational
state — events not yet published downstream, and trades not yet settled — that
no pipeline can recompute. Truncating them would silently drop undelivered
events and lose settlement obligations, so they are deliberately excluded from
`ANALYTICS_TABLES` and therefore from `reset_tables()`.

In production this layer lives in a separate database from the analytics
layers; keeping it in its own module makes that split a one-line change to
`init_schema` rather than a hunt through one large DDL string.
"""

from __future__ import annotations

TABLES: list[str] = [
    "outbox_events",
    "settlement_schedule",
]

DDL = """
-- Transactional Outbox: written in the same local transaction as the business
-- state change; a background EventPublisher polls and forwards to downstream.
CREATE TABLE IF NOT EXISTS outbox_events (
    event_id      TEXT PRIMARY KEY,
    aggregate_type TEXT NOT NULL,   -- e.g. 'order', 'cart', 'policy'
    aggregate_id  TEXT NOT NULL,   -- e.g. order_id / txn_id
    event_type    TEXT NOT NULL,   -- e.g. 'OrderPaid', 'CartAbandoned'
    payload_json  TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING | SENT | FAILED
    retry_count   INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT NOT NULL,
    sent_at       TEXT
);

CREATE INDEX IF NOT EXISTS idx_outbox_status_created
    ON outbox_events (status, created_at);

-- Settlement schedule: ZSET equivalent in SQLite for T+2 delayed clearing.
-- score = settle_date epoch seconds; trigger service polls WHERE settle_ts <= now.
CREATE TABLE IF NOT EXISTS settlement_schedule (
    txn_id          TEXT PRIMARY KEY,
    unified_customer_key TEXT NOT NULL,
    product         TEXT NOT NULL,
    amount          REAL NOT NULL,
    trade_date      TEXT NOT NULL,   -- ISO date YYYY-MM-DD
    settle_date     TEXT NOT NULL,   -- ISO date YYYY-MM-DD (T+N)
    settle_ts       REAL NOT NULL,   -- Unix epoch of settle_date market open
    status          TEXT NOT NULL DEFAULT 'PENDING_SETTLE',
    created_at      TEXT NOT NULL,
    settled_at      TEXT
);

CREATE INDEX IF NOT EXISTS idx_settlement_settle_ts
    ON settlement_schedule (settle_ts, status);
"""
