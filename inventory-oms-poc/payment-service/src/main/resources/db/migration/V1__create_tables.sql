-- [kb-land] Source: PRODUCTION_DEPLOYMENT_DEVSECOPS.md + AWS_DATABASE_AND_HA_LESSONS.md
-- Pattern: Flyway-managed DDL — schema lifecycle owned by migration scripts, not Hibernate.
-- How to activate: set DB_DDL_AUTO=validate + spring.flyway.enabled=true in production env.
-- Target DB: Aurora PostgreSQL 15.

-- ── PAYMENT TRANSACTION aggregate ────────────────────────────────────────────
-- [kb-land] Source: HOTEL_PAYMENT_DEVSECOPS_PLAYBOOK.md
--   Layer 1: uk_payment_order_idempotency  — DB-level idempotency guard
--   Layer 2: provider_ref UNIQUE           — prevents double-capture at provider level
--   Layer 3: status query before re-try    — never re-capture on PAYMENT_UNKNOWN
CREATE TABLE IF NOT EXISTS payment_transaction (
    payment_id      UUID            NOT NULL  DEFAULT gen_random_uuid(),
    order_id        VARCHAR(64)     NOT NULL,
    -- [kb-land] Source: HOTEL_PAYMENT_DEVSECOPS_PLAYBOOK.md — stable provider reference
    --           generated client-side so it survives provider timeout and retry
    provider_ref    VARCHAR(128)    NOT NULL,
    idempotency_key VARCHAR(128)    NOT NULL,
    amount_cents    BIGINT          NOT NULL  CHECK (amount_cents > 0),
    currency        CHAR(3)         NOT NULL,
    -- [kb-land] Source: ORDER_PAYMENT_FLOW.md — valid values include PAYMENT_UNKNOWN
    status          VARCHAR(32)     NOT NULL,
    created_at      TIMESTAMP       NOT NULL,
    updated_at      TIMESTAMP       NOT NULL,
    version         BIGINT          NOT NULL  DEFAULT 0,
    CONSTRAINT pk_payment_transaction           PRIMARY KEY (payment_id),
    CONSTRAINT uk_payment_provider_ref          UNIQUE      (provider_ref),
    CONSTRAINT uk_payment_order_idempotency     UNIQUE      (order_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_payment_transaction_order_id
    ON payment_transaction (order_id);

-- Reconciliation job scans for PAYMENT_UNKNOWN
CREATE INDEX IF NOT EXISTS idx_payment_transaction_status
    ON payment_transaction (status)
    WHERE status NOT IN ('CAPTURED', 'REFUNDED', 'VOID');

-- ── OUTBOX table ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS payment_outbox_event (
    event_id        UUID            NOT NULL,
    aggregate_id    VARCHAR(64)     NOT NULL,
    event_type      VARCHAR(128)    NOT NULL,
    -- [kb-land] Source: OMS_DESIGN_PATTERNS.md — KMS field encryption applied before insert
    payload_json    VARCHAR(4000)   NOT NULL,
    status          VARCHAR(32)     NOT NULL,
    created_at      TIMESTAMP       NOT NULL,
    published_at    TIMESTAMP,
    attempt_count   INT             NOT NULL  DEFAULT 0,
    next_attempt_at TIMESTAMP       NOT NULL,
    lease_id        VARCHAR(64),
    lease_until     TIMESTAMP,
    last_error      VARCHAR(2000),
    CONSTRAINT pk_payment_outbox_event      PRIMARY KEY (event_id)
);

CREATE INDEX IF NOT EXISTS idx_payment_outbox_relay
    ON payment_outbox_event (status, next_attempt_at)
    WHERE status IN ('PENDING', 'IN_FLIGHT');

-- ── LEDGER (append-only double-entry accounting) ──────────────────────────────
-- [kb-land] Source: HOTEL_PAYMENT_DEVSECOPS_PLAYBOOK.md
--           Never UPDATE or DELETE ledger rows — corrections are new entries.
CREATE TABLE IF NOT EXISTS payment_ledger_entry (
    entry_id        VARCHAR(64)     NOT NULL,
    ledger_txn_id   VARCHAR(64)     NOT NULL,
    order_id        VARCHAR(64)     NOT NULL,
    payment_id      VARCHAR(64)     NOT NULL,
    account_code    VARCHAR(32)     NOT NULL,
    direction       VARCHAR(16)     NOT NULL  CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount_cents    BIGINT          NOT NULL  CHECK (amount_cents > 0),
    currency        CHAR(3)         NOT NULL,
    created_at      TIMESTAMP       NOT NULL,
    CONSTRAINT pk_payment_ledger_entry          PRIMARY KEY (entry_id),
    CONSTRAINT uk_payment_ledger_txn_account    UNIQUE      (ledger_txn_id, account_code)
);

CREATE INDEX IF NOT EXISTS idx_payment_ledger_order_id
    ON payment_ledger_entry (order_id);
