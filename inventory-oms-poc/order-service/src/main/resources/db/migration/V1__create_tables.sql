-- [kb-land] Source: PRODUCTION_DEPLOYMENT_DEVSECOPS.md + AWS_DATABASE_AND_HA_LESSONS.md
-- Pattern: Flyway-managed DDL — schema lifecycle owned by migration scripts, not Hibernate.
-- How to activate: set DB_DDL_AUTO=validate + spring.flyway.enabled=true in production env.
-- PoC local dev: DB_DDL_AUTO defaults to 'update' and Flyway is off — H2 still works.
-- Target DB: Aurora PostgreSQL 15 (requires gen_random_uuid() from pgcrypto extension).

-- ── ORDER aggregate ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customer_order (
    order_id                VARCHAR(64)     NOT NULL,
    sku                     VARCHAR(64)     NOT NULL,
    qty                     INT             NOT NULL  CHECK (qty > 0),
    amount_cents            BIGINT          NOT NULL  CHECK (amount_cents >= 0),
    currency                CHAR(3)         NOT NULL,
    -- [kb-land] Source: ORDER_PAYMENT_FLOW.md — PAYMENT_UNKNOWN is a valid production state
    status                  VARCHAR(32)     NOT NULL,
    idempotency_key         VARCHAR(128)    NOT NULL,
    reservation_id          VARCHAR(64),
    payment_id              VARCHAR(64),
    payment_idempotency_key VARCHAR(128),
    provider_ref            VARCHAR(128),
    payment_succeed         BOOLEAN,
    created_at              TIMESTAMP       NOT NULL,
    updated_at              TIMESTAMP       NOT NULL,
    -- [kb-land] Source: OMS_DESIGN_PATTERNS.md — optimistic lock, never null after first write
    version                 BIGINT          NOT NULL  DEFAULT 0,
    CONSTRAINT pk_customer_order        PRIMARY KEY (order_id),
    CONSTRAINT uk_order_idempotency     UNIQUE      (idempotency_key)
);

-- [kb-land] Source: OMS_DESIGN_PATTERNS.md — composite cursor for keyset pagination
CREATE INDEX IF NOT EXISTS idx_customer_order_cursor
    ON customer_order (created_at DESC, order_id);

CREATE INDEX IF NOT EXISTS idx_customer_order_status
    ON customer_order (status)
    WHERE status NOT IN ('COMPLETED', 'CANCELLED');

-- ── OUTBOX table ─────────────────────────────────────────────────────────────
-- [kb-land] Source: ORDER_PAYMENT_FLOW.md — written in the same DB transaction as the order
CREATE TABLE IF NOT EXISTS order_outbox_event (
    event_id        UUID            NOT NULL,
    aggregate_id    VARCHAR(64)     NOT NULL,
    event_type      VARCHAR(128)    NOT NULL,
    payload_json    VARCHAR(4000)   NOT NULL,
    -- [kb-land] Source: ORDER_INVENTORY_FIFO_SQS.md — partition key = sku:<sku> for FIFO lanes
    partition_key   VARCHAR(128),
    status          VARCHAR(32)     NOT NULL,
    created_at      TIMESTAMP       NOT NULL,
    published_at    TIMESTAMP,
    attempt_count   INT             NOT NULL  DEFAULT 0,
    next_attempt_at TIMESTAMP       NOT NULL,
    lease_id        VARCHAR(64),
    lease_until     TIMESTAMP,
    last_error      VARCHAR(2000),
    CONSTRAINT pk_order_outbox_event    PRIMARY KEY (event_id)
);

-- Relay worker queries: WHERE status IN ('PENDING','IN_FLIGHT') ORDER BY next_attempt_at
CREATE INDEX IF NOT EXISTS idx_order_outbox_relay
    ON order_outbox_event (status, next_attempt_at)
    WHERE status IN ('PENDING', 'IN_FLIGHT');

-- ── SAGA STEP audit log ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS order_saga_step (
    step_id     UUID            NOT NULL,
    order_id    VARCHAR(64)     NOT NULL,
    step_name   VARCHAR(64)     NOT NULL,
    status      VARCHAR(32)     NOT NULL,
    detail      VARCHAR(1000)   NOT NULL,
    created_at  TIMESTAMP       NOT NULL,
    CONSTRAINT pk_order_saga_step       PRIMARY KEY (step_id)
);

CREATE INDEX IF NOT EXISTS idx_order_saga_step_order_id
    ON order_saga_step (order_id);
