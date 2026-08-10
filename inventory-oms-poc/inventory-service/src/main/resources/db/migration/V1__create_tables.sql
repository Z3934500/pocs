-- [kb-land] Source: PRODUCTION_DEPLOYMENT_DEVSECOPS.md + AWS_DATABASE_AND_HA_LESSONS.md
-- Pattern: Flyway-managed DDL — schema lifecycle owned by migration scripts, not Hibernate.
-- How to activate: set DB_DDL_AUTO=validate + spring.flyway.enabled=true in production env.
-- Target DB: Aurora PostgreSQL 15.

-- ── INVENTORY STOCK aggregate ─────────────────────────────────────────────────
-- [kb-land] Source: ARCHITECTURE_AND_DEVOPS.md — row-level lock on sku during reservations
CREATE TABLE IF NOT EXISTS inventory_stock (
    sku                     VARCHAR(64)     NOT NULL,
    available_qty           INT             NOT NULL  DEFAULT 0  CHECK (available_qty >= 0),
    reserved_qty            INT             NOT NULL  DEFAULT 0  CHECK (reserved_qty >= 0),
    sold_qty                INT             NOT NULL  DEFAULT 0  CHECK (sold_qty >= 0),
    -- [kb-land] Source: OMS_DESIGN_PATTERNS.md — seckill quota held in Redis stream
    seckill_allocated_qty   INT             NOT NULL  DEFAULT 0  CHECK (seckill_allocated_qty >= 0),
    updated_at              TIMESTAMP       NOT NULL,
    version                 BIGINT          NOT NULL  DEFAULT 0,
    CONSTRAINT pk_inventory_stock           PRIMARY KEY (sku)
);

-- ── INVENTORY RESERVATION ─────────────────────────────────────────────────────
-- [kb-land] Source: HOTEL_PAYMENT_DEVSECOPS_PLAYBOOK.md — idempotent reserve;
--           uk_inventory_reservation_order prevents double-reserve for the same order.
CREATE TABLE IF NOT EXISTS inventory_reservation (
    reservation_id  UUID            NOT NULL  DEFAULT gen_random_uuid(),
    order_id        VARCHAR(64)     NOT NULL,
    sku             VARCHAR(64)     NOT NULL,
    qty             INT             NOT NULL  CHECK (qty > 0),
    status          VARCHAR(32)     NOT NULL,
    idempotency_key VARCHAR(128)    NOT NULL,
    expires_at      TIMESTAMP       NOT NULL,
    created_at      TIMESTAMP       NOT NULL,
    updated_at      TIMESTAMP       NOT NULL,
    version         BIGINT          NOT NULL  DEFAULT 0,
    CONSTRAINT pk_inventory_reservation             PRIMARY KEY (reservation_id),
    CONSTRAINT uk_inventory_reservation_order       UNIQUE      (order_id),
    CONSTRAINT uk_inventory_reservation_idempotency UNIQUE      (idempotency_key)
);

-- Cleanup job: WHERE sku=? AND status='RESERVED' AND expires_at < NOW()
CREATE INDEX IF NOT EXISTS idx_inventory_reservation_sku_status_expires
    ON inventory_reservation (sku, status, expires_at)
    WHERE status = 'RESERVED';

-- ── OUTBOX table ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS inventory_outbox_event (
    event_id        UUID            NOT NULL,
    aggregate_id    VARCHAR(64)     NOT NULL,
    event_type      VARCHAR(128)    NOT NULL,
    payload_json    VARCHAR(4000)   NOT NULL,
    status          VARCHAR(32)     NOT NULL,
    created_at      TIMESTAMP       NOT NULL,
    published_at    TIMESTAMP,
    attempt_count   INT             NOT NULL  DEFAULT 0,
    next_attempt_at TIMESTAMP       NOT NULL,
    lease_id        VARCHAR(64),
    lease_until     TIMESTAMP,
    last_error      VARCHAR(2000),
    CONSTRAINT pk_inventory_outbox_event    PRIMARY KEY (event_id)
);

CREATE INDEX IF NOT EXISTS idx_inventory_outbox_relay
    ON inventory_outbox_event (status, next_attempt_at)
    WHERE status IN ('PENDING', 'IN_FLIGHT');
