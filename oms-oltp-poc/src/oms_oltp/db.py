from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .config import settings


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    segment TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sku_inventory (
    sku_id TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    available_stock INTEGER NOT NULL CHECK (available_stock >= 0),
    reserved_stock INTEGER NOT NULL CHECK (reserved_stock >= 0),
    sold_stock INTEGER NOT NULL CHECK (sold_stock >= 0),
    unit_price_cents INTEGER NOT NULL CHECK (unit_price_cents >= 0),
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    status TEXT NOT NULL,
    total_amount_cents INTEGER NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id TEXT NOT NULL,
    sku_id TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price_cents INTEGER NOT NULL,
    PRIMARY KEY (order_id, sku_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (sku_id) REFERENCES sku_inventory(sku_id)
);

CREATE TABLE IF NOT EXISTS inventory_reservations (
    reservation_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

CREATE TABLE IF NOT EXISTS inventory_movements (
    movement_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    sku_id TEXT NOT NULL,
    movement_type TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    created_at TEXT NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (sku_id) REFERENCES sku_inventory(sku_id)
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    provider_ref TEXT NOT NULL UNIQUE,
    amount_cents INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

CREATE TABLE IF NOT EXISTS order_status_history (
    history_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    from_status TEXT,
    to_status TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id TEXT PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    published_at TEXT
);

CREATE TABLE IF NOT EXISTS saga_log (
    log_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    step_name TEXT NOT NULL,
    status TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox_events(status, created_at);
CREATE INDEX IF NOT EXISTS idx_reservations_status_expires ON inventory_reservations(status, expires_at);
"""


REFERENCE_DATA = {
    "customers": [
        ("CUST-1001", "Acme Retail Buyer", "B2B"),
        ("CUST-1002", "Lina Chen", "VIP"),
        ("CUST-1003", "Walk-in Customer", "Retail"),
    ],
    "inventory": [
        ("SKU-RED-001", "Red Smart Scale", 120, 0, 0, 12900),
        ("SKU-BLK-002", "Black Barcode Scanner", 80, 0, 0, 25900),
        ("SKU-SIL-003", "Silver POS Terminal", 35, 0, 0, 49900),
        ("SKU-BAT-004", "Spare Battery Pack", 240, 0, 0, 3900),
    ],
}


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    path = db_path or settings.sqlite_path
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()


def reset_tables(conn: sqlite3.Connection) -> None:
    table_names = [
        "saga_log",
        "outbox_events",
        "order_status_history",
        "payments",
        "inventory_movements",
        "inventory_reservations",
        "order_items",
        "orders",
        "sku_inventory",
        "customers",
    ]
    for table_name in table_names:
        conn.execute(f"DELETE FROM {table_name}")


def seed_reference_data(conn: sqlite3.Connection, *, reset: bool = False) -> None:
    from .service import utc_now

    init_schema(conn)
    if reset:
        reset_tables(conn)

    now = utc_now()
    for customer_id, customer_name, segment in REFERENCE_DATA["customers"]:
        conn.execute(
            """
            INSERT OR IGNORE INTO customers (customer_id, customer_name, segment, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (customer_id, customer_name, segment, now),
        )

    for sku_id, product_name, available, reserved, sold, unit_price in REFERENCE_DATA["inventory"]:
        conn.execute(
            """
            INSERT OR IGNORE INTO sku_inventory (
                sku_id, product_name, available_stock, reserved_stock, sold_stock, unit_price_cents, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (sku_id, product_name, available, reserved, sold, unit_price, now),
        )
