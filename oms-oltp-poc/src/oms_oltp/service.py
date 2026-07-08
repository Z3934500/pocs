from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from .config import settings
from .db import connect, init_schema, seed_reference_data, transaction


ORDER_RESERVED = "RESERVED"
ORDER_CONFIRMED = "CONFIRMED"
ORDER_CANCELLED = "CANCELLED"
ORDER_SHIPPED = "SHIPPED"
ORDER_PAYMENT_FAILED = "PAYMENT_FAILED"

RESERVATION_RESERVED = "RESERVED"
RESERVATION_COMMITTED = "COMMITTED"
RESERVATION_RELEASED = "RELEASED"

EVENT_PENDING = "PENDING"
EVENT_PUBLISHED = "PUBLISHED"


class BusinessError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class OrderItemRequest:
    sku_id: str
    quantity: int


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:12].upper()}"


def money(cents: int) -> float:
    return round(cents / 100, 2)


def _row_to_dict(row: sqlite3.Row | None) -> dict | None:
    return dict(row) if row is not None else None


def _normalize_items(items: list[OrderItemRequest] | list[dict]) -> list[OrderItemRequest]:
    merged: dict[str, int] = {}
    for item in items:
        sku_id = item.sku_id if isinstance(item, OrderItemRequest) else str(item["sku_id"])
        quantity = item.quantity if isinstance(item, OrderItemRequest) else int(item["quantity"])
        sku_id = sku_id.strip().upper()
        if not sku_id:
            raise BusinessError("INVALID_ITEM", "sku_id is required")
        if quantity <= 0:
            raise BusinessError("INVALID_QUANTITY", f"quantity must be positive for {sku_id}")
        merged[sku_id] = merged.get(sku_id, 0) + quantity
    if not merged:
        raise BusinessError("EMPTY_ORDER", "at least one order item is required")
    return [OrderItemRequest(sku_id=sku_id, quantity=quantity) for sku_id, quantity in sorted(merged.items())]


class OMSService:
    def __init__(self, db_path: Path | None = None):
        self.db_path = db_path or settings.sqlite_path

    def initialize(self, *, reset: bool = False) -> None:
        with connect(self.db_path) as conn:
            seed_reference_data(conn, reset=reset)

    def place_order(
        self,
        *,
        customer_id: str,
        items: list[OrderItemRequest] | list[dict],
        idempotency_key: str | None = None,
        reservation_ttl_minutes: int | None = None,
    ) -> dict:
        normalized_items = _normalize_items(items)
        now = utc_now()
        order_id = new_id("ORD")
        reservation_id = new_id("RSV")
        idempotency_key = (idempotency_key or order_id).strip()
        ttl = reservation_ttl_minutes or settings.default_reservation_ttl_minutes
        expires_at = (datetime.now(UTC).replace(microsecond=0) + timedelta(minutes=ttl)).isoformat()

        with connect(self.db_path) as conn:
            init_schema(conn)
            with transaction(conn):
                existing = conn.execute(
                    "SELECT order_id FROM orders WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    return self._get_order(conn, existing["order_id"])

                customer = conn.execute(
                    "SELECT customer_id FROM customers WHERE customer_id = ?",
                    (customer_id,),
                ).fetchone()
                if not customer:
                    raise BusinessError("UNKNOWN_CUSTOMER", f"customer {customer_id} does not exist")

                priced_items: list[tuple[OrderItemRequest, int]] = []
                for item in normalized_items:
                    sku = conn.execute(
                        """
                        SELECT sku_id, unit_price_cents
                        FROM sku_inventory
                        WHERE sku_id = ?
                        """,
                        (item.sku_id,),
                    ).fetchone()
                    if not sku:
                        raise BusinessError("UNKNOWN_SKU", f"sku {item.sku_id} does not exist")

                    updated = conn.execute(
                        """
                        UPDATE sku_inventory
                        SET available_stock = available_stock - ?,
                            reserved_stock = reserved_stock + ?,
                            updated_at = ?
                        WHERE sku_id = ?
                          AND available_stock >= ?
                        """,
                        (item.quantity, item.quantity, now, item.sku_id, item.quantity),
                    )
                    if updated.rowcount != 1:
                        raise BusinessError("INSUFFICIENT_STOCK", f"not enough available stock for {item.sku_id}")
                    priced_items.append((item, int(sku["unit_price_cents"])))

                total_amount_cents = sum(item.quantity * price for item, price in priced_items)
                conn.execute(
                    """
                    INSERT INTO orders (
                        order_id, customer_id, status, total_amount_cents, idempotency_key, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (order_id, customer_id, ORDER_RESERVED, total_amount_cents, idempotency_key, now, now),
                )

                for item, unit_price in priced_items:
                    conn.execute(
                        """
                        INSERT INTO order_items (order_id, sku_id, quantity, unit_price_cents)
                        VALUES (?, ?, ?, ?)
                        """,
                        (order_id, item.sku_id, item.quantity, unit_price),
                    )
                    self._record_inventory_movement(conn, order_id, item.sku_id, "RESERVE", item.quantity, now)

                conn.execute(
                    """
                    INSERT INTO inventory_reservations (
                        reservation_id, order_id, status, expires_at, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (reservation_id, order_id, RESERVATION_RESERVED, expires_at, now, now),
                )
                self._record_status(conn, order_id, None, ORDER_RESERVED, "inventory reserved", now)
                self._record_saga(conn, order_id, "reserve_inventory", "COMPLETED", "stock moved to reserved bucket", now)
                self._record_event(
                    conn,
                    aggregate_type="order",
                    aggregate_id=order_id,
                    event_type="order.created",
                    payload={
                        "order_id": order_id,
                        "customer_id": customer_id,
                        "status": ORDER_RESERVED,
                        "total_amount_cents": total_amount_cents,
                    },
                    now=now,
                )
                self._record_event(
                    conn,
                    aggregate_type="inventory",
                    aggregate_id=reservation_id,
                    event_type="inventory.reserved",
                    payload={
                        "order_id": order_id,
                        "reservation_id": reservation_id,
                        "items": [item.__dict__ for item, _ in priced_items],
                        "expires_at": expires_at,
                    },
                    now=now,
                )
                return self._get_order(conn, order_id)

    def capture_payment(self, *, order_id: str, provider_ref: str | None = None, succeed: bool = True) -> dict:
        now = utc_now()
        provider_ref = provider_ref or new_id("PAYREF")
        with connect(self.db_path) as conn:
            init_schema(conn)
            with transaction(conn):
                existing_payment = conn.execute(
                    "SELECT order_id FROM payments WHERE provider_ref = ?",
                    (provider_ref,),
                ).fetchone()
                if existing_payment:
                    return self._get_order(conn, existing_payment["order_id"])

                order = self._require_order(conn, order_id)
                if order["status"] not in {ORDER_RESERVED, ORDER_PAYMENT_FAILED}:
                    raise BusinessError("INVALID_ORDER_STATE", f"order {order_id} cannot capture payment from {order['status']}")

                if not succeed:
                    conn.execute(
                        """
                        INSERT INTO payments (payment_id, order_id, provider_ref, amount_cents, status, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (new_id("PAY"), order_id, provider_ref, order["total_amount_cents"], "FAILED", now),
                    )
                    self._release_order(conn, order_id, now=now, reason="payment failed", final_order_status=ORDER_PAYMENT_FAILED)
                    self._record_event(
                        conn,
                        aggregate_type="payment",
                        aggregate_id=order_id,
                        event_type="payment.failed",
                        payload={"order_id": order_id, "provider_ref": provider_ref},
                        now=now,
                    )
                    return self._get_order(conn, order_id)

                conn.execute(
                    """
                    INSERT INTO payments (payment_id, order_id, provider_ref, amount_cents, status, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (new_id("PAY"), order_id, provider_ref, order["total_amount_cents"], "CAPTURED", now),
                )

                for item in self._order_items(conn, order_id):
                    updated = conn.execute(
                        """
                        UPDATE sku_inventory
                        SET reserved_stock = reserved_stock - ?,
                            sold_stock = sold_stock + ?,
                            updated_at = ?
                        WHERE sku_id = ?
                          AND reserved_stock >= ?
                        """,
                        (item["quantity"], item["quantity"], now, item["sku_id"], item["quantity"]),
                    )
                    if updated.rowcount != 1:
                        raise BusinessError("RESERVATION_MISMATCH", f"reserved stock mismatch for {item['sku_id']}")
                    self._record_inventory_movement(conn, order_id, item["sku_id"], "COMMIT", item["quantity"], now)

                conn.execute(
                    """
                    UPDATE inventory_reservations
                    SET status = ?, updated_at = ?
                    WHERE order_id = ? AND status = ?
                    """,
                    (RESERVATION_COMMITTED, now, order_id, RESERVATION_RESERVED),
                )
                self._set_order_status(conn, order_id, ORDER_CONFIRMED, "payment captured and inventory committed", now)
                self._record_saga(conn, order_id, "capture_payment", "COMPLETED", "payment authorized/captured", now)
                self._record_saga(conn, order_id, "commit_inventory", "COMPLETED", "reserved stock moved to sold bucket", now)
                self._record_event(
                    conn,
                    aggregate_type="payment",
                    aggregate_id=order_id,
                    event_type="payment.captured",
                    payload={"order_id": order_id, "provider_ref": provider_ref, "amount_cents": order["total_amount_cents"]},
                    now=now,
                )
                self._record_event(
                    conn,
                    aggregate_type="inventory",
                    aggregate_id=order_id,
                    event_type="inventory.committed",
                    payload={"order_id": order_id, "items": self._order_items(conn, order_id)},
                    now=now,
                )
                self._record_event(
                    conn,
                    aggregate_type="order",
                    aggregate_id=order_id,
                    event_type="order.confirmed",
                    payload={"order_id": order_id, "status": ORDER_CONFIRMED},
                    now=now,
                )
                return self._get_order(conn, order_id)

    def cancel_order(self, *, order_id: str, reason: str = "customer cancelled") -> dict:
        now = utc_now()
        with connect(self.db_path) as conn:
            init_schema(conn)
            with transaction(conn):
                order = self._require_order(conn, order_id)
                if order["status"] in {ORDER_CANCELLED, ORDER_PAYMENT_FAILED}:
                    return self._get_order(conn, order_id)
                if order["status"] != ORDER_RESERVED:
                    raise BusinessError("INVALID_ORDER_STATE", f"order {order_id} cannot be cancelled from {order['status']}")
                self._release_order(conn, order_id, now=now, reason=reason, final_order_status=ORDER_CANCELLED)
                self._record_event(
                    conn,
                    aggregate_type="order",
                    aggregate_id=order_id,
                    event_type="order.cancelled",
                    payload={"order_id": order_id, "reason": reason},
                    now=now,
                )
                return self._get_order(conn, order_id)

    def ship_order(self, *, order_id: str) -> dict:
        now = utc_now()
        with connect(self.db_path) as conn:
            init_schema(conn)
            with transaction(conn):
                order = self._require_order(conn, order_id)
                if order["status"] == ORDER_SHIPPED:
                    return self._get_order(conn, order_id)
                if order["status"] != ORDER_CONFIRMED:
                    raise BusinessError("INVALID_ORDER_STATE", f"order {order_id} cannot be shipped from {order['status']}")
                self._set_order_status(conn, order_id, ORDER_SHIPPED, "fulfillment created shipment", now)
                self._record_saga(conn, order_id, "create_shipment", "COMPLETED", "shipment handed to WMS", now)
                self._record_event(
                    conn,
                    aggregate_type="fulfillment",
                    aggregate_id=order_id,
                    event_type="shipment.created",
                    payload={"order_id": order_id},
                    now=now,
                )
                return self._get_order(conn, order_id)

    def expire_reservations(self) -> dict:
        now = utc_now()
        with connect(self.db_path) as conn:
            init_schema(conn)
            with transaction(conn):
                expired = [
                    row["order_id"]
                    for row in conn.execute(
                        """
                        SELECT order_id
                        FROM inventory_reservations
                        WHERE status = ? AND expires_at <= ?
                        ORDER BY expires_at
                        """,
                        (RESERVATION_RESERVED, now),
                    ).fetchall()
                ]
                for order_id in expired:
                    self._release_order(conn, order_id, now=now, reason="reservation timeout", final_order_status=ORDER_CANCELLED)
                    self._record_event(
                        conn,
                        aggregate_type="order",
                        aggregate_id=order_id,
                        event_type="order.timeout",
                        payload={"order_id": order_id},
                        now=now,
                    )
                return {"expired_count": len(expired), "order_ids": expired}

    def publish_outbox(self, *, limit: int = 50) -> list[dict]:
        now = utc_now()
        with connect(self.db_path) as conn:
            init_schema(conn)
            with transaction(conn):
                events = [
                    dict(row)
                    for row in conn.execute(
                        """
                        SELECT *
                        FROM outbox_events
                        WHERE status = ?
                        ORDER BY created_at, event_id
                        LIMIT ?
                        """,
                        (EVENT_PENDING, limit),
                    ).fetchall()
                ]
                for event in events:
                    conn.execute(
                        """
                        UPDATE outbox_events
                        SET status = ?, published_at = ?
                        WHERE event_id = ?
                        """,
                        (EVENT_PUBLISHED, now, event["event_id"]),
                    )
                    event["status"] = EVENT_PUBLISHED
                    event["published_at"] = now
                    event["payload"] = json.loads(event.pop("payload_json"))
                return events

    def inventory(self) -> list[dict]:
        with connect(self.db_path) as conn:
            init_schema(conn)
            return [
                {**dict(row), "unit_price": money(row["unit_price_cents"])}
                for row in conn.execute(
                    """
                    SELECT *
                    FROM sku_inventory
                    ORDER BY sku_id
                    """
                ).fetchall()
            ]

    def orders(self, *, limit: int = 50) -> list[dict]:
        with connect(self.db_path) as conn:
            init_schema(conn)
            rows = conn.execute(
                """
                SELECT o.*, c.customer_name
                FROM orders o
                JOIN customers c USING (customer_id)
                ORDER BY o.created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [self._get_order(conn, row["order_id"]) for row in rows]

    def order(self, order_id: str) -> dict:
        with connect(self.db_path) as conn:
            init_schema(conn)
            return self._get_order(conn, order_id)

    def outbox(self, *, limit: int = 100) -> list[dict]:
        with connect(self.db_path) as conn:
            init_schema(conn)
            events = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT *
                    FROM outbox_events
                    ORDER BY created_at DESC, event_id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            ]
        for event in events:
            event["payload"] = json.loads(event.pop("payload_json"))
        return events

    def summary(self) -> dict:
        with connect(self.db_path) as conn:
            init_schema(conn)
            order_counts = {
                row["status"]: row["count"]
                for row in conn.execute(
                    "SELECT status, COUNT(*) AS count FROM orders GROUP BY status ORDER BY status"
                ).fetchall()
            }
            stock = conn.execute(
                """
                SELECT
                    SUM(available_stock) AS available_stock,
                    SUM(reserved_stock) AS reserved_stock,
                    SUM(sold_stock) AS sold_stock
                FROM sku_inventory
                """
            ).fetchone()
            events = {
                row["status"]: row["count"]
                for row in conn.execute(
                    "SELECT status, COUNT(*) AS count FROM outbox_events GROUP BY status"
                ).fetchall()
            }
            payments = {
                row["status"]: row["count"]
                for row in conn.execute(
                    "SELECT status, COUNT(*) AS count FROM payments GROUP BY status"
                ).fetchall()
            }
            revenue_cents = conn.execute(
                """
                SELECT COALESCE(SUM(amount_cents), 0) AS revenue_cents
                FROM payments
                WHERE status = 'CAPTURED'
                """
            ).fetchone()["revenue_cents"]
            reservations = {
                row["status"]: row["count"]
                for row in conn.execute(
                    "SELECT status, COUNT(*) AS count FROM inventory_reservations GROUP BY status"
                ).fetchall()
            }
            return {
                "orders": order_counts,
                "payments": payments,
                "reservations": reservations,
                "outbox": events,
                "available_stock": int(stock["available_stock"] or 0),
                "reserved_stock": int(stock["reserved_stock"] or 0),
                "sold_stock": int(stock["sold_stock"] or 0),
                "captured_revenue_cents": int(revenue_cents),
                "captured_revenue": money(int(revenue_cents)),
            }

    def customers(self) -> list[dict]:
        with connect(self.db_path) as conn:
            init_schema(conn)
            return [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM customers ORDER BY customer_id"
                ).fetchall()
            ]

    def _require_order(self, conn: sqlite3.Connection, order_id: str) -> sqlite3.Row:
        order = conn.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone()
        if not order:
            raise BusinessError("ORDER_NOT_FOUND", f"order {order_id} does not exist")
        return order

    def _order_items(self, conn: sqlite3.Connection, order_id: str) -> list[dict]:
        return [
            dict(row)
            for row in conn.execute(
                """
                SELECT oi.order_id, oi.sku_id, i.product_name, oi.quantity, oi.unit_price_cents
                FROM order_items oi
                JOIN sku_inventory i USING (sku_id)
                WHERE oi.order_id = ?
                ORDER BY oi.sku_id
                """,
                (order_id,),
            ).fetchall()
        ]

    def _get_order(self, conn: sqlite3.Connection, order_id: str) -> dict:
        order = _row_to_dict(
            conn.execute(
                """
                SELECT o.*, c.customer_name, c.segment
                FROM orders o
                JOIN customers c USING (customer_id)
                WHERE o.order_id = ?
                """,
                (order_id,),
            ).fetchone()
        )
        if not order:
            raise BusinessError("ORDER_NOT_FOUND", f"order {order_id} does not exist")
        order["total_amount"] = money(order["total_amount_cents"])
        order["items"] = [
            {**item, "unit_price": money(item["unit_price_cents"]), "line_amount": money(item["quantity"] * item["unit_price_cents"])}
            for item in self._order_items(conn, order_id)
        ]
        order["reservation"] = _row_to_dict(
            conn.execute("SELECT * FROM inventory_reservations WHERE order_id = ?", (order_id,)).fetchone()
        )
        order["payments"] = [
            {**dict(row), "amount": money(row["amount_cents"])}
            for row in conn.execute("SELECT * FROM payments WHERE order_id = ? ORDER BY created_at", (order_id,)).fetchall()
        ]
        order["history"] = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM order_status_history WHERE order_id = ? ORDER BY created_at",
                (order_id,),
            ).fetchall()
        ]
        return order

    def _record_inventory_movement(
        self,
        conn: sqlite3.Connection,
        order_id: str,
        sku_id: str,
        movement_type: str,
        quantity: int,
        now: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO inventory_movements (movement_id, order_id, sku_id, movement_type, quantity, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (new_id("MOV"), order_id, sku_id, movement_type, quantity, now),
        )

    def _record_event(
        self,
        conn: sqlite3.Connection,
        *,
        aggregate_type: str,
        aggregate_id: str,
        event_type: str,
        payload: dict,
        now: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO outbox_events (
                event_id, aggregate_type, aggregate_id, event_type, payload_json, status, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (new_id("EVT"), aggregate_type, aggregate_id, event_type, json.dumps(payload, sort_keys=True), EVENT_PENDING, now),
        )

    def _record_saga(self, conn: sqlite3.Connection, order_id: str, step_name: str, status: str, message: str, now: str) -> None:
        conn.execute(
            """
            INSERT INTO saga_log (log_id, order_id, step_name, status, message, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (new_id("SAGA"), order_id, step_name, status, message, now),
        )

    def _record_status(
        self,
        conn: sqlite3.Connection,
        order_id: str,
        from_status: str | None,
        to_status: str,
        reason: str,
        now: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO order_status_history (history_id, order_id, from_status, to_status, reason, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (new_id("HIS"), order_id, from_status, to_status, reason, now),
        )

    def _set_order_status(self, conn: sqlite3.Connection, order_id: str, to_status: str, reason: str, now: str) -> None:
        current = self._require_order(conn, order_id)
        conn.execute(
            """
            UPDATE orders
            SET status = ?, updated_at = ?
            WHERE order_id = ?
            """,
            (to_status, now, order_id),
        )
        self._record_status(conn, order_id, current["status"], to_status, reason, now)

    def _release_order(
        self,
        conn: sqlite3.Connection,
        order_id: str,
        *,
        now: str,
        reason: str,
        final_order_status: str,
    ) -> None:
        order = self._require_order(conn, order_id)
        reservation = conn.execute(
            "SELECT * FROM inventory_reservations WHERE order_id = ?",
            (order_id,),
        ).fetchone()
        if not reservation or reservation["status"] != RESERVATION_RESERVED:
            self._set_order_status(conn, order_id, final_order_status, reason, now)
            return

        for item in self._order_items(conn, order_id):
            updated = conn.execute(
                """
                UPDATE sku_inventory
                SET reserved_stock = reserved_stock - ?,
                    available_stock = available_stock + ?,
                    updated_at = ?
                WHERE sku_id = ?
                  AND reserved_stock >= ?
                """,
                (item["quantity"], item["quantity"], now, item["sku_id"], item["quantity"]),
            )
            if updated.rowcount != 1:
                raise BusinessError("RESERVATION_MISMATCH", f"reserved stock mismatch for {item['sku_id']}")
            self._record_inventory_movement(conn, order_id, item["sku_id"], "RELEASE", item["quantity"], now)

        conn.execute(
            """
            UPDATE inventory_reservations
            SET status = ?, updated_at = ?
            WHERE order_id = ?
            """,
            (RESERVATION_RELEASED, now, order_id),
        )
        self._set_order_status(conn, order_id, final_order_status, reason, now)
        self._record_saga(conn, order_id, "release_inventory", "COMPLETED", reason, now)
        self._record_event(
            conn,
            aggregate_type="inventory",
            aggregate_id=order_id,
            event_type="inventory.released",
            payload={"order_id": order_id, "reason": reason, "items": self._order_items(conn, order_id)},
            now=now,
        )
