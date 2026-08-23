from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .config import settings
from .db import connect, init_schema, transaction
from .privacy import (
    SAFE_CROSS_BORDER_FIELDS,
    build_cross_border_order_summary,
    canonical_payload_hash,
    normalize_region,
    verify_payment_callback,
)
from .service import BusinessError, EVENT_PENDING, OMSService, new_id, utc_now


PAYMENT_PENDING = "PENDING"
PAYMENT_CAPTURED = "CAPTURED"
PAYMENT_FAILED = "FAILED"
CALLBACK_PROCESSED = "PROCESSED"
CALLBACK_REJECTED = "REJECTED"
SETTLEMENT_MATCHED = "MATCHED"
SETTLEMENT_MISMATCH = "MISMATCH"

CUSTOMER_REGIONS = {
    "CUST-1001": "CN",
    "CUST-1002": "CN",
    "CUST-1003": "OVERSEAS",
}

SECURITY_SCHEMA = """
CREATE TABLE IF NOT EXISTS payment_intents (
    intent_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    provider_ref TEXT NOT NULL UNIQUE,
    idempotency_key TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    currency TEXT NOT NULL,
    provider_region TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(order_id, idempotency_key),
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);
CREATE TABLE IF NOT EXISTS payment_callbacks (
    callback_id TEXT PRIMARY KEY,
    provider_ref TEXT NOT NULL,
    callback_status TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    payload_hash TEXT NOT NULL,
    signature_valid INTEGER NOT NULL,
    processed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS payment_ledger_entries (
    entry_id TEXT PRIMARY KEY,
    ledger_txn_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    provider_ref TEXT NOT NULL,
    callback_id TEXT NOT NULL,
    account_code TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ("DEBIT", "CREDIT")),
    amount_cents INTEGER NOT NULL CHECK (amount_cents > 0),
    currency TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(ledger_txn_id, account_code),
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);
CREATE TABLE IF NOT EXISTS settlement_records (
    settlement_id TEXT PRIMARY KEY,
    provider_ref TEXT NOT NULL UNIQUE,
    order_id TEXT NOT NULL,
    gross_amount_cents INTEGER NOT NULL,
    fee_amount_cents INTEGER NOT NULL,
    net_amount_cents INTEGER NOT NULL,
    status TEXT NOT NULL,
    provider_region TEXT NOT NULL,
    settled_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);
CREATE TABLE IF NOT EXISTS customer_data_policies (
    customer_id TEXT PRIMARY KEY,
    data_region TEXT NOT NULL,
    personal_data_classification TEXT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
CREATE TABLE IF NOT EXISTS data_export_audit (
    export_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    source_region TEXT NOT NULL,
    target_region TEXT NOT NULL,
    purpose TEXT NOT NULL,
    data_classification TEXT NOT NULL,
    requested_fields_json TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);
CREATE INDEX IF NOT EXISTS idx_secure_callbacks_provider_ref ON payment_callbacks(provider_ref);
CREATE INDEX IF NOT EXISTS idx_secure_ledger_provider_ref ON payment_ledger_entries(provider_ref, created_at);
CREATE INDEX IF NOT EXISTS idx_secure_settlement_status ON settlement_records(status, created_at);
CREATE INDEX IF NOT EXISTS idx_secure_export_audit_created_at ON data_export_audit(created_at);
"""


class SecurePaymentPrivacyService:
    """Payment-provider boundary and data-export policy for the OMS PoC."""

    def __init__(self, db_path: Path | None = None, oms: OMSService | None = None):
        self.db_path = db_path or settings.sqlite_path
        self.oms = oms or OMSService(self.db_path)

    def initialize(self) -> None:
        with connect(self.db_path) as conn:
            init_schema(conn)
            conn.executescript(SECURITY_SCHEMA)
            for customer_id, region in CUSTOMER_REGIONS.items():
                conn.execute(
                    """
                    INSERT OR IGNORE INTO customer_data_policies
                        (customer_id, data_region, personal_data_classification)
                    VALUES (?, ?, ?)
                    """,
                    (customer_id, region, "SENSITIVE_PERSONAL"),
                )
                conn.execute(
                    "UPDATE customer_data_policies SET data_region = ? WHERE customer_id = ?",
                    (region, customer_id),
                )

    @staticmethod
    def _region(value: str) -> str:
        try:
            return normalize_region(value)
        except ValueError as exc:
            raise BusinessError("INVALID_DATA_REGION", str(exc)) from exc

    def create_payment_intent(
        self,
        *,
        order_id: str,
        idempotency_key: str,
        provider_region: str = "CN",
        currency: str = "CNY",
    ) -> dict[str, Any]:
        provider_region = self._region(provider_region)
        idempotency_key = (idempotency_key or "").strip()
        currency = (currency or "").strip().upper()
        if not idempotency_key:
            raise BusinessError("INVALID_PAYMENT_IDEMPOTENCY_KEY", "payment idempotency_key is required")
        if len(idempotency_key) > 128:
            raise BusinessError("INVALID_PAYMENT_IDEMPOTENCY_KEY", "payment idempotency_key is too long")
        if len(currency) != 3:
            raise BusinessError("INVALID_CURRENCY", "currency must be a three-letter code")

        self.initialize()
        with connect(self.db_path) as conn:
            existing = conn.execute(
                "SELECT * FROM payment_intents WHERE order_id = ? AND idempotency_key = ?",
                (order_id, idempotency_key),
            ).fetchone()
        if existing:
            return dict(existing)
        order = self.oms.order(order_id)
        if order["status"] != "RESERVED":
            raise BusinessError("INVALID_PAYMENT_STATE", f"order {order_id} cannot start payment from {order['status']}")

        now = utc_now()
        with connect(self.db_path) as conn:
            with transaction(conn):
                existing = conn.execute(
                    "SELECT * FROM payment_intents WHERE order_id = ? AND idempotency_key = ?",
                    (order_id, idempotency_key),
                ).fetchone()
                if existing:
                    return dict(existing)

                intent = {
                    "intent_id": new_id("INT"),
                    "order_id": order_id,
                    "provider_ref": new_id("PAYREF"),
                    "idempotency_key": idempotency_key,
                    "amount_cents": order["total_amount_cents"],
                    "currency": currency,
                    "provider_region": provider_region,
                    "status": PAYMENT_PENDING,
                    "created_at": now,
                    "updated_at": now,
                }
                conn.execute(
                    """
                    INSERT INTO payment_intents (
                        intent_id, order_id, provider_ref, idempotency_key, amount_cents,
                        currency, provider_region, status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    tuple(intent.values()),
                )
                self._record_event(
                    conn,
                    aggregate_id=order_id,
                    event_type="payment.authorization.requested",
                    payload={
                        "intent_id": intent["intent_id"],
                        "order_id": order_id,
                        "provider_ref": intent["provider_ref"],
                        "amount_cents": intent["amount_cents"],
                        "currency": currency,
                        "provider_region": provider_region,
                    },
                    now=now,
                )
                return intent

    def handle_payment_callback(
        self,
        *,
        callback_id: str,
        provider_ref: str,
        status: str,
        amount_cents: int,
        signature: str,
    ) -> dict[str, Any]:
        callback_id = (callback_id or "").strip()
        provider_ref = (provider_ref or "").strip()
        status = (status or "").strip().upper()
        if not callback_id or not provider_ref:
            raise BusinessError("INVALID_PAYMENT_CALLBACK", "callback_id and provider_ref are required")
        if status not in {PAYMENT_CAPTURED, PAYMENT_FAILED}:
            raise BusinessError("INVALID_PAYMENT_CALLBACK", "callback status must be CAPTURED or FAILED")
        if amount_cents <= 0:
            raise BusinessError("INVALID_PAYMENT_AMOUNT", "amount_cents must be positive")

        payload = {
            "callback_id": callback_id,
            "provider_ref": provider_ref,
            "status": status,
            "amount_cents": amount_cents,
        }
        signature_valid = verify_payment_callback(
            callback_id=callback_id,
            provider_ref=provider_ref,
            status=status,
            amount_cents=amount_cents,
            signature=signature,
            secret=settings.payment_webhook_secret,
        )
        self.initialize()
        if not signature_valid:
            self._record_rejected_callback(payload, "INVALID_PROVIDER_SIGNATURE")
            raise BusinessError("INVALID_PROVIDER_SIGNATURE", "payment callback signature is invalid")

        payment_amount_mismatch = False
        payment_conflict = False
        with connect(self.db_path) as conn:
            existing_callback = conn.execute(
                "SELECT * FROM payment_callbacks WHERE callback_id = ?",
                (callback_id,),
            ).fetchone()
            if existing_callback:
                if existing_callback["callback_status"] == CALLBACK_REJECTED:
                    raise BusinessError("INVALID_PROVIDER_SIGNATURE", "payment callback was previously rejected")
                intent = conn.execute(
                    "SELECT * FROM payment_intents WHERE provider_ref = ?",
                    (provider_ref,),
                ).fetchone()
                if not intent:
                    raise BusinessError("PAYMENT_INTENT_NOT_FOUND", f"payment intent {provider_ref} does not exist")
                return {"callback": dict(existing_callback), "order": self.oms.order(intent["order_id"])}

            intent = conn.execute(
                "SELECT * FROM payment_intents WHERE provider_ref = ?",
                (provider_ref,),
            ).fetchone()
            if not intent:
                raise BusinessError("PAYMENT_INTENT_NOT_FOUND", f"payment intent {provider_ref} does not exist")
            payment_amount_mismatch = int(intent["amount_cents"]) != amount_cents
            if not payment_amount_mismatch:
                payment = conn.execute(
                    "SELECT status FROM payments WHERE provider_ref = ?",
                    (provider_ref,),
                ).fetchone()
                payment_conflict = bool(
                    payment
                    and payment["status"] in {PAYMENT_CAPTURED, PAYMENT_FAILED}
                    and payment["status"] != status
                )

        if payment_amount_mismatch:
            self._record_rejected_callback(payload, "PAYMENT_AMOUNT_MISMATCH", signature_valid=True)
            raise BusinessError("PAYMENT_AMOUNT_MISMATCH", "callback amount does not match payment intent")
        if payment_conflict:
            self._record_rejected_callback(payload, "PAYMENT_CALLBACK_CONFLICT", signature_valid=True)
            raise BusinessError(
                "PAYMENT_CALLBACK_CONFLICT",
                "provider callback conflicts with the recorded payment state",
            )
        order = self.oms.capture_payment(
            order_id=intent["order_id"],
            provider_ref=provider_ref,
            succeed=status == PAYMENT_CAPTURED,
        )
        now = utc_now()
        with connect(self.db_path) as conn:
            with transaction(conn):
                conn.execute(
                    """
                    INSERT INTO payment_callbacks (
                        callback_id, provider_ref, callback_status, amount_cents,
                        payload_hash, signature_valid, processed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        callback_id,
                        provider_ref,
                        CALLBACK_PROCESSED,
                        amount_cents,
                        canonical_payload_hash(payload),
                        1,
                        now,
                    ),
                )
                conn.execute(
                    "UPDATE payment_intents SET status = ?, updated_at = ? WHERE provider_ref = ?",
                    (status, now, provider_ref),
                )
                if status == PAYMENT_CAPTURED:
                    self._record_capture_ledger(
                        conn,
                        intent=intent,
                        callback_id=callback_id,
                        now=now,
                    )
                self._record_event(
                    conn,
                    aggregate_id=intent["order_id"],
                    event_type="payment.callback.accepted",
                    payload={
                        "callback_id": callback_id,
                        "provider_ref": provider_ref,
                        "status": status,
                        "order_id": intent["order_id"],
                    },
                    now=now,
                )
        return {"callback_id": callback_id, "status": CALLBACK_PROCESSED, "order": order}

    def reconcile_settlement(
        self,
        *,
        settlement_id: str,
        provider_ref: str,
        gross_amount_cents: int,
        fee_amount_cents: int,
        settled_at: str,
    ) -> dict[str, Any]:
        if gross_amount_cents <= 0 or fee_amount_cents < 0 or fee_amount_cents > gross_amount_cents:
            raise BusinessError("INVALID_SETTLEMENT_AMOUNT", "settlement amounts are invalid")
        self.initialize()
        now = utc_now()
        with connect(self.db_path) as conn:
            with transaction(conn):
                existing = conn.execute(
                    "SELECT * FROM settlement_records WHERE provider_ref = ?",
                    (provider_ref,),
                ).fetchone()
                if existing:
                    return dict(existing)
                intent = conn.execute(
                    "SELECT * FROM payment_intents WHERE provider_ref = ?",
                    (provider_ref,),
                ).fetchone()
                payment = conn.execute(
                    "SELECT * FROM payments WHERE provider_ref = ?",
                    (provider_ref,),
                ).fetchone()
                if not intent or not payment:
                    raise BusinessError("PAYMENT_NOT_FOUND", f"captured payment {provider_ref} does not exist")
                if payment["status"] != PAYMENT_CAPTURED:
                    raise BusinessError("PAYMENT_NOT_CAPTURED", f"payment {provider_ref} is not captured")

                status = SETTLEMENT_MATCHED if gross_amount_cents == payment["amount_cents"] else SETTLEMENT_MISMATCH
                record = {
                    "settlement_id": settlement_id,
                    "provider_ref": provider_ref,
                    "order_id": intent["order_id"],
                    "gross_amount_cents": gross_amount_cents,
                    "fee_amount_cents": fee_amount_cents,
                    "net_amount_cents": gross_amount_cents - fee_amount_cents,
                    "status": status,
                    "provider_region": intent["provider_region"],
                    "settled_at": settled_at,
                    "created_at": now,
                }
                conn.execute(
                    """
                    INSERT INTO settlement_records (
                        settlement_id, provider_ref, order_id, gross_amount_cents,
                        fee_amount_cents, net_amount_cents, status, provider_region,
                        settled_at, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    tuple(record.values()),
                )
                self._record_event(
                    conn,
                    aggregate_id=intent["order_id"],
                    event_type="payment.settlement.reconciled",
                    payload={
                        "settlement_id": settlement_id,
                        "provider_ref": provider_ref,
                        "status": status,
                        "gross_amount_cents": gross_amount_cents,
                        "fee_amount_cents": fee_amount_cents,
                    },
                    now=now,
                )
                return record

    def export_order_summary(
        self,
        *,
        order_id: str,
        target_region: str,
        purpose: str,
        requested_fields: list[str] | None = None,
    ) -> dict[str, Any]:
        target_region = self._region(target_region)
        purpose = (purpose or "").strip()
        if not purpose:
            raise BusinessError("INVALID_EXPORT_PURPOSE", "export purpose is required")
        requested = set(requested_fields or SAFE_CROSS_BORDER_FIELDS)
        forbidden = sorted(requested - SAFE_CROSS_BORDER_FIELDS)
        self.initialize()
        now = utc_now()
        with connect(self.db_path) as conn:
            order = conn.execute(
                "SELECT order_id, customer_id, status, created_at FROM orders WHERE order_id = ?",
                (order_id,),
            ).fetchone()
            if not order:
                raise BusinessError("ORDER_NOT_FOUND", f"order {order_id} does not exist")
            policy = conn.execute(
                "SELECT data_region FROM customer_data_policies WHERE customer_id = ?",
                (order["customer_id"],),
            ).fetchone()
            source_region = policy["data_region"] if policy else "CN"
            item_count = conn.execute(
                "SELECT COALESCE(SUM(quantity), 0) FROM order_items WHERE order_id = ?",
                (order_id,),
            ).fetchone()[0]

            if forbidden:
                self._write_export_audit(
                    conn,
                    order_id=order_id,
                    source_region=source_region,
                    target_region=target_region,
                    purpose=purpose,
                    requested_fields=sorted(requested),
                    decision="DENIED",
                    reason=f"fields are outside the minimised export contract: {', '.join(forbidden)}",
                    now=now,
                )
                raise BusinessError("CROSS_BORDER_FIELD_DENIED", "requested fields are not allowed for export")

            payload = build_cross_border_order_summary(
                order_id=order["order_id"],
                customer_id=order["customer_id"],
                order_status=order["status"],
                item_count=int(item_count),
                created_at=order["created_at"],
                source_region=source_region,
                target_region=target_region,
                token_secret=settings.privacy_token_secret,
            )
            export_id = new_id("EXP")
            self._write_export_audit(
                conn,
                order_id=order_id,
                source_region=source_region,
                target_region=target_region,
                purpose=purpose,
                requested_fields=sorted(requested),
                decision="ALLOWED",
                reason="fixed pseudonymized operational contract",
                now=now,
                export_id=export_id,
            )
            return {"export_id": export_id, "decision": "ALLOWED", "purpose": purpose, "data": payload}

    def payment_intents(self, *, limit: int = 100) -> list[dict]:
        self.initialize()
        with connect(self.db_path) as conn:
            return [dict(row) for row in conn.execute(
                "SELECT * FROM payment_intents ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()]

    def ledger_entries(self, *, limit: int = 100) -> list[dict]:
        self.initialize()
        with connect(self.db_path) as conn:
            return [dict(row) for row in conn.execute(
                "SELECT * FROM payment_ledger_entries ORDER BY created_at, entry_id LIMIT ?",
                (limit,),
            ).fetchall()]

    def settlements(self, *, limit: int = 100) -> list[dict]:
        self.initialize()
        with connect(self.db_path) as conn:
            return [dict(row) for row in conn.execute(
                "SELECT * FROM settlement_records ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()]

    def export_audit(self, *, limit: int = 100) -> list[dict]:
        self.initialize()
        with connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT * FROM data_export_audit ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [
                {**dict(row), "requested_fields": json.loads(row["requested_fields_json"])}
                for row in rows
            ]

    def summary(self) -> dict[str, Any]:
        self.initialize()
        with connect(self.db_path) as conn:
            return {
                "payment_intents": self._counts(conn, "payment_intents", "status"),
                "payment_callbacks": self._counts(conn, "payment_callbacks", "callback_status"),
                "ledger_entries": conn.execute("SELECT COUNT(*) AS count FROM payment_ledger_entries").fetchone()["count"],
                "settlements": self._counts(conn, "settlement_records", "status"),
                "data_exports": self._counts(conn, "data_export_audit", "decision"),
            }

    def _record_capture_ledger(
        self,
        conn: sqlite3.Connection,
        *,
        intent: sqlite3.Row,
        callback_id: str,
        now: str,
    ) -> None:
        """Append a deterministic double-entry record for a captured payment."""
        ledger_txn_id = f"LEDGER-{intent['provider_ref']}"
        entries = [
            ("customer_receivable", "DEBIT"),
            ("merchant_cash_pending", "CREDIT"),
        ]
        for account_code, direction in entries:
            conn.execute(
                """
                INSERT OR IGNORE INTO payment_ledger_entries (
                    entry_id, ledger_txn_id, order_id, provider_ref, callback_id,
                    account_code, direction, amount_cents, currency, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"{ledger_txn_id}-{direction}",
                    ledger_txn_id,
                    intent["order_id"],
                    intent["provider_ref"],
                    callback_id,
                    account_code,
                    direction,
                    intent["amount_cents"],
                    intent["currency"],
                    now,
                ),
            )
    def _record_rejected_callback(self, payload: dict[str, Any], reason: str, *, signature_valid: bool = False) -> None:
        now = utc_now()
        with connect(self.db_path) as conn:
            with transaction(conn):
                conn.execute(
                    """
                    INSERT OR IGNORE INTO payment_callbacks (
                        callback_id, provider_ref, callback_status, amount_cents,
                        payload_hash, signature_valid, processed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload["callback_id"], payload["provider_ref"], CALLBACK_REJECTED,
                        payload["amount_cents"], canonical_payload_hash(payload), int(signature_valid), now,
                    ),
                )
                self._record_event(
                    conn,
                    aggregate_id=payload["provider_ref"],
                    event_type="payment.callback.rejected",
                    payload={
                        "callback_id": payload["callback_id"],
                        "provider_ref": payload["provider_ref"],
                        "reason": reason,
                        "signature_valid": bool(signature_valid),
                    },
                    now=now,
                )

    def _write_export_audit(
        self,
        conn: sqlite3.Connection,
        *,
        order_id: str,
        source_region: str,
        target_region: str,
        purpose: str,
        requested_fields: list[str],
        decision: str,
        reason: str,
        now: str,
        export_id: str | None = None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO data_export_audit (
                export_id, order_id, source_region, target_region, purpose,
                data_classification, requested_fields_json, decision, reason, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                export_id or new_id("EXP-AUDIT"), order_id, source_region, target_region,
                purpose, "SENSITIVE_PERSONAL_TO_PSEUDONYMIZED",
                json.dumps(requested_fields, sort_keys=True), decision, reason, now,
            ),
        )

    def _record_event(
        self,
        conn: sqlite3.Connection,
        *,
        aggregate_id: str,
        event_type: str,
        payload: dict[str, Any],
        now: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO outbox_events (
                event_id, aggregate_type, aggregate_id, event_type,
                payload_json, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                new_id("EVT"), "payment_security", aggregate_id, event_type,
                json.dumps(payload, sort_keys=True), EVENT_PENDING, now,
            ),
        )

    @staticmethod
    def _counts(conn: sqlite3.Connection, table: str, column: str) -> dict[str, int]:
        return {
            row["value"]: row["count"]
            for row in conn.execute(
                f"SELECT {column} AS value, COUNT(*) AS count FROM {table} GROUP BY {column}"
            ).fetchall()
        }
