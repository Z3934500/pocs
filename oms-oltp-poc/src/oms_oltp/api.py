from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .config import settings
from .payment_security import SecurePaymentPrivacyService
from .privacy import SAFE_CROSS_BORDER_FIELDS
from .service import BusinessError, OMSService


app = FastAPI(
    title="OMS OLTP PoC",
    version="0.2.0",
    description="Order-management OLTP system with payment-provider callbacks, settlement reconciliation, saga compensation and data-residency controls.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

service = OMSService()
secure_service = SecurePaymentPrivacyService()


class OrderItemIn(BaseModel):
    sku_id: str = Field(..., examples=["SKU-RED-001"])
    quantity: int = Field(..., gt=0, examples=[2])


class CreateOrderIn(BaseModel):
    customer_id: str = Field(..., examples=["CUST-1001"])
    items: list[OrderItemIn]
    idempotency_key: str | None = None


class PaymentIn(BaseModel):
    provider_ref: str | None = None
    succeed: bool = True


class PaymentIntentIn(BaseModel):
    idempotency_key: str = Field(..., min_length=1, max_length=128)
    provider_region: str = Field(default="CN", examples=["CN", "OVERSEAS"])
    currency: str = Field(default="CNY", min_length=3, max_length=3)


class PaymentCallbackIn(BaseModel):
    callback_id: str = Field(..., min_length=1, max_length=128)
    provider_ref: str = Field(..., min_length=1, max_length=128)
    status: str = Field(..., examples=["CAPTURED", "FAILED"])
    amount_cents: int = Field(..., gt=0)
    signature: str = Field(..., min_length=1)


class SettlementIn(BaseModel):
    settlement_id: str = Field(..., min_length=1, max_length=128)
    provider_ref: str = Field(..., min_length=1, max_length=128)
    gross_amount_cents: int = Field(..., gt=0)
    fee_amount_cents: int = Field(default=0, ge=0)
    settled_at: str


class ExportIn(BaseModel):
    target_region: str = Field(..., examples=["OVERSEAS"])
    purpose: str = Field(..., examples=["customer_analytics"])
    requested_fields: list[str] | None = None


class CancelIn(BaseModel):
    reason: str = "customer cancelled"


def ensure_data() -> None:
    service.initialize(reset=False)
    secure_service.initialize()


def handle_business_error(exc: BusinessError) -> HTTPException:
    status_code = 404 if exc.code.endswith("NOT_FOUND") else 409
    if exc.code in {
        "INVALID_ITEM",
        "INVALID_QUANTITY",
        "EMPTY_ORDER",
        "UNKNOWN_CUSTOMER",
        "UNKNOWN_SKU",
        "INVALID_CURRENCY",
        "INVALID_PAYMENT_AMOUNT",
        "INVALID_SETTLEMENT_AMOUNT",
        "INVALID_PAYMENT_CALLBACK",
        "INVALID_DATA_REGION",
        "INVALID_EXPORT_PURPOSE",
    }:
        status_code = 400
    if exc.code in {"INVALID_PROVIDER_SIGNATURE", "CROSS_BORDER_FIELD_DENIED"}:
        status_code = 403
    return HTTPException(status_code=status_code, detail={"code": exc.code, "message": exc.message})


@app.get("/api/health")
def health() -> dict[str, str]:
    ensure_data()
    return {"status": "ok", "database": str(settings.sqlite_path)}


@app.post("/api/demo/reset")
def reset_demo() -> dict[str, object]:
    service.initialize(reset=True)
    secure_service.initialize()
    return {"status": "reset", "summary": service.summary()}


@app.post("/api/orders")
def create_order(payload: CreateOrderIn) -> dict:
    ensure_data()
    try:
        return service.place_order(
            customer_id=payload.customer_id,
            items=[item.model_dump() for item in payload.items],
            idempotency_key=payload.idempotency_key or f"api-{uuid4().hex}",
        )
    except BusinessError as exc:
        raise handle_business_error(exc) from exc


@app.post("/api/orders/{order_id}/payment")
def capture_payment(order_id: str, payload: PaymentIn) -> dict:
    """Legacy synchronous demo endpoint; production flow uses intent + callback below."""
    ensure_data()
    try:
        return service.capture_payment(order_id=order_id, provider_ref=payload.provider_ref, succeed=payload.succeed)
    except BusinessError as exc:
        raise handle_business_error(exc) from exc


@app.post("/api/orders/{order_id}/payment-intents")
def create_payment_intent(order_id: str, payload: PaymentIntentIn) -> dict:
    ensure_data()
    try:
        return secure_service.create_payment_intent(
            order_id=order_id,
            idempotency_key=payload.idempotency_key,
            provider_region=payload.provider_region,
            currency=payload.currency,
        )
    except BusinessError as exc:
        raise handle_business_error(exc) from exc


@app.post("/api/payments/callback")
def payment_callback(payload: PaymentCallbackIn) -> dict:
    ensure_data()
    try:
        return secure_service.handle_payment_callback(
            callback_id=payload.callback_id,
            provider_ref=payload.provider_ref,
            status=payload.status,
            amount_cents=payload.amount_cents,
            signature=payload.signature,
        )
    except BusinessError as exc:
        raise handle_business_error(exc) from exc


@app.post("/api/payments/settlements")
def reconcile_settlement(payload: SettlementIn) -> dict:
    ensure_data()
    try:
        return secure_service.reconcile_settlement(
            settlement_id=payload.settlement_id,
            provider_ref=payload.provider_ref,
            gross_amount_cents=payload.gross_amount_cents,
            fee_amount_cents=payload.fee_amount_cents,
            settled_at=payload.settled_at,
        )
    except BusinessError as exc:
        raise handle_business_error(exc) from exc


@app.post("/api/data-exports/order-summary/{order_id}")
def export_order_summary(order_id: str, payload: ExportIn) -> dict:
    ensure_data()
    try:
        return secure_service.export_order_summary(
            order_id=order_id,
            target_region=payload.target_region,
            purpose=payload.purpose,
            requested_fields=payload.requested_fields,
        )
    except BusinessError as exc:
        raise handle_business_error(exc) from exc


@app.post("/api/orders/{order_id}/cancel")
def cancel_order(order_id: str, payload: CancelIn) -> dict:
    ensure_data()
    try:
        return service.cancel_order(order_id=order_id, reason=payload.reason)
    except BusinessError as exc:
        raise handle_business_error(exc) from exc


@app.post("/api/orders/{order_id}/ship")
def ship_order(order_id: str) -> dict:
    ensure_data()
    try:
        return service.ship_order(order_id=order_id)
    except BusinessError as exc:
        raise handle_business_error(exc) from exc


@app.post("/api/reservations/expire")
def expire_reservations() -> dict:
    ensure_data()
    return service.expire_reservations()


@app.post("/api/outbox/publish")
def publish_outbox(limit: int = Query(default=50, ge=1, le=500)) -> list[dict]:
    ensure_data()
    return service.publish_outbox(limit=limit)


@app.get("/api/summary")
def summary() -> dict:
    ensure_data()
    result = service.summary()
    result["payment_security"] = secure_service.summary()
    return result


@app.get("/api/customers")
def customers() -> list[dict]:
    ensure_data()
    return service.customers()


@app.get("/api/inventory")
def inventory() -> list[dict]:
    ensure_data()
    return service.inventory()


@app.get("/api/orders")
def orders(limit: int = Query(default=50, ge=1, le=200)) -> list[dict]:
    ensure_data()
    return service.orders(limit=limit)


@app.get("/api/orders/{order_id}")
def order(order_id: str) -> dict:
    ensure_data()
    try:
        return service.order(order_id)
    except BusinessError as exc:
        raise handle_business_error(exc) from exc


@app.get("/api/payment-intents")
def payment_intents(limit: int = Query(default=100, ge=1, le=500)) -> list[dict]:
    ensure_data()
    return secure_service.payment_intents(limit=limit)


@app.get("/api/ledger")
def ledger(limit: int = Query(default=100, ge=1, le=500)) -> list[dict]:
    ensure_data()
    return secure_service.ledger_entries(limit=limit)


@app.get("/api/reconciliation")
def reconciliation(limit: int = Query(default=100, ge=1, le=500)) -> list[dict]:
    ensure_data()
    return secure_service.settlements(limit=limit)


@app.get("/api/data-exports/audit")
def data_export_audit(limit: int = Query(default=100, ge=1, le=500)) -> list[dict]:
    ensure_data()
    return secure_service.export_audit(limit=limit)


@app.get("/api/outbox")
def outbox(limit: int = Query(default=100, ge=1, le=500)) -> list[dict]:
    ensure_data()
    return service.outbox(limit=limit)


@app.get("/api/lineage")
def lineage() -> dict[str, object]:
    return {
        "mode": "OLTP",
        "write_model": "3NF-style row tables: orders, order_items, payments, inventory_reservations and outbox_events.",
        "payment_flow": [
            "payment intent uses an idempotency key and creates a provider reference",
            "provider callback is authenticated with HMAC and deduplicated by callback_id",
            "accepted CAPTURED or FAILED callbacks reuse the OMS Saga commit/compensation path",
            "settlement records compare provider gross amount with the captured payment amount",
        ],
        "privacy_flow": [
            "customer data is assigned to CN or OVERSEAS residency policy",
            "cross-border exports use a fixed pseudonymized operational contract",
            "raw customer identifiers, names, provider references and payment amounts are denied",
            "allowed and denied exports are written to an audit table and Outbox event",
        ],
        "transaction_flow": [
            "place_order reserves stock and creates order in one ACID transaction",
            "payment success commits reserved stock to sold stock",
            "payment failure, cancel or timeout releases reserved stock",
            "outbox_events is the reliable handoff to Kafka/CDC and downstream OLAP systems",
        ],
        "downstream_olap": [
            "OEE Data Platform reads curated operational history for analytics",
            "CCE Feature Platform reads customer/order events for features and segmentation",
        ],
        "safe_export_fields": sorted(SAFE_CROSS_BORDER_FIELDS),
    }


@app.get("/")
def index() -> FileResponse:
    ensure_data()
    return FileResponse(settings.frontend_dir / "index.html")


static_path = Path(settings.frontend_dir)
if static_path.exists():
    app.mount("/assets", StaticFiles(directory=static_path), name="assets")
