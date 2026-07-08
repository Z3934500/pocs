from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .config import settings
from .service import BusinessError, OMSService


app = FastAPI(
    title="OMS OLTP PoC",
    version="0.1.0",
    description="Order-management OLTP system with inventory reservation, saga compensation and outbox events.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

service = OMSService()


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


class CancelIn(BaseModel):
    reason: str = "customer cancelled"


def ensure_data() -> None:
    service.initialize(reset=False)


def handle_business_error(exc: BusinessError) -> HTTPException:
    status_code = 404 if exc.code.endswith("NOT_FOUND") else 409
    if exc.code in {"INVALID_ITEM", "INVALID_QUANTITY", "EMPTY_ORDER", "UNKNOWN_CUSTOMER", "UNKNOWN_SKU"}:
        status_code = 400
    return HTTPException(status_code=status_code, detail={"code": exc.code, "message": exc.message})


@app.get("/api/health")
def health() -> dict[str, str]:
    ensure_data()
    return {"status": "ok", "database": str(settings.sqlite_path)}


@app.post("/api/demo/reset")
def reset_demo() -> dict[str, object]:
    service.initialize(reset=True)
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
    ensure_data()
    try:
        return service.capture_payment(order_id=order_id, provider_ref=payload.provider_ref, succeed=payload.succeed)
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
    return service.summary()


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


@app.get("/api/outbox")
def outbox(limit: int = Query(default=100, ge=1, le=500)) -> list[dict]:
    ensure_data()
    return service.outbox(limit=limit)


@app.get("/api/lineage")
def lineage() -> dict[str, object]:
    return {
        "mode": "OLTP",
        "write_model": "3NF-style row tables: orders, order_items, payments, inventory_reservations and outbox_events.",
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
    }


@app.get("/")
def index() -> FileResponse:
    ensure_data()
    return FileResponse(settings.frontend_dir / "index.html")


static_path = Path(settings.frontend_dir)
if static_path.exists():
    app.mount("/assets", StaticFiles(directory=static_path), name="assets")
