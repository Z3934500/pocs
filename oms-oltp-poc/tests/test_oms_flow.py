from __future__ import annotations

import pytest

from oms_oltp.service import BusinessError, OMSService


@pytest.fixture()
def service(tmp_path):
    svc = OMSService(tmp_path / "oms_test.sqlite")
    svc.initialize(reset=True)
    return svc


def stock_for(service: OMSService, sku_id: str) -> dict:
    return next(row for row in service.inventory() if row["sku_id"] == sku_id)


def test_order_payment_commit_moves_inventory_and_events(service: OMSService) -> None:
    order = service.place_order(
        customer_id="CUST-1001",
        idempotency_key="idem-commit",
        items=[{"sku_id": "SKU-RED-001", "quantity": 2}],
    )
    assert order["status"] == "RESERVED"
    assert stock_for(service, "SKU-RED-001")["available_stock"] == 118
    assert stock_for(service, "SKU-RED-001")["reserved_stock"] == 2

    confirmed = service.capture_payment(order_id=order["order_id"], provider_ref="pay-commit", succeed=True)

    assert confirmed["status"] == "CONFIRMED"
    assert confirmed["reservation"]["status"] == "COMMITTED"
    assert stock_for(service, "SKU-RED-001")["available_stock"] == 118
    assert stock_for(service, "SKU-RED-001")["reserved_stock"] == 0
    assert stock_for(service, "SKU-RED-001")["sold_stock"] == 2
    assert {"order.created", "inventory.reserved", "payment.captured", "inventory.committed", "order.confirmed"}.issubset(
        {event["event_type"] for event in service.outbox()}
    )


def test_cancel_releases_reserved_stock(service: OMSService) -> None:
    order = service.place_order(
        customer_id="CUST-1002",
        idempotency_key="idem-cancel",
        items=[{"sku_id": "SKU-BLK-002", "quantity": 3}],
    )

    cancelled = service.cancel_order(order_id=order["order_id"], reason="buyer changed mind")

    assert cancelled["status"] == "CANCELLED"
    assert cancelled["reservation"]["status"] == "RELEASED"
    assert stock_for(service, "SKU-BLK-002")["available_stock"] == 80
    assert stock_for(service, "SKU-BLK-002")["reserved_stock"] == 0


def test_idempotency_key_returns_existing_order(service: OMSService) -> None:
    first = service.place_order(
        customer_id="CUST-1001",
        idempotency_key="same-request",
        items=[{"sku_id": "SKU-BAT-004", "quantity": 1}],
    )
    second = service.place_order(
        customer_id="CUST-1001",
        idempotency_key="same-request",
        items=[{"sku_id": "SKU-BAT-004", "quantity": 1}],
    )

    assert second["order_id"] == first["order_id"]
    assert stock_for(service, "SKU-BAT-004")["available_stock"] == 239
    assert service.summary()["orders"] == {"RESERVED": 1}


def test_insufficient_stock_does_not_oversell(service: OMSService) -> None:
    with pytest.raises(BusinessError) as error:
        service.place_order(
            customer_id="CUST-1001",
            idempotency_key="too-large",
            items=[{"sku_id": "SKU-SIL-003", "quantity": 999}],
        )

    assert error.value.code == "INSUFFICIENT_STOCK"
    assert stock_for(service, "SKU-SIL-003")["available_stock"] == 35
    assert stock_for(service, "SKU-SIL-003")["reserved_stock"] == 0


def test_expired_reservations_release_stock(service: OMSService) -> None:
    order = service.place_order(
        customer_id="CUST-1003",
        idempotency_key="expire-me",
        reservation_ttl_minutes=-1,
        items=[{"sku_id": "SKU-BAT-004", "quantity": 5}],
    )

    result = service.expire_reservations()

    assert result["expired_count"] == 1
    assert result["order_ids"] == [order["order_id"]]
    assert service.order(order["order_id"])["status"] == "CANCELLED"
    assert stock_for(service, "SKU-BAT-004")["available_stock"] == 240
