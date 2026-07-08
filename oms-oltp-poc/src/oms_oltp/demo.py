from __future__ import annotations

import argparse
import json

from .service import OMSService


def run_demo(reset: bool = True) -> dict:
    service = OMSService()
    service.initialize(reset=reset)
    order = service.place_order(
        customer_id="CUST-1001",
        idempotency_key="demo-order-001",
        items=[
            {"sku_id": "SKU-RED-001", "quantity": 2},
            {"sku_id": "SKU-BAT-004", "quantity": 4},
        ],
    )
    confirmed = service.capture_payment(order_id=order["order_id"], provider_ref="demo-payment-001", succeed=True)
    shipped = service.ship_order(order_id=confirmed["order_id"])
    published = service.publish_outbox()
    return {
        "order": shipped,
        "published_events": [event["event_type"] for event in published],
        "summary": service.summary(),
        "inventory": service.inventory(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local OMS OLTP demo flow.")
    parser.add_argument("--no-reset", action="store_true", help="Keep existing data before running the demo.")
    args = parser.parse_args()
    print(json.dumps(run_demo(reset=not args.no_reset), indent=2))


if __name__ == "__main__":
    main()
