from __future__ import annotations

import sys
from pathlib import Path

from .monitor import DataGovernanceMonitor, load_contract, render_table


def main() -> int:
    repo_root = Path(__file__).resolve().parents[3]
    oms_src = repo_root / "oms-oltp-poc" / "src"
    if str(oms_src) not in sys.path:
        sys.path.insert(0, str(oms_src))

    from oms_oltp.service import OMSService

    db_path = repo_root / "data-governance-poc" / "data" / "oms_governance_demo.sqlite"
    contract_path = repo_root / "data-governance-poc" / "contracts" / "oms_event_contract.json"

    service = OMSService(db_path)
    service.initialize(reset=True)
    order = service.place_order(
        customer_id="CUST-1001",
        idempotency_key="governance-demo-order",
        items=[{"sku_id": "SKU-RED-001", "quantity": 2}],
    )
    service.capture_payment(order_id=order["order_id"], provider_ref="governance-demo-payment", succeed=True)
    service.publish_outbox(limit=100)

    contract = load_contract(contract_path)
    results = DataGovernanceMonitor(db_path, contract).run_all()
    print(render_table(results))
    print(f"\nDemo database: {db_path}")
    return 1 if any(result.is_failure for result in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())