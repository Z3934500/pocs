from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
PROJECT = ROOT / "data-governance-poc"
sys.path.insert(0, str(PROJECT / "src"))
sys.path.insert(0, str(ROOT / "oms-oltp-poc" / "src"))

from data_governance.monitor import DataGovernanceMonitor, STATUS_FAIL, STATUS_OK, load_contract
from oms_oltp.service import OMSService


def build_demo_db() -> Path:
    runtime_dir = PROJECT / "data" / "test_runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    db_path = runtime_dir / f"oms_test_{uuid4().hex}.sqlite"
    service = OMSService(db_path)
    service.initialize(reset=True)
    order = service.place_order(
        customer_id="CUST-1001",
        idempotency_key="test-governance",
        items=[{"sku_id": "SKU-RED-001", "quantity": 2}],
    )
    service.capture_payment(order_id=order["order_id"], provider_ref="test-payment", succeed=True)
    service.publish_outbox(limit=100)
    return db_path


def test_governance_checks_pass_for_clean_oms_flow() -> None:
    db_path = build_demo_db()
    contract = load_contract(PROJECT / "contracts" / "oms_event_contract.json")

    results = DataGovernanceMonitor(db_path, contract).run_all()

    assert {result.check: result.status for result in results}["event.payload_contract"] == STATUS_OK
    assert {result.check: result.status for result in results}["reconciliation.inventory_movements"] == STATUS_OK
    assert not [result for result in results if result.status == STATUS_FAIL]


def test_governance_checks_detect_bad_payload_and_reconciliation() -> None:
    db_path = build_demo_db()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE outbox_events
            SET payload_json = '{"order_id": "missing-required-fields"}'
            WHERE event_type = 'order.created'
            """
        )
        conn.execute(
            """
            UPDATE sku_inventory
            SET sold_stock = sold_stock + 99
            WHERE sku_id = 'SKU-RED-001'
            """
        )

    contract = load_contract(PROJECT / "contracts" / "oms_event_contract.json")
    results = {result.check: result for result in DataGovernanceMonitor(db_path, contract).run_all()}

    assert results["event.payload_contract"].status == STATUS_FAIL
    assert results["reconciliation.inventory_movements"].status == STATUS_FAIL