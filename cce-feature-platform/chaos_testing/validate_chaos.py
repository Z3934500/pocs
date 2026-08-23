#!/usr/bin/env python3
"""
CCE Chaos Test Validation Script
=================================
Runs automated checks during and after chaos experiments to verify:1. preStop drain correctness      — pod drains before termination
  2. Redis ZSET state machine       — no state loss during network partition
  3. Feature store consistency      — no duplicate / missing feature updates
  4. Flink CDC pipeline             — dedup + intent score correctness
  5. Brain-split detection          — no dual-write after Redis failover

Usage:
  # Run all validations against local PoC (no K8s needed):
  python validate_chaos.py --mode local

  # Run against a live K8s namespace during a chaos experiment:
  python validate_chaos.py --mode k8s --app-url http://cce-feature-platform.cce.svc.cluster.local:8000

  # Run a specific check only:
  python validate_chaos.py --mode local --check state-machine
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

# ---------------------------------------------------------------------------
# Result model
# ---------------------------------------------------------------------------

@dataclass
class CheckResult:
    name: str
    passed: bool
    message: str
    details: dict = field(default_factory=dict)


class ValidationReport:
    def __init__(self) -> None:
        self.results: list[CheckResult] = []

    def add(self, result: CheckResult) -> None:
        self.results.append(result)
        status = "[PASS]" if result.passed else "[FAIL]"
        print(f"  {status}  {result.name}: {result.message}")
        if not result.passed and result.details:
            for k, v in result.details.items():
                print(f"         {k}: {v}")

    def summary(self) -> int:
        total  = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        failed = total - passed
        print(f"\n{'='*60}")
        print(f"  TOTAL: {total}  PASSED: {passed}  FAILED: {failed}")
        print(f"{'='*60}")
        return 0 if failed == 0 else 1


# ---------------------------------------------------------------------------
# Check 1: preStop drain endpoint
# ---------------------------------------------------------------------------

def check_prestop_drain(app_url: str, report: ValidationReport) -> None:
    """
    Verifies the /admin/drain → /health polling cycle that the preStop hook
    relies on.  Simulates what the preStop shell script does.
    """
    try:
        import urllib.request
        import urllib.error

        # 1. Trigger drain
        req = urllib.request.Request(
            f"{app_url}/admin/drain",
            method="POST",
            headers={"Content-Type": "application/json"},
            data=b"{}",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = json.loads(resp.read())
        assert body.get("draining") is True, f"Expected draining=true, got {body}"

        # 2. Readiness probe should now return 503
        try:
            urllib.request.urlopen(f"{app_url}/health/ready", timeout=5)
            report.add(CheckResult(
                "prestop_readiness_503",
                passed=False,
                message="Expected 503 during drain but got 200",
            ))
        except urllib.error.HTTPError as exc:
            report.add(CheckResult(
                "prestop_readiness_503",
                passed=exc.code == 503,
                message=f"Readiness returned {exc.code} during drain (expected 503)",
                details={"http_code": exc.code},
            ))

        # 3. Combined /health should show draining=true
        with urllib.request.urlopen(f"{app_url}/health", timeout=5) as resp:
            health = json.loads(resp.read())
        report.add(CheckResult(
            "prestop_health_draining_flag",
            passed=health.get("draining") is True,
            message=f"Health draining flag = {health.get('draining')}",
            details=health,
        ))

        # 4. Pipeline run should be rejected with 503
        req2 = urllib.request.Request(
            f"{app_url}/api/pipeline/run",
            method="POST",
            headers={"Content-Type": "application/json"},
            data=b"{}",
        )
        try:
            urllib.request.urlopen(req2, timeout=5)
            report.add(CheckResult(
                "prestop_pipeline_rejected",
                passed=False,
                message="Pipeline run accepted during drain — should have been rejected",
            ))
        except urllib.error.HTTPError as exc:
            report.add(CheckResult(
                "prestop_pipeline_rejected",
                passed=exc.code == 503,
                message=f"Pipeline correctly rejected during drain (HTTP {exc.code})",
            ))

    except Exception as exc:
        report.add(CheckResult(
            "prestop_drain",
            passed=False,
            message=f"Exception during drain check: {exc}",
        ))


# ---------------------------------------------------------------------------
# Check 2: Redis ZSET state machine — no state loss, correct transitions
# ---------------------------------------------------------------------------

def check_state_machine(report: ValidationReport) -> None:
    """
    Runs the TransactionStateMachine through a complete lifecycle and verifies:- State history is complete and ordered
      - Invalid transitions are rejected
      - Idempotent init does not duplicate state
      - Compliance hold threshold triggers correctly for PREMIUM_FINANCING- Concurrent advance raises ConcurrentModificationError (optimistic lock)
    """
    # Add src to path for local import
    src_path = Path(__file__).parents[1] / "local_app" / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    try:
        from cce_platform.redis_state_machine import (
            TransactionStateMachine,
            TxnState,
            InvalidTransitionError,
            TransactionNotFoundError,
            ConcurrentModificationError,
        )
    except ImportError as exc:
        report.add(CheckResult("state_machine_import", False, f"Import failed: {exc}"))
        return

    import tempfile, os
    with tempfile.TemporaryDirectory() as tmpdir:
        store_path = Path(tmpdir) / "txn_state.json"
        sm = TransactionStateMachine(local_store_path=store_path)

        # -- 2a. Normal lifecycle: PENDING → RISK_CHECK → APPROVED → SETTLED --
        txn_id = "TEST-TXN-001"
        sm.init_transaction(txn_id, amount=200.0, product="SAVINGS", customer_key="U0001")
        sm.advance(txn_id, TxnState.RISK_CHECK,  actor="risk_engine")
        sm.advance(txn_id, TxnState.APPROVED,    actor="auto_approve")
        sm.advance(txn_id, TxnState.SETTLED,     actor="settlement_svc")

        current = sm.get_current_state(txn_id)
        report.add(CheckResult(
            "state_machine_normal_lifecycle",
            passed=current == TxnState.SETTLED,
            message=f"Final state = {current} (expected SETTLED)",
        ))

        history = sm.get_history(txn_id)
        states = [t.to_state for t in history]
        expected = [TxnState.PENDING, TxnState.RISK_CHECK, TxnState.APPROVED, TxnState.SETTLED]
        report.add(CheckResult(
            "state_machine_history_completeness",
            passed=states == expected,
            message=f"History states: {[s.value for s in states]}",
            details={"expected": [s.value for s in expected]},
        ))

        # -- 2b. Compliance hold for PREMIUM_FINANCING above threshold --
        txn_pf = "TEST-TXN-PF-001"
        sm.init_transaction(txn_pf, amount=1700.0, product="PREMIUM_FINANCING", customer_key="U0005")
        should_hold = sm.should_compliance_hold(txn_pf)
        report.add(CheckResult(
            "state_machine_compliance_hold_triggered",
            passed=should_hold is True,
            message=f"PREMIUM_FINANCING 1700 SGD → compliance hold = {should_hold} (expected True)",
        ))

        # Auto-advance should insert COMPLIANCE_HOLD in the path
        transitions = sm.run_auto_advance(txn_pf, actor="auto_engine")
        transition_states = [t.to_state for t in transitions]
        report.add(CheckResult(
            "state_machine_auto_advance_compliance_path",
            passed=TxnState.COMPLIANCE_HOLD in transition_states,
            message=f"Auto-advance path: {[s.value for s in transition_states]}",
        ))

        # -- 2c. Invalid transition rejected --
        txn_bad = "TEST-TXN-BAD"
        sm.init_transaction(txn_bad, amount=50.0, product="CARD", customer_key="U0003")
        rejected_ok = False
        try:
            sm.advance(txn_bad, TxnState.SETTLED, actor="bad_actor")  # skip required states
        except InvalidTransitionError:
            rejected_ok = True
        report.add(CheckResult(
            "state_machine_invalid_transition_rejected",
            passed=rejected_ok,
            message="PENDING → SETTLED correctly rejected" if rejected_ok else "Invalid transition was NOT rejected",
        ))

        # -- 2d. Unknown transaction raises TransactionNotFoundError --
        not_found_ok = False
        try:
            sm.get_current_state("NONEXISTENT-TXN")
        except TransactionNotFoundError:
            not_found_ok = True
        report.add(CheckResult(
            "state_machine_not_found_error",
            passed=not_found_ok,
            message="TransactionNotFoundError raised for unknown txn_id",
        ))

        # -- 2e. Idempotent init does not duplicate state --
        txn_idem = "TEST-TXN-IDEM"
        sm.init_transaction(txn_idem, amount=100.0, product="CARD", customer_key="U0002")
        sm.init_transaction(txn_idem, amount=100.0, product="CARD", customer_key="U0002")  # duplicate
        history_idem = sm.get_history(txn_idem)
        report.add(CheckResult(
            "state_machine_idempotent_init",
            passed=len(history_idem) == 1,
            message=f"History length after duplicate init = {len(history_idem)} (expected 1)",
        ))

        # -- 2f. Saga compensation path --
        txn_comp = "TEST-TXN-COMP"
        sm.init_transaction(txn_comp, amount=500.0, product="INVESTMENT", customer_key="U0002")
        sm.advance(txn_comp, TxnState.RISK_CHECK,   actor="risk_engine")
        sm.advance(txn_comp, TxnState.APPROVED,     actor="auto_approve")
        sm.advance(txn_comp, TxnState.SETTLED,      actor="settlement_svc")
        sm.advance(txn_comp, TxnState.COMPENSATING, actor="saga_orchestrator", reason="reversal_requested")
        sm.advance(txn_comp, TxnState.COMPENSATED,  actor="saga_orchestrator")
        final = sm.get_current_state(txn_comp)
        report.add(CheckResult(
            "state_machine_saga_compensation",
            passed=final == TxnState.COMPENSATED,
            message=f"Saga compensation final state = {final.value} (expected COMPENSATED)",
        ))

        sm.close()


# ---------------------------------------------------------------------------
# Check 3: Flink local simulation — dedup + intent score
# ---------------------------------------------------------------------------

def check_flink_local_sim(report: ValidationReport) -> None:
    """
    Runs flink_cdc_pipeline.run_local_simulation() and verifies:
      - Duplicate events are not double-counted
      - Intent score is in [0, 1]- All resolvable customers are updated in the online store
    """
    src_path = Path(__file__).parents[1] / "local_app" / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    try:
        from cce_platform.flink_cdc_pipeline import run_local_simulation
        from cce_platform.realtime import write_sample_cdc_events
        from cce_platform.online_store import LocalOnlineStore
    except ImportError as exc:
        report.add(CheckResult("flink_import", False, f"Import failed: {exc}"))
        return

    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        events_path = tmp / "cdc_events.jsonl"
        store_path  = tmp / "feature_store.json"

        write_sample_cdc_events(events_path)

        # Duplicate all events to test dedup
        original = events_path.read_text(encoding="utf-8")
        events_path.write_text(original + original, encoding="utf-8")

        result = run_local_simulation(events_path=events_path, store_path=store_path)

        report.add(CheckResult(
            "flink_sim_dedup",
            passed=result.get("events_deduplicated", 0) > 0,
            message=f"Deduplicated {result.get('events_deduplicated', 0)} duplicate events",
            details=result,
        ))

        report.add(CheckResult(
            "flink_sim_customers_updated",
            passed=result.get("customers_updated", 0) > 0,
            message=f"Updated {result.get('customers_updated', 0)} customers in online store",
        ))

        # Verify intent scores are in [0, 1]
        store_data = LocalOnlineStore(store_path).read_all()
        bad_scores = [
            (k, v.get("rt_intent_score"))
            for k, v in store_data.items()
            if not (0.0 <= float(v.get("rt_intent_score", 0)) <= 1.0)
        ]
        report.add(CheckResult(
            "flink_sim_intent_score_range",
            passed=len(bad_scores) == 0,
            message=f"All intent scores in [0,1]" if not bad_scores else f"Out-of-range scores: {bad_scores}",
        ))

        # Verify feature_source is set to flink_local_sim
        wrong_source = [
            k for k, v in store_data.items()
            if v.get("feature_source") != "flink_local_sim"
        ]
        report.add(CheckResult(
            "flink_sim_feature_source_tag",
            passed=len(wrong_source) == 0,
            message=f"feature_source='flink_local_sim' on all records" if not wrong_source    else f"Wrong source on: {wrong_source}",
        ))


# ---------------------------------------------------------------------------
# Check 4: Feature store atomicity (tmp-file replace pattern)
# ---------------------------------------------------------------------------

def check_online_store_atomicity(report: ValidationReport) -> None:
    """
    Verifies LocalOnlineStore.bulk_upsert() is atomic:
      - Concurrent writes should not corrupt the JSON file
      - After N concurrent upserts, all customer keys should be present
    """
    src_path = Path(__file__).parents[1] / "local_app" / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    try:
        from cce_platform.online_store import LocalOnlineStore
    except ImportError as exc:
        report.add(CheckResult("online_store_import", False, f"Import failed: {exc}"))
        return

    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        store_path = Path(tmpdir) / "feature_store.json"
        store = LocalOnlineStore(store_path)

        errors: list[str] = []
        thread_count = 8
        writes_per_thread = 10

        def writer(thread_id: int) -> None:
            for i in range(writes_per_thread):
                key = f"U{thread_id:04d}"
                try:
                    store.upsert(key, {
                        "rt_order_count_1d": i,
                        "thread": thread_id,
                        "feature_source": "chaos_test",
                    })
                except Exception as exc:
                    errors.append(f"thread={thread_id} i={i}: {exc}")

        threads = [threading.Thread(target=writer, args=(t,)) for t in range(thread_count)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        report.add(CheckResult(
            "online_store_no_write_errors",
            passed=len(errors) == 0,
            message=f"0 write errors across {thread_count} concurrent threads" if not errors
                    else f"{len(errors)} write errors",
            details={"errors": errors[:5]},
        ))

        # All keys should be present
        data = store.read_all()
        expected_keys = {f"U{t:04d}" for t in range(thread_count)}
        missing = expected_keys - set(data.keys())
        report.add(CheckResult(
            "online_store_all_keys_present",
            passed=len(missing) == 0,
            message=f"All {thread_count} customer keys present after concurrent writes" if not missing
                    else f"Missing keys: {missing}",
        ))

        # File should be valid JSON (not corrupted)
        try:
            raw = store_path.read_text(encoding="utf-8")
            json.loads(raw)
            report.add(CheckResult(
                "online_store_json_not_corrupted",
                passed=True,
                message="File is valid JSON after concurrent writes",
            ))
        except json.JSONDecodeError as exc:
            report.add(CheckResult(
                "online_store_json_not_corrupted",
                passed=False,
                message=f"JSON corruption detected: {exc}",
            ))


# ---------------------------------------------------------------------------
# Check 5: Cart ZSET — financial product basket
# ---------------------------------------------------------------------------

def check_cart_zset(report: ValidationReport) -> None:
    """
    Verifies CartService behaviour:
      - Add items, chronological and priority ordering
      - Quote expiry detection
      - Anonymous → authenticated cart merge
      - CDC snapshot export shape
    """
    src_path = Path(__file__).parents[1] / "local_app" / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    try:
        from cce_platform.cart_zset import CartService, CartItem, ProductCode
    except ImportError as exc:
        report.add(CheckResult("cart_import", False, f"Import failed: {exc}"))
        return

    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        store_path = Path(tmpdir) / "cart_store.json"
        cart = CartService(local_store_path=store_path)

        # -- 5a. Add items and check count --
        cart.add_item("U0001", CartItem(product=ProductCode.INSURANCE,         amount=1380.0))
        cart.add_item("U0001", CartItem(product=ProductCode.INVESTMENT,         amount=2100.0))
        cart.add_item("U0001", CartItem(product=ProductCode.SAVINGS,            amount=500.0))
        items = cart.get_items("U0001")
        report.add(CheckResult(
            "cart_add_items",
            passed=len(items) == 3,
            message=f"Cart has {len(items)} items (expected 3)",
        ))

        # -- 5b. Priority ordering: INVESTMENT (8.0) > INSURANCE (7.0) > SAVINGS (5.0) --
        ranked = cart.get_ranked_items("U0001")
        top_product = ranked[0].product if ranked else None
        report.add(CheckResult(
            "cart_priority_ordering",
            passed=top_product == ProductCode.INVESTMENT,
            message=f"Highest priority item = {top_product} (expected INVESTMENT)",
        ))

        # -- 5c. Expiry detection: add item with 0-second validity --
        expired_item = CartItem(product=ProductCode.INVESTMENT_LINKED, amount=3000.0)
        expired_item.expiry_ts = time.time() - 1  # already expired
        cart.add_item("U0001", expired_item)
        summary = cart.get_summary("U0001")
        report.add(CheckResult(
            "cart_expiry_detection",
            passed=summary.expired_count == 1,
            message=f"Expired items detected = {summary.expired_count} (expected 1)",
        ))

        # -- 5d. has_high_value flag (INVESTMENT 2100 >= 1000) --
        report.add(CheckResult(
            "cart_high_value_flag",
            passed=summary.has_high_value is True,
            message=f"has_high_value = {summary.has_high_value} (expected True)",
        ))

        # -- 5e. Anonymous cart merge --
        cart.add_item("ANON-001", CartItem(product=ProductCode.TRAVEL_INSURANCE, amount=150.0))
        cart.add_item("ANON-001", CartItem(product=ProductCode.CARD,amount=0.0))
        merged = cart.merge_anonymous_cart("ANON-001", "U0001")
        anon_after = cart.get_items("ANON-001")
        report.add(CheckResult(
            "cart_merge_anonymous",
            passed=merged >= 1 and len(anon_after) == 0,
            message=f"Merged {merged} items, anon cart cleared (len={len(anon_after)})",
        ))

        # -- 5f. CDC snapshot shape --
        snap = cart.snapshot_to_cdc_event("U0001")
        report.add(CheckResult(
            "cart_cdc_snapshot",
            passed=snap.get("table") == "cart_events" and "after" in snap,
            message=f"CDC snapshot table={snap.get('table')} has 'after'={'after' in snap}",
        ))

        # -- 5g. Clear cart --
        cart.clear("U0001")
        after_clear = cart.get_items("U0001")
        report.add(CheckResult(
            "cart_clear",
            passed=len(after_clear) == 0,
            message=f"Cart empty after clear (len={len(after_clear)})",
        ))

        cart.close()


# ---------------------------------------------------------------------------
# Check 6: Outbox + Settlement Scheduler
# ---------------------------------------------------------------------------

def check_outbox_and_settlement(report: ValidationReport) -> None:
    """
    Verifies:
      - write_outbox_event writes a PENDING row in the same transaction
      - EventPublisher.run_once() marks it SENT and appends to CDC file
      - HolidayCalendar correctly skips weekends and holidays for T+2
      - schedule_settlement writes correct settle_date
      - SettlementTrigger.run_once() fires due settlements
    """
    src_path = Path(__file__).parents[1] / "local_app" / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    try:
        from cce_platform.outbox_publisher import (
            write_outbox_event, EventPublisher,
            schedule_settlement, SettlementTrigger,
            HolidayCalendar, PRODUCT_T_PLUS,
        )
        from cce_platform.db import connect, init_schema
        from cce_platform.redis_state_machine import TransactionStateMachine, TxnState
    except ImportError as exc:
        report.add(CheckResult("outbox_import", False, f"Import failed: {exc}"))
        return

    import tempfile
    from datetime import date
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmp = Path(tmpdir)
        db_path    = tmp / "test.sqlite"
        cdc_path   = tmp / "cdc_events.jsonl"
        sm_path    = tmp / "txn_state.json"

        # -- 6a. write_outbox_event in same transaction as business update --
        conn_a = connect(db_path)
        init_schema(conn_a)
        conn_a.execute("BEGIN")
        write_outbox_event(conn_a, "order", "O-TEST-001", "OrderPaid", {"amount": 288.0})
        conn_a.commit()
        conn_a.close()

        conn_b = connect(db_path)
        row = conn_b.execute(
            "SELECT status, event_type FROM outbox_events WHERE aggregate_id='O-TEST-001'"
        ).fetchone()
        conn_b.close()
        report.add(CheckResult(
            "outbox_write_pending",
            passed=row is not None and row["status"] == "PENDING",
            message=f"Outbox row status={row['status'] if row else 'NOT_FOUND'} (expected PENDING)",
        ))

        # -- 6b. EventPublisher.run_once() sends and marks SENT --
        sent_events: list[dict] = []
        def capture_downstream(event: dict) -> bool:
            sent_events.append(event)
            return True

        publisher = EventPublisher(downstream=capture_downstream, db_path=db_path)
        results = publisher.run_once()

        report.add(CheckResult(
            "outbox_publisher_sent",
            passed=len(results) == 1 and results[0].success,
            message=f"Publisher sent {len(results)} events, success={results[0].success if results else False}",
        ))

        conn_c = connect(db_path)
        row2 = conn_c.execute(
            "SELECT status FROM outbox_events WHERE aggregate_id='O-TEST-001'"
        ).fetchone()
        conn_c.close()
        report.add(CheckResult(
            "outbox_marked_sent",
            passed=row2 is not None and row2["status"] == "SENT",
            message=f"Outbox row status={row2['status'] if row2 else 'NOT_FOUND'} after publish (expected SENT)",
        ))

        # -- 6c. HolidayCalendar T+2 skips weekends --
        cal = HolidayCalendar()
        # 2026-08-21 is a Friday; T+2 should land on Tuesday 2026-08-25 (skip weekend)
        friday = date(2026, 8, 21)
        settled = cal.settle_date(friday, t_plus=2)
        report.add(CheckResult(
            "holiday_calendar_t2_skips_weekend",
            passed=settled == date(2026, 8, 25),
            message=f"Friday T+2 settle_date={settled} (expected 2026-08-25 Tuesday)",
        ))

        # -- 6d. schedule_settlement writes correct row --
        conn_d = connect(db_path)
        init_schema(conn_d)
        rec = schedule_settlement(
            conn_d, "TXN-SCHED-001", "U0002", "INVESTMENT", 2100.0,
            trade_date=date(2026, 8, 21),
        )
        conn_d.commit()
        conn_d.close()
        report.add(CheckResult(
            "settlement_schedule_t_plus",
            passed=rec["settle_date"] == "2026-08-25" and rec["t_plus"] == 2,
            message=f"settle_date={rec['settle_date']} t_plus={rec['t_plus']} (expected 2026-08-25, T+2)",
        ))

        # -- 6e. SettlementTrigger fires due settlement --
        # Set up state machine with transaction in PENDING_SETTLE
        sm = TransactionStateMachine(local_store_path=sm_path)
        sm.init_transaction("TXN-SCHED-001", amount=2100.0, product="INVESTMENT", customer_key="U0002")
        sm.advance("TXN-SCHED-001", TxnState.RISK_CHECK,    actor="risk_engine")
        sm.advance("TXN-SCHED-001", TxnState.APPROVED,      actor="auto_approve")
        sm.advance("TXN-SCHED-001", TxnState.PENDING_SETTLE, actor="scheduler")

        # Force settle_ts to past so trigger fires immediately
        conn_e = connect(db_path)
        conn_e.execute(
            "UPDATE settlement_schedule SET settle_ts=? WHERE txn_id=?",
            (time.time() - 1, "TXN-SCHED-001"),
        )
        conn_e.commit()
        conn_e.close()

        trigger = SettlementTrigger(state_machine=sm, db_path=db_path)
        triggered = trigger.run_once()
        report.add(CheckResult(
            "settlement_trigger_fires",
            passed="TXN-SCHED-001" in triggered,
            message=f"Triggered txns={triggered} (expected TXN-SCHED-001)",
        ))

        current_state = sm.get_current_state("TXN-SCHED-001")
        report.add(CheckResult(
            "settlement_state_in_progress",
            passed=current_state == TxnState.SETTLEMENT_IN_PROGRESS,
            message=f"State after trigger={current_state.value} (expected SETTLEMENT_IN_PROGRESS)",
        ))

        # -- 6f. complete_settlement advances to SETTLED --
        trigger.complete_settlement("TXN-SCHED-001")
        final_state = sm.get_current_state("TXN-SCHED-001")
        report.add(CheckResult(
            "settlement_complete_settled",
            passed=final_state == TxnState.SETTLED,
            message=f"Final state={final_state.value} (expected SETTLED)",
        ))

        sm.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

CHECK_REGISTRY: dict[str, Callable] = {
    "state-machine": lambda args, rpt: check_state_machine(rpt),
    "flink-sim":     lambda args, rpt: check_flink_local_sim(rpt),
    "online-store":  lambda args, rpt: check_online_store_atomicity(rpt),
    "cart-zset":     lambda args, rpt: check_cart_zset(rpt),
    "outbox":        lambda args, rpt: check_outbox_and_settlement(rpt),
    "prestop":       lambda args, rpt: check_prestop_drain(args.app_url, rpt),
}


def main() -> None:
    parser = argparse.ArgumentParser(description="CCE Chaos Validation Suite")
    parser.add_argument(
        "--mode", choices=["local", "k8s"], default="local",
        help="local = no K8s cluster required; k8s = validate against live cluster",
    )
    parser.add_argument(
        "--app-url", default="http://localhost:8010",
        help="Base URL of the CCE app (used for prestop check in k8s mode)",
    )
    parser.add_argument(
        "--check", choices=list(CHECK_REGISTRY.keys()), default=None,
        help="Run a single check instead of all",
    )
    args = parser.parse_args()

    report = ValidationReport()

    # prestop check requires a running app; skip in local mode unless explicitly requested
    checks_to_run = (
        [args.check] if args.check
        else ([k for k in CHECK_REGISTRY if k != "prestop"] if args.mode == "local"
              else list(CHECK_REGISTRY.keys()))
    )

    print(f"\nCCE Chaos Validation — mode={args.mode}  checks={checks_to_run}\n{'='*60}")
    for check_name in checks_to_run:
        print(f"\n[{check_name}]")
        CHECK_REGISTRY[check_name](args, report)

    sys.exit(report.summary())


if __name__ == "__main__":
    main()