"""Harness for the transactional (OLTP) package.

These modules had no unit coverage before the extraction: the only executable
verification was `chaos_testing/validate_chaos.py`, whose imports sat inside
`try/except ImportError` blocks that reported a missing module as an ordinary
failed check. A broken move would therefore have looked like a logic regression
rather than an import error, so the boundary needs assertions that fail loudly.

Structured as a harness rather than a grab-bag of unit tests:

  Input set        fixture transactions per scenario, built in-test
  Environment      in-process, tmp-scoped LocalZSetStore + tmp OLTP sqlite;
                   never touches data/ or the real warehouse
  Assertions       one named check per invariant, below
  Score            unittest's own pass count; all must pass

The invariants covered are the ones whose violation would be silent: state
transitions that should be refused, idempotency that should hold, a threshold
that must come from policy rather than a literal, and the import direction that
keeps the analytics side independent of this package.
"""

from __future__ import annotations

import ast
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from cce_platform.L2_oltp import (
    ConcurrentModificationError,
    EventPublisher,
    HolidayCalendar,
    InvalidTransitionError,
    LocalZSetAdapter,
    RedisZSetAdapter,
    RiskDecision,
    ZSetStore,
    decode_member,
    encode_member,
    SettlementTrigger,
    ThresholdRiskEvaluator,
    TransactionNotFoundError,
    TransactionStateMachine,
    TxnState,
    schedule_settlement,
    write_outbox_event,
)
from cce_platform.L2_oltp import store as oltp_store
from cce_platform.L1_business_data import PRODUCT_T_PLUS, compliance_hold_threshold


class StateMachineTest(unittest.TestCase):
    """Lifecycle, refusal and idempotency invariants."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store_path = Path(self._tmp.name) / "txn_state.json"

    def tearDown(self) -> None:
        # Windows holds the file handle until the store is closed; ignore
        # cleanup races since the directory is per-test anyway.
        try:
            self._tmp.cleanup()
        except (PermissionError, OSError):
            pass

    def _sm(self, **kwargs) -> TransactionStateMachine:
        return TransactionStateMachine(local_store_path=self.store_path, **kwargs)

    def test_forward_path_reaches_settled(self) -> None:
        sm = self._sm()
        sm.init_transaction("TXN-1", amount=200.0, product="SAVINGS", customer_key="U0001")
        for state in (
            TxnState.RISK_CHECK,
            TxnState.APPROVED,
            TxnState.PENDING_SETTLE,
            TxnState.SETTLEMENT_IN_PROGRESS,
            TxnState.SETTLED,
        ):
            sm.advance("TXN-1", state, actor="test")
        self.assertEqual(sm.get_current_state("TXN-1"), TxnState.SETTLED)

    def test_backward_transition_is_refused(self) -> None:
        """A backward edge would corrupt the audit trail, so it must raise."""
        sm = self._sm()
        sm.init_transaction("TXN-2", amount=200.0, product="SAVINGS")
        sm.advance("TXN-2", TxnState.RISK_CHECK, actor="test")
        sm.advance("TXN-2", TxnState.APPROVED, actor="test")
        with self.assertRaises(InvalidTransitionError):
            sm.advance("TXN-2", TxnState.RISK_CHECK, actor="test")

    def test_skipping_states_is_refused(self) -> None:
        sm = self._sm()
        sm.init_transaction("TXN-3", amount=200.0, product="SAVINGS")
        with self.assertRaises(InvalidTransitionError):
            sm.advance("TXN-3", TxnState.SETTLED, actor="test")

    def test_repeated_init_is_idempotent(self) -> None:
        """Re-init must not append a second history entry."""
        sm = self._sm()
        sm.init_transaction("TXN-4", amount=200.0, product="SAVINGS")
        sm.init_transaction("TXN-4", amount=999.0, product="INVESTMENT")
        self.assertEqual(len(sm.get_history("TXN-4")), 1)
        self.assertEqual(sm.get_current_state("TXN-4"), TxnState.PENDING)

    def test_terminal_state_accepts_nothing(self) -> None:
        sm = self._sm()
        sm.init_transaction("TXN-5", amount=200.0, product="SAVINGS")
        sm.advance("TXN-5", TxnState.RISK_CHECK, actor="test")
        sm.advance("TXN-5", TxnState.REJECTED, actor="test")
        with self.assertRaises(InvalidTransitionError):
            sm.advance("TXN-5", TxnState.APPROVED, actor="test")


class AuditTrailTest(unittest.TestCase):
    """Category 3: the trail must record who and why, not only what and when.

    Before attribution was stored in the member, every one of these assertions
    failed the same way — `actor` and `reason` came back empty because
    `get_history()` rebuilt entries from `{state}:{event_id}` alone.
    """

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store_path = Path(self._tmp.name) / "txn_state.json"

    def tearDown(self) -> None:
        try:
            self._tmp.cleanup()
        except (PermissionError, OSError):
            pass

    def _sm(self, **kwargs) -> TransactionStateMachine:
        return TransactionStateMachine(local_store_path=self.store_path, **kwargs)

    def test_actor_and_reason_survive_the_round_trip(self) -> None:
        sm = self._sm()
        sm.init_transaction("TXN-A1", amount=200.0, product="SAVINGS", actor="teller_07")
        sm.advance("TXN-A1", TxnState.RISK_CHECK, actor="risk_engine", reason="periodic_review")

        history = sm.get_history("TXN-A1")
        self.assertEqual([t.actor for t in history], ["teller_07", "risk_engine"])
        self.assertEqual(history[1].reason, "periodic_review")
        self.assertTrue(all(t.attributed for t in history))

    def test_reason_containing_the_legacy_delimiter_is_not_corrupted(self) -> None:
        """A positional `{state}:{event_id}` format could not carry this."""
        sm = self._sm()
        sm.init_transaction("TXN-A2", amount=200.0, product="SAVINGS")
        sm.advance("TXN-A2", TxnState.RISK_CHECK, actor="ops",
                   reason="escalated by: compliance desk (ref 12:45)")

        entry = sm.get_history("TXN-A2")[1]
        self.assertEqual(entry.reason, "escalated by: compliance desk (ref 12:45)")
        self.assertEqual(entry.to_state, TxnState.RISK_CHECK)

    def test_metadata_survives_and_carries_evaluator_provenance(self) -> None:
        sm = self._sm()
        sm.init_transaction("TXN-A3", amount=1700.0, product="PREMIUM_FINANCING",
                            customer_key="U0005")
        sm.run_auto_advance("TXN-A3", actor="auto_engine")

        held = [t for t in sm.get_history("TXN-A3")
                if t.to_state == TxnState.COMPLIANCE_HOLD]
        self.assertEqual(len(held), 1, "expected exactly one COMPLIANCE_HOLD entry")
        self.assertEqual(held[0].actor, "auto_engine")
        self.assertEqual(
            held[0].metadata.get("evaluator"), ThresholdRiskEvaluator.VERSION,
            "a hold must record which evaluator version produced it",
        )

    def test_default_reason_records_state_names_not_enum_repr(self) -> None:
        """`f"{TxnState.APPROVED}"` renders as `TxnState.APPROVED` on 3.11+."""
        sm = self._sm()
        sm.init_transaction("TXN-A4", amount=200.0, product="SAVINGS")
        sm.advance("TXN-A4", TxnState.RISK_CHECK, actor="ops")

        reason = sm.get_history("TXN-A4")[1].reason
        self.assertEqual(reason, "PENDING→RISK_CHECK")
        self.assertNotIn("TxnState", reason)

    def test_legacy_member_decodes_as_unattributed(self) -> None:
        """Pre-existing members must stay readable, and say so."""
        record = decode_member("APPROVED:abc-123")
        self.assertEqual(record.state, "APPROVED")
        self.assertEqual(record.event_id, "abc-123")
        self.assertFalse(record.attributed,
                         "legacy actor is unknown, which is not the same as empty")
        self.assertEqual(record.actor, "")

    def test_encoded_member_is_byte_stable(self) -> None:
        """The optimistic lock compares members as whole strings."""
        first = encode_member("APPROVED", "e1", actor="ops", reason="r",
                              metadata={"b": "2", "a": "1"})
        second = encode_member("APPROVED", "e1", actor="ops", reason="r",
                               metadata={"a": "1", "b": "2"})
        self.assertEqual(first, second)

    def test_unparseable_member_does_not_break_history_read(self) -> None:
        record = decode_member("{not valid json")
        self.assertFalse(record.attributed)

    def test_history_is_append_only_under_repeated_advance(self) -> None:
        """Each transition adds an entry; none overwrite an earlier one."""
        sm = self._sm()
        sm.init_transaction("TXN-A5", amount=200.0, product="SAVINGS")
        for state in (TxnState.RISK_CHECK, TxnState.APPROVED, TxnState.SETTLED):
            sm.advance("TXN-A5", state, actor="ops")

        history = sm.get_history("TXN-A5")
        self.assertEqual(len(history), 4)
        self.assertEqual(len({t.event_id for t in history}), 4)
        self.assertEqual([t.timestamp for t in history],
                         sorted(t.timestamp for t in history))


class RiskSeamTest(unittest.TestCase):
    """The RISK_CHECK gate: policy-driven, and substitutable."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store_path = Path(self._tmp.name) / "txn_state.json"

    def tearDown(self) -> None:
        try:
            self._tmp.cleanup()
        except (PermissionError, OSError):
            pass

    def _sm(self, **kwargs) -> TransactionStateMachine:
        return TransactionStateMachine(local_store_path=self.store_path, **kwargs)

    def test_hold_boundary_follows_policy_not_a_literal(self) -> None:
        """The threshold must come from policy, so assert against the accessor.

        Hardcoding 1000.0 here would let a policy change break production while
        the test stayed green.
        """
        threshold = compliance_hold_threshold("PREMIUM_FINANCING")
        sm = self._sm()

        sm.init_transaction("TXN-AT", amount=threshold, product="PREMIUM_FINANCING")
        self.assertTrue(sm.should_compliance_hold("TXN-AT"))

        sm.init_transaction("TXN-UNDER", amount=threshold - 0.01, product="PREMIUM_FINANCING")
        self.assertFalse(sm.should_compliance_hold("TXN-UNDER"))

    def test_unknown_product_is_never_held_on_amount_alone(self) -> None:
        """Policy default is infinity — documents the fail-open, by design."""
        sm = self._sm()
        sm.init_transaction("TXN-UNK", amount=10_000_000.0, product="NOT_A_REAL_PRODUCT")
        self.assertFalse(sm.should_compliance_hold("TXN-UNK"))

    def test_auto_advance_inserts_hold_above_threshold(self) -> None:
        sm = self._sm()
        threshold = compliance_hold_threshold("PREMIUM_FINANCING")
        sm.init_transaction("TXN-HOLD", amount=threshold + 700.0, product="PREMIUM_FINANCING")
        path = [t.to_state for t in sm.run_auto_advance("TXN-HOLD")]
        self.assertEqual(
            path,
            [TxnState.RISK_CHECK, TxnState.COMPLIANCE_HOLD, TxnState.APPROVED],
        )

    def test_auto_advance_skips_hold_below_threshold(self) -> None:
        sm = self._sm()
        sm.init_transaction("TXN-CLEAR", amount=10.0, product="PREMIUM_FINANCING")
        path = [t.to_state for t in sm.run_auto_advance("TXN-CLEAR")]
        self.assertEqual(path, [TxnState.RISK_CHECK, TxnState.APPROVED])

    def test_evaluator_is_substitutable(self) -> None:
        """The seam exists so a real risk engine can replace the threshold rule."""

        class AlwaysHold:
            VERSION = "test-always-hold"

            def evaluate(self, txn_id: str, meta: dict[str, str]) -> RiskDecision:
                return RiskDecision(
                    hold=True, reason="test_forced", evaluator_version=self.VERSION
                )

        sm = self._sm(risk_evaluator=AlwaysHold())
        sm.init_transaction("TXN-SUB", amount=1.0, product="SAVINGS")
        self.assertTrue(sm.should_compliance_hold("TXN-SUB"))
        self.assertEqual(sm.evaluate_risk("TXN-SUB").evaluator_version, "test-always-hold")

    def test_default_decision_records_that_no_feature_was_read(self) -> None:
        """features_used empty is the honest record of the current gap."""
        decision = ThresholdRiskEvaluator().evaluate(
            "TXN-X", {"product": "INVESTMENT", "amount": "999999"}
        )
        self.assertTrue(decision.hold)
        self.assertEqual(decision.features_used, ())
        self.assertIsNone(decision.feature_age_s)

    def test_malformed_amount_does_not_raise(self) -> None:
        decision = ThresholdRiskEvaluator().evaluate(
            "TXN-Y", {"product": "INVESTMENT", "amount": "not-a-number"}
        )
        self.assertFalse(decision.hold)


class SettlementCalendarTest(unittest.TestCase):
    """T+N settlement must skip weekends and holidays."""

    def test_t2_skips_weekend(self) -> None:
        cal = HolidayCalendar()
        # Friday 2026-08-21 + 2 business days, with Mon 2026-08-10 irrelevant
        self.assertEqual(cal.settle_date(date(2026, 8, 21), 2), date(2026, 8, 25))

    def test_t2_skips_public_holiday(self) -> None:
        """2026-08-10 is an SG holiday in the calendar; a T+2 from the 6th
        (Thursday) must land on the 11th, not the 10th."""
        cal = HolidayCalendar()
        settle = cal.settle_date(date(2026, 8, 6), 2)
        self.assertTrue(cal.is_business_day(settle))
        self.assertEqual(settle, date(2026, 8, 11))

    def test_t0_on_holiday_rolls_forward(self) -> None:
        cal = HolidayCalendar()
        settle = cal.settle_date(date(2026, 8, 10), 0)
        self.assertTrue(cal.is_business_day(settle))

    def test_product_cycles_come_from_policy(self) -> None:
        self.assertIn("INVESTMENT", PRODUCT_T_PLUS)


class OutboxTest(unittest.TestCase):
    """Outbox delivery and settlement triggering against a tmp OLTP database."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        tmp = Path(self._tmp.name)
        self.db_path = tmp / "oltp.sqlite"
        self.store_path = tmp / "txn_state.json"
        conn = oltp_store.connect(self.db_path)
        oltp_store.init_schema(conn)
        conn.close()

    def tearDown(self) -> None:
        try:
            self._tmp.cleanup()
        except (PermissionError, OSError):
            pass

    def test_operational_schema_creates_only_operational_tables(self) -> None:
        """The OLTP database must not carry analytics tables."""
        conn = oltp_store.connect(self.db_path)
        try:
            names = {
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        finally:
            conn.close()
        self.assertEqual(names, {"outbox_events", "settlement_schedule"})

    def test_dedup_key_makes_a_producer_retry_collide(self) -> None:
        """The producer half of at-least-once: a retry must not queue twice.

        `event_id` is the table's PRIMARY KEY and the insert is
        `INSERT OR IGNORE`, so the guard already exists at the storage layer —
        what was missing was an id a retry could actually reproduce. With an
        explicit `dedup_key` the second write collides and is ignored.

        This is the producer half; the consumer half is covered by
        `tests/test_pipeline.py::CdcIdempotencyTest`. Neither implies the other:
        a consumer filter cannot recognise two rows that carry different ids.
        """
        conn = oltp_store.connect(self.db_path)
        try:
            first = write_outbox_event(
                conn, aggregate_type="order", aggregate_id="O-RETRY",
                event_type="OrderPaid", payload={"amount": 288.0},
                dedup_key="O-RETRY:OrderPaid",
            )
            second = write_outbox_event(
                conn, aggregate_type="order", aggregate_id="O-RETRY",
                event_type="OrderPaid", payload={"amount": 288.0},
                dedup_key="O-RETRY:OrderPaid",
            )
            conn.commit()
            rows = conn.execute(
                "SELECT COUNT(*) AS n FROM outbox_events WHERE aggregate_id='O-RETRY'"
            ).fetchone()["n"]
        finally:
            conn.close()

        self.assertEqual(
            first, second,
            "the same dedup_key must derive the same event_id, or INSERT OR IGNORE "
            "has nothing to collide on and the idempotency clause is decorative",
        )
        self.assertEqual(
            rows, 1,
            "a producer-side retry carrying the same dedup_key must leave exactly "
            "one queued row; two rows means the event is delivered twice downstream",
        )

    def test_events_without_a_dedup_key_stay_distinct(self) -> None:
        """Two legitimately identical business events must not collapse.

        This is why the fix is a caller-supplied key rather than hashing the
        payload: two real purchases of the same product for the same amount are
        distinct events, and silently merging them would lose one.
        """
        conn = oltp_store.connect(self.db_path)
        try:
            a = write_outbox_event(
                conn, aggregate_type="order", aggregate_id="O-TWICE",
                event_type="OrderPaid", payload={"amount": 50.0},
            )
            b = write_outbox_event(
                conn, aggregate_type="order", aggregate_id="O-TWICE",
                event_type="OrderPaid", payload={"amount": 50.0},
            )
            conn.commit()
            rows = conn.execute(
                "SELECT COUNT(*) AS n FROM outbox_events WHERE aggregate_id='O-TWICE'"
            ).fetchone()["n"]
        finally:
            conn.close()

        self.assertNotEqual(a, b, "without a dedup_key, ids must remain distinct")
        self.assertEqual(rows, 2, "both events must survive")

    def test_pending_event_is_delivered_and_marked_sent(self) -> None:
        delivered: list[dict] = []
        conn = oltp_store.connect(self.db_path)
        try:
            event_id = write_outbox_event(
                conn, aggregate_type="order", aggregate_id="O-1",
                event_type="OrderPaid", payload={"amount": 288.0},
            )
            conn.commit()
            status = conn.execute(
                "SELECT status FROM outbox_events WHERE event_id=?", (event_id,)
            ).fetchone()["status"]
        finally:
            conn.close()
        self.assertEqual(status, "PENDING")

        publisher = EventPublisher(
            downstream=lambda e: (delivered.append(e), True)[1], db_path=self.db_path
        )
        results = publisher.run_once()
        self.assertEqual([r.success for r in results], [True])
        self.assertEqual(len(delivered), 1)

        conn = oltp_store.connect(self.db_path)
        try:
            status = conn.execute(
                "SELECT status FROM outbox_events WHERE event_id=?", (event_id,)
            ).fetchone()["status"]
        finally:
            conn.close()
        self.assertEqual(status, "SENT")

    def test_failing_downstream_reaches_failed_after_max_retry(self) -> None:
        conn = oltp_store.connect(self.db_path)
        try:
            event_id = write_outbox_event(
                conn, aggregate_type="order", aggregate_id="O-2",
                event_type="OrderPaid", payload={},
            )
            conn.commit()
        finally:
            conn.close()

        publisher = EventPublisher(downstream=lambda e: False, db_path=self.db_path)
        for _ in range(EventPublisher.MAX_RETRY):
            publisher.run_once()

        conn = oltp_store.connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT status, retry_count FROM outbox_events WHERE event_id=?", (event_id,)
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(row["status"], "FAILED")
        self.assertEqual(row["retry_count"], EventPublisher.MAX_RETRY)

    def test_settlement_and_outbox_share_one_transaction(self) -> None:
        """Both tables live in one file, so one commit covers both.

        This is the only atomicity the Outbox pattern can currently deliver
        here; the transaction state itself is in Redis. See L2_oltp/store.py.
        """
        with oltp_store.transaction(self.db_path) as conn:
            schedule_settlement(conn, "TXN-ATOMIC", "U0001", "INVESTMENT", 2100.0,
                                trade_date=date(2026, 8, 21))
            write_outbox_event(conn, aggregate_type="settlement",
                               aggregate_id="TXN-ATOMIC",
                               event_type="SettlementScheduled", payload={})

        conn = oltp_store.connect(self.db_path)
        try:
            sched = conn.execute("SELECT COUNT(*) c FROM settlement_schedule").fetchone()["c"]
            events = conn.execute("SELECT COUNT(*) c FROM outbox_events").fetchone()["c"]
        finally:
            conn.close()
        self.assertEqual((sched, events), (1, 1))

    def test_rollback_leaves_neither_row(self) -> None:
        with self.assertRaises(RuntimeError):
            with oltp_store.transaction(self.db_path) as conn:
                schedule_settlement(conn, "TXN-RB", "U0001", "INVESTMENT", 1.0,
                                    trade_date=date(2026, 8, 21))
                raise RuntimeError("boom")

        conn = oltp_store.connect(self.db_path)
        try:
            count = conn.execute("SELECT COUNT(*) c FROM settlement_schedule").fetchone()["c"]
        finally:
            conn.close()
        self.assertEqual(count, 0)

    def test_due_settlement_fires_once_and_not_twice(self) -> None:
        sm = TransactionStateMachine(local_store_path=self.store_path)
        sm.init_transaction("TXN-DUE", amount=2100.0, product="INVESTMENT",
                            customer_key="U0001")
        sm.run_auto_advance("TXN-DUE")
        sm.advance("TXN-DUE", TxnState.PENDING_SETTLE, actor="test")

        conn = oltp_store.connect(self.db_path)
        try:
            # A past trade date makes settle_ts already due.
            schedule_settlement(conn, "TXN-DUE", "U0001", "INVESTMENT", 2100.0,
                                trade_date=date(2026, 1, 5))
            conn.commit()
        finally:
            conn.close()

        trigger = SettlementTrigger(state_machine=sm, db_path=self.db_path)
        self.assertEqual(trigger.run_once(), ["TXN-DUE"])
        self.assertEqual(sm.get_current_state("TXN-DUE"), TxnState.SETTLEMENT_IN_PROGRESS)
        # Second poll must not re-fire: the row is no longer PENDING_SETTLE.
        self.assertEqual(trigger.run_once(), [])


class FakeZSetStore:
    """In-memory `ports.ZSetStore`. No file, no Redis, no tempdir.

    Deliberately not a subclass of anything: the point of a Protocol is that
    satisfying it structurally is enough.
    """

    def __init__(self, lose_every_race: bool = False) -> None:
        self.zsets: dict[str, list[tuple[float, str]]] = {}
        self.hashes: dict[str, dict[str, str]] = {}
        self.lose_every_race = lose_every_race
        self.closed = False

    def append_scored(self, key: str, score: float, member: str) -> None:
        entries = [e for e in self.zsets.get(key, []) if e[1] != member]
        entries.append((score, member))
        entries.sort(key=lambda e: e[0])
        self.zsets[key] = entries

    def newest_member(self, key: str) -> str | None:
        entries = self.zsets.get(key, [])
        return entries[-1][1] if entries else None

    def all_entries(self, key: str) -> list[tuple[str, float]]:
        return [(member, score) for score, member in self.zsets.get(key, [])]

    def compare_and_append(
        self, key: str, expected_newest: str, member: str, score: float
    ) -> bool:
        if self.lose_every_race or self.newest_member(key) != expected_newest:
            return False
        self.append_scored(key, score, member)
        return True

    def put_fields(self, key: str, mapping: dict[str, str]) -> None:
        self.hashes[key] = {**self.hashes.get(key, {}), **mapping}

    def get_fields(self, key: str) -> dict[str, str]:
        return self.hashes.get(key, {})

    def close(self) -> None:
        self.closed = True


class StorePortTest(unittest.TestCase):
    """The store seam: what `ports.ZSetStore` buys, asserted rather than claimed."""

    def test_fake_satisfies_the_port(self) -> None:
        """A plain class satisfies ZSetStore structurally — no base class needed."""
        self.assertIsInstance(FakeZSetStore(), ZSetStore)

    def test_both_adapters_satisfy_the_port(self) -> None:
        """The wiring check: neither adapter drifts from the port it implements."""
        self.assertIsInstance(LocalZSetAdapter(FakeZSetStore()), ZSetStore)
        self.assertIsInstance(RedisZSetAdapter(object()), ZSetStore)

    def test_lifecycle_runs_with_no_file_and_no_redis(self) -> None:
        """The whole forward path, entirely in memory."""
        fake = FakeZSetStore()
        sm = TransactionStateMachine(store=fake)
        sm.init_transaction("TXN-P1", amount=200.0, product="SAVINGS")
        for state in (TxnState.RISK_CHECK, TxnState.APPROVED, TxnState.SETTLED):
            sm.advance("TXN-P1", state, actor="test")
        self.assertEqual(sm.get_current_state("TXN-P1"), TxnState.SETTLED)
        self.assertEqual(len(sm.get_history("TXN-P1")), 4)

    def test_refused_transition_writes_nothing(self) -> None:
        """The invariant that needed a real store before: refusal leaves no trace.

        PENDING → SETTLED is not in ALLOWED_TRANSITIONS. Asserting "nothing was
        written" means inspecting the store, which is only cheap because the
        store is a fake.
        """
        fake = FakeZSetStore()
        sm = TransactionStateMachine(store=fake)
        sm.init_transaction("TXN-P2", amount=100.0, product="CARD")
        before = list(fake.zsets["txn:state:TXN-P2"])

        with self.assertRaises(InvalidTransitionError):
            sm.advance("TXN-P2", TxnState.SETTLED)

        self.assertEqual(fake.zsets["txn:state:TXN-P2"], before)

    def test_unknown_transaction_raises_before_any_write(self) -> None:
        fake = FakeZSetStore()
        sm = TransactionStateMachine(store=fake)
        with self.assertRaises(TransactionNotFoundError):
            sm.advance("TXN-NOPE", TxnState.RISK_CHECK)
        self.assertEqual(fake.zsets, {})

    def test_lost_race_exhausts_retries_and_writes_nothing(self) -> None:
        """Retry exhaustion, which no real backend can be made to do on demand.

        `compare_and_append` returning False every time is exactly the lost-race
        case. Against Redis this needs a competing writer timed into the WATCH
        window; against the file store it is not reachable at all. Behind the
        port it is one constructor flag.
        """
        fake = FakeZSetStore()
        sm = TransactionStateMachine(store=fake)
        sm.init_transaction("TXN-P3", amount=100.0, product="CARD")
        fake.lose_every_race = True
        before = list(fake.zsets["txn:state:TXN-P3"])

        with self.assertRaises(ConcurrentModificationError):
            sm.advance("TXN-P3", TxnState.RISK_CHECK)

        self.assertEqual(fake.zsets["txn:state:TXN-P3"], before)

    def test_close_reaches_the_store(self) -> None:
        fake = FakeZSetStore()
        with TransactionStateMachine(store=fake):
            pass
        self.assertTrue(fake.closed)


PKG = ROOT / "src" / "cce_platform"
OLTP_UNIT = "L2_oltp"


def _imported_modules(path: Path) -> list[tuple[int, str]]:
    """Every module `path` imports, as absolute dotted names with line numbers.

    Parsed rather than pattern-matched. The earlier version of this check read
    lines and asked whether the substring "oltp" appeared in one starting with
    `import` or `from`, which failed in both directions the moment the packages
    grew layer prefixes:

      * it stopped excluding the transactional package itself, because the
        exclusion tested `"oltp" in parts` and the part became `L2_oltp`;
      * it flagged a docstring line whose text merely began with the word
        "import", and two docstring examples that quote an import statement.

    None of those three are dependencies. An `ast` walk reaches into function
    bodies as well, so a deferred import -- the exact thing an anchored `^from`
    pattern missed three times during this refactor -- counts like any other.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    parts = path.relative_to(PKG.parent).with_suffix("").parts
    package = parts[:-1] if path.name != "__init__.py" else parts[:-1]

    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.extend((node.lineno, alias.name) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                base = package[: len(package) - (node.level - 1)]
            else:
                base = ()
            tail = tuple(node.module.split(".")) if node.module else ()
            found.append((node.lineno, ".".join(base + tail)))
    return found


class BoundaryTest(unittest.TestCase):
    """The architectural invariant: nothing analytics-side imports oltp."""

    def _scan(self) -> tuple[list[Path], list[str]]:
        scanned, offenders = [], []
        for path in sorted(PKG.rglob("*.py")):
            if path.relative_to(PKG).parts[0] == OLTP_UNIT:
                continue
            scanned.append(path)
            for lineno, module in _imported_modules(path):
                if OLTP_UNIT in module.split("."):
                    offenders.append(f"{path.relative_to(ROOT)}:{lineno}: {module}")
        return scanned, offenders

    def test_no_analytics_module_imports_oltp(self) -> None:
        _, offenders = self._scan()
        self.assertEqual(
            offenders, [],
            "analytics/serving code must not depend on the transactional package; "
            "the batch pipeline and feature API must run with cce_platform.L2_oltp absent",
        )

    def test_the_scan_reached_the_analytics_modules(self) -> None:
        """A scan whose input silently empties would pass the check above.

        The exclusion is a path comparison against one hard-coded folder name.
        Rename `L2_oltp` again, or move the package, and the loop above could
        skip everything or nothing without either outcome announcing itself.
        Naming the modules the invariant is actually about makes that visible.
        """
        scanned, _ = self._scan()
        names = {p.relative_to(PKG).as_posix() for p in scanned}
        self.assertLessEqual(
            {"L2_olap/api.py", "L2_olap/pipeline.py", "L2_olap/batch_importer.py"},
            names,
            f"the analytics modules this invariant protects were not scanned: {sorted(names)}",
        )

    def test_analytics_schema_excludes_operational_tables(self) -> None:
        from cce_platform.L0_schema import ANALYTICS_TABLES, OPERATIONAL_TABLES

        self.assertEqual(set(OPERATIONAL_TABLES), {"outbox_events", "settlement_schedule"})
        self.assertEqual(set(ANALYTICS_TABLES) & set(OPERATIONAL_TABLES), set())

    def test_analytics_init_schema_does_not_create_operational_tables(self) -> None:
        from cce_platform.L1_mechanism import connect as analytics_connect
        from cce_platform.L1_mechanism import init_schema as analytics_init

        with tempfile.TemporaryDirectory() as tmp:
            conn = analytics_connect(Path(tmp) / "analytics.sqlite")
            try:
                analytics_init(conn)
                names = {
                    r[0] for r in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                }
            finally:
                conn.close()
        self.assertNotIn("outbox_events", names)
        self.assertNotIn("settlement_schedule", names)


if __name__ == "__main__":
    unittest.main()
