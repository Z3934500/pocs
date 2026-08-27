# `cce_platform.L2_oltp`

The transactional side of the platform: the state that no pipeline can
recompute.

| Module | Owns |
| --- | --- |
| `state_machine.py` | Transaction lifecycle — append-only transition history in a Redis ZSET |
| `outbox.py` | Undelivered events, T+N settlement obligations, holiday calendar |
| `store.py` | The operational SQLite database, separate from the analytics warehouse |
| `risk.py` | The `RISK_CHECK` gate — a typed slot, currently filled by an amount threshold |
| `audit.py` | What one audit entry says — state, event, actor, reason, provenance |
| `ports.py` | What this package needs from a store, declared by the consumer |
| `adapters.py` | Mapping redis-py and the local file store onto that port |

**Boundary rule:** imports may point from here into `cce_platform.*`, never the
reverse. The batch pipeline, the Feature API and the online store must all still
run with this package absent. `tests/test_oltp.py::BoundaryTest` enforces it.

Everything else in `cce_platform` derives recomputable outputs and rebuilds them
on every run. A lost analytics row is a rerun; a lost settlement obligation is
money that never moves. That asymmetry is why this folder exists.

**Known gaps** are registered in three categories in
[`KNOWN_GAPS.md`](KNOWN_GAPS.md) — the broken transaction boundary around the
outbox, the placeholder `RISK_CHECK`, and the audit trail — together with the
reasons none of them changes an analytical answer, the feature layer (Gold, one
unified semantic) remaining the analytics single source of truth.

For the architectural position, consistency and failure model, and the linkage to
risk-control requirements, see
[`docs/ARCHITECTURE_OLTP_BOUNDARY.md`](../../../docs/ARCHITECTURE_OLTP_BOUNDARY.md).

The usage material below lived in the top-level `README.md` until this package
existed. It moved here on purpose: that README describes the feature platform —
a batch medallion pipeline and its serving path — and transaction lifecycle,
settlement obligations and compliance holds are a different concern that was
making the platform's story harder to read. Keeping the two apart in prose is the
same separation the import rule enforces in code.

## Transaction State Machine

```python
from cce_platform.L2_oltp import TransactionStateMachine, TxnState

sm = TransactionStateMachine()   # Redis if REDIS_URL set; local JSON only where the fallback is allowed
sm.init_transaction("TXN-001", amount=1700.0, product="PREMIUM_FINANCING", customer_key="U0005")
sm.run_auto_advance("TXN-001")   # PENDING → RISK_CHECK → COMPLIANCE_HOLD → APPROVED
sm.advance("TXN-001", TxnState.PENDING_SETTLE, actor="scheduler")
# ... T+2 settlement date arrives ...
sm.advance("TXN-001", TxnState.SETTLEMENT_IN_PROGRESS, actor="settlement_trigger")
sm.advance("TXN-001", TxnState.SETTLED, actor="settlement_worker")
```

State flow:

```
PENDING → RISK_CHECK → COMPLIANCE_HOLD ┐
                     ↘ APPROVED ────────┤→ PENDING_SETTLE → SETTLEMENT_IN_PROGRESS → SETTLED
                       REJECTED (terminal)                ↓
                                COMPENSATING → COMPENSATED
```

**Why a ZSET instead of a status column:**

- `ZRANGEBYSCORE` gives the full transition history in O(log n), and each entry
  carries actor, reason and evaluator provenance (see [`audit.py`](audit.py)).
  Append-only by construction — no backward edge, no `ZREM`. Immutability is not
  yet *enforced* and `actor` is self-declared, so this is an audit trail rather
  than a regulator-grade one; see [`KNOWN_GAPS.md`](KNOWN_GAPS.md) §3.
- Score = Unix timestamp microseconds: natural ordering without a separate
  `version` column.
- `WATCH/MULTI/EXEC` optimistic locking prevents the split-brain case where a
  saga compensation thread and a normal processing thread race on one order.
- The `COMPLIANCE_HOLD` threshold is product-aware and loaded from
  `config/business_policy.json`, not hardcoded — `PREMIUM_FINANCING >= 1000 SGD`,
  `INVESTMENT >= 500 SGD`.

## Transactional Outbox + T+N Settlement Scheduler

`outbox.py` addresses the "DB updated, Kafka send failed" atomicity problem and
implements settlement scheduling with holiday awareness:

```python
from cce_platform.L0_configuration import settings
from cce_platform.L2_oltp import (
    write_outbox_event, EventPublisher,
    schedule_settlement, SettlementTrigger, HolidayCalendar,
    transaction,
)

# Step 1 — business code: one transaction over the operational database
with transaction(settings.oltp_sqlite_path) as conn:
    write_outbox_event(conn, "order", order_id, "OrderPaid", {"amount": 288.0})
    schedule_settlement(conn, order_id, customer_key, "INVESTMENT", 2100.0)
    # both rows commit together, or neither does

# Step 2 — background thread: EventPublisher polls and forwards
publisher = EventPublisher()
publisher.start_background()   # polls every 2s, marks SENT after downstream ACK

# Step 3 — background thread: SettlementTrigger fires on the settle date
trigger = SettlementTrigger()
trigger.start_background()     # polls every 10s, advances the state machine when due
```

**Why an outbox at all:** without it, `UPDATE orders` commits, the process
crashes before the Kafka send, and the event is lost forever — the order sits in
`PAID` with no downstream notification. With it the event row commits in the same
transaction, so `EventPublisher` keeps retrying across restarts until downstream
confirms.

**Two honest caveats**, both detailed in [`KNOWN_GAPS.md`](KNOWN_GAPS.md) §1:

- The atomicity above covers the two SQLite tables. It does **not** cover
  `sm.advance()`, because the transaction state it describes lives in Redis. That
  is why `SettlementTrigger` carries `SKIPPED_ADVANCED` / `SKIPPED_NOT_FOUND`
  reconciliation branches.
- `event_id` mixes `time.time()` in, so its `INSERT OR IGNORE` can never collide
  and the idempotency it implies is decorative.

**Concurrent writers against SQLite:** both background threads poll while request
handlers write, so `store.connect()` sets `journal_mode=WAL` and
`busy_timeout=5000`. Under the default rollback journal a reader blocks writers,
and a writer finding the database locked fails immediately with `database is
locked` rather than waiting — with a 2s publisher poll and a 10s settlement poll
that collision is routine, not a load-test artifact. This is a local-development
property, not a substitute for a real OLTP engine; no RDS or Aurora is in scope,
which is the only reason SQLite is here.

`SettlementTrigger` constructs `TransactionStateMachine` once and caches it. Each
construction builds a connection pool and pings Redis, and the helper is called
once per `run_once()` plus once per `complete_settlement()` — every 10s for the
life of the loop. Building it per call also turns a Redis outage from "fails once
at startup" into "raises on every poll".

**T+N holiday calendar:**

```python
cal = HolidayCalendar()                         # SG + HK holidays 2025-2026 built in
cal.settle_date(date(2026, 8, 21), t_plus=2)    # Friday → Tuesday 2026-08-25
```

| Product | T+N | Reason |
|---------|-----|--------|
| `PREMIUM_FINANCING` | T+2 | HKEX standard equities settlement |
| `INVESTMENT` / `INVESTMENT_LINKED` | T+2 | Fund NAV calculation cycle |
| `INSURANCE` | T+1 | Policy activation next business day |
| `SAVINGS` / `CARD` / `TRAVEL_INSURANCE` | T+0 | Immediate activation |

Cycles come from `config/business_policy.json`;
`test_product_cycles_come_from_policy` asserts that rather than the table above.
In production the holiday set would be overridden from Consul KV
(`cce/config/holiday_calendar`) so typhoon closures take effect without a deploy.

## Verification

Two harnesses, different granularity and different failure modes.

```bash
PYTHONPATH=src python -m unittest discover -s tests -p test_oltp.py   # 43 tests
PYTHONPATH=src python -m unittest discover -s tests -p test_layers.py # 7 tests
PYTHONPATH=src python -m unittest discover -s tests                  # 66 tests (whole app)
PYTHONPATH=src python -m chaos_testing.validate_chaos --mode local    # 30 checks
```

**Unit harness** — `tests/test_oltp.py`, 43 tests: `AuditTrailTest` (8),
`StorePortTest` (7), `RiskSeamTest` (7), `OutboxTest` (8), `StateMachineTest`
(5), `SettlementCalendarTest` (4), `BoundaryTest` (4). Each scenario builds its
own fixture transactions against temp-scoped stores that never touch `data/`,
with one named assertion per invariant. The remaining 22 in the full run are
`tests/test_layers.py` (7), `tests/test_pipeline.py` (9) and
`tests/test_docs.py` (7).

`StorePortTest` is the one group that needs no store at all: `FakeZSetStore`
satisfies `ports.ZSetStore` in memory, which is what makes retry exhaustion
assertable — `compare_and_append` returning False every time is a constructor
flag, where against Redis it would need a competing writer timed into the WATCH
window and against the file store it is not reachable at all.

**System harness** — `chaos_testing/validate_chaos.py`, 30 checks across
`state-machine` (8), `outbox` (8), `cart-zset` (7), `flink-sim` (4) and
`online-store` (3). It runs the real modules end to end, prints a report, and
exits with a code equal to the failure count. The two transactional groups are
the ones that cover this package.

Both are harnesses rather than "tests" in the sense this project uses
throughout: a fixed input set, a defined execution environment, named
assertions, and a score — reviewable by re-running rather than by reading. They
are complementary. `BoundaryTest` can assert a structural constraint like import
direction, which no end-to-end run would notice; conversely only the chaos suite
catches a broken import in a wired-up module, and it surfaces as a named check
flipping to FAIL. Import failures there are prefixed
`IMPORT ERROR (not a check failure)` so the two cannot be confused.

Pass criteria: 42 OK for this package, 52 for the app, and 30/30 with exit 0.


