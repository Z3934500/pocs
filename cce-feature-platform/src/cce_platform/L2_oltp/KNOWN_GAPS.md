# Known Gaps In The Transactional Domain

Three gaps, deliberately named and kept together rather than scattered across
the architecture document, because they share one property: **each is a
write-authority problem, and none of them changes an analytical answer.** The
final section states why, and what would have to be true for that to stop
holding.

`docs/ARCHITECTURE_OLTP_BOUNDARY.md` covers the position of this package in the
architecture and the reasoning behind the split. This file is the gap register.

| # | Category | Where | Status |
|---|----------|-------|--------|
| 1 | Transaction boundary still broken — outbox | `store.py`, `outbox.py` | Open, understood, reconciled at runtime |
| 2 | Risk check (`RISK_CHECK`) | `risk.py`, `state_machine.py` | Typed seam, no real evaluator |
| 3 | Audit trail | `audit.py`, `state_machine.py` | Attribution closed; immutability open |

---

## 1. 事务边界依然断裂 — Transaction Boundary Still Broken (Outbox)

### What holds

`schedule_settlement()` and `write_outbox_event()` both write tables in the
operational SQLite database, so they share one transaction and commit or roll
back together:

```python
with transaction(settings.oltp_sqlite_path) as conn:
    schedule_settlement(conn, txn_id, customer_key, product, amount)
    write_outbox_event(conn, "settlement", txn_id, "SettlementScheduled", payload)
```

`tests/test_oltp.py::OutboxTest::test_settlement_and_outbox_share_one_transaction`
and `::test_rollback_leaves_neither_row` assert both directions.

### What does not hold

The Outbox pattern requires the event write to be atomic with **the business
state change it describes**. That business state is the transaction's state,
and it lives in Redis under `txn:state:{txn_id}` — not in SQLite. So:

```
sm.advance(txn_id, TxnState.SETTLEMENT_IN_PROGRESS, ...)   # Redis ZADD
conn.execute("UPDATE settlement_schedule SET status=... ")  # SQLite UPDATE
conn.commit()
```

Two stores, two round trips, no common transaction. A crash between them leaves
the ZSET advanced and the schedule row stale. This is visible in
[outbox.py:478-494](outbox.py#L478-L494).

**The split did not cause this and does not deepen it.** The gap is a
consequence of where the state machine keeps state, which predates the
extraction. What the split bought is the one real atomicity opportunity that
does exist — the two SQLite tables now provably share a transaction — plus a
named place to record that the rest is unresolved.

### The reconciliation already written for that window

`SettlementTrigger.trigger_due_settlements()` catches `InvalidTransitionError`
and writes `status='SKIPPED_ADVANCED'`
([outbox.py:510-519](outbox.py#L510-L519)); the sibling
branch writes `SKIPPED_NOT_FOUND` for `TransactionNotFoundError`. Both exist
precisely because the ZSET can be ahead of the schedule table. They are
compensation logic, and their presence is the honest evidence of the gap: the
system is eventually consistent across the two stores and reconciles forward,
rather than being atomic.

`ALLOWED_TRANSITIONS` forbidding backward edges is what makes forward
reconciliation safe — a replayed trigger raises instead of corrupting the
history.

### What closing it would require

Not a smaller change than it sounds. Either:

- **Move the transaction state into SQLite** alongside the outbox, making Redis
  a pure serving projection of it. Correct, and it costs the sub-millisecond
  `ZREVRANGE` current-state read that the state machine exists to provide.
- **Or accept the two stores and make the reconciliation authoritative**: a
  periodic sweep that treats the ZSET as the system of record and repairs
  schedule rows, with a monitored lag metric and an alert when it stops
  converging.

Choosing is an architectural decision, so it is recorded here rather than
guessed at in code.

### Unrelated defect found in the same file, now closed

`write_outbox_event()` mixed `time.time()` into the `event_id`, so its
`INSERT OR IGNORE` could never actually collide and the idempotency the clause
implied was decorative. Reproduced before fixing: the same logical event written
twice produced two distinct ids and two queued rows.

Fixed by the judgement the previous version of this section called for — an
explicit `dedup_key` parameter ([outbox.py:156](outbox.py#L156),
[outbox.py:177](outbox.py#L177)) rather than hashing the payload. `event_id` is
already the table's PRIMARY KEY, so the guard existed at the storage layer; what
was missing was an id a retry could reproduce. Passing a key derived from the
business fact makes the second write collide and be ignored.

Omitting the key keeps the clock-seeded behaviour, and that is deliberate: two
legitimately identical business events — the same customer buying the same
product for the same amount twice — must both survive. Hashing the payload would
have silently collapsed them. Only the caller can tell the two situations apart,
so the decision stays with the caller.

Both halves are asserted:
`OutboxTest::test_dedup_key_makes_a_producer_retry_collide` (a retry leaves one
row) and `OutboxTest::test_events_without_a_dedup_key_stay_distinct` (two real
events both survive). The second is what stops the fix from becoming a
regression.

Still open: no caller passes a `dedup_key` yet — the parameter exists and is
tested, but `schedule_settlement()`'s internal call and the chaos suite both
still use the default. Threading real keys through is a per-caller judgement
about what identifies each business fact.

---

## 2. 风险检查 — Risk Check (`RISK_CHECK`)

### What holds

`RISK_CHECK` is a real state with real guards: `PENDING → RISK_CHECK` is the
only way in, and `RISK_CHECK` may go only to `COMPLIANCE_HOLD`, `APPROVED` or
`REJECTED`. The decision point is a typed, substitutable seam —
`RiskEvaluator` (Protocol) and `RiskDecision` (frozen dataclass) in `risk.py`,
injected through `TransactionStateMachine(risk_evaluator=...)`. The threshold
comes from `L1_business_data/policy.py`, not a literal, and
`test_hold_boundary_follows_policy_not_a_literal` asserts against the policy
accessor rather than a hardcoded number, so moving the policy cannot leave a
stale test passing.

### What does not hold

`ThresholdRiskEvaluator` is the whole implementation, and its only logic is
`amount >= compliance_hold_threshold(product)`. It is a placeholder with a type,
which is more useful than a placeholder without one but is not a risk engine.

Specifically:

- **`actor="risk_engine"` exists only as a string literal.** No such component
  is deployed. The trail names an operator that is a naming convention.
- **The evaluator reads no features.** `features_used=()` and
  `feature_age_s=None` are the machine-readable statement of that, chosen so the
  absence is recorded rather than implied by a blank.
- **`customer_key` is already stored and never read.** `init_transaction()`
  writes it into `txn:meta:{txn_id}`, and nothing consults it. This is the
  cheapest-looking gap and the most instructive: the join key to the customer's
  features is present at the decision point, and joining on it would still be
  wrong, for the reasons below.

### Why it does not simply read the online store

Wiring `evaluate()` to `cce:features:{key}` would produce a plausible demo and
an unsound control. Four things are missing:

| Requirement | Current state |
|---|---|
| Per-field freshness | The store carries no per-field timestamp, so an evaluator cannot tell a 4-second-old value from a 4-hour-old one |
| Sub-second velocity | Gold is batch truncate/rebuild; a velocity feature computed there is stale by construction |
| A trustworthy label | There is no fraud/loss outcome anywhere in the platform, so no threshold on these features has ever been validated against reality |
| Semantically coherent inputs | See below |

The semantics are the disqualifying one. `risk_score` aggregates only
positive-signal terms, and `risk_band` in practice measures *purchase
intent*, not risk. A control that reads `risk_band` would hold the customers who
were most likely to buy. The names are worse than useless — they invite exactly
the wrong wiring, which is why the seam is left explicitly empty rather than
plugged with the nearest available number.

There is also live semantic drift: `risk_score` has two definitions — three
terms locally, four in the Spark variant, which adds `campaign_clicks_30d` —
one shared name, and no version field to tell them apart. Any risk decision
that read it could not say which definition it got.

### What closing it would require

A real-time decision needs a request-time evaluator with its own feature
freshness contract (see `docs/ARCHITECTURE_OLTP_BOUNDARY.md`, *What a
request-time risk decision would need*), renamed and versioned features, and a
label to validate against. Until then the honest artifact is the typed seam plus
the recorded fact that no feature was read.

---

## 3. 审计日志 — Audit Trail

### What holds

The trail is append-only by construction, not by convention. `ALLOWED_TRANSITIONS`
contains no backward edge, `REJECTED` and `COMPENSATED` are terminal, and no code
path issues `ZREM` or `DEL` against `txn:state:{txn_id}`. A reversal is a forward
transition to `COMPENSATING → COMPENSATED`, so the original decision stays
readable. `test_history_is_append_only_under_repeated_advance` asserts that four
transitions produce four distinct entries in ascending timestamp order.

### What was broken, and is now closed

The member was `{state}:{event_id}`. `advance()` accepted `actor` and `reason`,
used them in one log line, returned them to the immediate caller — and never
stored them. `get_history()` rebuilt entries from the member alone, so it
returned `actor=""` and `reason=""` for every entry, including compliance holds.
The trail recorded *what* changed and *when*, never *who* or *why*.

For a financial audit trail that is the wrong half. "This transaction was held"
without "held by whom, on what basis" does not answer the only question an
auditor asks.

`audit.py` now owns the member encoding: one JSON object carrying state,
`event_id`, actor, reason and a small metadata map, written in the same `ZADD` as
the transition.

Two choices worth their justification:

- **Attribution is in the member, not a side hash.** A side
  `txn:audit:{txn_id}` keyed by `event_id` would keep members small, but the
  attribution would become a second write that can fail independently of the
  transition it describes — reintroducing category 1's split-write problem
  *inside* the audit fix. One member is one `ZADD`.
- **JSON rather than a positional format.** `reason` is free text and may
  contain the `:` that the old format used as a delimiter.
  `test_reason_containing_the_legacy_delimiter_is_not_corrupted` pins that.

Legacy members still decode, flagged `attributed=False` — unknown, not empty —
so an audit view can render the difference instead of printing a blank operator.
Same convention as `features_used=()` in category 2.

One incidental bug fixed by the same change: the default reason was
`f"{current_state}→{to_state}"`, and for a `str`-mixin Enum on Python 3.11+ that
renders as `TxnState.PENDING→TxnState.RISK_CHECK`. Harmless in a log line;
wrong once it is stored audit data.

### What is still open

- **Immutability is process-discipline, not enforced.** Nothing in Redis
  prevents an operator with credentials from issuing `ZREM`. Real immutability
  needs an append-only store or a hash chain over entries (each member carrying
  the digest of its predecessor) so that a deletion is detectable rather than
  merely unlikely.
- **`actor` is self-declared.** Callers pass whatever string they choose; there
  is no authenticated principal behind it, because there is no authorization on
  the serving path at all. An attributed trail whose attribution is unverified is
  a strictly better starting point than an empty one, and still not an
  authenticated one.

---

## Why None Of This Affects The OLAP Side

All three gaps live on the write-authority side of the boundary. The analytical
answer does not depend on any of them, and the reason is structural rather than
a matter of care.

**The feature layer is the semantic single source of truth for analytics.** The
Gold tables define what `customer_features` and `policy_features` *mean* — one
unified semantic per field, computed by the deterministic pipeline. Every
analytical or serving consumer resolves a definition there:

```
source data ──► Bronze ──► Silver ──► Gold (unified semantic) ──► cce:features:{key}
                                       ▲                          (serving projection)
                                       └── semantic SSOT                  ▲
                                                                          │ rt_* only
  command side: outbox_events ──► cdc_events.jsonl ──► stream ────────────┘
```

Four independent reasons the three gaps cannot change an analytical answer:

1. **No shared state.** The operational tables live in their own SQLite
   database (`settings.oltp_sqlite_path`), excluded from `ANALYTICS_TABLES` and
   therefore from `reset_tables()`. `test_analytics_schema_excludes_operational_tables`
   and `test_analytics_init_schema_does_not_create_operational_tables` assert the
   schemas are disjoint in both directions.

2. **No import edge.** Imports may point from `cce_platform.L2_oltp` into
   `cce_platform.*`, never the reverse. `test_no_analytics_module_imports_oltp`
   scans every analytics module's imports, so the rule fails loudly instead of
   eroding. The batch pipeline, the feature API and the online store all still
   run with this package absent.

3. **Recomputability.** Gold is truncate-and-rebuild from source on every run.
   A broken transaction boundary, a placeholder evaluator or a missing operator
   name changes no derived value — rerun the pipeline and the same inputs produce
   the same features.

4. **Directional authority.** The OLTP side never *reads* a feature to make a
   decision — that is exactly what category 2 records as absent. So a defect in
   the transactional domain has no path into a feature value, and the two sides
   share no consistency requirement.

### The CQRS reading of this

Named as a pattern, the split is Command Query Responsibility Segregation: the
transactional package is the command side (write authority, normalised,
correctness-critical), the feature layer is the query side (derived,
denormalised, rebuildable). All three gaps are command-side, which is the
compact reason none of them changes a query-side answer.

| CQRS element | What plays it here |
|---|---|
| Command model | `txn:state:{txn_id}`, `outbox_events`, `settlement_schedule` |
| Query model | Gold tables + `cce:features:{key}` |
| Separate stores | own SQLite file, disjoint schemas, one-way import rule |
| Projection transport | `write_outbox_event()` → `EventPublisher` |
| Read model is disposable | Gold is truncate-and-rebuild; Redis is a cache |

Where the analogy stops is the part worth stating: **the query model here is not
a projection of the command model.** In textbook CQRS both sides describe the
same aggregates and the read side is eventually consistent with the write side.
Gold is built from Bronze source extracts (CAS, AJO), not from `outbox_events` —
the two sides describe different facts, so there is no read-your-writes
expectation between them and no eventual-consistency SLA to miss. A settled
transaction never appears in `customer_features` at all.

One real projection edge does exist, and it is narrow.
`EventPublisher._default_downstream` appends to `data/bronze/cdc_events.jsonl`
([outbox.py:286-298](outbox.py#L286-L298)), which the stream path aggregates
into `rt_*` fields on `cce:features:{key}`. Three things keep it contained:
those fields are prefixed `rt_`, tagged `feature_source="cdc_stream"`, and
absent from the Gold schema entirely — so a defect arriving that way cannot
alter a batch feature. It is also latent rather than running: no process
constructs `EventPublisher()` with its default downstream today, since the chaos
suite and the tests both inject their own.

That edge no longer inherits the whole category 1 defect — both halves of
at-least-once now have a guard, and they are independent.

The *consumer* half: `realtime.process_cdc_events()` filters on `event_id`, so a
redelivered event with an identical id is counted once and reported as
`events_deduplicated`, asserted by
`tests/test_pipeline.py::CdcIdempotencyTest` (a redelivery must not inflate the
aggregate; distinct events must still accumulate).

The *producer* half: `write_outbox_event()` takes an explicit `dedup_key`
([outbox.py:156](outbox.py#L156)), which makes a producer retry of the same
business fact derive the same id and collide against the PRIMARY KEY. Consumer
dedup could never have covered this — a consumer filter cannot recognise two rows
that carry different ids.

What remains open is adoption rather than mechanism: no caller passes a
`dedup_key` yet, so in practice a producer retry still mints a fresh id today.
The Flink path filters on `event_id` keyed state and inherits exactly the same
adoption gap.

The blast radius is unchanged: this is a serving-side counter rather than a Gold
value, and it is the single place where a command-side gap has any visible
query-side effect.

### Stated precisely, because the inverse is the common error

The Gold tables and `cce:features:{key}` are the semantic authority **for
analytics**, and a *serving projection* with no authority at all for
transactional truth. For a balance, a payment, or a final order state, the
authoritative answer comes from the OLTP side — never from Gold or Redis,
however fresh they look. The three roles do not overlap:

| Role | Holder | Authority |
|---|---|---|
| System of record | operational store + `txn:state:{txn_id}` | write authority over transactional state |
| Semantic SSOT | Gold tables | authority over what a feature *means* |
| Serving projection | `cce:features:{key}` | none — a cache of the above |

The one thing that would break this containment is a feature-layer output
feeding back into a transactional decision — the wiring category 2 declines to
add. If that link is ever built, the freshness and semantic-versioning
requirements listed there become preconditions, not improvements, and the
import-direction rule stops being sufficient on its own.

---

## Verification

```bash
PYTHONPATH=src python -m unittest discover -s tests -p test_oltp.py   # 43 tests
PYTHONPATH=src python -m unittest discover -s tests                  # 66 tests (whole app)
PYTHONPATH=src python -m chaos_testing.validate_chaos --mode local    # 30 checks
```

Assertions specific to these categories: `AuditTrailTest` (8, category 3),
`RiskSeamTest` (7, category 2), `OutboxTest::test_settlement_and_outbox_share_one_transaction`
and `::test_rollback_leaves_neither_row` (category 1), `BoundaryTest` (3,
containment), `StorePortTest` (7, the store seam).
