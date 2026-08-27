# OLTP Boundary: Position, Consistency Model And Risk-Control Linkage

This platform is an analytics platform. Bronze, Silver and Gold derive customer
features from source systems; the Feature API and the online store serve them.
Every one of those outputs is recomputable — the pipeline truncates and rebuilds
them on each run, and a lost row is one rerun away from being restored.

Three components never fitted that description. A financial transaction state
machine, a transactional outbox and a T+N settlement scheduler do not *derive*
anything. They hold state that is authoritative, mutable, and recoverable from
no upstream source. They were sitting in the same package as the feature
pipeline, distinguished only by a convention that `reset_tables()` iterated
`ANALYTICS_TABLES` rather than every table.

`src/cce_platform/L2_oltp/` is that distinction made structural.

## Purpose And Scope

| Moved into `L2_oltp/` | Was | Why |
| --- | --- | --- |
| `state_machine.py` | `redis_state_machine.py` | Owns transaction lifecycle state |
| `outbox.py` | `outbox_publisher.py` | Owns undelivered events and settlement obligations |
| `store.py` | new | Operational SQLite, separate file from the warehouse |
| `risk.py` | new | Types the `RISK_CHECK` gate that had no evaluator |

Deliberately **not** moved: `cart_zset.py` (see [What Is Deliberately
Excluded](#what-is-deliberately-excluded)), and `L0_schema/ops.py`, which stays in
the schema registry because it is DDL data consumed by the registry's
import-time disjointness guards — moving it would invert the dependency
direction this boundary exists to establish.

## Position In The Architecture

The platform has two data paths that meet only at the online store, and the
direction of authority runs opposite to the direction of data volume.

```text
  Source systems (CAS / AJO / transactions)
          |
          | CDC / batch landing
          v
  +--------------------------------------------------+
  |  Bronze -> Silver -> Gold                        |   recomputable
  |  truncate + rebuild every run                    |   T+1
  +------------------------+-------------------------+
                           | batch_importer / Flink sink
                           v
  +--------------------------------------------------+
  |  cce:features:{unified_customer_key}   (Redis)   |   SERVING PROJECTION
  |  no write authority over any business fact       |   no authority
  +------------------------+-------------------------+
                           | read
                           v
  +--------------------------------------------------+
  |  Feature API / dashboard / campaign tools        |
  +--------------------------------------------------+

  - - - - - - - - - - write-authority boundary - - - - - - - - - -

  +--------------------------------------------------+
  |  cce_platform.L2_oltp     SYSTEM OF RECORD       |
  |                                                  |
  |  txn:state:{txn_id}     append-only history      |
  |  txn:meta:{txn_id}      transaction attributes   |
  |  outbox_events          not yet delivered        |
  |  settlement_schedule    not yet discharged       |
  |                                                  |
  |  RISK_CHECK --> [ evaluator: threshold only ]    |
  +--------------------------------------------------+
```

The boundary is enforced as an import rule rather than a naming convention:
`cce_platform.L2_oltp` may import from `cce_platform.*`, never the reverse. The
batch pipeline, the Feature API and the online store must all still run with
this package absent. `tests/test_oltp.py::BoundaryTest` asserts it by scanning
every import statement outside `L2_oltp/`, so a violation fails a test instead of
quietly coupling the two paths.

## Constraints

Constraints come first because they are the only part of this document that is
not a decision. They are properties of the problem, and they outlive every
technology named further down.

These are derived from *this* platform, not from a generic ledger example:

| # | Constraint | Where it comes from |
| --- | --- | --- |
| C1 | A settlement obligation must survive process restart | `settlement_schedule` rows are the only record that a trade owes settlement |
| C2 | A transaction must never settle twice | Duplicate settlement moves money twice; no compensating read exists |
| C3 | Every state transition must be reviewable after the fact | Financial regulatory review, cited in the state machine's own docstring |
| C4 | Redis may be unreachable, and the local fallback is per-process | `make_kv_backend` degrades to a JSON file that replicas cannot share |
| C5 | The serving API may run several replicas | K8s deployment with HPA |
| C6 | Gold is truncated and rebuilt on every pipeline run | `reset_tables()` — by design, not a defect |
| C7 | Compliance thresholds change by regulation, without a deploy | Thresholds are set by regulators, not by this team |
| C8 | A feature used in a decision must have a knowable age | A stale feature and a fresh one are not interchangeable in a hold decision |

C6 is the one that forces the split. A table that is deleted and repopulated on
every run cannot live beside a table whose rows are irreplaceable, because one
mistaken entry in the truncation list silently destroys settlement obligations.
Before this change, the only thing standing between those two outcomes was that
`reset_tables()` happened to iterate the right list — and an earlier revision of
that list had in fact drifted out of sync with the DDL.

## Required Properties

Each property is what C1–C8 demand, stated so it can be checked:

| Property | Demanded by | Current state |
| --- | --- | --- |
| Durability of obligations | C1 | Held — SQLite WAL, own database file |
| Exactly-once settlement effect | C2 | Held — optimistic locking plus the state machine refusing backward and repeat transitions |
| Immutable audit trail | C3 | **Partial** — transitions are append-only and now carry actor, reason and provenance, but immutability is not enforced and `actor` is unauthenticated (see [The Audit Gap](#the-audit-gap)) |
| Explicit failure over silent degradation | C4, C5 | Held — `CCE_REQUIRE_REDIS` makes staging and production fail fast rather than fork state per replica |
| Isolation from recomputable state | C6 | Held — separate database, separate package, enforced import direction |
| Policy externalisation | C7 | Held — thresholds and settlement cycles load from `config/business_policy.json` |
| Freshness observability | C8 | **Absent** — the online store records `feature_source` but no per-field timestamp |

Two of seven are not met. Naming them here is the point: a boundary document
that claims all seven would be worth less than one that says which two are
outstanding and why.

## Trade-offs

The two paths are optimised for opposite things, which is the substantive reason
they should not share a package, a database or a release cadence.

| Property | `L2_oltp/` block | Gold + serving path |
| --- | --- | --- |
| Correctness | ★★★★★ | ★★★ |
| Consistency | ★★★★★ | ★★ |
| Auditability | ★★★★★ *aspired* / ★★ *actual* | ★★★ |
| Latency | ★★★ | ★★★★★ |
| Throughput | ★★ | ★★★★★ |
| Recomputability | ★ *by design* | ★★★★★ |

Reading the table: the serving path may return a feature that is a day stale, or
drop a row and recover it on the next run, and the business outcome is a slightly
worse campaign. The OLTP block may do neither. Conversely the OLTP block is
allowed to be slower and to handle far less volume, because a settlement
obligation is written once per trade rather than recomputed for 480K customers.

Optimising both paths for the same properties would mean either paying
correctness overhead across the whole feature pipeline, or accepting
"eventually, mostly" semantics on money. The boundary is what lets each path
pick its own answer.

## System Of Record Versus Semantic Single Source Of Truth

These are routinely conflated, and conflating them is what produces the mistake
of treating a Gold table as authoritative. The useful question is not "is the
warehouse the single source of truth" but **which system actually owns write
authority for this fact**.

| Role | What holds it here | What that means |
| --- | --- | --- |
| Transactional System of Record | `txn:state:{txn_id}`, `txn:meta:{txn_id}`, `outbox_events`, `settlement_schedule` | Owns write authority. The answer it gives is the fact. |
| Semantic SSOT | Gold feature definitions — `gold_customer_features`, `gold_policy_features`, `gold_customer_model_scores` | Owns *definition* authority: what `velocity_7d` means. Not write authority over any business event. |
| Serving projection | `cce:features:{unified_customer_key}` | Owns nothing. A cache of a derivation. Safe to delete and rebuild. |
| Neither | Bronze | A copy of someone else's record. The upstream source systems are SoR for customer facts; this platform never is. |

The operational consequence, and the reason this document exists: **for a
balance, a payment, or a final order state, Gold and Redis are not a source of
truth.** OLTP remains authoritative and Redis is a projection of it. A campaign
decision may read the projection. A settlement decision may not.

That distinction also means the platform can legitimately own the *semantics* of
a feature without owning the *record* — Gold can define what "high value" means
while never being the place you ask whether a payment cleared.

Named as a pattern, this is Command Query Responsibility Segregation: OLTP is
write-shaped (normalised, transactional, correctness-critical), the feature layer
is read-shaped (denormalised, precomputed, rebuildable), and each gets the model
its access pattern wants instead of one schema compromising for both. The
analogy is worth carrying only as far as it holds — in textbook CQRS the read
model is a projection of the write model and is eventually consistent with it,
whereas Gold is built from Bronze source extracts rather than from
`outbox_events`. The two sides describe different facts, so there is no
read-your-writes expectation between them. `L2_oltp/KNOWN_GAPS.md` works through
the one narrow projection edge that does exist and why it stays contained.

## Failure Model And Consistency Boundaries

Not every hop deserves the same guarantee. Splitting them by business nature is
what keeps the strong-consistency budget affordable.

| Hop | Boundary | Acceptable? |
| --- | --- | --- |
| Transaction state change → settlement obligation recorded | Must be atomic | Required |
| Settlement obligation → downstream notification | At-least-once, eventual | Fine |
| Gold rebuild → serving projection refresh | Eventual, T+1 | Fine |
| Transaction state change → "has the money moved" answer | Must be strongly consistent | Required |

The honest finding is that the first row is **not currently satisfied, and the
database split is not what breaks it — it was already broken.**

The Outbox pattern exists so that an event and the business state change it
describes commit together. Here the business state lives in Redis
(`txn:state:{txn_id}`) while the outbox lives in SQLite. `SettlementTrigger.run_once()`
calls `sm.advance()` against Redis and then `UPDATE settlement_schedule` against
SQLite, with no shared transaction. A crash between the two leaves the state
machine advanced and the schedule row not, which the code recognises: the
`SKIPPED_ADVANCED` branch is reconciliation logic written to absorb exactly that
window. It self-heals and does not double-settle, but the atomicity the pattern
promises is not delivered.

What the split *does* preserve is the only atomicity actually available: both
operational tables now live in one file, so `schedule_settlement()` and
`write_outbox_event()` can share a single transaction. `L2_oltp/store.py` exposes
`transaction()` for exactly that, and `tests/test_oltp.py` asserts both the
commit and the rollback case.

Closing the remaining gap means moving transaction state into the same
transactional store as the outbox. That is now a change to one module rather
than a change spread across the package — which is the practical payoff of
drawing the boundary.

Two further failure characteristics worth stating:

**The local fallback is single-replica only.** When Redis is unreachable,
`make_kv_backend` degrades to a per-process JSON file. For derived read-only
features that is harmless. For transaction state it means each replica keeps its
own divergent history, so staging and production set `CCE_REQUIRE_REDIS` and
fail fast at construction instead.

**Settlement polling is restart-safe by construction.** Rows still marked
`PENDING_SETTLE` are re-queried after a restart and re-triggered; idempotency
comes from the state machine refusing a repeat transition rather than from the
poller remembering what it did.

## Risk-Control Linkage

This is the functional link the boundary was drawn to clarify, and it is worth
stating the finding before the detail: **the platform computes risk-shaped
numbers on the analytics side and makes risk decisions on the transactional
side, and the two have never been connected.**

### What the transactional side decides

The lifecycle has a `RISK_CHECK` state. Until this change it had no evaluator.
`run_auto_advance()` moved every transaction from `RISK_CHECK` straight to
`APPROVED` unless one comparison tripped:

```text
amount >= compliance_hold_threshold(product)  ->  COMPLIANCE_HOLD
```

That is the entire automated risk gate. It reads `product` and `amount` from
`txn:meta:{txn_id}` and nothing else. It does not know who the customer is. The
string `actor="risk_engine"` appears in this repository only as a literal in the
chaos suite; there is no risk engine.

`L2_oltp/risk.py` does not add one. It gives the empty slot a type — a
`RiskEvaluator` protocol and a `RiskDecision` carrying `evaluator_version`,
`features_used` and `feature_age_s` — with `ThresholdRiskEvaluator` reproducing
the existing rule exactly. Behaviour is unchanged. What changes is that the gap
is now visible in a signature rather than only in prose, a real evaluator can be
injected without touching the state machine, and every decision records which
rule version fired.

`features_used` being empty and `feature_age_s` being `None` on every current
decision is not an oversight. It is the machine-readable statement that this
decision consulted no customer features at all.

### What the analytics side computes

All of it is offline, population-level, T+1, and rebuilt on every run:

| Feature | Where | What it actually is |
| --- | --- | --- |
| `risk_score` | `pipeline.py` → `gold_customer_features` | Weighted sum of `velocity_7d`, `product_diversity`, `monetary_30d` |
| `lapse_risk_score` | `pipeline.py` → `gold_policy_features` | Claim count, renewal proximity, premium size |
| `propensity_score` | `mlops.py` → `gold_customer_model_scores` | Hand-written logistic, no training |
| `risk_band` | `mlops.py` | Buckets of `propensity_score` |
| `velocity_7d`, `tx_count_30d`, `monetary_30d` | `pipeline.py` | Window aggregates anchored on the newest row in the batch |
| `gold_transaction_anomalies` | `deploy/emr_delta/4_anomaly_detection.py` | p99 and z-score outliers, plus a planted label |

Three of these would mislead anyone who wired them into a hold decision under
their current names:

- **`risk_score` sums only positive terms.** An active, diversified, high-value
  customer therefore scores *riskier* than a dormant one. It is a
  behavioural-intensity composite, not a loss probability.
- **`risk_band` buckets a propensity score.** "High risk_band" means high buying
  intent. The name says the opposite of what the number measures.
- **`velocity_7d` is a count, not a rate**, and it is anchored on the maximum
  transaction timestamp in the data rather than on wall clock — so it does not
  answer "how active is this customer right now".

There is also a live semantic-drift instance: the local `risk_score` uses three
terms, while the Spark implementation in `deploy/emr_delta/3_gold_segmentation.py`
adds a fourth (`campaign_clicks_30d`). Two definitions, one name, no version
field to tell them apart. This is precisely why a feature layer has to own
semantics, computation, version, freshness, quality and lineage rather than just
column names — renaming columns would not have caught this.

The only fraud signal in the repository, `is_fraud_label`, is planted by the
synthetic generator at `pmod(txn_num, 997) == 0`. The anomaly detector's
highest-priority reason therefore detects a label the generator injected. Nothing
has been learned from real outcomes.

### The seam, in one sentence

`init_transaction()` already stores `customer_key` in `txn:meta:{txn_id}`, and
nothing reads it. **The join key between the risk decision and the customer's
features is present and unused.**

### What a request-time risk decision would need

Wiring that key to the online store is a small code change and a large
correctness claim, which is why it is deliberately not made here:

| Requirement | Status |
| --- | --- |
| Per-field freshness on `cce:features:{key}` | Absent — batch and stream merge field by field, recording only `feature_source`, so a reader cannot tell whether a value is four seconds or four days old |
| Sub-second velocity | Absent — the five-transactions-in-five-minutes CEP check is documented in `flink_cdc_pipeline`'s docstring and in README, but does not exist in code |
| A trustworthy label | Absent — see `is_fraud_label` above |
| Features whose names match their semantics | Not yet — see the three defects above |
| Persisted decision provenance | Absent — see [The Audit Gap](#the-audit-gap) |
| Authorization on the feature read path | Absent — every API endpoint is unauthenticated |
| One threshold source | Partial — `cart_zset.py` hardcodes a flat `1000.0` cart-review flag. Not the same concept as the per-product hold threshold; see the note below before "fixing" it |

Which existing features *could* feed a decision, and at what freshness:
`velocity_7d`, `monetary_30d` and `product_diversity` are usable at **T+1** as a
slow-moving prior — "is this customer historically unusual" — and are not usable
as a per-transaction gate. The freshness ladder is T0 source change → T1 CDC →
T2 Silver → T3 Gold → T4 projection materialised. Using a feature in a hold
decision requires committing to a **Freshness SLA** at T4, for example P95 under
five seconds. The platform currently measures no stage of that ladder, so the
SLA cannot yet be stated, let alone enforced.

### The Audit Gap

The state machine's docstring and README both claim a full audit trail suitable
for financial regulatory review. The transition history genuinely is append-only:
`ALLOWED_TRANSITIONS` permits no backward edge, and `REJECTED` and `COMPENSATED`
are terminal.

The member used to be only `{state}:{event_id}`. `advance()` accepted `actor`,
`reason` and `metadata`, logged them, and persisted none of them — so
`get_history()` returned every transition with `actor=""` and `reason=""`. The
trail recorded **what state, and when. Not who, and not why.** For a compliance
hold that is the material half: "this transaction was held on 14 March" does not
answer "why, by which rule, on what evidence".

That half is now closed. `L2_oltp/audit.py` owns the member encoding — one JSON
object carrying state, `event_id`, actor, reason and a small provenance map,
written in the same `ZADD` as the transition it describes. The side-hash
alternative (`txn:audit:{txn_id}` keyed to the same event IDs) was the more
obvious design and was rejected: it would make attribution a second write that
can fail independently of its transition, reproducing the split-write problem of
the section above *inside* the audit fix. Members carrying their own attribution
cost index size, which is why callers pass small provenance maps rather than
payloads. Legacy members still decode, flagged `attributed=False` — unknown
rather than empty, the same convention as `features_used=()` in `risk.py`.

`RiskDecision` was shaped to be persisted this way, which was the other reason to
introduce it: `run_auto_advance()` now records `evaluator_version` on the hold, so
a hold names the code that produced it.

What remains open is immutability itself and the identity behind `actor`.
Nothing in Redis prevents an operator with credentials from issuing `ZREM`;
detectable immutability needs an append-only store or a hash chain over entries.
And `actor` is self-declared, because there is no authorization on the serving
path at all — an attributed trail whose attribution is unverified is a better
starting point than an empty one, and still not an authenticated one.

See `src/cce_platform/L2_oltp/KNOWN_GAPS.md` for this gap alongside the other two,
and for why none of the three changes an analytical answer.

## What Is Deliberately Excluded

**`cart_zset.py` stays on the analytics side.** It holds authoritative mutable
state in Redis, so the write-authority test alone would pull it in — which shows
that test is necessary but not sufficient.

| Test | `cart:*` ZSETs | `txn:state:*`, `outbox_events`, `settlement_schedule` |
| --- | --- | --- |
| Owns its writes | Yes | Yes |
| Recomputable | Effectively — the customer re-adds items | No |
| Blast radius if lost | One session, a UX regression | Money that does not move, and an audit finding |
| Consistency boundary | Best-effort | Strong; must not double-settle |
| Direction of flow | **Producer** for the feature path | Terminal authoritative state |

The separator is the consistency boundary. A cart is the "notification" case:
losing it is annoying and recoverable by the user. A settlement obligation is the
"did the money move" case. The cart also flows *toward* Gold —
`snapshot_to_cdc_event` emits `cart_events`, which the streaming path folds into
`rt_cart_value_1d` — so it is a pre-transaction engagement signal, and a feature
source belongs with the features.

Including it would also degrade the package's defining predicate from a business
property ("financial transaction integrity") to an infrastructure coincidence
("things that keep mutable state in Redis"). The first is defensible in a
sentence; the second is not a boundary at all.

**Not addressed by this change**, and stated so it is not mistaken for done:

- The Redis↔SQLite atomicity gap described above.
- Enforced audit immutability, and an authenticated identity behind `actor`.
  Attribution is now persisted; nothing prevents a privileged `ZREM`, and the
  operator name is still self-declared.
- `cart_zset.py`'s hardcoded `1000.0` cart-review flag. Left as a literal
  deliberately, and the earlier prescription here ("should read policy") was
  wrong: `compliance_hold_threshold()` is a **per-product** gate that routes a
  transaction to `COMPLIANCE_HOLD`, while `has_high_value` is a **flat** hint that
  an RM should eyeball the basket. Substituting one for the other was measured
  against six representative baskets and loses 4 of 6 flags — `CARD` 5000,
  `SAVINGS` 4000 and `TRAVEL_INSURANCE` 2500 all have no configured threshold, so
  the policy default of infinity silently stops flagging them, and `INSURANCE`
  1500 falls under its 2000 gate. Losing a compliance flag is strictly worse than
  a literal. The real gap is that the flat number has no policy home: it needs its
  own section (a `cart_review_threshold_sgd`), not a redirect to an existing one.
- `write_outbox_event()` now takes an explicit `dedup_key`, so a producer retry of
  the same business fact derives the same `event_id` and collides against the
  PRIMARY KEY. Hashing the payload was rejected: it would have silently collapsed
  two legitimately identical business events, and only the caller can tell those
  two situations apart. What remains open is adoption — the parameter is tested
  but no caller passes one yet, so a retry still mints a fresh id in practice.
- No authorization anywhere on the serving path, while the docs claim
  request-time authorization. Real controls exist only Databricks-side, via
  `is_account_group_member('cce-compliance')` row filters.

## Verification

The extraction is verified by two harnesses rather than by inspection.

```powershell
PYTHONPATH=src python -m unittest discover -s tests -p test_oltp.py   # 43 tests
PYTHONPATH=src python -m unittest discover -s tests -p test_layers.py # 7 tests
PYTHONPATH=src python -m unittest discover -s tests                  # 66 tests
PYTHONPATH=src python -m chaos_testing.validate_chaos --mode local    # 30 checks
```

`tests/test_oltp.py` is the unit harness — fixture transactions per scenario, an
in-process execution environment using temp-scoped stores that never touch
`data/`, and named assertions per invariant. It covers the lifecycle, refused
transitions, init idempotency, the policy-driven hold boundary, evaluator
substitution, holiday-aware T+N dates, outbox delivery and retry exhaustion,
shared-transaction commit and rollback, single-fire settlement, audit
attribution surviving the round trip, the store port and its two adapters, and
the import direction rule. `tests/test_pipeline.py` gained `CdcIdempotencyTest`,
which pins that a redelivered CDC event does not double-count while genuinely
distinct events still accumulate. `tests/test_docs.py` checks the claims these
documents make — documented paths must exist in a clone rather than only on the
author's disk, and every test count written here must equal what the loader
finds.

`chaos_testing/validate_chaos.py` is the system harness: 30 checks, a printed
report, and an exit code equal to the failure count.

Pass criteria for this change: **66 unit tests OK, and the chaos suite reporting
30/30 with exit 0 — the same check-name set as before the move.** An identical
name set matters more than "no crash", because a broken import in that suite
surfaces as a check flipping to FAIL rather than as an exception. Those import
failures are now prefixed `IMPORT ERROR (not a check failure)` so the two cannot
be confused.

One prerequisite fix was needed to establish any baseline at all: the suite
bootstrapped `sys.path` from a path inherited from an older directory layout
(`../local_app/src`) that does not exist here, so all three transactional check
groups had been silently reporting import failures rather than running. They pass
now.

## Related

- `src/cce_platform/L2_oltp/KNOWN_GAPS.md` — the three open gaps as a register
  (transaction boundary / `RISK_CHECK` / audit trail), and why none of them
  reaches the analytics side
- `src/cce_platform/L2_oltp/__init__.py` — the boundary rule, stated where an
  importer will see it
- `src/cce_platform/L2_oltp/store.py` — why the operational database is separate,
  and what that costs
- `src/cce_platform/L2_oltp/risk.py` — what a feature-reading evaluator would need
- `src/cce_platform/L2_oltp/audit.py` — why attribution lives inside the ZSET
  member rather than in a side hash
- `src/cce_platform/L0_schema/ops.py` — the table-ownership declaration this
  boundary grew out of
