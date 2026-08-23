# CCE Feature Platform PoC

Customer Campaign Engine / CDP style feature platform demonstrating medallion data modeling, identity resolution, feature serving and deployment patterns.

## What This Shows

- Bronze -> Silver -> Gold medallion data flow
- NRIC / FIN / Passport identity resolution into a unified customer key
- Customer feature engineering for campaign activation
- Policy-level feature engineering for insurance and premium-financing use cases
- Lightweight customer segmentation
- Campaign eligibility rules
- GraphML-style identity candidate matching for same-person records with missing deterministic IDs
- MLOps outputs: model scores, model-run metadata and feature drift metrics
- FastAPI service for feature and eligibility lookup
- Static dashboard served by the backend
- Docker, Kubernetes and GitHub Actions examples
- Databricks job template for enterprise deployment discussion
- CDC-to-online-feature-store simulation for real-time feature discussion
- POC classification roadmap for POC pilot users, MVP1 20K active users, MVP2 480K active users and after-MVP2 extensions
- Delivery plan covering indicative duration, sprint cadence, fast wins, rollout waves and success gates
- 480K-active-user AWS sizing and deployment notes
- Big-data EMR / Delta extension notes for Spark synthetic data, Airflow orchestration and S3 lakehouse layout
- AI vector DB extension notes for LLM-assisted best offer, product/offer RAG and retrieval-aware MLOps
- Operations maturity and cost notes for realistic rollout constraints
- Business insight runbook with reproducible local output and product interpretation

## Delivery And Product Summary

Current delivery shape:

- Terraform exists for the after-MVP2 real-time extension, but it currently provisions MSK and ElastiCache Redis only. It does not yet create EKS/AKS, RDS, S3/Glue/EMR or Airflow infrastructure.
- Helm exists as an optional application chart for the same CCE API runtime. It is not currently the main path for shared middleware such as Nginx Ingress.
- Kustomize overlays are the clearest application deployment path today. The dev, staging and production overlays patch namespace, image tag, replica count and `CCE_RUNTIME_ENV`.
- CI/CD examples exist for GitHub Actions, GitLab CI and Jenkins. GitLab/Jenkins can deploy the Kustomize overlays after test and image build stages.

Current runtime scale:

- 1 business API microservice: `cce-feature-platform`, a FastAPI runtime serving feature lookup, campaign eligibility, data quality, identity candidate and MLOps endpoints.
- 3 background/platform workloads: `cce-gold-to-redis-importer` CronJob, `cce-realtime-feature-stream` StatefulSet and `cce-mlops-monitor` CronJob.

Product value:

- The platform turns fragmented CAS, AJO, transaction and policy data into governed customer features.
- It resolves NRIC / FIN / Passport identities into unified customer keys and flags graph-style identity candidates for review.
- It produces customer segments, campaign eligibility, policy lapse risk, propensity scores, drift checks and data quality evidence.
- The API and online store expose those outputs as a campaign decision layer for downstream activation tools.

Current sample-data insights:

- The Priority segment has 2 customers, with average 30-day monetary value of 8400 and average propensity of 0.946; it is the best audience to prioritize for outreach.
- The INS_NEW campaign has 5/6 eligible customers, and PF_UPSELL has 2/6 eligible customers; both can be turned directly into campaign audiences.
- Priya Raman is the highest-value and highest-intent customer: monetary_30d=10710, propensity=0.979, risk_band=high.
- The highest policy lapse risk is Priya's premium financing policy, with lapse_risk_score=0.627; the next highest is Rahul's pending renewal policy, with lapse_risk_score=0.488.
- Identity governance is valuable: Alicia's CAS/AJO records can be deterministically merged, while Mei Ling's temporary AJO identity can be routed to manual review and attached to a known customer.
- Data quality risk is measurable: there are 4 DQ issues, mainly unmapped_identifier, unmapped_transaction_identifier and unmapped_policy_holder.
- Drift alerts are strong: monetary_30d, velocity_7d, risk_score and tx_count_30d are all high drift, so model and segmentation rollout needs monitoring and recalibration before production scale.

See `03_business_insights/README.md` for the full reproducible runbook, expected outputs and SQL checks behind these insights.

## Function Scope And Prioritization

This repository is intentionally split into foundation capabilities and extension capabilities. The foundation proves that customer campaign features can be computed, trusted and served. The extensions are added only after that base is stable, because they increase operational cost and coordination surface.

| Layer | Capability | What It Includes | Why It Comes Here |
| --- | --- | --- | --- |
| Foundation POC | Runnable CCE domain flow | Deterministic Bronze -> Silver -> Gold pipeline, NRIC / FIN / Passport identity resolution, customer and policy features, segmentation, eligibility rules, API, dashboard and tests | Proves the business logic with a small, inspectable dataset before adding platform complexity |
| Foundation MVP1 | Controlled serving path | Docker image, CI/CD, Kubernetes manifests, HPA, batch Gold-to-online-store importer and dev/staging/production shape | Makes the PoC deployable and reviewable without pretending it is already a full real-time platform |
| Foundation MVP2 | Scaled batch foundation | EMR/Delta or Databricks mapping, Spark jobs, Airflow/MWAA shape, S3/Delta layout, replay/backfill thinking and 480K-active-user sizing | Batch correctness, replay and data quality are the base that real-time and AI paths depend on |
| Extension 1 | Real-time feature serving | CDC simulation, Debezium/MSK direction, stream job, Redis/ElastiCache online store and low-latency feature updates | Adds freshness after the historical baseline is trusted; otherwise low-latency serving can amplify bad data quickly |
| Extension 2 | MLOps and identity governance | Model scores, model-run metadata, drift metrics, graph-style identity candidates, monitoring cronjob and promotion evidence | Governs model behavior after feature definitions are stable enough to monitor meaningfully |
| Extension 2.1 | Vector DB / AI best-offer path | Embedding sync, vector index checks, product/offer RAG, LLM-assisted best offer and retrieval-aware validation | AI is useful only when deterministic features, model evidence and fallback paths already exist |

The priority is correctness first, scale second, freshness third and intelligence last. That is the main trade-off: the early phases are less flashy, but they reduce ambiguity around data meaning, identity resolution and campaign eligibility. The later extensions add lower latency and richer decisioning, but they also add brokers, caches, streaming state, model governance, vector indexes and more failure modes.

| Decision | Benefit | Trade-off |
| --- | --- | --- |
| Start with a local deterministic pipeline | Fast to run, easy to inspect and good for explaining the CCE domain | Does not prove distributed scale by itself |
| Build the batch foundation before real time | Keeps replay, backfill and data-quality checks clear | Some campaign signals are not served at sub-second freshness yet |
| Add Kubernetes and CI/CD before advanced streaming | Creates a controlled promotion path and rollback habit | More delivery scaffolding before adding new product features |
| Put MLOps after stable feature definitions | Drift and model-run evidence become meaningful | Model governance is delayed until feature semantics settle |
| Put Vector DB after MLOps | LLM/RAG answers can rely on governed features and deterministic fallbacks | AI-assisted best-offer UX arrives later than the core feature platform |

## Architecture

```text
CAS / AJO / Transaction Events
          |
          v
Bronze raw JSON landing
          |
          v
Silver standardized customer identity + transactions
          |
          v
Gold customer features + segmentation + campaign eligibility
          |
          v
FastAPI / dashboard / downstream campaign tools
```

## Local Run

From the local app directory:

```powershell
cd 01_foundation/01_poc_pilot_users/dev/local_app
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pip install -e .
$env:PYTHONPATH="src"
python -m cce_platform.pipeline run
python -m uvicorn cce_platform.api:app --host 127.0.0.1 --port 8010
```

Open:

```text
http://127.0.0.1:8010
```

Useful APIs:

```text
GET /api/summary
GET /api/features
GET /api/policies/features
GET /api/online-features/U0001
GET /api/identity/candidates
GET /api/mlops/model-runs
GET /api/mlops/drift
GET /api/campaigns/INS_NEW/eligibility
GET /api/data-quality/issues
GET /api/lineage
```

## Real-Time Feature Demo

After running the batch pipeline, load Gold features into the local online store and apply CDC-style updates:

```powershell
python -m cce_platform.batch_importer --replace
python -m cce_platform.realtime run
```

The local online store is a JSON-backed stand-in for Redis. The production discussion maps it to Debezium + MSK + EKS stream job + ElastiCache.

### Flink CDC Pipeline (Extension)

The `realtime.py` module is a batch simulation. For production workloads the `flink_cdc_pipeline` module provides a true streaming path:

```powershell
# Local simulation — no Flink cluster required, reuses same dedup + intent-score logic
python -m cce_platform.flink_cdc_pipeline run

# Submit to a running Flink cluster (requires PyFlink + Kafka)
python -m cce_platform.flink_cdc_pipeline submit --kafka-brokers localhost:9092
```

**When Flink is required instead of the batch simulation:**

| Scenario | Why Flink, not batch |
|----------|---------------------|
| CDC events arrive out of order across Kafka partitions | Flink Event Time + Watermark correctly assigns events to windows; batch re-scan cannot |
| Exactly-once dedup across restarts | Flink Checkpoint + RocksDB keyed state + `stable_event_id`; batch re-run may double-count |
| `rt_order_count_1d` must use a true sliding window | Flink `SlidingEventTimeWindows(1d, 1min slide)` increments in place; batch scans full history every run |
| Fraud velocity check (5 transactions in 5 minutes) | Flink CEP `Pattern.begin().times(5).within(5 min)`; impossible in a periodic batch job |
| `PREMIUM_FINANCING` / `INVESTMENT` amounts must not be double-billed | Flink exactly-once sink with Redis `MULTI/EXEC` per checkpoint boundary |

The module degrades gracefully: if `REDIS_URL` is not set the sink writes to `LocalOnlineStore`; if PyFlink is not installed the `submit` command raises a clear error while `run` still works.

### Financial Transaction State Machine

The `redis_state_machine` module implements a ZSET-backed state machine for financial order lifecycle management:

```python
from cce_platform.redis_state_machine import TransactionStateMachine, TxnState

sm = TransactionStateMachine()   # Redis if REDIS_URL set, else local JSON
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

**Why ZSET instead of a status column:**

- `ZRANGEBYSCORE` gives the full audit trail in O(log n) — required for financial regulatory review.
- Score = Unix timestamp microseconds: natural ordering without a separate `version` column.
- `WATCH/MULTI/EXEC` optimistic lock prevents the brain-split scenario where a Saga compensation thread and a normal processing thread race on the same order.
- `COMPLIANCE_HOLD` threshold is product-aware: `PREMIUM_FINANCING >= 1000 SGD`, `INVESTMENT >= 500 SGD`.

### Financial Product Cart (ZSET)

The `cart_zset` module implements a Redis ZSET-backed product basket for insurance and wealth products:

```python
from cce_platform.cart_zset import CartService, CartItem, ProductCode

cart = CartService()   # Redis if REDIS_URL set, else local JSON
cart.add_item("U0001", CartItem(product=ProductCode.INVESTMENT, amount=2100.0))
cart.add_item("U0001", CartItem(product=ProductCode.INSURANCE,  amount=1380.0))
ranked  = cart.get_ranked_items("U0001")      # INVESTMENT first (priority weight 8.0)
expiring = cart.get_expiring_soon("U0001", within_minutes=15)  # quote expiry alert
cart.merge_anonymous_cart("ANON-session-1", "U0001")# post-login merge
snapshot = cart.snapshot_to_cdc_event("U0001")                 # feeds flink_cdc_pipeline
```

**Why three ZSETs per customer instead of one Hash:**

| ZSET key | Score | Purpose |
|----------|-------|---------|
| `cart:items:{key}` | `add_ts` | Chronological order — default customer view |
| `cart:priority:{key}` | product weight | RM/advisor ranked view (high-margin first) |
| `cart:expiry:{key}` | `expiry_ts` | Quote expiry polling — `ZRANGEBYSCORE now deadline` |

Financial product quotes have time-limited pricing (`INVESTMENT_LINKED` 30 min, `PREMIUM_FINANCING` 60 min). A plain Hash has no native range query on expiry; ZSET score makes expiry lookup O(log n + k).

### Transactional Outbox + T+2 Settlement Scheduler

The `outbox_publisher` module solves the "DB updated, Kafka send failed" atomicity problem and implements T+2 settlement scheduling with holiday awareness:

```python
from cce_platform.outbox_publisher import (
    write_outbox_event, EventPublisher,
    schedule_settlement, SettlementTrigger, HolidayCalendar,
)

# Step 1 — business code: same transaction as state change
with connect() as conn:
    conn.execute("UPDATE orders SET status='PAID' WHERE order_id=?",(order_id,))
    write_outbox_event(conn, "order", order_id, "OrderPaid", {"amount": 288.0})
    schedule_settlement(conn, order_id, customer_key, "INVESTMENT", 2100.0)
    conn.commit()   # outbox row + settlement row committed atomically

# Step 2 — background thread: EventPublisher polls and forwards
publisher = EventPublisher()
publisher.start_background()   # polls every 2s, marks SENT after downstream ACK

# Step 3 — background thread: SettlementTrigger fires on T+2 date
trigger = SettlementTrigger()
trigger.start_background()     # polls every 10s, advances state machine on due settlements
```

**Why Transactional Outbox:**

Without it: `UPDATE orders` commits → process crashes before Kafka send → event lost forever, order stuck in `PAID` with no downstream notification.

With it: the outbox row is committed in the same SQLite transaction. Even if the process restarts 100 times, `EventPublisher` will keep retrying until the downstream confirms. `event_id` (UUID5 of aggregate + type + timestamp) guarantees idempotent consumption.

**T+2 Holiday Calendar:**

```python
cal = HolidayCalendar()           # SG + HK holidays 2025-2026 built in
cal.settle_date(date(2026, 8, 21), t_plus=2)  # Friday → Tuesday 2026-08-25 (skips weekend)
```

Production: override the holiday set from Consul KV `cce/config/holiday_calendar` so typhoon closures or ad-hoc exchange halts take effect without a code deploy.

| Product | T+N | Reason |
|---------|-----|--------|
| `PREMIUM_FINANCING` | T+2 | HKEX standard equities settlement |
| `INVESTMENT` / `INVESTMENT_LINKED` | T+2 | Fund NAV calculation cycle |
| `INSURANCE` | T+1 | Policy activation next business day |
| `SAVINGS` / `CARD` / `TRAVEL_INSURANCE` | T+0 | Immediate activation |

Detailed architecture material:

```text
DELIVERY_PLAN.md
01_foundation/docs/POC_CLASSIFICATION_AND_ROADMAP.md
02_extensions/01_realtime/docs/REALTIME_FEATURE_PLATFORM_480K.md
02_extensions/02_mlops/docs/ARCHITECTURE_MLOPS_GRAPHML_DEPLOYMENT.md
01_foundation/docs/BIG_DATA_EMR_DELTA_EXTENSION.md
02_extensions/02_mlops/2_1_vector_db/docs/AI_VECTOR_DB_EXTENSION.md
01_foundation/docs/OPERATIONS_MATURITY_AND_COST.md
```

## Docker Run

```powershell
docker build -t cce-feature-platform 01_foundation/01_poc_pilot_users/dev/local_app
docker run --rm -p 8010:8000 cce-feature-platform
```

## CI/CD

The repository-level workflow is in:

```text
.github/workflows/poc-ci.yml
```

It installs dependencies, runs tests and builds Docker images for both PoCs.

## Chaos Testing

K8s chaos experiments and validation scripts are in `dev/chaos_testing/`.

### Running the validation suite (no K8s cluster needed)

```powershell
cd 01_foundation/01_poc_pilot_users/dev/chaos_testing
python validate_chaos.py --mode local
```

This runs 30 automated checks covering all new modules:

| Check group | What it verifies |
|-------------|------------------|
| `state-machine` (8 checks) | Normal lifecycle, compliance hold, invalid transition rejection, idempotent init, saga compensation |
| `flink-sim` (4 checks) | Deduplication of 6 duplicate events, intent score range [0,1], feature_source tag |
| `online-store` (3 checks) | 8-thread concurrent writes, no corruption, all keys present |
| `cart-zset` (7 checks) | Add/rank/expire/merge/clear/CDC snapshot |
| `outbox` (8 checks) | Outbox PENDING→SENT, EventPublisher delivery, T+2 holiday calendar, settlement trigger lifecycle |

To run a single check:

```powershell
python validate_chaos.py --mode local --check cart-zset
python validate_chaos.py --mode local --check outbox
python validate_chaos.py --mode local --check state-machine
```

### K8s Chaos Experiments (requires Chaos Mesh)

```powershell
# Apply all experiments
kubectl apply -f k8s_manifests/chaos-experiments.yaml -n cce

# Stop all experiments
kubectl delete -f k8s_manifests/chaos-experiments.yaml -n cce
```

| Experiment | What it tests | Expected outcome |
|------------|---------------|------------------|
| `redis-brain-split` | Redis master-slave partition (60s) | Sentinel elects new leader in <40s, no dual-write |
| `cce-redis-partition` | CCE pods lose Redis (90s) | Readiness probe returns 503, liveness restarts pod after 30s |
| `cce-pod-kill-prestop-test` | Kill one pod every 3 min | preStop hook drains batch within 40s grace period, no half-written features |
| `cce-kafka-high-latency` | 500ms ±100ms jitter to Kafka (120s) | Flink watermark advances correctly, no window stall |
| `cce-disk-io-delay` | 200ms IO delay on /app/data (60s) | SQLite writes atomic (tmp replace), no JSON corruption |

### preStop hook

The deployment manifest `k8s_manifests/cce-deployment-with-prestop.yaml` adds:

- `preStop` exec hook: POST `/admin/drain` → poll `/health` until `active_batches=0` → allow SIGTERM
- `terminationGracePeriodSeconds: 40` (= preStop 25s + SIGTERM 5s + buffer 10s)
- `/health/live` and `/health/ready` endpoints for K8s liveness/readiness probes
- Pod anti-affinity to spread replicas across nodes (brain-split mitigation)
- `PodDisruptionBudget` with `minAvailable: 1` so chaos experiments never fully disrupt the service

**Why preStop is mandatory for financial batch workloads:**

Without it, K8s sends SIGTERM immediately after removing the pod from Service endpoints. If `process_cdc_events()` is mid-way through `bulk_upsert()`, the online store is left half-written. The next run cannot distinguish which customer features were committed. The preStop hook gives the pod a guaranteed window to finish the current batch before termination.

## Design Notes

This PoC is based on a customer campaign data platform. It separates raw ingestion, standardized identity and feature engineering into Bronze, Silver and Gold layers. The important design point is resolving scattered NRIC, FIN and Passport identifiers into a unified customer key before feature computation, then adding graph-style candidate matching for missing-ID records that need controlled review.

Databricks owns offline customer/policy features, MLflow model runs and drift monitoring. EKS and Redis own the online Feature API, HPA scaling, request-time authorization and low-latency campaign serving. The optional AI vector DB extension adds semantic retrieval over customer features, product/offer documents and similar-customer context for Bedrock/LLM best-offer generation, while Redis remains the deterministic fallback path. This keeps transactional RDS and Databricks workloads isolated from campaign lookup traffic while still giving the models governed features.

