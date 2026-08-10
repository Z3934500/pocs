# CCE Feature Platform Delivery Plan

This plan turns the CCE PoC roadmap into an execution view: how long the work should take, what can be shown quickly, how sprint cadence works, how rollout should be staged and how success is judged.

The dates are relative and should be recalibrated after Sprint 0. They assume that data access, PII handling, pilot-user availability and cloud environment provisioning are not blocked.

## 1. Executive Timeline

| Track | Indicative duration | Main outcome | Rollout scope |
| --- | ---: | --- | --- |
| Fast-win PoC hardening | 1-2 weeks | Runnable demo, validated sample insights, API walkthrough, data-quality and identity review | Pilot stakeholders only |
| MVP1 controlled serving | 6 weeks after fast-win | Containerized Feature API, CI/CD, dev/staging/prod shape, basic monitoring and rollback | One or two controlled campaign journeys, up to 20K active users |
| MVP2 big-data foundation | 10-12 weeks after MVP1 | EMR/Delta or Databricks Bronze/Silver/Gold jobs, orchestration, replay/backfill and batch SLA | 480K-active-user T+1 batch foundation |
| MLOps baseline | 4-6 weeks, can overlap late MVP2 after Gold is stable | Model registry discipline, score versioning, drift thresholds and promotion evidence | Governed model-driven outputs |
| Realtime extension | 6-8 weeks after MVP2, only if low-latency use cases are approved | CDC/MSK or Kafka, stream job, Redis online updates and lag monitoring | Roll out by event family, source table or campaign journey |
| Vector DB pilot | 4-8 weeks after MLOps is stable | Retrieval-aware best-offer or explanation pilot with fallback path | One campaign or one product family |

If the goal is only to reach a credible customer-facing MVP, the first decision point is the first 6-8 weeks: 1-2 weeks of fast-win PoC hardening plus 6 weeks of MVP1 delivery.

## 2. Cost And Effort Estimate

These estimates are planning ranges, not vendor quotes. Hardware cost means incremental cloud or platform run-rate for the CCE scope, assuming existing enterprise networking, IAM and baseline observability are available. People cost should be calculated by multiplying person-days by the local blended day rate.

Planning assumptions:

- Cloud region: AWS `ap-southeast-1` / Singapore, because the deployment examples use Southeast Asia ECR-style paths.
- Billing month: 730 hours.
- Currency: USD, before tax, support plan, enterprise discount, committed-use discount and marketplace uplift. China people-cost benchmark also shows RMB using USD/CNY 7.25 as a planning FX rate.
- Instance prices below are budget placeholders for planning. Replace them with AWS Pricing Calculator output and enterprise contract rates before sign-off.
- Person-day means 8 hours of effective delivery time. Rates are delivery fee benchmarks, not salary rates. Default people-cost planning should use senior China/India offshore scenarios. The RMB 31,900/month China industry average is kept as a floor/reference, not as the default senior delivery rate.
- Databricks can replace EMR in MVP2/MLOps, but Databricks DBU pricing is contract-specific, so the line items below use EMR/managed AWS units as the default estimate.

Pricing references to verify before final approval:

- [AWS EKS pricing](https://aws.amazon.com/eks/pricing/) charges a per-cluster hourly fee and worker nodes are billed separately through EC2/EBS/VPC.
- [AWS EMR pricing](https://aws.amazon.com/emr/pricing/) adds EMR charges to the underlying EC2/EBS cost, or charges EMR Serverless by vCPU, memory and storage used.
- [AWS MSK pricing](https://aws.amazon.com/msk/pricing/) is driven by broker-hours, storage and data transfer.
- [AWS S3 pricing](https://aws.amazon.com/s3/pricing/) is driven by storage class, stored GB-months and request volume.

### 2.1 Unit Price Assumptions

| Cost unit | Planning instance / meter | Planning unit price | Notes |
| --- | --- | ---: | --- |
| EKS control plane | 1 standard-support EKS cluster | USD 0.10/hour, about USD 73/month | Use shared cluster namespaces for MVP1 if possible |
| Small API worker | `t4g.medium`, 2 vCPU / 4 GB | USD 0.04/hour, about USD 29/month | Good for dev/demo or low-traffic staging |
| Standard API worker | `m6i.large`, 2 vCPU / 8 GB | USD 0.12/hour, about USD 88/month | Default planning node for API and utility pods |
| Larger app worker | `m6i.xlarge`, 4 vCPU / 16 GB | USD 0.24/hour, about USD 175/month | Use when API, importer and monitor share a node group |
| EBS gp3 | Block storage | USD 0.10/GB-month | Used for worker volumes, stream state and small persistent stores |
| S3 Standard | Lakehouse/object storage | USD 0.025/GB-month | Request and transfer costs are extra |
| Redis small | `cache.t4g.small` single node | USD 0.04/hour, about USD 29/month | PoC/MVP1 non-HA online store |
| Redis HA | 2-3 x `cache.r6g.large` | USD 0.16/hour/node, about USD 234-350/month for 2-3 nodes | Realtime or production-like online serving |
| MSK broker | 3 x `kafka.m5.large` or equivalent | USD 0.27/hour/broker, about USD 591/month for 3 brokers | Storage, MSK Connect and transfer extra |
| MSK storage | Broker EBS storage | USD 0.12/GB-month | Depends on retention and topic volume |
| Stream worker | 2-4 x `m6i.large` | USD 175-350/month plus EBS | For Kafka Streams/Flink-style feature updates |
| EMR Serverless worker | 4 vCPU / 16-32 GB Spark worker | USD 0.45-0.70/worker-hour | Blended vCPU/memory planning unit; scale by worker-hours |
| MWAA / Airflow | Small managed environment | USD 400-900/month | Depends on scheduler/worker size and runtime hours |
| OpenSearch/vector node | 3 x `r6g.large.search` or managed equivalent | USD 0.20/hour/node, about USD 438/month for 3 nodes | Vector DB can also be OpenSearch Serverless/pgvector/vendor service |
| Bedrock/LLM calls | Prompt + completion/token usage | USD 500-8K/month for pilot guardrail | Highly usage/model dependent |
| Observability buffer | Logs, metrics, traces, dashboards | 10-20% of infra subtotal | Increase if debug logs or high-cardinality metrics are retained |

### 2.2 Delivery Location And Person-Day Rate Card

The earlier USD 700+/day assumption is not the default delivery model. It represents premium regional/onshore consulting or global system-integrator pricing. For a realistic CCE budget, use senior China/India offshore scenarios unless the client requires onshore delivery, regulated-domain premium staffing or a named-vendor rate card.

Location scenarios:

| Scenario | Planning rate | When to use |
| --- | ---: | --- |
| China industry average benchmark | RMB 31,900/person-month, RMB 1,467/person-day, about USD 202/person-day | Floor/reference only, based on the 2025 China industry benchmark provided by the sponsor; too low for a senior data/platform delivery team |
| China senior/offshore delivery | USD 50-80/hour, USD 400-640/person-day | Default China delivery estimate for senior developers, data engineers, DevOps, Spark/ML engineers and architects |
| India senior offshore delivery | USD 30-60/hour, USD 240-480/person-day | Offshore comparison case; typically 20-40% lower than China for comparable senior delivery roles |
| Mixed China/India offshore | USD 320-430/person-day blended | Good planning case when core implementation is offshore but architecture, data ownership and production approval stay closer to the client |
| Premium regional/onshore vendor | USD 700-1,200/person-day | Use only for Singapore/US/onshore consulting, regulated expert review or global SI commercial rate cards |

Role rate card for planning:

| Role | China senior/offshore | India senior offshore | Premium regional/onshore comparison |
| --- | ---: | ---: | ---: |
| Product owner / BA | USD 400/day | USD 240/day | USD 700/day |
| QA / data tester | USD 400/day | USD 240/day | USD 750/day |
| Pilot SME / data owner | USD 400/day | USD 280/day | USD 800/day |
| Backend engineer | USD 440/day | USD 280/day | USD 900/day |
| Data engineer | USD 480/day | USD 320/day | USD 950/day |
| DevOps / platform engineer | USD 520/day | USD 360/day | USD 1,000/day |
| Spark / data platform engineer | USD 560/day | USD 400/day | USD 1,100/day |
| ML engineer / data scientist | USD 560/day | USD 400/day | USD 1,100/day |
| Streaming engineer | USD 560/day | USD 400/day | USD 1,100/day |
| Security / privacy / risk reviewer | USD 560/day | USD 360/day | USD 1,100/day |
| Architect / tech lead | USD 640/day | USD 480/day | USD 1,200/day |

Sourcing guidance:

- Use China senior/offshore rates as the default for China delivery. Do not use the RMB 31,900/month industry average as the delivery rate for senior platform roles.
- Use India senior offshore as a cost comparison, not as an automatic replacement; time zone, language, domain knowledge and data-access constraints can erase nominal rate savings.
- The India column is intentionally lower than the China senior column by roughly 20-40% for most implementation roles.
- Keep architecture, data ownership, privacy review, release approval and production incident ownership close to the client even if implementation is offshore.
- Use premium regional/onshore rates only for short expert bursts, regulated reviews or when the client explicitly requires that commercial model.

### 2.3 Stage Hardware Breakdown

| Phase | Cost item | Sizing / instance | Monthly or one-time estimate |
| --- | --- | --- | ---: |
| Fast-win PoC | Developer runtime | Laptop/local Python/SQLite/JSON fixtures | USD 0 |
| Fast-win PoC | Optional sandbox VM | 1 x `t4g.medium` or `t3.medium`, 80 demo hours | USD 10-50 total |
| Fast-win PoC | Demo storage/logs | Less than 100 GB S3/EBS/logs | USD 10-50 total |
| Fast-win PoC | Short-lived container registry/builds | Existing registry or free-tier-style usage | USD 0-100 total |
| Fast-win PoC | Hardware subtotal | No HA, no production CD | USD 0-500 total |
| MVP1 controlled serving | EKS control plane | 1 shared EKS cluster or namespace allocation | USD 73/month if incremental |
| MVP1 controlled serving | API worker nodes | 2 x `m6i.large`, dev/staging/prod namespaces share capacity | USD 175/month |
| MVP1 controlled serving | EBS | 200-500 GB gp3 for nodes and small runtime data | USD 20-50/month |
| MVP1 controlled serving | Redis/online store | 1-2 x `cache.t4g.small`, non-HA or light HA | USD 30-60/month |
| MVP1 controlled serving | Registry, logs, metrics, network | Container registry, log retention, basic dashboards, NAT/transfer buffer | USD 300-1,500/month |
| MVP1 controlled serving | Dedicated environment uplift | Extra cluster/env isolation if shared platform cannot be used | USD 2K-8K/month |
| MVP1 controlled serving | Hardware subtotal | Shared path vs small dedicated path | USD 1K-4K/month shared, USD 4K-10K/month dedicated |
| MVP2 big-data foundation | Spark compute | EMR Serverless, 4-12 workers, 2-4 hours/day, dev+staging+prod runs | USD 1K-6K/month |
| MVP2 big-data foundation | Orchestration | MWAA small or Databricks workflow equivalent | USD 400-1,500/month |
| MVP2 big-data foundation | Lakehouse storage | 1-5 TB S3/Delta plus requests and table metadata | USD 200-1,000/month |
| MVP2 big-data foundation | API/Redis refresh | `m6i.large` importer plus Redis refresh target | USD 300-2K/month |
| MVP2 big-data foundation | Catalog/governance/secrets | Glue/Unity-style catalog, secrets, KMS and metadata services | USD 200-1K/month |
| MVP2 big-data foundation | Observability and retention | Spark logs, Airflow logs, data-quality metrics and alerts | USD 1K-5K/month |
| MVP2 big-data foundation | Production HA/platform uplift | Separate prod controls, support overhead, backup and larger retention | USD 5K-45K/month |
| MVP2 big-data foundation | Hardware subtotal | Scheduled nonprod/prod jobs vs heavier HA | USD 8K-25K/month, up to USD 60K/month with heavy HA/retention |
| MLOps baseline | Model registry | MLflow/managed registry/workspace equivalent | USD 500-2K/month |
| MLOps baseline | Training/scoring compute | EMR Serverless or `m6i.xlarge`/Spark scheduled jobs | USD 1K-5K/month |
| MLOps baseline | Drift storage and metrics | S3/Delta tables plus metrics/log retention | USD 300-1K/month |
| MLOps baseline | Monitor runtime | CronJob on `m6i.large` node capacity or equivalent | USD 100-500/month |
| MLOps baseline | Review/audit overhead | Model cards, approval evidence storage, dashboards | USD 500-3.5K/month |
| MLOps baseline | Hardware subtotal | Depends on model frequency and history retained | USD 3K-12K/month |
| Realtime extension | MSK brokers | 3 x `kafka.m5.large` or equivalent, 24x7 | USD 600-1.5K/month |
| Realtime extension | MSK storage | 1-5 TB broker storage, retention-dependent | USD 120-600/month |
| Realtime extension | MSK Connect/Debezium | Connector workers, task capacity and offset/config storage | USD 500-2K/month |
| Realtime extension | Stream workers | 2-4 x `m6i.large` plus 100-500 GB RocksDB/EBS | USD 300-1.5K/month |
| Realtime extension | Redis HA | 2-3 x `cache.r6g.large` plus backup/replica overhead | USD 300-1.5K/month |
| Realtime extension | Observability/network/DLQ | Lag dashboards, logs, NAT/data transfer, DLQ storage and alerting | USD 2K-10K/month |
| Realtime extension | Production HA/platform uplift | Multi-AZ hardening, source-system support, replay rehearsal overhead | USD 5K-43K/month |
| Realtime extension | Hardware subtotal | Narrow pilot vs production HA | USD 8K-30K/month, or USD 25K-60K/month for HA production |
| Vector DB pilot | Vector index | 3 x `r6g.large.search` or serverless/vector service equivalent | USD 500-3K/month |
| Vector DB pilot | Embedding batch compute | `m6i.large`/EMR batch jobs for embedding sync | USD 100-800/month |
| Vector DB pilot | Vector storage/snapshots | Index storage, snapshots and source document S3 | USD 100-1K/month |
| Vector DB pilot | LLM/token usage | Bedrock/LLM pilot traffic with rate limits | USD 500-8K/month |
| Vector DB pilot | Orchestration API | 1-2 x `m6i.large` shared EKS capacity plus logs | USD 200-1K/month |
| Vector DB pilot | Evaluation/privacy overhead | Retrieval evaluation records, prompt/version audit evidence | USD 500-6K/month |
| Vector DB pilot | Hardware subtotal | One campaign or product-family pilot | USD 5K-20K/month |

### 2.4 Stage People Breakdown

Each people-cost column is calculated from the role-specific rate card in section 2.2. The China column uses senior/offshore China rates, not the RMB 31,900/month industry-average benchmark.

| Phase | Role day mix | Person-days | China senior/offshore people cost | India senior offshore people cost | Premium regional people cost |
| --- | --- | ---: | ---: | ---: | ---: |
| Fast-win PoC | Data engineer 4d, backend engineer 4d, product/BA 2d, pilot SME 2d, architect 1d | 13d | USD 5.9K | USD 3.9K | USD 11.6K |
| MVP1 controlled serving | Backend 15d, data engineer 12d, DevOps/platform 12d, QA 8d, product/BA 6d, SME 4d, architect 4d | 61d | USD 28.4K | USD 18.8K | USD 55.1K |
| MVP2 big-data foundation | Spark/data platform 30d, data engineer 25d, DevOps/platform 20d, backend 10d, QA/data tester 15d, product owner 10d, data owners 10d, architect 10d | 130d | USD 64.0K | USD 43.6K | USD 124.0K |
| MLOps baseline | ML engineer 15d, data scientist 10d, data engineer 10d, platform engineer 8d, risk/model reviewer 5d, QA 5d | 53d | USD 27.8K | USD 19.1K | USD 54.3K |
| Realtime extension | Streaming engineer 20d, backend 12d, DevOps/platform 15d, source-system owner 8d, QA/data tester 10d, product/BA 5d, architect 5d | 75d | USD 36.7K | USD 25.0K | USD 71.2K |
| Vector DB pilot | ML/AI engineer 15d, backend 8d, data engineer 8d, product/content owner 6d, privacy/risk reviewer 5d, QA 5d, architect 5d | 52d | USD 26.2K | USD 17.6K | USD 50.8K |

### 2.5 Phase Totals For Planning

| Phase | Hardware/cloud | China senior/offshore total | India senior offshore total | Premium regional total |
| --- | ---: | ---: | ---: | ---: |
| Fast-win PoC | USD 0-500 total | USD 5.9K-6.4K | USD 3.9K-4.4K | USD 11.6K-12.1K |
| MVP1 controlled serving | USD 1K-4K/month shared, or USD 4K-10K/month dedicated | USD 30K-36K shared, USD 36K-48K dedicated | USD 21K-27K shared, USD 27K-39K dedicated | USD 57K-75K |
| MVP2 big-data foundation | USD 8K-25K/month, up to USD 60K/month with heavy HA/retention | USD 88K-139K standard, up to USD 244K heavy | USD 68K-119K standard, up to USD 224K heavy | USD 148K-304K |
| MLOps baseline | USD 3K-12K/month | USD 34K-52K | USD 25K-43K | USD 60K-78K |
| Realtime extension | USD 8K-30K/month pilot, or USD 25K-60K/month for HA production | USD 53K-97K pilot, up to USD 157K HA | USD 41K-85K pilot, up to USD 145K HA | USD 87K-191K |
| Vector DB pilot | USD 5K-20K/month | USD 36K-66K | USD 28K-58K | USD 61K-91K |

Interpretation:

- The RMB 31,900/month figure is useful as a China industry average benchmark, but this CCE plan needs senior delivery roles; the China senior/offshore column is therefore the more realistic China build estimate.
- India senior offshore remains meaningfully cheaper than China senior/offshore for people cost, typically around 20-40% lower in this model.
- From MVP2 onward, cloud/platform run-rate becomes large enough that location arbitrage helps less than controlling EMR/Databricks runtime, Redis/MSK sizing, logging volume and retention.
- Premium regional/onshore rates should be treated as a procurement comparison or expert-review scenario, not the default build plan.

Budget checkpoints:

- End of fast-win: confirm whether MVP1 is worth funding and which campaign journey is in scope.
- End of MVP1 Sprint 1: confirm whether shared infrastructure is enough or a dedicated environment is required.
- Before MVP2 production gate: review Spark/Databricks job runtime, storage growth, Redis memory, logs/metrics retention and support ownership.
- Before realtime: approve the fixed run-rate of MSK/Kafka, Redis HA, stream workers and observability.
- Before Vector DB: approve LLM usage guardrails, embedding refresh frequency and retrieval quality review effort.

## 3. Delivery Cadence

| Phase | Cadence | Release rhythm | Decision gate |
| --- | --- | --- | --- |
| Fast-win PoC | Weekly demo loop, faster if data is ready | Manual local/demo refresh | Business confirms value, field semantics, privacy boundary and MVP1 scope |
| MVP1 | 2-week sprint cadence | One staging release candidate per sprint | Staging smoke test, data-quality review, rollback command and stakeholder approval |
| MVP2 | 2-week engineering sprint cadence | Monthly integrated platform release | Lakehouse validation, batch SLA, cost review, ownership matrix and backfill runbook |
| MLOps | 2-week model release candidate cadence when active model work is happening | Monthly model health review | Model metrics, drift thresholds, model card, champion/challenger and rollback target |
| Realtime | 2-week increments | Staging soak before each production pilot | Lag SLO, duplicate handling, replay/DLQ rehearsal and fallback policy |

Standard sprint outputs:

- Sprint plan with environment target, data contracts and acceptance gates.
- Mid-sprint checkpoint for data semantics, blockers and source-system access.
- End-sprint demo with API/dashboard output, release notes and known risks.
- Staging release candidate before production or shadow-mode rollout.

## 4. Fast Wins

These are the early wins that should be visible within the first 1-2 weeks.

| Fast win | What is delivered | Success signal |
| --- | --- | --- |
| Runnable CCE demo | Local Bronze/Silver/Gold pipeline, FastAPI and dashboard | Tests pass, pipeline runs end to end and stakeholders can inspect features |
| Campaign audience proof | INS_NEW and PF_UPSELL eligibility outputs from sample data | Business can review an audience list and explain why users are eligible |
| Identity resolution proof | NRIC/FIN/Passport merge plus graph-style candidate review | Known deterministic matches are merged and ambiguous records are routed for review |
| Data-quality visibility | DQ issue list for unmapped identifiers, policy holders and transactions | DQ issues are visible as release evidence, not hidden application defects |
| MLOps evidence preview | Propensity scores, drift output and model-run metadata endpoints | Reviewers can see why model governance is needed before scale |

Fast-win exit gate:

```text
Local run is reproducible
Pilot users accept the feature semantics
PII/anonymization boundary is agreed
MVP1 data contracts and campaign scope are named
No one mistakes the PoC for production readiness
```

## 5. MVP1 Sprint Plan

Target: 6 weeks after fast-win, or 8 weeks total including fast-win.

| Sprint | Calendar | Delivery | Exit gate |
| --- | --- | --- | --- |
| Sprint 0 | Week 1-2 | Fast-win PoC hardening, pilot walkthrough, data contract draft and MVP1 scope | Business approves MVP1 campaign journey and source fields |
| Sprint 1 | Week 3-4 | CI test/build path, Docker image, Kustomize render, dev deployment and API smoke test | Image builds, tests pass and dev namespace is healthy |
| Sprint 2 | Week 5-6 | Staging release candidate, online-store import, DQ checks, feature freshness checks and release notes | Staging API/DQ checks pass and rollback command is documented |
| Sprint 3 | Week 7-8 | Controlled production or shadow-mode deployment, monitoring dashboard, immutable image tag and rollback rehearsal | Stakeholder approval, rollback target known and production/shadow run is accepted |

MVP1 success criteria:

| Area | Target |
| --- | --- |
| Business | One or two campaign journeys can consume or review CCE feature output |
| Users/scale | Controlled rollout supports up to 20K active users in the agreed scope |
| API | Feature and eligibility endpoints pass smoke tests in staging and controlled production |
| Latency | Initial p95 feature lookup target is under 300 ms for controlled load, then tuned with real traffic |
| Reliability | Error rate stays below 1% during pilot traffic windows |
| Freshness | Gold-to-online feature refresh meets daily or on-demand pilot requirement |
| Governance | DQ issues, identity candidates and model/drift evidence are visible in release review |
| Rollback | Previous image tag or previous GitOps revision can be restored in under 30 minutes |

## 6. MVP2 Sprint Plan

Target: 10-12 weeks after MVP1 once data contracts and platform access are ready.

| Sprint | Delivery | Exit gate |
| --- | --- | --- |
| Sprint 4 | Lakehouse setup, source contract review, Bronze layout and small-data Spark validation | Bronze landing and schema checks pass in dev |
| Sprint 5 | Silver identity and transaction feature engineering in EMR/Delta or Databricks | Identity rules, rejects and row-count checks are reviewed |
| Sprint 6 | Gold customer/policy features, segmentation, campaign eligibility and drift/anomaly outputs | Gold tables match local reference behavior on validation data |
| Sprint 7 | Airflow/MWAA or Databricks workflow orchestration, retries, timeout and validation callbacks | T+1 chain runs in staging with observable task status |
| Sprint 8 | API/Redis refresh integration, capacity sizing and load validation for 480K active users | Online store sizing, API p95 and refresh behavior are accepted |
| Sprint 9 | Replay/backfill rehearsal, production readiness, cost review, runbook and owner/SLA matrix | Production gate is approved or documented blockers are assigned |

MVP2 success criteria:

| Area | Target |
| --- | --- |
| Scale | 480K-active-user feature tables and online serving shape are validated |
| Batch SLA | T+1 pipeline finishes before the agreed business cutoff |
| Data quality | Schema, freshness, volume and row-count gates run after critical jobs |
| Replay | Backfill and replay runbook is rehearsed in staging |
| Operations | Alert routes, owners, rollback target and support handoff are documented |
| Cost | Spark, storage, Redis, observability and orchestration cost drivers are reviewed before production |

## 7. Rollout Model

Rollout should move from evidence to exposure. Do not start with full production traffic.

| Wave | Scope | Success condition |
| --- | --- | --- |
| Wave 0: Local evidence | Deterministic sample data and demo dashboard/API | Demo is reproducible and business agrees the feature semantics |
| Wave 1: Staging validation | Masked or limited production-like data | API smoke, DQ, identity sample and feature freshness checks pass |
| Wave 2: Shadow mode | Offline reports or read-only production-like evaluation | CCE outputs match expected campaign logic without affecting customers |
| Wave 3: Controlled production | One or two campaign journeys | p95 latency, error rate, freshness and rollback target meet MVP1 gate |
| Wave 4: MVP1 expansion | Wider 20K-active-user scope | Stability, support model and cost remain acceptable |
| Wave 5: MVP2 foundation | 480K-active-user T+1 batch platform | SLA, data quality, backfill and ownership gates are production-ready |
| Wave 6: Extensions | MLOps, realtime and Vector DB | Each extension rolls out only after its specific gate is met |

Realtime rollout should be especially narrow: start with one or two event families, then expand by source table or campaign journey after soak testing.

## 8. Definition Of Success

Overall delivery is successful when the platform proves business value and can be operated safely.

Business success:

- At least one campaign journey or audience workflow can use CCE feature output.
- Pilot stakeholders can explain the customer features, eligibility rules and identity outcomes.
- Data owners agree the source contracts and DQ handling are good enough for the next phase.
- Product owners approve whether to proceed to MVP1, MVP2 or a specific extension.

Technical success:

- CI tests pass and deployment artifacts are reproducible.
- Feature API, data-quality, identity and MLOps evidence endpoints are available.
- Feature freshness, p95 latency, error rate and rollback target are known for each release.
- Batch jobs have row-count, freshness, schema and replay/backfill checks.
- Production changes use immutable image tags or GitOps revisions, not `latest`.

Operational success:

- Each critical dataset and service has an owner, alert route and recovery runbook.
- Cost drivers are reviewed before scaling always-on services such as Redis, MSK and observability.
- Production rollout is gated by staging validation and stakeholder approval.
- Rollback is rehearsed before expanding exposure.

## 9. Replanning Triggers

Add one or more sprints if any of these conditions appear:

| Trigger | Likely impact |
| --- | --- |
| PII approval or anonymized pilot data is delayed | Fast-win and MVP1 shift by 1-2 weeks |
| Shared cluster, registry or network access is not ready | MVP1 shifts by 2-4 weeks |
| Source field semantics are disputed | MVP1 scope must narrow until data contracts are signed off |
| CDC or Outbox is not available | Realtime extension moves later or starts with batch-only increments |
| Gold feature definitions are unstable | MLOps and Vector DB should not start production rollout |
| Batch jobs miss T+1 SLA in staging | MVP2 needs capacity, partitioning or compaction work before production |

## 10. What Is Not Promised By The PoC

The PoC proves value and design direction. It does not, by itself, promise:

- Production high availability.
- Fully automated production CD.
- Enterprise IAM, audit and PII controls.
- Full CDC onboarding for every source system.
- 480K-user batch SLA without EMR/Delta or Databricks validation.
- Model or AI output promotion without MLOps gates.

Those items become explicit delivery scope in MVP1, MVP2 or the relevant extension phase.
