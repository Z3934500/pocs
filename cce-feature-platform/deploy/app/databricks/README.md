# Databricks MVP2 Deployment And Operations Runbook

This runbook belongs to the CCE Foundation MVP2 stage:

```text
cce-feature-platform/01_foundation/03_mvp2_480k_emr_delta/dev_stage_prod/deploy/databricks
```

It covers the Databricks side of the 480K-active-user batch foundation: compute choice, deployment flow, Unity Catalog layout, Delta table optimization, governance policies and how the executable checks from `data-governance-poc` map into a governed Delta lakehouse.

## Scope

Use Databricks for:

- Bronze, Silver and Gold Delta tables.
- Offline customer and policy feature engineering.
- MLflow model-run metadata and feature-drift outputs.
- Unity Catalog ownership, lineage, row filters and column masks.
- Scheduled workflows for T+1 feature generation, backfills and monitoring.

Do not put low-latency campaign serving only on Databricks. The CCE serving split remains:

```text
Databricks = offline features, governance, training, lineage and backfills
EKS/Redis  = online feature lookup, campaign API, HPA scaling and request-time controls
```

## Environment Layout

Use separate catalogs or separate workspaces depending on company policy. The simplest PoC-to-production mapping is:

| Environment | Catalog | Storage root | Purpose |
| --- | --- | --- | --- |
| Dev | `cce_dev` | `s3://.../cce/dev/` | Low-cost integration, synthetic data, schema and policy development. |
| Staging | `cce_staging` | `s3://.../cce/staging/` | Production-like validation, masked data, backfill rehearsal and release sign-off. |
| Production | `cce_prod` | `s3://.../cce/prod/` | Governed feature tables, scheduled runs, audited access and retention policy. |

Recommended schemas:

```text
bronze      raw source-shaped Delta tables
silver      identity-resolved facts, dimensions and DQ rejects
gold        customer features, policy features and eligibility outputs
mlops       model scores, model runs and feature drift
governance  row filters, masks, audit helper functions and policy views
reference   identity bridge, campaign rules and product reference data
```

## Compute Choice

Prefer job compute for scheduled production work. Keep all-purpose clusters for investigation and notebook development only.

| Environment | Recommended compute | Notes |
| --- | --- | --- |
| Dev | Serverless job compute if allowed, otherwise a small job cluster with autoscale 1-2 workers. | Optimize for fast iteration and low idle cost. Spot is acceptable for synthetic runs. |
| Staging | Job cluster with Photon enabled and autoscale 2-6 workers, plus a small/medium SQL warehouse for release validation. | Match production runtime version and access controls; use masked or sampled production-like data. |
| Production | Job clusters per workflow, autoscale by data volume, Photon enabled, cluster policy enforced and owner/cost tags required. | Avoid interactive writes. Use serverless jobs if network/security policy allows it; otherwise use classic job clusters or pools. |

Runtime guidance:

- Use a Databricks Runtime LTS version for stable production jobs.
- Use a runtime that supports the chosen Delta optimization feature. Liquid clustering requires newer runtimes than legacy Z-order paths, and some streaming/liquid combinations require newer DBR versions.
- Keep the CCE job runtime consistent across staging and production. Runtime drift can create subtle differences in Delta, Spark SQL and MLflow behavior.
- Use cluster policies to enforce Unity Catalog, tags, max workers, approved node types, no public DBFS secrets and approved init scripts only.

For the CCE MVP2 scale target, start with this sizing and tune with actual shuffle/input metrics:

| Workload | Initial sizing |
| --- | --- |
| Bronze ingest | 1 driver, 2-4 workers, moderate CPU, autoscale enabled. |
| Silver identity and transaction joins | 1 driver, 4-8 workers, memory-optimized if identity joins are skewed. |
| Gold feature aggregation | 1 driver, 4-12 workers, Photon enabled, partition pruning by `business_date`. |
| Backfill | Separate larger job cluster, 8-16 workers, scheduled outside campaign-serving windows. |
| SQL validation | SQL warehouse, small for dev/staging, medium+ only when release validation needs parallel users. |

## Deployment Flow

Recommended deployment path:

1. Create the Unity Catalog metastore, storage credentials and external locations outside this PoC folder through the platform team's IaC.
2. Create environment catalogs and schemas using the SQL template in `sql/cce_delta_maintenance_and_governance.sql`.
3. Store workspace secrets or secret references for service principals, source storage and downstream export credentials.
4. Package this folder with Databricks Asset Bundles or the Databricks CLI.
5. Deploy the workflow to Dev.
6. Run the medallion job with synthetic or sampled data.
7. Run table-count, freshness, DQ and lineage checks.
8. Promote the same workflow configuration to Staging.
9. Run a staging backfill rehearsal and Redis export rehearsal.
10. Promote to Production after data owner and platform owner sign-off.

Typical bundle commands:

```powershell
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run cce_mvp2_medallion -t dev
```

Production promotion should be tag or release based:

```powershell
databricks bundle validate -t prod
databricks bundle deploy -t prod
databricks bundle run cce_mvp2_medallion -t prod --refresh-all
```

The workflow should run these tasks in order:

| Task | Artifact | Output |
| --- | --- | --- |
| Bronze ingest | `cce_medallion_job.py` or split Bronze notebook/job | Raw source-shaped Delta tables with ingest metadata. |
| Silver feature engineering | `cce_medallion_job.py` or split Silver task | Identity crosswalk, candidate matches, cleaned transactions, cleaned policies and DQ rejects. |
| Gold features | `cce_medallion_job.py` or split Gold task | Customer features, policy features, campaign eligibility, model scores and drift rows. |
| Maintenance | `sql/cce_delta_maintenance_and_governance.sql` | OPTIMIZE, liquid clustering or Z-order, VACUUM dry run and retention controls. |
| Governance check | Ported `data-governance-poc` checks or SQL assertions | Schema, freshness, volume, duplicate and reconciliation evidence. |
| Redis export handoff | Existing EKS CronJob path or Databricks export task | Gold feature snapshot ready for online serving. |

## Delta Optimization Plan

Use one optimization pattern per table. Do not mix legacy Z-order and liquid clustering on the same table as a casual habit.

| Table | Access pattern | Dev | Staging | Production |
| --- | --- | --- | --- | --- |
| `bronze.*` | Append, replay, source audit | Partition by ingest or business date. Compact only when file count hurts tests. | Daily OPTIMIZE on recent partitions. | Daily or weekly OPTIMIZE on recent partitions; retain enough history for replay. |
| `silver.transactions` | Feature windows, joins by customer and business date | Partition by `business_date`; test skew. | Use liquid clustering or Z-order by `business_date`, `unified_customer_key`. | Prefer liquid clustering for new unpartitioned or lightly partitioned tables; otherwise Z-order recent partitions. |
| `silver.identity_crosswalk` | Point lookup by ID and unified key | Small table; no heavy optimization needed. | OPTIMIZE after material refresh. | OPTIMIZE after refresh; enforce ownership and PII controls. |
| `gold.customer_features` | Campaign audience, Redis export, point lookup | Z-order by `unified_customer_key` if using legacy layout. | Compare Z-order vs liquid clustering on release data. | Prefer liquid clustering by `business_date`, `unified_customer_key` for new tables; legacy tables can keep Z-order. |
| `gold.policy_features` | Retention lists, policy lookup | Z-order by `policy_id`, `unified_customer_key`. | Compare liquid clustering by `business_date`, `policy_id`, `unified_customer_key`. | Keep clustering keys aligned to policy/customer lookup and renewal backfills. |
| `mlops.feature_drift` | Time-series monitoring | Small table; compact weekly. | Cluster by `created_at`, `feature_name` if needed. | Keep retention and audit policy aligned with model governance. |

Maintenance cadence:

| Frequency | Action |
| --- | --- |
| After each successful T+1 run | Run table-count/freshness checks, then optimize only changed Gold partitions or liquid-clustered Gold tables. |
| Daily | Optimize recent `gold.customer_features`, `gold.policy_features` and high-write Silver tables. |
| Weekly | Run `VACUUM ... DRY RUN`, review deleted-file candidates and compact Silver if small files accumulate. |
| Monthly | Run production `VACUUM` after the replay and time-travel retention window is approved. Avoid retention shorter than the platform rollback requirement. |
| Quarterly | Review clustering keys, query history, scan bytes, job duration and table growth. Rebaseline if campaign access patterns change. |

## Liquid Clustering Vs Z-order

Use this decision rule:

- New Unity Catalog managed Delta tables: prefer liquid clustering where the runtime and table design support it.
- Existing partitioned/Z-ordered tables: keep Z-order until a planned migration shows better scan cost and no regression.
- High-cardinality feature lookup tables: liquid clustering can reduce the need for manual partition design.
- Short-lived development tables: do not over-optimize. Measure first.

Migration plan:

1. Dev: create a shadow table with liquid clustering and replay a small business-date range.
2. Staging: run the same feature queries against legacy and liquid tables; compare elapsed time, files scanned and row counts.
3. Production: migrate one Gold table first, keep a fallback view or previous table version and monitor query history for two release cycles.

## Governance Plan

The governance model has two layers:

1. `data-governance-poc` style executable checks prove the data is healthy enough to publish.
2. Unity Catalog row filters and column masks control who can see which rows and fields after publication.

Mapping from `data-governance-poc`:

| Data governance PoC check | Databricks / Delta implementation |
| --- | --- |
| Schema contract | Table constraints, SQL assertions, DLT expectations or workflow validation queries before Gold publish. |
| Event payload contract | Bronze parser rejects and `silver.dq_rejects` tables with source event ID and reason. |
| Freshness | Workflow task checks latest `event_time` and `ingest_time`; alert if stale. |
| Pending lag and publish delay | CDC or source export SLA metrics in Bronze audit tables. |
| Duplicate semantic events | Delta MERGE idempotency and duplicate checks by `event_type`, `aggregate_id`, payload hash and event time. |
| Timestamp validity and clock skew | Silver validation rules; invalid records stay queryable in DQ reject tables. |
| Reconciliation | Source-control totals versus Silver/Gold aggregates by business date and source system. |

Row-block pattern:

- Use row filters to block unauthorized rows at the table level.
- Use column masks to hide PII or sensitive keys for broad BI users.
- Give the Feature API service principal narrow read access to Gold serving views, not broad owner access.
- Keep analysts on governed views or governed tables; avoid path-based access for policy-protected tables.

Examples are in:

```text
sql/cce_delta_maintenance_and_governance.sql
```

## Production Readiness Checklist

Before promoting MVP2 Databricks jobs to production:

- Catalogs, schemas and table owners are defined.
- External locations and storage credentials are managed by the platform team.
- Service principals are separated for deployment, job runtime and read-only serving export.
- Cluster policy enforces runtime, tags, max workers and Unity Catalog.
- Dev/staging/prod jobs use the same task graph and only differ by target variables.
- Gold tables have row-count, freshness, drift and DQ gates.
- Row filters and masks are tested with marketing, data science, compliance and API service identities.
- Maintenance policy is approved: OPTIMIZE cadence, VACUUM retention, backfill windows and rollback window.
- Table lineage is visible from Bronze to Silver to Gold to MLflow/model outputs.
- Redis export handoff is tested without querying Databricks in the user-facing request path.

## Official Documentation References

- Databricks compute and job compute: https://docs.databricks.com/aws/en/compute/
- Databricks Jobs and scheduled workflows: https://docs.databricks.com/aws/en/jobs/
- Databricks Asset Bundles: https://docs.databricks.com/aws/en/dev-tools/bundles/
- Delta OPTIMIZE and ZORDER: https://docs.databricks.com/aws/en/sql/language-manual/delta-optimize
- Delta VACUUM: https://docs.databricks.com/aws/en/sql/language-manual/delta-vacuum
- Liquid clustering: https://docs.databricks.com/aws/en/delta/clustering
- Unity Catalog row filters and column masks: https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks/
- Unity Catalog lineage: https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage
