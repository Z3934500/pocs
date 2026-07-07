# CCE Feature Platform: Architecture, MLOps, GraphML and Deployment

This PoC is designed to explain a production-style Customer Campaign Engine feature platform without making the local demo hard to run.

## Target Architecture

```text
CAS / AJO / policy admin / RDS events
  -> Databricks Bronze Delta landing
  -> Silver identity resolution
       - deterministic NRIC / FIN / Passport mapping
       - graph-style candidate matching for missing IDs
  -> Gold offline feature tables
       - customer grain: unified_customer_key + business_date
       - policy grain: policy_id + unified_customer_key + business_date
  -> Databricks MLflow
       - propensity model run metadata
       - customer model scores
       - feature drift metrics
  -> EKS CronJob sync
  -> Redis online feature store
  -> EKS FastAPI Feature API with HPA
  -> AJO / CDP / campaign decisioning
```

## Technology Choices

| Area | Choice | Reason |
| --- | --- | --- |
| Offline ETL and feature engineering | Databricks Spark + Delta | Best fit for Bronze/Silver/Gold joins, T+1 feature computation, lineage and backfills. |
| Feature governance | Unity Catalog / Delta tables | Central place for ownership, column permissions, PII masking and table lineage. |
| Model training and registry | Databricks MLflow | Records feature version, run parameters, metrics, model version and promotion status. |
| Online feature serving | EKS FastAPI + Redis | Low-latency reads, API-level authorization, stable request contracts and HPA-based scaling. |
| Real-time updates | Debezium + MSK + EKS stream job | Keeps RDS isolated while updating intent/velocity features within seconds. |
| Identity resolution | Deterministic rules + GraphML-style similarity | Exact IDs remain authoritative; graph candidates catch same-person records when IDs are missing or inconsistent. |
| Deployment | Docker + Kubernetes manifests | Keeps API, stream worker, batch importer and MLOps monitor independently deployable. |

## Why Not Put Everything on Databricks?

Databricks should own offline features, model training and governance. It should not be the only runtime path for campaign serving.

- Campaign tools need predictable low-latency feature lookup. Redis plus a small Feature API is a better online serving path.
- EKS HPA scales request handling independently from Databricks jobs and SQL warehouses.
- CDC-driven real-time features can update Redis in seconds without waiting for a Databricks batch schedule.
- The Feature API can enforce request-time field filtering, caller scopes, audit logging, model-version routing and graceful fallbacks.
- Separating online serving protects training/ETL jobs from campaign traffic spikes.

The intended split is:

```text
Databricks = offline feature store + model training + lineage + drift monitoring
EKS/Redis  = online feature layer + low-latency serving + autoscaling + API governance
```

## Feature Grain and Indexing

Customer-level features:

```text
gold_customer_features
  primary key: unified_customer_key
  partition: business_date
  optimize: ZORDER / clustering by unified_customer_key
```

Policy-level features:

```text
gold_policy_features
  primary key: policy_id
  foreign key: unified_customer_key
  partition: business_date
  optimize: ZORDER / clustering by policy_id, unified_customer_key
```

Online Redis key pattern:

```text
customer:{unified_customer_key}:features
policy:{policy_id}:features
customer_policy:{unified_customer_key}:{policy_id}:features
```

## Local Code Artifacts

| Path | Purpose |
| --- | --- |
| `src/cce_platform/pipeline.py` | Local Bronze/Silver/Gold pipeline, policy features, model scores and drift outputs. |
| `src/cce_platform/graph_identity.py` | Graph-style identity candidate scoring for same-person matching. |
| `src/cce_platform/mlops.py` | Lightweight propensity scoring and feature drift calculations. |
| `src/cce_platform/api.py` | Feature API, identity candidate API and MLOps drift/model-run API. |
| `deploy/databricks/cce_medallion_job.py` | Spark/Delta/MLflow version of the same architecture. |
| `deploy/k8s/*.yaml` | EKS API, HPA, stream job, Gold-to-Redis importer and MLOps monitor. |

## Local Run

```powershell
cd pocs\cce-feature-platform
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pip install -e .
python -m cce_platform.pipeline run
python -m cce_platform.batch_importer --replace
python -m uvicorn cce_platform.api:app --host 127.0.0.1 --port 8010
```

Useful endpoints:

```text
GET /api/features
GET /api/policies/features
GET /api/identity/candidates
GET /api/mlops/model-runs
GET /api/mlops/drift
GET /api/online-features/U0001
GET /api/lineage
```

## Production Deployment Steps

1. Build and publish the container image:

```powershell
docker build -t <account>.dkr.ecr.ap-southeast-1.amazonaws.com/cce-feature-platform:2026.06 .
docker push <account>.dkr.ecr.ap-southeast-1.amazonaws.com/cce-feature-platform:2026.06
```

2. Provision managed services:

```powershell
cd deploy/terraform
terraform init
terraform plan
terraform apply
```

3. Create Databricks assets:

```text
Unity Catalog schemas:
  cce_bronze
  cce_silver
  cce_gold

Delta tables:
  cce_silver.identity_crosswalk
  cce_silver.identity_candidates
  cce_gold.customer_features
  cce_gold.policy_features
  cce_gold.customer_model_scores
  cce_gold.feature_drift

Workflow:
  deploy/databricks/cce_medallion_job.py
```

4. Deploy CDC and streaming:

```powershell
kubectl apply -n cce-platform -f deploy/k8s/stream-statefulset.yaml
```

MSK Connect uses `deploy/msk/debezium-mysql-connector.json` for RDS MySQL orders and cart events.

5. Deploy online serving:

```powershell
kubectl create namespace cce-platform
kubectl apply -n cce-platform -f deploy/k8s/deployment.yaml
kubectl apply -n cce-platform -f deploy/k8s/service.yaml
kubectl apply -n cce-platform -f deploy/k8s/hpa.yaml
kubectl apply -n cce-platform -f deploy/k8s/batch-importer-cronjob.yaml
kubectl apply -n cce-platform -f deploy/k8s/mlops-monitor-cronjob.yaml
```

6. Monitor:

| Layer | Metrics |
| --- | --- |
| API | p95 latency, error rate, request rate, HPA replicas |
| Redis | memory, evictions, CPU, replication lag |
| MSK / stream | consumer lag, rebalance count, failed events |
| Databricks | job duration, Silver rejects, Gold row counts |
| MLOps | AUC, precision@20, model version, drift severity |

## Interview Talk Track

> I split the platform into offline and online responsibilities. Databricks computes governed customer and policy features, records model runs in MLflow and monitors feature drift. EKS and Redis serve the low-latency online feature API, so campaign tools do not query the warehouse or transactional RDS at request time. Identity resolution starts with deterministic NRIC/FIN/Passport matching, then graph-style similarity candidates catch missing-ID cases for manual review or controlled merge.
