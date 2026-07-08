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
- 480K-active-user AWS sizing and deployment notes
- Big-data EMR / Delta extension notes for Spark synthetic data, Airflow orchestration and S3 lakehouse layout

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

From this directory:

```powershell
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

Detailed architecture material:

```text
docs/REALTIME_FEATURE_PLATFORM_480K.md
docs/ARCHITECTURE_MLOPS_GRAPHML_DEPLOYMENT.md
docs/BIG_DATA_EMR_DELTA_EXTENSION.md
```

## Docker Run

```powershell
docker build -t cce-feature-platform .
docker run --rm -p 8010:8000 cce-feature-platform
```

## CI/CD

The repository-level workflow is in:

```text
.github/workflows/poc-ci.yml
```

It installs dependencies, runs tests and builds Docker images for both PoCs.

## Design Narrative

This PoC is based on a customer campaign data platform. It separates raw ingestion, standardized identity and feature engineering into Bronze, Silver and Gold layers. The important design point is resolving scattered NRIC, FIN and Passport identifiers into a unified customer key before feature computation, then adding graph-style candidate matching for missing-ID records that need controlled review.

Databricks owns offline customer/policy features, MLflow model runs and drift monitoring. EKS and Redis own the online Feature API, HPA scaling, request-time authorization and low-latency campaign serving. This keeps transactional RDS and Databricks workloads isolated from campaign lookup traffic while still giving the models governed features.
