# PoC CI/CD and Deployment Guide

## Repository CI

Workflow:

```text
.github/workflows/poc-ci.yml
```

What it does for each PoC:

1. Checks out the repository.
2. Installs Python dependencies.
3. Runs unit tests.
4. Builds the Docker image.

This is enough for an interview-grade CI pipeline because it proves the data pipeline code and the deployable container both work.

## Local Development

CCE:

```powershell
cd pocs\cce-feature-platform
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pip install -e .
$env:PYTHONPATH="src"
python -m cce_platform.pipeline run
python -m uvicorn cce_platform.api:app --host 127.0.0.1 --port 8010
```

OEE:

```powershell
cd pocs\oee-data-platform
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pip install -e .
$env:PYTHONPATH="src"
python -m oee_platform.pipeline run
python -m uvicorn oee_platform.api:app --host 127.0.0.1 --port 8020
```

## Docker

CCE:

```powershell
cd pocs\cce-feature-platform
docker compose up --build
```

OEE:

```powershell
cd pocs\oee-data-platform
docker compose up --build
```

## Kubernetes

Update image names in:

```text
pocs/cce-feature-platform/deploy/k8s/deployment.yaml
pocs/oee-data-platform/deploy/k8s/deployment.yaml
```

Then deploy:

```powershell
kubectl create namespace interview-pocs
kubectl apply -n interview-pocs -f pocs/cce-feature-platform/deploy/k8s
kubectl apply -n interview-pocs -f pocs/oee-data-platform/deploy/k8s
```

## Production Upgrade Path

For a real company implementation, the natural upgrade path is:

| PoC component | Production equivalent |
| --- | --- |
| SQLite | PostgreSQL, SQL Server, Databricks SQL or Snowflake |
| Local JSON files | S3, ADLS, DBFS or Kafka topics |
| Pure Python ETL | Databricks Jobs, Spark, Airflow or MWAA |
| Static frontend | React, Tableau or Power BI |
| Docker Compose | EKS, AKS, ECS or Azure App Service |
| GitHub Actions build | CI pipeline with image scanning and deployment approval |

## Interview Framing

Say this clearly:

> These PoCs are not meant to pretend I built a massive production platform alone. They are compact, runnable demonstrations of how I would design and deliver the same kind of platform end to end: ingestion, data modeling, ETL, API, dashboard, tests, containerization and CI/CD.
