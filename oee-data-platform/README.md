# OEE Data Platform PoC

Industrial equipment data engineering PoC for Senior Data Engineer interviews.

## What This Shows

- Multi-site machine API ingestion
- JSON schema standardization across Suzhou and Kunshan factories
- Dynamic API data joined with static machine master data
- Bronze -> Silver -> Gold analytical layers
- OEE, Availability, Performance and Quality calculation
- Data quality issue tracking
- Simple anomaly detection for abnormal OEE drops and downtime spikes
- FastAPI service and dashboard
- Docker, Kubernetes and GitHub Actions examples

## Architecture

```text
Factory Web Service APIs + Excel/CSV master data
          |
          v
Bronze raw JSON and machine master landing
          |
          v
Silver standardized machine events and validated dimensions
          |
          v
Gold daily OEE, downtime Pareto and anomaly alerts
          |
          v
FastAPI / dashboard / Tableau or BI tools
```

## Local Run

From this directory:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pip install -e .
$env:PYTHONPATH="src"
python -m oee_platform.pipeline run
python -m uvicorn oee_platform.api:app --host 127.0.0.1 --port 8020
```

Open:

```text
http://127.0.0.1:8020
```

Useful APIs:

```text
GET /api/summary
GET /api/oee/daily
GET /api/oee/machines
GET /api/downtime/pareto
GET /api/anomalies
GET /api/data-quality/issues
```

## Docker Run

```powershell
docker build -t oee-data-platform .
docker run --rm -p 8020:8000 oee-data-platform
```

## Interview Talk Track

> My real OEE project involved Web Service APIs, inconsistent JSON fields across Suzhou and Kunshan, static Excel master data, Python data quality logic, SQL loading and Tableau refresh. This PoC rebuilds the same business problem as a full-stack data platform: ingestion, medallion layers, database model, API service, data quality, anomaly detection and dashboard.
