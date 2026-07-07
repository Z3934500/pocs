# CCE Deployment Notes

## Local Docker

```powershell
docker compose up --build
```

Open `http://127.0.0.1:8010`.

## Kubernetes

```powershell
kubectl create namespace data-platform-pocs
kubectl apply -n data-platform-pocs -f deploy/k8s/deployment.yaml
kubectl apply -n data-platform-pocs -f deploy/k8s/service.yaml
kubectl apply -n data-platform-pocs -f deploy/k8s/hpa.yaml
kubectl apply -n data-platform-pocs -f deploy/k8s/stream-statefulset.yaml
kubectl apply -n data-platform-pocs -f deploy/k8s/batch-importer-cronjob.yaml
kubectl apply -n data-platform-pocs -f deploy/k8s/mlops-monitor-cronjob.yaml
```

The Kubernetes files assume an image named:

```text
ghcr.io/<owner>/cce-feature-platform:latest
```

Replace the image before deploying to a real cluster.

## Databricks Discussion Path

Use `deploy/databricks/cce_medallion_job.py` as the enterprise version of the local pipeline. In a real implementation:

- Bronze tables are Delta tables populated from CDC or landing storage.
- Silver applies deterministic identity resolution plus graph-style candidate matching.
- Gold materializes customer and policy feature tables partitioned by `business_date`.
- MLflow records model run metadata, metrics and model version.
- Feature drift is written to a governed Delta table for monitoring.
- FastAPI serves from Redis for low latency; Databricks remains the offline feature and training system.

## Production Discussion Path

- `deploy/terraform` provisions the MSK and ElastiCache shape used in the 480K-user sizing.
- `deploy/msk/debezium-mysql-connector.json` captures RDS MySQL order/cart changes with Debezium.
- `deploy/k8s/stream-statefulset.yaml` represents the stateful real-time feature job with 20Gi RocksDB storage per pod.
- `deploy/k8s/batch-importer-cronjob.yaml` refreshes Redis from Databricks Gold features daily.
- `deploy/k8s/mlops-monitor-cronjob.yaml` represents the scheduled model/drift monitoring job.
- `deploy/k8s/hpa.yaml` scales the Feature API from 2 to 5 pods.

See `../docs/REALTIME_FEATURE_PLATFORM_480K.md` for sizing and `../docs/ARCHITECTURE_MLOPS_GRAPHML_DEPLOYMENT.md` for the Databricks/EKS split, MLOps and GraphML rationale.

## CI/CD Flow

1. Pull request opens.
2. GitHub Actions runs unit tests.
3. Docker image is built.
4. Main branch can push image to GHCR/ECR/ACR.
5. ArgoCD or `kubectl apply` deploys the new image.
