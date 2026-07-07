# OEE Deployment Notes

## Local Docker

```powershell
docker compose up --build
```

Open `http://127.0.0.1:8020`.

## Kubernetes

```powershell
kubectl create namespace data-platform-pocs
kubectl apply -n data-platform-pocs -f deploy/k8s/deployment.yaml
kubectl apply -n data-platform-pocs -f deploy/k8s/service.yaml
```

The Kubernetes files assume an image named:

```text
ghcr.io/<owner>/oee-data-platform:latest
```

Replace the image before deploying to a real cluster.

## Production Extension

For a real industrial deployment:

- Replace simulated payloads with factory Web Service API calls.
- Store Bronze in object storage such as S3 or ADLS.
- Run Silver and Gold jobs with Databricks, Spark or Airflow.
- Store Gold tables in PostgreSQL, SQL Server, Databricks SQL or a BI-serving warehouse.
- Connect Tableau, Power BI or a React dashboard to the API or warehouse.
- Send alerts through email, Teams or an incident channel.

## CI/CD Flow

1. Pull request opens.
2. GitHub Actions runs unit tests.
3. Docker image is built.
4. Main branch can push image to GHCR/ECR/ACR.
5. Kubernetes or an App Service deploys the new image.
