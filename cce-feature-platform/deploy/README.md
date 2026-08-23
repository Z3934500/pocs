# CCE Deployment — Infra Layer + Application Layer

The `deploy/` directory is split into two independent layers with separate lifecycles and ownership.

```
deploy/
├── infra/                  ← Infra layer (Terraform-managed, platform/SRE team)
│   ├── terraform/              PoC template: MSK + ElastiCache Redis
│   └── terraform_env/          Multi-env split: dev / staging / production workspaces
│       └── environments/       dev.tfvars / staging.tfvars / production.tfvars examples
│
└── app/                    ← Application layer (CI/CD-managed, dev team)
    ├── k8s/                    Kubernetes manifests (Kustomize base)
    │   ├── flink/              Flink CDC job submission (cce_platform.flink_cdc_pipeline)
    │   └── kustomize/          Overlays: dev / staging / production
    ├── helm/                   Optional Helm chart for the CCE API runtime
    ├── argocd/                 Argo CD Application + ApplicationSet manifests
    ├── cicd/scripts/           Deploy scripts (kubectl, image promote, ops readiness)
    ├── airflow/                Airflow DAG for batch feature pipeline
    ├── databricks/             Databricks medallion job + Delta governance SQL
    └── emr_delta/              EMR/Delta Spark jobs (Bronze → Silver → Gold → Anomaly)
```

## Infra Layer

Terraform provisions the shared cloud resources that the application depends on.
Apply infra changes through a change-approval window, separate from application deploys.

### PoC template (`infra/terraform/`)

Provisions MSK and ElastiCache Redis for the real-time extension:

```powershell
cd deploy/infra/terraform
terraform init
terraform apply
```

### Multi-environment split (`infra/terraform_env/`)

Production-style workspace per environment with distinct sizing:

| Environment | MSK | Redis | Apply policy |
|---|---|---|---|
| Dev | 1 × kafka.t3.small | cache.t4g.micro, no replica | apply_immediately=true |
| Staging | 2 × kafka.t3.small | cache.t4g.small, 1 replica | apply_immediately=true |
| Production | 3 × kafka.m5.large | cache.m6g.large, 1 replica | apply_immediately=false |

```powershell
cd deploy/infra/terraform_env
terraform workspace new dev
terraform apply -var-file environments/dev.tfvars
```

See `infra/terraform_env/README.md` for step-by-step and state strategy.

---

## Application Layer

The application layer is deployed by CI/CD. Infra secrets (MSK brokers, Redis URL) are
injected as Kubernetes Secrets; the application code never touches Terraform state.

### Local Docker

```powershell
docker compose up --build
```

Open `http://127.0.0.1:8010`.

### Kubernetes (Kustomize)

Base manifests are in `app/k8s/`. Apply for a specific environment with overlays:

```powershell
# Dev
kubectl apply -k deploy/app/k8s/kustomize/overlays/dev

# Staging
kubectl apply -k deploy/app/k8s/kustomize/overlays/staging

# Production
kubectl apply -k deploy/app/k8s/kustomize/overlays/production
```

Or apply base manifests directly:

```powershell
kubectl create namespace data-platform-pocs
kubectl apply -n data-platform-pocs -f deploy/app/k8s/deployment.yaml
kubectl apply -n data-platform-pocs -f deploy/app/k8s/service.yaml
kubectl apply -n data-platform-pocs -f deploy/app/k8s/hpa.yaml
kubectl apply -n data-platform-pocs -f deploy/app/k8s/stream-statefulset.yaml
kubectl apply -n data-platform-pocs -f deploy/app/k8s/batch-importer-cronjob.yaml
kubectl apply -n data-platform-pocs -f deploy/app/k8s/mlops-monitor-cronjob.yaml
```

### Flink CDC Pipeline (`app/k8s/flink/`)

The `flink_cdc_pipeline` module provides exactly-once CDC-to-Redis feature updates.
Two run modes — no cluster switch needed for local development.

| Mode | Command | When to use |
|---|---|---|
| Local simulation | `python -m cce_platform.flink_cdc_pipeline run` | Dev / PoC, no Flink cluster needed |
| Cluster submit | `python -m cce_platform.flink_cdc_pipeline submit --kafka-brokers <brokers>` | Staging / Production with MSK |

Submit to Kubernetes as a Job:

```powershell
kubectl apply -n data-platform-pocs -f deploy/app/k8s/flink/flink-cce-job.yaml
```

The Job reads `KAFKA_BROKERS` and `REDIS_URL` from Kubernetes Secrets.
Flink state backend is RocksDB; checkpoints write to S3 via `CCE_CHECKPOINT_DIR`.

Key Flink guarantees (see `src/cce_platform/flink_cdc_pipeline.py`):
- Deduplication: event_id keyed state, 1h TTL
- Watermark: BoundedOutOfOrderness 10 s, late events to DLQ topic
- Windows: SlidingEventTime (1d / 1min slide) for `rt_*_1d`; Tumbling 5min for fraud velocity
- Sink: Redis `MULTI/EXEC` per checkpoint boundary for exactly-once writes

### Helm (optional, `app/helm/`)

```powershell
helm upgrade --install cce-feature-platform deploy/app/helm/cce-feature-platform \
  --namespace data-platform-pocs \
  --set image.tag=<git-sha>
```

### Argo CD (`app/argocd/`)

```powershell
kubectl apply -f deploy/app/argocd/cce-applicationset.yaml
```

ApplicationSet generates dev / staging / production Applications from a single manifest.

### CI/CD Scripts (`app/cicd/scripts/`)

| Script | Purpose |
|---|---|
| `deploy-kubectl.sh / .ps1` | `kubectl apply` with image override |
| `deploy-with-image.sh / .ps1` | Build + push + deploy in one step |
| `promote-image.ps1` | Tag and push a release image to ECR/GHCR |
| `ops_readiness_callback.py` | Post-deploy health check and ops readiness signal |

### Databricks / EMR Delta (`app/databricks/`, `app/emr_delta/`)

Offline Bronze → Silver → Gold feature computation for the 480K-user batch foundation.

```powershell
# EMR Delta (Spark)
spark-submit deploy/app/emr_delta/1_bronze_ingest.py
spark-submit deploy/app/emr_delta/2_silver_feature_eng.py
spark-submit deploy/app/emr_delta/3_gold_segmentation.py

# Databricks
# Upload deploy/app/databricks/cce_medallion_job.py as a Databricks Notebook job.
# Run deploy/app/databricks/cce_delta_maintenance_and_governance.sql for table governance.
```

---

## Layer Boundary Summary

| Concern | Owner | Tool |
|---|---|---|
| MSK cluster, ElastiCache Redis, VPC/SG | Infra / SRE | Terraform |
| K8s namespaces, RBAC, node groups | Infra / SRE | Terraform / cluster bootstrap |
| Application Docker image | Dev team | CI/CD (GitHub Actions / GitLab CI) |
| K8s Deployments, Services, HPA | Dev team | Kustomize / Helm / Argo CD |
| Flink job submission | Dev team | K8s Job (`app/k8s/flink/`) |
| Airflow DAGs, Databricks jobs | Data team | Airflow / Databricks Workflows |

See `docs/REALTIME_FEATURE_PLATFORM_480K.md` for 480K-user sizing detail.
See `docs/ARCHITECTURE_MLOPS_GRAPHML_DEPLOYMENT.md` for Databricks/EKS split and MLOps rationale.
