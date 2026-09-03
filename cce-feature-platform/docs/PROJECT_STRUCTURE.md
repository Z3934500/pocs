# CCE Feature Platform - Project Structure

## Directory Layout

```
cce-feature-platform/
├── src/cce_platform/              # Application code
│   ├── L0_configuration/          # Configuration layer
│   ├── L0_primitives/             # Primitives layer
│   ├── L0_schema/                 # Schema definitions
│   ├── L1_business_data/          # Business data layer
│   ├── L1_mechanism/              # Mechanism layer
│   ├── L2_olap/                   # OLAP (Analytical)
│   │   ├── realtime_stream_job.py # ✨ Production Stream Job
│   │   ├── batch_importer.py      # Batch Gold → Redis sync
│   │   ├── online_store.py        # Online feature store
│   │   └── pipeline.py            # Medallion pipeline
│   └── L2_oltp/                   # OLTP (Transactional)
│
├── deploy/                        # Deployment configurations
│   ├── app/                       # Application deployment
│   │   ├── k8s/                   # Kubernetes manifests
│   │   │   ├── deployment.yaml
│   │   │   ├── hpa.yaml
│   │   │   └── batch-importer-cronjob.yaml
│   │   └── helm/                  # Helm charts
│   ├── infra/                     # Infrastructure
│   │   ├── terraform/             # AWS resources (MSK, ElastiCache, EKS)
│   │   └── msk/                   # MSK Connect configs
│   └── local/                     # ✨ Local testing environment
│       ├── docker-compose.yml     # Kafka, Redis, Prometheus, Grafana
│       ├── monitoring/            # Monitoring configs
│       │   ├── prometheus.yml
│       │   └── grafana-datasources.yml
│       └── scripts/               # Test scripts
│           ├── setup_kafka.sh
│           ├── send_test_events.sh
│           └── send_test_events.bat
│
├── tests/                         # Test suite
│   ├── unit/                      # ✨ Unit tests
│   ├── integration/               # ✨ Integration tests
│   └── e2e/                       # ✨ End-to-end tests
│
├── docs/                          # Documentation
│   ├── ADR/                       # Architecture Decision Records
│   │   ├── ADR-005-two-phase-autoscaling-strategy.md
│   │   ├── ADR-005-keda-kafka-scaler.yaml
│   │   ├── ADR-005-phase1-monitoring.yaml
│   │   ├── ADR-005-appendix-io-bottleneck-analysis.md
│   │   ├── ADR-005-appendix-redis-scaling-strategy.md
│   │   ├── ADR-005-appendix-distributed-consistency.md
│   │   ├── ADR-005-appendix-backpressure-memory-mgmt.md
│   │   └── ADR-005-appendix-financial-exactly-once.md
│   └── testing/                   # ✨ Testing guides
│       └── TESTING_GUIDE.md
│
├── 01_foundation/                 # PoC and MVP stages
├── 02_extensions/                 # Real-time and MLOps extensions
├── 03_business_insights/          # Business logic
├── chaos_testing/                 # Chaos engineering tests
├── config/                        # Configuration files
├── data/                          # Sample data
└── frontend/                      # UI components
```

## Quick Start

### Local Development & Testing

```bash
# 1. Start local infrastructure
cd deploy/local
docker-compose up -d

# 2. Setup Kafka topics
bash scripts/setup_kafka.sh

# 3. Run Stream Job
cd ../..
export PYTHONPATH=src
python -m cce_platform.L2_olap.realtime_stream_job \
  --kafka-bootstrap localhost:9092 \
  --redis-url redis://localhost:6379

# 4. Send test events
cd deploy/local
bash scripts/send_test_events.sh
```

### Testing

```bash
# Unit tests
pytest tests/unit/

# Integration tests (requires Docker services)
cd deploy/local && docker-compose up -d
pytest tests/integration/

# End-to-end tests
pytest tests/e2e/
```

### Production Deployment

```bash
# Deploy to Kubernetes
kubectl apply -k deploy/app/k8s/overlays/production

# Or using Helm
helm install cce-platform deploy/app/helm/cce-feature-platform \
  --namespace cce-platform \
  --create-namespace
```

## Key Components

### Stream Processing (Real-time)
- **Code**: `src/cce_platform/L2_olap/realtime_stream_job.py`
- **Deploy**: `deploy/app/k8s/stream-statefulset.yaml`
- **Docs**: `docs/ADR/ADR-005-*`

### Batch Processing (T+1)
- **Code**: `src/cce_platform/L2_olap/batch_importer.py`
- **Deploy**: `deploy/app/k8s/batch-importer-cronjob.yaml`
- **Runs**: Daily at 02:10 UTC

### Feature API
- **Code**: `src/cce_platform/L2_olap/api.py`
- **Deploy**: `deploy/app/k8s/deployment.yaml`
- **HPA**: `deploy/app/k8s/hpa.yaml`

## Documentation

- **Architecture**: `docs/ADR/`
- **Testing**: `docs/testing/TESTING_GUIDE.md`
- **Delivery Plan**: `DELIVERY_PLAN.md`
- **Main README**: `README.md`

## Monitoring

- **Prometheus**: http://localhost:9090 (local)
- **Grafana**: http://localhost:3000 (local, admin/admin)
- **Kafka UI**: http://localhost:8080 (local)

## License

[Your License]
