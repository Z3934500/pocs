# Real-Time Feature & Campaign Platform - 480K Active Users

## 1. Architecture

### 1.1 Promotion Stage Architecture

```text
                           ┌────────────────────────────────────────────────────────┐
                           │                       AWS Region                       │
                           │                    ap-southeast-1                      │
┌──────────┐               │                                                        │
│   RDS    │  CDC Binlog   │  ┌──────────┐    ┌────────────┐    ┌───────────────┐  │
│  MySQL   │───────────────│─▶│   MSK    │───▶│ EKS Stream │───▶│ ElastiCache   │  │
│ Orders   │               │  │ Kafka    │    │ StatefulSet│    │ Redis Online  │  │
└──────────┘               │  └────┬─────┘    └─────┬──────┘    └──────┬────────┘  │
                           │       │                │                  │           │
                           │  ┌────▼─────┐     ┌────▼─────┐      ┌─────▼──────┐    │
                           │  │   MSK    │     │ Feature  │      │   Batch    │    │
                           │  │ Connect  │     │   API    │      │ Importer   │    │
                           │  │ Debezium │     │ FastAPI  │      │ CronJob    │    │
                           │  └──────────┘     └──────────┘      └─────┬──────┘    │
                           │                                            │           │
                           │                                      ┌─────▼─────┐     │
                           │                                      │Databricks │     │
                           │                                      │Bronze/    │     │
                           │                                      │Silver/Gold│     │
                           │                                      └───────────┘     │
                           └────────────────────────────────────────────────────────┘
```

### 1.2 Data Flow

```text
RDS MySQL orders/cart tables
  -> Debezium CDC on MSK Connect
  -> MSK topics partitioned by unified_customer_key
  -> EKS stream job updates real-time features
  -> Redis online feature store
  -> FastAPI Feature API for AJO/CDP/POS journey lookup

CAS / EDL / transaction landing
  -> Databricks Bronze raw Delta tables
  -> Silver identity resolution and data quality rules
  -> Gold customer features, segmentation, campaign eligibility
  -> EKS CronJob incrementally syncs Gold features into Redis
```

## 2. 480K User Sizing

### 2.1 Assumptions

| Item | Assumption |
| --- | --- |
| Active users after rollout | 480,000 |
| MVP active users | 20,000 |
| Online features per user | 20 |
| Average feature key/value size | 100 bytes |
| Raw feature payload | 480,000 x 20 x 100B = 960MB |
| Redis sizing multiplier | 3-4x for object overhead, buffers, failover headroom |
| Expected average event rate | 1.44M events/day = 16.7 events/sec |
| Campaign peak event rate | hundreds events/sec |
| API read QPS | 5+ QPS baseline, burst handled by HPA |

### 2.2 Instance Selection

| Component | MVP | Promotion / Production | Reason |
| --- | --- | --- | --- |
| Feature API on EKS | 1 pod, 250m CPU, 512Mi | 2-5 pods, 250m-1 CPU, 512Mi-1Gi | Mostly Redis reads; HPA on CPU and request rate is enough for 5+ QPS. |
| Redis online store | `cache.t3.micro`, no replica | `cache.t4g.medium` or `cache.m6g.large`, 1 primary + 1 replica, Multi-AZ | 1GB logical data needs 3-6.5GB memory after overhead and failover headroom. |
| MSK Kafka | `kafka.t3.small`, 1-2 brokers for PoC | 3 x `kafka.m5.large`, 500GB EBS, 3-6 partitions | Throughput is low, but 3 brokers protect availability and offset/topic replication. |
| Debezium / MSK Connect | 1 worker, `tasks.max=1` | 1 worker MCU, `tasks.max=1` initially | Few source tables; single task preserves simple ordering and operational clarity. |
| Stream job | 1 pod, 0.5 CPU, 1Gi, 10Gi EBS | 2 pods, 1 CPU, 2Gi, 20Gi EBS each | RocksDB state store needs persistent disk; two replicas support partition rebalancing and failover. |
| Databricks ETL | single job cluster | scheduled job cluster or workflow | Batch feature engineering and segmentation remain offloaded from transactional RDS. |
| Batch importer | manual script | EKS CronJob at 02:10 daily | Incrementally refreshes Redis from Gold features without full online rebuild during business hours. |

Partition strategy: hash by `unified_customer_key`. This keeps each customer's event ordering on the same Kafka partition and avoids cross-partition state merges for velocity and intent features.

## 3. Operating Model

The real-time and big-data paths are complementary:

| Path | Responsibility | Runtime | Output |
| --- | --- | --- | --- |
| Big-data / batch | Build the trusted historical baseline | Databricks, Spark, Delta, EMR, Airflow | T+1 features, segments, model scores, anomaly tables |
| Real-time / streaming | Apply low-latency incremental updates | Debezium, MSK, stream job, Redis | intent, velocity and recent activity features |
| Online serving | Serve a combined feature view | Redis, FastAPI, EKS HPA | feature lookup and campaign eligibility |

The big-data path is optimized for correctness, replay and full-history processing. The real-time path is optimized for latency, ordering and incremental updates. Redis and the Feature API are the convergence point.

This document describes a target operating model. Real environments may be less mature: API logic, batch jobs and reporting tables may still be coupled; source systems may expose fields whose business meaning is ambiguous; and some source databases may not support analytical rules such as historical discount calculations. Those gaps should be handled with data contracts, rejected-row handling, reconciliation and an anti-corruption layer between OLTP sources and analytical consumers.

For cost drivers, operational maturity and rollout constraints, see `OPERATIONS_MATURITY_AND_COST.md`.

## 4. Repository Artifacts

| Path | Purpose |
| --- | --- |
| `src/cce_platform/pipeline.py` | Local Bronze -> Silver -> Gold Medallion pipeline. |
| `src/cce_platform/realtime.py` | Local CDC event simulation and real-time feature update. |
| `src/cce_platform/batch_importer.py` | Gold-to-online-store batch importer. |
| `src/cce_platform/online_store.py` | JSON-backed local replacement for Redis. |
| `deploy/databricks/cce_medallion_job.py` | Spark/Delta version of batch feature engineering. |
| `deploy/k8s/deployment.yaml` | Feature API deployment. |
| `deploy/k8s/hpa.yaml` | API autoscaling, 2-5 pods. |
| `deploy/k8s/stream-statefulset.yaml` | Stream processing job with 20Gi RocksDB state volume per pod. |
| `deploy/k8s/batch-importer-cronjob.yaml` | Daily Databricks/Gold-to-Redis refresh job. |
| `deploy/msk/debezium-mysql-connector.json` | MSK Connect Debezium MySQL connector template. |
| `deploy/terraform` | MSK and ElastiCache sizing template. |

## 5. Local POC Step-by-Step

Run from `pocs/cce-feature-platform`.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pip install -e .
$env:PYTHONPATH="src"
```

Build the T+1 batch features:

```powershell
python -m cce_platform.pipeline run
```

Load Gold features into the local online store:

```powershell
python -m cce_platform.batch_importer --replace
```

Seed and process CDC-style events:

```powershell
python -m cce_platform.realtime run
```

Serve the Feature API:

```powershell
python -m uvicorn cce_platform.api:app --host 127.0.0.1 --port 8010
```

Useful APIs:

```text
GET /api/summary
GET /api/features
GET /api/customers/U0001/features
GET /api/online-features/U0001
GET /api/campaigns/INS_NEW/eligibility
GET /api/data-quality/issues
GET /api/lineage
```

## 6. Production Deployment Step-by-Step

### 6.1 Build and Publish Images

```powershell
docker build -t <account>.dkr.ecr.ap-southeast-1.amazonaws.com/cce-feature-platform:2026.06 .
docker push <account>.dkr.ecr.ap-southeast-1.amazonaws.com/cce-feature-platform:2026.06
```

For the production stream job, build a dedicated image containing the Kafka Streams application and set it in `deploy/k8s/stream-statefulset.yaml`.

### 6.2 Provision AWS Managed Services

```powershell
cd deploy/terraform
terraform init
terraform plan `
  -var "private_subnet_ids=[\"subnet-a\",\"subnet-b\"]" `
  -var "msk_security_group_id=sg-msk" `
  -var "redis_security_group_id=sg-redis"
terraform apply
```

Promotion defaults:

- MSK: 3 x `kafka.m5.large`, 500GB EBS.
- Redis: `cache.m6g.large`, 1 primary + 1 replica, Multi-AZ.
- Region: `ap-southeast-1` for Singapore deployment story.

### 6.3 Create Kafka Topics

```powershell
kafka-topics.sh --bootstrap-server <msk-bootstrap> --create --topic cce.rds.orders --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server <msk-bootstrap> --create --topic cce.rds.cart_events --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server <msk-bootstrap> --create --topic schema-history.cce.rds.mysql --partitions 1 --replication-factor 3
```

### 6.4 Deploy Debezium Connector

Use `deploy/msk/debezium-mysql-connector.json` as the MSK Connect connector template.

Important production settings:

- `table.include.list`: `orders` and `cart_events`.
- `tasks.max`: `1` initially for simple ordering and low source volume.
- `message.key.columns`: customer identifier or unified key when available.
- Offset and schema history topics use replication factor 3 outside PoC.

### 6.5 Deploy EKS Workloads

```powershell
kubectl create namespace cce-platform
kubectl apply -n cce-platform -f deploy/k8s/deployment.yaml
kubectl apply -n cce-platform -f deploy/k8s/service.yaml
kubectl apply -n cce-platform -f deploy/k8s/hpa.yaml
kubectl apply -n cce-platform -f deploy/k8s/stream-statefulset.yaml
kubectl apply -n cce-platform -f deploy/k8s/batch-importer-cronjob.yaml
```

Secrets expected by the templates:

```text
cce-feature-platform-secrets
  kafka-bootstrap-servers
  redis-url
  databricks-host
  databricks-token
```

### 6.6 Operate and Monitor

| Area | Metric |
| --- | --- |
| API | p95 latency, request rate, error rate, HPA replica count |
| Redis | used memory, evictions, replication lag, CPU |
| MSK | bytes in/out, under-replicated partitions, consumer lag |
| Debezium | connector state, source lag, restart count |
| Stream job | consumer lag, RocksDB state size, rebalance count, checkpoint/restart time |
| Databricks | job duration, late data count, Silver DQ rejects, Gold row count |

## 6. Design Narrative

Short version:

> The platform separates T+1 batch features from real-time intent features. Databricks or EMR computes Bronze/Silver/Gold features and segmentation, then an EKS CronJob syncs Gold features into Redis. For low-latency updates, Debezium captures MySQL order and cart changes into MSK, and an EKS stream job updates Redis within seconds. The Feature API reads from Redis, so downstream campaign tools do not join against transactional RDS.

Detailed bullets:

- API layer: FastAPI runs on EKS with HPA. Promotion stage uses 2-5 pods, scaling on CPU and `http_requests_per_second`; this comfortably supports 5+ QPS for feature lookups.
- Real-time features: CDC events are keyed by `unified_customer_key`; the stream job keeps RocksDB state on 20Gi EBS and writes intent/velocity features to Redis.
- Batch features: Databricks builds Bronze -> Silver -> Gold features and segmentation daily; the importer syncs incrementally to Redis at off-peak time.
- Cache: Redis `cache.m6g.large` gives 6.5GB memory, enough for about 1GB logical features plus Redis overhead, buffers, and failover headroom.
- CDC: MSK Connect runs Debezium for MySQL binlog capture on orders and cart tables; initial `tasks.max=1` is enough for the estimated 16.7 events/sec average and preserves simple ordering.
- Identity: NRIC/FIN/Passport are normalized in Silver and resolved to `unified_customer_key` before both batch and real-time feature calculation.

## 7. Implementation Mapping

| Resume wording | Concrete implementation to mention |
| --- | --- |
| Built T+1 feature platform on Databricks + Spark using Bronze -> Silver -> Gold layering | `pipeline.py` for local demo and `deploy/databricks/cce_medallion_job.py` for production Spark/Delta mapping. |
| Designed multi-identifier resolution | Silver identity crosswalk maps NRIC/FIN/Passport to `unified_customer_key`. |
| Migrated batch pipeline to Kafka + CDC event streaming | Debezium + MSK captures RDS changes; stream job updates Redis online features. |
| Customer Campaign Engine PM/BA story | Three-layer segmentation and eligibility rules map to Gold features and campaign eligibility output. |
| Real-time segmentation platform | Spark/Gold segmentation provides baseline segments; stream features add current intent and velocity for downstream campaign decisions. |
| All components run on EKS, Terraform, CI images | K8s manifests, HPA, StatefulSet, CronJob and Terraform templates show the deployment path. |

## 9. Scope Statement

This repository is intentionally a compact interview PoC. It demonstrates architecture, data contracts, deployment shape and sizing rationale. In a production implementation, the local JSON online store becomes ElastiCache Redis, the JSONL CDC sample becomes Debezium topics on MSK, and the local Python pipeline maps to Databricks Delta jobs.

## 10. AWS Reference Links

- Amazon ElastiCache supported node types: https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/CacheNodes.SupportedTypes.html
- Amazon MSK broker sizes: https://docs.aws.amazon.com/msk/latest/developerguide/broker-instance-sizes.html
- Amazon MSK Connect capacity: https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect-capacity.html
