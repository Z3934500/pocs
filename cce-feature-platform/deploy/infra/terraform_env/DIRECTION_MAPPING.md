# Terraform Direction Mapping

This Terraform stack primarily supports this architecture direction:

```text
02_extensions/01_realtime/docs/REALTIME_FEATURE_PLATFORM_480K.md
```

It provisions the managed runtime infrastructure used by the online feature-serving path:

```text
RDS / source events
  -> Debezium / MSK Connect
  -> MSK Kafka
  -> stream job on Kubernetes
  -> Redis / ElastiCache online feature store
  -> FastAPI Feature API
```

In concrete terms, this folder owns the AWS managed services behind CDC event streaming and low-latency online feature lookup:

```text
MSK Kafka
ElastiCache Redis
environment-specific names, sizing and tags
```

## Secondary Relationships

This stack also supports these docs indirectly:

```text
01_foundation/docs/OPERATIONS_MATURITY_AND_COST.md
02_extensions/02_mlops/docs/ARCHITECTURE_MLOPS_GRAPHML_DEPLOYMENT.md
```

It relates to `01_foundation/docs/OPERATIONS_MATURITY_AND_COST.md` because it defines Dev, Staging and Production sizing, cost tags, apply policy and state separation.

It relates to `02_extensions/02_mlops/docs/ARCHITECTURE_MLOPS_GRAPHML_DEPLOYMENT.md` because Redis and the Feature API are part of the serving layer for model scores, drift outputs and identity-resolved customer features.

## What This Terraform Does Not Cover

This stack is not the main Terraform implementation for these directions:

```text
01_foundation/docs/BIG_DATA_EMR_DELTA_EXTENSION.md
02_extensions/02_mlops/2_1_vector_db/docs/AI_VECTOR_DB_EXTENSION.md
```

The EMR Delta direction would normally need separate Terraform for:

```text
S3 lakehouse buckets
Glue Catalog
EMR Serverless or EMR on EKS
MWAA / Airflow
IAM roles and job permissions
Delta table lifecycle policies
```

The AI Vector direction would normally need separate Terraform for:

```text
OpenSearch Serverless, pgvector/RDS, Milvus or Pinecone connectivity
embedding job runtime
vector index lifecycle
secrets and network policy
retrieval API configuration
```

## Environment Mapping

The environment split in this Terraform stack maps to the real-time platform rollout path:

| Environment | Terraform resource prefix | Purpose |
| --- | --- | --- |
| Dev | `cce-feature-platform-dev` | Low-cost integration testing for stream/API runtime |
| Staging | `cce-feature-platform-staging` | Production-like release validation with smaller capacity |
| Production | `cce-feature-platform-production` | HA managed infrastructure for real-time feature serving |




