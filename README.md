# Data Engineering PoCs

This folder contains two independent proof-of-concept projects that demonstrate end-to-end data platform design and implementation.

## Projects

| PoC | Focus area | Core story |
| --- | --- | --- |
| `cce-feature-platform` | Databricks ETL, medallion architecture, customer features | Customer Campaign Engine / CDP style feature platform with identity resolution, segmentation, campaign eligibility and API consumption |
| `oee-data-platform` | Industrial equipment analytics, full-stack data engineering | Multi-site OEE data ingestion, schema standardization, data quality, anomaly detection and dashboard |

## Project Selection Guide

Use `oee-data-platform` for industrial equipment, mining, manufacturing or operations analytics discussions. Use `cce-feature-platform` for enterprise customer data platform topics: Bronze -> Silver -> Gold modeling, identity resolution, feature serving and downstream campaign activation. For Kafka / CDC / EKS architecture, see `cce-feature-platform/docs/REALTIME_FEATURE_PLATFORM_480K.md`.

Both PoCs are intentionally compact. They prioritize runnable architecture and engineering patterns over pretending to be full production implementations.
