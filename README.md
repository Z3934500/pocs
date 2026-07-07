# Interview PoCs

This folder contains two independent proof-of-concept projects designed for Senior Data Engineer interviews.

## Projects

| PoC | Interview angle | Core story |
| --- | --- | --- |
| `cce-feature-platform` | Databricks ETL, medallion architecture, customer features | Customer Campaign Engine / CDP style feature platform with identity resolution, segmentation, campaign eligibility and API consumption |
| `oee-data-platform` | Industrial equipment analytics, full-stack data engineering | Multi-site OEE data ingestion, schema standardization, data quality, anomaly detection and dashboard |

## Recommended positioning

Lead with `oee-data-platform` for industrial equipment / mining / operations roles. Use `cce-feature-platform` to show enterprise data platform thinking: Bronze -> Silver -> Gold, identity resolution, feature serving and downstream campaign activation. For Kafka / CDC / EKS discussions, use `cce-feature-platform/docs/REALTIME_FEATURE_PLATFORM_480K.md`.

Both PoCs are intentionally compact. They demonstrate the architecture and engineering patterns without claiming to be a full production implementation.
