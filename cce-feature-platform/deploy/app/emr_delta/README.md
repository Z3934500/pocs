# EMR / Delta Big-Data Jobs

PySpark skeletons for running the CCE Bronze -> Silver -> Gold feature pipeline on EMR Serverless, EMR on EKS, EMR on EC2 or any Spark runtime with Delta Lake support.

These scripts are production-shape references. They do not replace the local `src/cce_platform/pipeline.py`; they show how the same feature-platform logic maps to distributed Spark jobs, S3 storage, Delta tables, Glue Catalog and Airflow/MWAA orchestration.

## Scripts

| Script | Layer | Purpose |
| --- | --- | --- |
| `0_generate_synthetic_data.py` | Raw | Generate domain-specific customer, transaction, policy and campaign-event data. |
| `1_bronze_ingest.py` | Bronze | Ingest raw JSON, add metadata columns and write Delta Bronze tables. |
| `2_silver_feature_eng.py` | Silver | Normalize identities, filter bad records and build customer, transaction and policy facts. |
| `3_gold_segmentation.py` | Gold | Build customer features and assign value/engagement segments. |
| `4_anomaly_detection.py` | Gold | Flag transaction and customer-feature anomalies for monitoring and risk review. |

## Local Spark Example

```powershell
pip install -r requirements.txt

spark-submit --packages io.delta:delta-spark_2.12:3.2.0 0_generate_synthetic_data.py `
  --base-path file:///tmp/cce-lakehouse `
  --users 10000 `
  --transactions 100000 `
  --business-date 2026-06-20

spark-submit --packages io.delta:delta-spark_2.12:3.2.0 1_bronze_ingest.py --base-path file:///tmp/cce-lakehouse
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 2_silver_feature_eng.py --base-path file:///tmp/cce-lakehouse
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 3_gold_segmentation.py --base-path file:///tmp/cce-lakehouse
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 4_anomaly_detection.py --base-path file:///tmp/cce-lakehouse
```

## EMR Serverless Shape

Upload this folder to S3, then submit each script as a job step with the same `--base-path s3://.../cce-lakehouse` argument. In production, the jobs would be orchestrated by MWAA/Airflow with retries, SLA alerts and data-governance checks between layers.

## Lake Layout

```text
s3://bucket/cce-lakehouse/
  raw/
    customers/
    transactions/
    policies/
    campaign_events/
  bronze/
    customers/
    transactions/
    policies/
    campaign_events/
  silver/
    dim_customer/
    fact_transaction/
    dim_policy/
    dq_rejects/
  gold/
    customer_features/
    customer_segments/
    transaction_anomalies/
    customer_feature_anomalies/
```

## Design Notes

- Keep both `event_time` and `ingest_time`. Event time drives feature windows; ingest time supports freshness and late-arrival monitoring.
- Use `event_id` for idempotent event facts when available. Otherwise use a stable business key plus event time and a source sequence.
- Partition facts by `business_date`; avoid customer-level partitioning because it creates too many small partitions.
- Compact small files after incremental writes.
- Register Delta tables in Glue Catalog or Unity Catalog so Athena, Spark and downstream governance tooling can discover them.