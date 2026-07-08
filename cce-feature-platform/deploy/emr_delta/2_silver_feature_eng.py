from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from config import build_config, build_spark, parse_common_args, register_table, write_delta


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def normalize_identity(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("id_type_norm", F.upper(F.trim("id_type")))
        .withColumn("id_value_norm", F.regexp_replace(F.upper(F.col("id_value")), "[^A-Z0-9]", ""))
        .withColumn("identity_key", F.sha2(F.concat_ws(":", "id_type_norm", "id_value_norm"), 256))
    )


def reject_rows(df: DataFrame, source_name: str, entity_col: str, issue_type: str, condition: F.Column) -> DataFrame:
    return (
        df.filter(condition)
        .select(
            F.lit(source_name).alias("source_name"),
            F.col(entity_col).cast("string").alias("entity_key"),
            F.lit(issue_type).alias("issue_type"),
            F.lit("high").alias("severity"),
            F.col("record_hash"),
            F.col("event_time"),
            F.col("ingest_time"),
            F.col("business_date"),
        )
        .withColumn("reject_id", F.sha2(F.concat_ws("||", "source_name", "entity_key", "issue_type", "record_hash"), 256))
    )


def main() -> None:
    args = parse_common_args("Build Silver identity, transaction, policy and campaign facts.")
    config = build_config(args)
    spark = build_spark("cce-silver-feature-engineering")

    customers = normalize_identity(read_delta(spark, config.path("bronze", "customers")))
    transactions = normalize_identity(read_delta(spark, config.path("bronze", "transactions")))
    policies = normalize_identity(read_delta(spark, config.path("bronze", "policies")))
    campaign_events = normalize_identity(read_delta(spark, config.path("bronze", "campaign_events")))

    customer_rejects = reject_rows(
        customers,
        "customers",
        "source_customer_ref",
        "invalid_identity",
        F.col("unified_customer_key").isNull() | (F.length("id_value_norm") == 0),
    )
    transaction_rejects = reject_rows(
        transactions,
        "transactions",
        "txn_id",
        "invalid_transaction",
        F.col("unified_customer_key").isNull() | F.col("event_id").isNull() | (F.col("amount") <= 0),
    )
    policy_rejects = reject_rows(
        policies,
        "policies",
        "policy_id",
        "invalid_policy",
        F.col("unified_customer_key").isNull() | F.col("policy_id").isNull(),
    )
    campaign_rejects = reject_rows(
        campaign_events,
        "campaign_events",
        "event_id",
        "invalid_campaign_event",
        F.col("unified_customer_key").isNull() | F.col("event_id").isNull(),
    )

    dim_customer = (
        customers.filter(F.col("unified_customer_key").isNotNull() & (F.length("id_value_norm") > 0))
        .dropDuplicates(["unified_customer_key"])
        .select(
            "unified_customer_key",
            "source_customer_ref",
            "source_system",
            "identity_key",
            "id_type_norm",
            "customer_type",
            "name",
            F.to_date("first_seen_date").alias("first_seen_date"),
            "phone_hash",
            "email_hash",
            F.to_date("date_of_birth").alias("date_of_birth"),
            "postal_code",
            "business_date",
            F.current_timestamp().alias("updated_at"),
        )
    )

    fact_transaction = (
        transactions.filter(F.col("unified_customer_key").isNotNull() & F.col("event_id").isNotNull() & (F.col("amount") > 0))
        .dropDuplicates(["event_id"])
        .select(
            "event_id",
            "txn_id",
            "unified_customer_key",
            "identity_key",
            F.col("event_time").alias("txn_ts"),
            F.to_date("event_time").alias("event_date"),
            F.upper("product").alias("product"),
            F.upper("channel").alias("channel"),
            "amount",
            "is_fraud_label",
            "source_sequence",
            "business_date",
            "ingest_time",
        )
    )

    dim_policy = (
        policies.filter(F.col("unified_customer_key").isNotNull() & F.col("policy_id").isNotNull())
        .dropDuplicates(["policy_id"])
        .select(
            "policy_id",
            "unified_customer_key",
            "identity_key",
            F.upper("policy_type").alias("policy_type"),
            F.upper("policy_status").alias("policy_status"),
            F.to_date("effective_date").alias("effective_date"),
            "premium_amount",
            "claim_count_12m",
            "renewal_due_days",
            "business_date",
            F.current_timestamp().alias("updated_at"),
        )
    )

    fact_campaign_event = (
        campaign_events.filter(F.col("unified_customer_key").isNotNull() & F.col("event_id").isNotNull())
        .dropDuplicates(["event_id"])
        .select(
            "event_id",
            "unified_customer_key",
            "identity_key",
            "campaign_id",
            F.upper("event_type").alias("event_type"),
            F.col("event_time").alias("campaign_event_time"),
            "source_sequence",
            "business_date",
            "ingest_time",
        )
    )

    dq_rejects = customer_rejects.unionByName(transaction_rejects).unionByName(policy_rejects).unionByName(campaign_rejects)

    outputs = {
        "dim_customer": (dim_customer, ["business_date"]),
        "fact_transaction": (fact_transaction, ["business_date"]),
        "dim_policy": (dim_policy, ["business_date"]),
        "fact_campaign_event": (fact_campaign_event, ["business_date"]),
        "dq_rejects": (dq_rejects, ["business_date", "source_name"]),
    }
    for table_name, (df, partition_cols) in outputs.items():
        path = config.path("silver", table_name)
        write_delta(df, path, partition_by=partition_cols)
        register_table(spark, config, f"silver_{table_name}", path)
        print(f"Wrote Silver {table_name} to {path}")

    spark.stop()


if __name__ == "__main__":
    main()