from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

from config import build_config, build_spark, parse_common_args, register_table, write_delta


SCHEMAS = {
    "customers": StructType(
        [
            StructField("customer_num", LongType()),
            StructField("source_system", StringType()),
            StructField("source_customer_ref", StringType()),
            StructField("id_type", StringType()),
            StructField("id_value", StringType()),
            StructField("unified_customer_key", StringType()),
            StructField("name", StringType()),
            StructField("customer_type", StringType()),
            StructField("first_seen_date", StringType()),
            StructField("phone_hash", StringType()),
            StructField("email_hash", StringType()),
            StructField("date_of_birth", StringType()),
            StructField("postal_code", StringType()),
            StructField("event_time", TimestampType()),
            StructField("business_date", StringType()),
        ]
    ),
    "transactions": StructType(
        [
            StructField("txn_num", LongType()),
            StructField("customer_num", LongType()),
            StructField("event_id", StringType()),
            StructField("txn_id", StringType()),
            StructField("id_type", StringType()),
            StructField("id_value", StringType()),
            StructField("unified_customer_key", StringType()),
            StructField("event_time", TimestampType()),
            StructField("product", StringType()),
            StructField("channel", StringType()),
            StructField("amount", DoubleType()),
            StructField("is_fraud_label", IntegerType()),
            StructField("source_sequence", LongType()),
            StructField("business_date", StringType()),
        ]
    ),
    "policies": StructType(
        [
            StructField("policy_num", LongType()),
            StructField("customer_num", LongType()),
            StructField("policy_id", StringType()),
            StructField("id_type", StringType()),
            StructField("id_value", StringType()),
            StructField("unified_customer_key", StringType()),
            StructField("policy_type", StringType()),
            StructField("policy_status", StringType()),
            StructField("effective_date", StringType()),
            StructField("premium_amount", DoubleType()),
            StructField("claim_count_12m", IntegerType()),
            StructField("renewal_due_days", IntegerType()),
            StructField("event_time", TimestampType()),
            StructField("business_date", StringType()),
        ]
    ),
    "campaign_events": StructType(
        [
            StructField("event_num", LongType()),
            StructField("customer_num", LongType()),
            StructField("event_id", StringType()),
            StructField("campaign_id", StringType()),
            StructField("event_type", StringType()),
            StructField("id_type", StringType()),
            StructField("id_value", StringType()),
            StructField("unified_customer_key", StringType()),
            StructField("event_time", TimestampType()),
            StructField("source_sequence", LongType()),
            StructField("business_date", StringType()),
        ]
    ),
}


def add_bronze_metadata(df: DataFrame, source_name: str) -> DataFrame:
    payload_cols = [F.col(column).cast("string") for column in df.columns]
    return (
        df.withColumn("ingest_time", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .withColumn("source_name", F.lit(source_name))
        .withColumn("record_hash", F.sha2(F.concat_ws("||", *payload_cols), 256))
    )


def main() -> None:
    args = parse_common_args("Ingest raw CCE JSON data into Delta Bronze tables.")
    config = build_config(args)
    spark = build_spark("cce-bronze-ingest")

    for source_name, schema in SCHEMAS.items():
        raw_path = config.path("raw", source_name)
        bronze_path = config.path("bronze", source_name)
        df = spark.read.schema(schema).json(raw_path)
        bronze = add_bronze_metadata(df, source_name)
        write_delta(bronze, bronze_path, partition_by=["business_date"])
        register_table(spark, config, f"bronze_{source_name}", bronze_path)
        print(f"Wrote Bronze {source_name} to {bronze_path}")

    spark.stop()


if __name__ == "__main__":
    main()