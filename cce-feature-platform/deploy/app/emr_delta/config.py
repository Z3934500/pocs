from __future__ import annotations

import argparse
import os
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class JobConfig:
    base_path: str
    business_date: str
    users: int
    transactions: int
    partitions: int
    database: str

    def path(self, *parts: str) -> str:
        clean_parts = [part.strip("/") for part in parts if part]
        return self.base_path.rstrip("/") + "/" + "/".join(clean_parts)


def parse_common_args(description: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--base-path",
        default=os.getenv("CCE_LAKEHOUSE_BASE_PATH", "s3://example-bucket/cce-lakehouse"),
        help="S3 or local file URI for the CCE lakehouse root.",
    )
    parser.add_argument(
        "--business-date",
        default=os.getenv("CCE_BUSINESS_DATE", "2026-06-20"),
        help="Business date for partitioned outputs.",
    )
    parser.add_argument("--users", type=int, default=int(os.getenv("CCE_SYNTHETIC_USERS", "10000")))
    parser.add_argument("--transactions", type=int, default=int(os.getenv("CCE_SYNTHETIC_TRANSACTIONS", "100000")))
    parser.add_argument("--partitions", type=int, default=int(os.getenv("CCE_SPARK_PARTITIONS", "16")))
    parser.add_argument("--database", default=os.getenv("CCE_GLUE_DATABASE", "cce_feature_platform"))
    return parser.parse_args()


def build_config(args: argparse.Namespace) -> JobConfig:
    return JobConfig(
        base_path=args.base_path,
        business_date=args.business_date,
        users=args.users,
        transactions=args.transactions,
        partitions=args.partitions,
        database=args.database,
    )


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", os.getenv("CCE_SPARK_SHUFFLE_PARTITIONS", "200"))
        .getOrCreate()
    )


def ensure_database(spark: SparkSession, config: JobConfig) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.database}")


def write_delta(df: DataFrame, path: str, *, partition_by: list[str] | None = None, mode: str = "overwrite") -> None:
    writer = df.write.format("delta").mode(mode).option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def register_table(spark: SparkSession, config: JobConfig, table_name: str, path: str) -> None:
    ensure_database(spark, config)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {config.database}.{table_name} USING DELTA LOCATION '{path}'")