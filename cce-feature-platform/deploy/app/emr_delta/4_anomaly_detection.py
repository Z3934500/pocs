from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F

from config import build_config, build_spark, parse_common_args, register_table, write_delta


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def build_transaction_anomalies(transactions: DataFrame, business_date: str) -> DataFrame:
    product_stats = (
        transactions.groupBy("product")
        .agg(
            F.avg("amount").alias("avg_amount"),
            F.stddev_samp("amount").alias("std_amount"),
            F.expr("percentile_approx(amount, 0.99)").alias("p99_amount"),
        )
        .fillna({"std_amount": 0.0})
    )

    return (
        transactions.join(product_stats, "product", "left")
        .withColumn(
            "amount_z_score",
            F.when(F.col("std_amount") > 0, (F.col("amount") - F.col("avg_amount")) / F.col("std_amount")).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "anomaly_reason",
            F.when(F.col("is_fraud_label") == 1, "synthetic_fraud_label")
            .when(F.col("amount") > F.col("p99_amount"), "above_product_p99")
            .when(F.col("amount_z_score") >= 3.0, "amount_z_score_high")
            .otherwise(F.lit(None)),
        )
        .filter(F.col("anomaly_reason").isNotNull())
        .select(
            "event_id",
            "txn_id",
            "unified_customer_key",
            "product",
            "channel",
            "amount",
            "avg_amount",
            "p99_amount",
            F.round("amount_z_score", 3).alias("amount_z_score"),
            "anomaly_reason",
            "business_date",
            F.current_timestamp().alias("detected_at"),
        )
        .withColumn("anomaly_id", F.sha2(F.concat_ws("||", "event_id", "anomaly_reason"), 256))
    )


def z_score(df: DataFrame, metric_name: str) -> DataFrame:
    stats = df.agg(F.avg(metric_name).alias(f"avg_{metric_name}"), F.stddev_samp(metric_name).alias(f"std_{metric_name}"))
    return (
        df.crossJoin(stats)
        .fillna({f"std_{metric_name}": 0.0})
        .withColumn(
            f"{metric_name}_z_score",
            F.when(
                F.col(f"std_{metric_name}") > 0,
                (F.col(metric_name) - F.col(f"avg_{metric_name}")) / F.col(f"std_{metric_name}"),
            ).otherwise(F.lit(0.0)),
        )
    )


def build_customer_feature_anomalies(customer_features: DataFrame, business_date: str) -> DataFrame:
    scored = z_score(customer_features, "monetary_30d")
    scored = z_score(scored, "tx_count_30d")
    scored = z_score(scored, "velocity_7d")

    return (
        scored.withColumn(
            "anomaly_reason",
            F.when(F.col("risk_score") >= 0.85, "high_risk_score")
            .when(F.col("monetary_30d_z_score") >= 3.0, "monetary_outlier")
            .when(F.col("tx_count_30d_z_score") >= 3.0, "transaction_count_outlier")
            .when(F.col("velocity_7d_z_score") >= 3.0, "velocity_outlier")
            .otherwise(F.lit(None)),
        )
        .filter(F.col("anomaly_reason").isNotNull())
        .select(
            "unified_customer_key",
            "recency_days",
            "tx_count_30d",
            "monetary_30d",
            "product_diversity",
            "velocity_7d",
            "risk_score",
            F.round("monetary_30d_z_score", 3).alias("monetary_30d_z_score"),
            F.round("tx_count_30d_z_score", 3).alias("tx_count_30d_z_score"),
            F.round("velocity_7d_z_score", 3).alias("velocity_7d_z_score"),
            "anomaly_reason",
            F.lit(business_date).alias("business_date"),
            F.current_timestamp().alias("detected_at"),
        )
        .withColumn("anomaly_id", F.sha2(F.concat_ws("||", "unified_customer_key", "anomaly_reason", "business_date"), 256))
    )


def build_feature_drift_metrics(customer_features: DataFrame, business_date: str) -> DataFrame:
    baseline_rows = [
        ("monetary_30d", 900.0),
        ("tx_count_30d", 4.0),
        ("velocity_7d", 2.0),
        ("risk_score", 0.38),
    ]
    spark = customer_features.sparkSession
    baseline = spark.createDataFrame(baseline_rows, ["feature_name", "baseline_mean"])
    current = customer_features.agg(
        F.avg("monetary_30d").alias("monetary_30d"),
        F.avg("tx_count_30d").alias("tx_count_30d"),
        F.avg("velocity_7d").alias("velocity_7d"),
        F.avg("risk_score").alias("risk_score"),
    ).select(
        F.expr("stack(4, 'monetary_30d', monetary_30d, 'tx_count_30d', tx_count_30d, 'velocity_7d', velocity_7d, 'risk_score', risk_score) as (feature_name, current_mean)")
    )

    return (
        baseline.join(current, "feature_name", "left")
        .withColumn("drift_ratio", F.abs(F.col("current_mean") - F.col("baseline_mean")) / F.col("baseline_mean"))
        .withColumn("severity", F.when(F.col("drift_ratio") >= 0.5, "high").when(F.col("drift_ratio") >= 0.25, "medium").otherwise("low"))
        .withColumn("business_date", F.lit(business_date))
        .withColumn("created_at", F.current_timestamp())
    )


def main() -> None:
    args = parse_common_args("Detect transaction and feature anomalies for CCE Gold tables.")
    config = build_config(args)
    spark = build_spark("cce-gold-anomaly-detection")

    transactions = read_delta(spark, config.path("silver", "fact_transaction"))
    customer_features = read_delta(spark, config.path("gold", "customer_features"))

    transaction_anomalies = build_transaction_anomalies(transactions, config.business_date)
    customer_feature_anomalies = build_customer_feature_anomalies(customer_features, config.business_date)
    feature_drift_metrics = build_feature_drift_metrics(customer_features, config.business_date)

    outputs = {
        "transaction_anomalies": transaction_anomalies,
        "customer_feature_anomalies": customer_feature_anomalies,
        "feature_drift_metrics": feature_drift_metrics,
    }
    for table_name, df in outputs.items():
        path = config.path("gold", table_name)
        write_delta(df, path, partition_by=["business_date"])
        register_table(spark, config, f"gold_{table_name}", path)
        print(f"Wrote Gold {table_name} to {path}")

    spark.stop()


if __name__ == "__main__":
    main()