from __future__ import annotations

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, SparkSession, functions as F

from config import build_config, build_spark, parse_common_args, register_table, write_delta


FEATURE_COLUMNS = ["recency_days", "tx_count_30d", "monetary_30d", "product_diversity", "velocity_7d", "campaign_clicks_30d"]


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def build_customer_features(transactions: DataFrame, campaign_events: DataFrame, business_date: str) -> DataFrame:
    anchor = transactions.agg(F.max("event_date").alias("anchor_date"))
    tx = transactions.crossJoin(anchor)

    tx_features = (
        tx.filter(F.col("event_date") >= F.date_sub(F.col("anchor_date"), 30))
        .groupBy("unified_customer_key")
        .agg(
            F.max("anchor_date").alias("anchor_date"),
            F.count("*").alias("tx_count_30d"),
            F.round(F.sum("amount"), 2).alias("monetary_30d"),
            F.countDistinct("product").alias("product_diversity"),
            F.max("event_date").alias("last_txn_date"),
            F.sum(F.when(F.col("event_date") >= F.date_sub(F.col("anchor_date"), 7), 1).otherwise(0)).alias("velocity_7d"),
        )
        .withColumn("recency_days", F.datediff("anchor_date", "last_txn_date"))
    )

    campaign_features = (
        campaign_events.withColumn("campaign_event_date", F.to_date("campaign_event_time"))
        .filter(F.col("campaign_event_date") >= F.date_sub(F.to_date(F.lit(business_date)), 30))
        .groupBy("unified_customer_key")
        .agg(
            F.sum(F.when(F.col("event_type") == "CLICK", 1).otherwise(0)).alias("campaign_clicks_30d"),
            F.sum(F.when(F.col("event_type") == "CONVERT", 1).otherwise(0)).alias("campaign_conversions_30d"),
        )
    )

    return (
        tx_features.join(campaign_features, "unified_customer_key", "left")
        .fillna({"campaign_clicks_30d": 0, "campaign_conversions_30d": 0})
        .withColumn(
            "risk_score",
            F.round(
                F.least(
                    F.lit(1.0),
                    F.col("velocity_7d") * F.lit(0.08)
                    + F.col("product_diversity") * F.lit(0.05)
                    + F.col("monetary_30d") / F.lit(10000.0)
                    + F.col("campaign_clicks_30d") * F.lit(0.01),
                ),
                3,
            ),
        )
        .withColumn("business_date", F.lit(business_date))
        .withColumn("updated_at", F.current_timestamp())
    )


def assign_segments(features: DataFrame) -> DataFrame:
    assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="raw_features", handleInvalid="keep")
    scaler = StandardScaler(inputCol="raw_features", outputCol="scaled_features", withStd=True, withMean=True)

    assembled = assembler.transform(features.fillna(0, subset=FEATURE_COLUMNS))
    scaled_model = scaler.fit(assembled)
    scaled = scaled_model.transform(assembled)
    model = KMeans(k=4, seed=42, featuresCol="scaled_features", predictionCol="cluster_id").fit(scaled)

    return (
        model.transform(scaled)
        .drop("raw_features", "scaled_features")
        .withColumn(
            "segment_name",
            F.when((F.col("monetary_30d") >= 3000) & (F.col("velocity_7d") >= 3), "high_value_active")
            .when(F.col("recency_days") <= 7, "recently_active")
            .when(F.col("monetary_30d") >= 1500, "high_value_slow")
            .otherwise("nurture"),
        )
    )


def build_policy_features(policies: DataFrame, business_date: str) -> DataFrame:
    return (
        policies.withColumn("policy_tenure_days", F.datediff(F.to_date(F.lit(business_date)), "effective_date"))
        .withColumn(
            "lapse_risk_score",
            F.round(
                F.least(
                    F.lit(1.0),
                    F.col("claim_count_12m") * F.lit(0.18)
                    + F.greatest(F.lit(0), F.lit(60) - F.col("renewal_due_days")) / F.lit(120.0)
                    + F.col("premium_amount") / F.lit(20000.0),
                ),
                3,
            ),
        )
        .withColumn("business_date", F.lit(business_date))
        .withColumn("updated_at", F.current_timestamp())
    )


def build_campaign_eligibility(customer_segments: DataFrame, policy_features: DataFrame, business_date: str) -> DataFrame:
    insurance_customers = policy_features.filter(F.col("policy_type") == "INSURANCE").select("unified_customer_key").distinct()
    premium_financing_customers = policy_features.filter(F.col("policy_type") == "PREMIUM_FINANCING").select("unified_customer_key").distinct()

    insurance_campaign = (
        customer_segments.join(insurance_customers.withColumn("has_insurance", F.lit(1)), "unified_customer_key", "left")
        .withColumn("campaign_id", F.lit("INS_NEW"))
        .withColumn("is_eligible", (F.col("monetary_30d") >= 300) & F.col("has_insurance").isNull())
        .withColumn("reason", F.when(F.col("is_eligible"), "eligible").otherwise("existing_insurance_or_low_value"))
        .select("campaign_id", "unified_customer_key", "is_eligible", "reason")
    )

    pf_campaign = (
        customer_segments.join(premium_financing_customers.withColumn("has_pf", F.lit(1)), "unified_customer_key", "left")
        .withColumn("campaign_id", F.lit("PF_UPSELL"))
        .withColumn("is_eligible", (F.col("monetary_30d") >= 1500) & F.col("has_pf").isNotNull())
        .withColumn("reason", F.when(F.col("is_eligible"), "eligible").otherwise("missing_pf_or_low_value"))
        .select("campaign_id", "unified_customer_key", "is_eligible", "reason")
    )

    return insurance_campaign.unionByName(pf_campaign).withColumn("business_date", F.lit(business_date)).withColumn("updated_at", F.current_timestamp())


def main() -> None:
    args = parse_common_args("Build Gold customer segmentation and campaign feature tables.")
    config = build_config(args)
    spark = build_spark("cce-gold-segmentation")

    transactions = read_delta(spark, config.path("silver", "fact_transaction"))
    campaign_events = read_delta(spark, config.path("silver", "fact_campaign_event"))
    policies = read_delta(spark, config.path("silver", "dim_policy"))

    customer_features = build_customer_features(transactions, campaign_events, config.business_date)
    customer_segments = assign_segments(customer_features)
    policy_features = build_policy_features(policies, config.business_date)
    campaign_eligibility = build_campaign_eligibility(customer_segments, policy_features, config.business_date)

    outputs = {
        "customer_features": customer_features,
        "customer_segments": customer_segments,
        "policy_features": policy_features,
        "campaign_eligibility": campaign_eligibility,
    }
    for table_name, df in outputs.items():
        path = config.path("gold", table_name)
        write_delta(df, path, partition_by=["business_date"])
        register_table(spark, config, f"gold_{table_name}", path)
        print(f"Wrote Gold {table_name} to {path}")

    spark.stop()


if __name__ == "__main__":
    main()