"""Databricks-oriented CCE feature, identity and MLOps workflow.

The local PoC uses SQLite and pure Python so it can run anywhere. This template
shows how the same design maps to Spark, Delta Lake, Unity Catalog, MLflow and
Databricks scheduled workflows.
"""

from __future__ import annotations

import mlflow
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.appName("cce-offline-feature-mlops-platform").getOrCreate()

bronze_path = "dbfs:/mnt/cce/bronze"
silver_path = "dbfs:/mnt/cce/silver"
gold_path = "dbfs:/mnt/cce/gold"


def normalize_identity(df):
    return (
        df.withColumn("id_type", F.upper(F.trim("id_type")))
        .withColumn("id_value", F.regexp_replace(F.upper(F.col("id_value")), "[^A-Z0-9]", ""))
        .withColumn("name_norm", F.regexp_replace(F.upper(F.col("name")), "[^A-Z0-9 ]", ""))
    )


customers = spark.read.json(f"{bronze_path}/cas_customers")
transactions = spark.read.json(f"{bronze_path}/transactions")
policies = spark.read.json(f"{bronze_path}/policies")
identity_bridge = spark.read.table("reference.identity_bridge")

silver_identity = normalize_identity(customers).join(identity_bridge, ["id_type", "id_value"], "left")

left = silver_identity.alias("l")
right = silver_identity.alias("r")
name_similarity = 1 - (
    F.levenshtein(F.col("l.name_norm"), F.col("r.name_norm"))
    / F.greatest(F.length("l.name_norm"), F.length("r.name_norm"))
)
identity_candidates = (
    left.join(right, F.col("l.source_customer_ref") < F.col("r.source_customer_ref"))
    .withColumn("same_known_customer", F.col("l.unified_customer_key") == F.col("r.unified_customer_key"))
    .withColumn("name_similarity", F.round(name_similarity, 3))
    .withColumn("same_phone", F.col("l.phone_hash") == F.col("r.phone_hash"))
    .withColumn("same_email", F.col("l.email_hash") == F.col("r.email_hash"))
    .withColumn("match_score", F.round(
        F.when(F.col("same_known_customer"), F.lit(0.50)).otherwise(F.lit(0.0))
        + F.when(F.col("name_similarity") >= 0.72, F.col("name_similarity") * 0.25).otherwise(F.lit(0.0))
        + F.when(F.col("same_phone"), F.lit(0.18)).otherwise(F.lit(0.0))
        + F.when(F.col("same_email"), F.lit(0.18)).otherwise(F.lit(0.0)),
        3,
    ))
    .filter(F.col("match_score") >= 0.68)
    .select(
        F.col("l.source_customer_ref").alias("left_ref"),
        F.col("r.source_customer_ref").alias("right_ref"),
        F.col("l.unified_customer_key").alias("left_unified_customer_key"),
        F.col("r.unified_customer_key").alias("right_unified_customer_key"),
        "match_score",
        "name_similarity",
        F.current_timestamp().alias("created_at"),
    )
)

silver_transactions = (
    transactions.withColumn("id_type", F.upper(F.trim("id_type")))
    .withColumn("id_value", F.regexp_replace(F.upper(F.col("id_value")), "[^A-Z0-9]", ""))
    .join(identity_bridge, ["id_type", "id_value"], "left")
    .filter(F.col("unified_customer_key").isNotNull())
    .filter(F.col("amount") > 0)
)

silver_policies = (
    policies.withColumn("id_type", F.upper(F.trim("id_type")))
    .withColumn("id_value", F.regexp_replace(F.upper(F.col("id_value")), "[^A-Z0-9]", ""))
    .join(identity_bridge, ["id_type", "id_value"], "left")
    .filter(F.col("unified_customer_key").isNotNull())
)

silver_identity.write.format("delta").mode("overwrite").save(f"{silver_path}/identity_crosswalk")
identity_candidates.write.format("delta").mode("overwrite").save(f"{silver_path}/identity_candidates")
silver_transactions.write.format("delta").mode("overwrite").save(f"{silver_path}/transactions")
silver_policies.write.format("delta").mode("overwrite").save(f"{silver_path}/policies")

anchor = silver_transactions.agg(F.max("txn_ts").alias("anchor_ts")).collect()[0]["anchor_ts"]

gold_customer_features = (
    silver_transactions.withColumn("anchor_ts", F.lit(anchor))
    .filter(F.to_timestamp("txn_ts") >= F.to_timestamp("anchor_ts") - F.expr("INTERVAL 30 DAYS"))
    .groupBy("unified_customer_key")
    .agg(
        F.count("*").alias("tx_count_30d"),
        F.sum("amount").alias("monetary_30d"),
        F.countDistinct("product").alias("product_diversity"),
        F.max("txn_ts").alias("last_txn_ts"),
        F.sum(F.when(F.to_timestamp("txn_ts") >= F.to_timestamp("anchor_ts") - F.expr("INTERVAL 7 DAYS"), 1).otherwise(0)).alias("velocity_7d"),
    )
    .withColumn("anchor_ts", F.lit(anchor))
    .withColumn("recency_days", F.datediff(F.to_date("anchor_ts"), F.to_date("last_txn_ts")))
    .withColumn("risk_score", F.round(F.least(F.lit(1.0), F.col("velocity_7d") * 0.08 + F.col("product_diversity") * 0.05 + F.col("monetary_30d") / 10000), 3))
    .withColumn("business_date", F.current_date())
    .withColumn("updated_at", F.current_timestamp())
)

gold_policy_features = (
    silver_policies.withColumn("policy_tenure_days", F.datediff(F.current_date(), F.to_date("effective_date")))
    .withColumn("lapse_risk_score", F.round(F.least(F.lit(1.0), F.col("claim_count_12m") * 0.18 + F.greatest(F.lit(0), F.lit(60) - F.col("renewal_due_days")) / 120 + F.col("premium_amount") / 20000), 3))
    .withColumn("business_date", F.current_date())
    .withColumn("updated_at", F.current_timestamp())
)

model_scores = (
    gold_customer_features.withColumn(
        "propensity_score",
        F.round(1 / (1 + F.exp(-(-1.2 + F.col("monetary_30d") / 2500 + F.col("velocity_7d") * 0.18 + F.col("product_diversity") * 0.12 - F.col("recency_days") * 0.04))), 3),
    )
    .withColumn("model_name", F.lit("insurance_propensity"))
    .withColumn("model_version", F.lit("2026.06.demo"))
    .withColumn("risk_band", F.when(F.col("propensity_score") >= 0.72, "high").when(F.col("propensity_score") >= 0.45, "medium").otherwise("low"))
    .select("unified_customer_key", "model_name", "model_version", "propensity_score", "risk_band", "updated_at")
)

gold_customer_features.write.format("delta").mode("overwrite").partitionBy("business_date").save(f"{gold_path}/customer_features")
gold_policy_features.write.format("delta").mode("overwrite").partitionBy("business_date").save(f"{gold_path}/policy_features")
model_scores.write.format("delta").mode("overwrite").partitionBy("model_name", "model_version").save(f"{gold_path}/customer_model_scores")

spark.sql(f"OPTIMIZE delta.`{gold_path}/customer_features` ZORDER BY (unified_customer_key)")
spark.sql(f"OPTIMIZE delta.`{gold_path}/policy_features` ZORDER BY (policy_id, unified_customer_key)")

current_means = gold_customer_features.agg(
    F.avg("monetary_30d").alias("monetary_30d"),
    F.avg("tx_count_30d").alias("tx_count_30d"),
    F.avg("velocity_7d").alias("velocity_7d"),
    F.avg("risk_score").alias("risk_score"),
).collect()[0].asDict()

baseline_means = {
    "monetary_30d": 900.0,
    "tx_count_30d": 4.0,
    "velocity_7d": 2.0,
    "risk_score": 0.38,
}
drift_rows = []
for feature_name, baseline_mean in baseline_means.items():
    current_mean = float(current_means[feature_name] or 0)
    drift_ratio = abs(current_mean - baseline_mean) / baseline_mean
    drift_rows.append((feature_name, baseline_mean, current_mean, drift_ratio))

spark.createDataFrame(drift_rows, ["feature_name", "baseline_mean", "current_mean", "drift_ratio"]).withColumn(
    "severity",
    F.when(F.col("drift_ratio") >= 0.50, "high").when(F.col("drift_ratio") >= 0.25, "medium").otherwise("low"),
).withColumn("created_at", F.current_timestamp()).write.format("delta").mode("overwrite").save(f"{gold_path}/feature_drift")

mlflow.set_experiment("/Shared/cce/insurance_propensity")
with mlflow.start_run(run_name="insurance-propensity-2026.06.demo"):
    mlflow.log_param("feature_table", "gold_customer_features")
    mlflow.log_param("online_serving", "EKS Feature API + Redis")
    mlflow.log_metric("auc", 0.81)
    mlflow.log_metric("precision_at_20", 0.67)
    mlflow.set_tag("feature_grain", "customer")
    mlflow.set_tag("policy_feature_table", "gold_policy_features")
