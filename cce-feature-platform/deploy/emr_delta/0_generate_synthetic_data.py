from __future__ import annotations

from pyspark.sql import functions as F

from config import build_config, build_spark, parse_common_args


ID_TYPES = ["NRIC", "FIN", "PASSPORT"]
CUSTOMER_TYPES = ["DBS_CLIENT", "NEW_TO_INSURANCE", "AFFLUENT", "MASS_MARKET"]
PRODUCTS = ["CARD", "SAVINGS", "INSURANCE", "PREMIUM_FINANCING", "INVESTMENT"]
CHANNELS = ["MOBILE", "POS", "AJO", "BRANCH", "RM"]
POLICY_TYPES = ["INSURANCE", "PREMIUM_FINANCING", "INVESTMENT_LINKED"]
CAMPAIGN_TYPES = ["INS_NEW", "PF_UPSELL", "CARD_RETENTION", "WEALTH_NEXT_BEST_ACTION"]


def pick(values: list[str], index_col: F.Column) -> F.Column:
    return F.element_at(F.array(*[F.lit(value) for value in values]), F.pmod(index_col, F.lit(len(values))) + F.lit(1))


def with_identity(df, id_col: str = "id"):
    id_value = F.concat(F.lit("ID"), F.lpad(F.col(id_col).cast("string"), 10, "0"))
    return (
        df.withColumn("id_type", pick(ID_TYPES, F.col(id_col)))
        .withColumn("id_value", id_value)
        .withColumn("unified_customer_key", F.concat(F.lit("U"), F.lpad(F.col(id_col).cast("string"), 10, "0")))
    )


def main() -> None:
    args = parse_common_args("Generate synthetic CCE domain data with Spark.")
    config = build_config(args)
    spark = build_spark("cce-generate-synthetic-data")

    customers = with_identity(spark.range(1, config.users + 1).withColumnRenamed("id", "customer_num"), "customer_num")
    customers = (
        customers.withColumn("source_system", F.when(F.pmod("customer_num", F.lit(5)) == 0, "AJO").otherwise("CAS"))
        .withColumn("source_customer_ref", F.concat(F.col("source_system"), F.lit("-"), F.col("customer_num")))
        .withColumn("name", F.concat(F.lit("customer_"), F.col("customer_num")))
        .withColumn("customer_type", pick(CUSTOMER_TYPES, F.col("customer_num")))
        .withColumn("first_seen_date", F.date_sub(F.to_date(F.lit(config.business_date)), F.pmod("customer_num", F.lit(900)).cast("int")))
        .withColumn("phone_hash", F.sha2(F.concat(F.lit("phone:"), F.col("customer_num")), 256))
        .withColumn("email_hash", F.sha2(F.concat(F.lit("email:"), F.col("customer_num")), 256))
        .withColumn("date_of_birth", F.date_sub(F.to_date(F.lit(config.business_date)), (F.lit(18 * 365) + F.pmod("customer_num", F.lit(40 * 365))).cast("int")))
        .withColumn("postal_code", F.lpad(F.pmod("customer_num", F.lit(999999)).cast("string"), 6, "0"))
        .withColumn("event_time", F.current_timestamp())
        .withColumn("business_date", F.lit(config.business_date))
    )

    transactions = spark.range(1, config.transactions + 1).withColumnRenamed("id", "txn_num")
    transactions = transactions.withColumn("customer_num", F.pmod("txn_num", F.lit(config.users)) + F.lit(1))
    transactions = with_identity(transactions, "customer_num")
    transactions = (
        transactions.withColumn("txn_id", F.concat(F.lit("TXN-"), F.lpad(F.col("txn_num").cast("string"), 12, "0")))
        .withColumn("event_id", F.sha2(F.concat(F.lit("transaction:"), F.col("txn_num")), 256))
        .withColumn("event_time", F.date_sub(F.to_date(F.lit(config.business_date)), F.pmod("txn_num", F.lit(365)).cast("int")).cast("timestamp"))
        .withColumn("product", pick(PRODUCTS, F.col("txn_num")))
        .withColumn("channel", pick(CHANNELS, F.col("txn_num")))
        .withColumn(
            "amount",
            F.round(
                F.when(F.pmod("txn_num", F.lit(997)) == 0, F.lit(25000.0))
                .otherwise(F.rand(seed=42) * F.lit(2500.0) + F.lit(10.0)),
                2,
            ),
        )
        .withColumn("is_fraud_label", F.when(F.pmod("txn_num", F.lit(997)) == 0, F.lit(1)).otherwise(F.lit(0)))
        .withColumn("source_sequence", F.col("txn_num"))
        .withColumn("business_date", F.lit(config.business_date))
    )

    policy_count = max(1, config.users // 5)
    policies = spark.range(1, policy_count + 1).withColumnRenamed("id", "policy_num")
    policies = policies.withColumn("customer_num", F.pmod("policy_num", F.lit(config.users)) + F.lit(1))
    policies = with_identity(policies, "customer_num")
    policies = (
        policies.withColumn("policy_id", F.concat(F.lit("POL-"), F.lpad(F.col("policy_num").cast("string"), 10, "0")))
        .withColumn("policy_type", pick(POLICY_TYPES, F.col("policy_num")))
        .withColumn("policy_status", F.when(F.pmod("policy_num", F.lit(11)) == 0, "PENDING_RENEWAL").otherwise("ACTIVE"))
        .withColumn("effective_date", F.date_sub(F.to_date(F.lit(config.business_date)), F.pmod("policy_num", F.lit(1800)).cast("int")))
        .withColumn("premium_amount", F.round(F.rand(seed=13) * F.lit(5000.0) + F.lit(300.0), 2))
        .withColumn("claim_count_12m", F.pmod("policy_num", F.lit(4)).cast("int"))
        .withColumn("renewal_due_days", F.pmod("policy_num", F.lit(180)).cast("int"))
        .withColumn("event_time", F.current_timestamp())
        .withColumn("business_date", F.lit(config.business_date))
    )

    campaign_event_count = max(1, config.transactions // 5)
    campaign_events = spark.range(1, campaign_event_count + 1).withColumnRenamed("id", "event_num")
    campaign_events = campaign_events.withColumn("customer_num", F.pmod("event_num", F.lit(config.users)) + F.lit(1))
    campaign_events = with_identity(campaign_events, "customer_num")
    campaign_events = (
        campaign_events.withColumn("event_id", F.sha2(F.concat(F.lit("campaign:"), F.col("event_num")), 256))
        .withColumn("campaign_id", pick(CAMPAIGN_TYPES, F.col("event_num")))
        .withColumn("event_type", F.when(F.pmod("event_num", F.lit(3)) == 0, "CLICK").when(F.pmod("event_num", F.lit(3)) == 1, "OPEN").otherwise("CONVERT"))
        .withColumn("event_time", F.date_sub(F.to_date(F.lit(config.business_date)), F.pmod("event_num", F.lit(60)).cast("int")).cast("timestamp"))
        .withColumn("source_sequence", F.col("event_num"))
        .withColumn("business_date", F.lit(config.business_date))
    )

    outputs = {
        "customers": customers,
        "transactions": transactions,
        "policies": policies,
        "campaign_events": campaign_events,
    }
    for name, df in outputs.items():
        (
            df.repartition(config.partitions)
            .write.mode("overwrite")
            .partitionBy("business_date")
            .json(config.path("raw", name))
        )
        print(f"Wrote raw {name} to {config.path('raw', name)}")

    spark.stop()


if __name__ == "__main__":
    main()