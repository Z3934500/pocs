from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator


APPLICATION_ID = os.environ.get("EMR_SERVERLESS_APPLICATION_ID", "<emr-serverless-application-id>")
EXECUTION_ROLE_ARN = os.environ.get("EMR_SERVERLESS_EXECUTION_ROLE_ARN", "<emr-serverless-execution-role-arn>")
SCRIPT_BUCKET = os.environ.get("CCE_EMR_SCRIPT_BUCKET", "s3://example-bucket/cce-scripts/emr_delta")
LAKEHOUSE_BASE_PATH = os.environ.get("CCE_LAKEHOUSE_BASE_PATH", "s3://example-bucket/cce-lakehouse")
BUSINESS_DATE = os.environ.get("CCE_BUSINESS_DATE", "{{ ds }}")

DEFAULT_SPARK_SUBMIT_PARAMETERS = " ".join(
    [
        "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--conf spark.sql.adaptive.enabled=true",
        "--packages io.delta:delta-spark_2.12:3.2.0",
    ]
)


def spark_job(script_name: str, extra_args: list[str] | None = None) -> dict:
    args = [
        f"{SCRIPT_BUCKET}/{script_name}",
        "--base-path",
        LAKEHOUSE_BASE_PATH,
        "--business-date",
        BUSINESS_DATE,
    ]
    if extra_args:
        args.extend(extra_args)
    return {
        "sparkSubmit": {
            "entryPoint": args[0],
            "entryPointArguments": args[1:],
            "sparkSubmitParameters": DEFAULT_SPARK_SUBMIT_PARAMETERS,
        }
    }


def emr_task(task_id: str, script_name: str, extra_args: list[str] | None = None) -> EmrServerlessStartJobOperator:
    return EmrServerlessStartJobOperator(
        task_id=task_id,
        application_id=APPLICATION_ID,
        execution_role_arn=EXECUTION_ROLE_ARN,
        job_driver=spark_job(script_name, extra_args),
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"{LAKEHOUSE_BASE_PATH.rstrip('/')}/logs/emr-serverless/"
                }
            }
        },
        wait_for_completion=True,
    )


with DAG(
    dag_id="cce_feature_emr_delta_pipeline",
    description="Run CCE synthetic data, Bronze, Silver, Gold segmentation and anomaly jobs on EMR Serverless.",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(hours=2),
    },
    tags=["cce", "spark", "delta", "emr-serverless"],
) as dag:
    generate = emr_task(
        "generate_synthetic_data",
        "0_generate_synthetic_data.py",
        ["--users", os.environ.get("CCE_SYNTHETIC_USERS", "10000"), "--transactions", os.environ.get("CCE_SYNTHETIC_TRANSACTIONS", "100000")],
    )
    bronze = emr_task("bronze_ingest", "1_bronze_ingest.py")
    silver = emr_task("silver_feature_eng", "2_silver_feature_eng.py")
    segmentation = emr_task("gold_segmentation", "3_gold_segmentation.py")
    anomaly = emr_task("gold_anomaly_detection", "4_anomaly_detection.py")

    generate >> bronze >> silver >> segmentation >> anomaly