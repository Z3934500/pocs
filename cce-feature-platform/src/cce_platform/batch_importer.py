from __future__ import annotations

import argparse
import json
from pathlib import Path

from .db import connect, init_schema
from .online_store import LocalOnlineStore
from .pipeline import run_pipeline


def export_gold_features_to_online_store(
    store_path: Path | None = None,
    replace: bool = False,
) -> dict[str, int]:
    """Copy T+1 Gold features into the online store.

    In production this maps to an EKS CronJob loading a Delta/Databricks result
    table into ElastiCache Redis using incremental keys.
    """

    with connect() as conn:
        init_schema(conn)
        feature_count = conn.execute("SELECT COUNT(*) FROM gold_customer_features").fetchone()[0]
        model_score_count = conn.execute("SELECT COUNT(*) FROM gold_customer_model_scores").fetchone()[0]
    if feature_count == 0 or model_score_count == 0:
        run_pipeline(reset=True)

    with connect() as conn:
        rows = [
            dict(row)
            for row in conn.execute(
                """
                SELECT f.unified_customer_key, recency_days, tx_count_30d, monetary_30d,
                       product_diversity, velocity_7d, cluster_id, segment_name,
                       risk_score, propensity_score, risk_band, f.updated_at
                FROM gold_customer_features f
                LEFT JOIN gold_customer_model_scores s
                  ON f.unified_customer_key = s.unified_customer_key
                """
            ).fetchall()
        ]

    payloads = {
        row.pop("unified_customer_key"): {
            **row,
            "feature_source": "gold_batch",
        }
        for row in rows
    }
    upserted = LocalOnlineStore(store_path).bulk_upsert(payloads, replace=replace)
    return {"customers_exported": upserted}


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Gold customer features to the local online feature store.")
    parser.add_argument("--store-path", type=Path, default=None)
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args()
    result = export_gold_features_to_online_store(store_path=args.store_path, replace=args.replace)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
