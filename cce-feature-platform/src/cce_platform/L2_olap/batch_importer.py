"""Load T+1 Gold customer features into the online store.

Data path (production):

    Databricks medallion job → Delta Gold tables → this importer → ElastiCache

The importer is the last hop only. It never computes features: Bronze→Silver→Gold
happens in Databricks (`03_mvp2_480k_emr_delta/.../cce_medallion_job.py`), and
this job reads the finished Gold tables and publishes them to Redis under the
`cce:features:{key}` HASH namespace that the Feature API and the realtime stream
sink share.

Two sources, selected by configuration rather than by silent fallback:

  * DatabricksGoldSource — reads Gold via a SQL warehouse. Used when
    DATABRICKS_HOST / DATABRICKS_TOKEN / DATABRICKS_HTTP_PATH are all set.
  * SQLite Gold source — the local PoC path, where pipeline.py plays the part of
    Databricks and writes Gold into SQLite.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

from ..L1_mechanism import connect, init_schema
from .online_store import make_online_store
from .pipeline import run_pipeline

logger = logging.getLogger(__name__)

# Columns the API serves. The Databricks Gold schema is authoritative in
# production; cluster_id / segment_name exist only in the local pipeline, so the
# Databricks query omits them and those fields are simply absent from the payload
# rather than being invented here.
_SQLITE_GOLD_QUERY = """
    SELECT f.unified_customer_key, recency_days, tx_count_30d, monetary_30d,
           product_diversity, velocity_7d, cluster_id, segment_name,
           risk_score, propensity_score, risk_band, f.updated_at
    FROM gold_customer_features f
    LEFT JOIN gold_customer_model_scores s
      ON f.unified_customer_key = s.unified_customer_key
"""

_DATABRICKS_GOLD_QUERY = """
    SELECT f.unified_customer_key, f.recency_days, f.tx_count_30d, f.monetary_30d,
           f.product_diversity, f.velocity_7d, f.risk_score,
           s.propensity_score, s.risk_band, f.updated_at
    FROM {customer_features} f
    LEFT JOIN {model_scores} s
      ON f.unified_customer_key = s.unified_customer_key
"""


def _fetch_gold_rows_sqlite() -> list[dict]:
    with connect() as conn:
        init_schema(conn)
        feature_count = conn.execute("SELECT COUNT(*) FROM gold_customer_features").fetchone()[0]
        model_score_count = conn.execute("SELECT COUNT(*) FROM gold_customer_model_scores").fetchone()[0]
    if feature_count == 0 or model_score_count == 0:
        run_pipeline()

    with connect() as conn:
        return [dict(row) for row in conn.execute(_SQLITE_GOLD_QUERY).fetchall()]


def _fetch_gold_rows_databricks(
    host: str,
    token: str,
    http_path: str,
    customer_features_table: str,
    model_scores_table: str,
) -> list[dict]:
    """Read Gold features from Databricks over a SQL warehouse connection."""
    try:
        from databricks import sql as databricks_sql  # type: ignore[import]
    except ImportError as exc:
        raise RuntimeError(
            "databricks-sql-connector is not installed but Databricks Gold tables were "
            "configured. Install databricks-sql-connector>=3.0, or unset DATABRICKS_HTTP_PATH "
            "to use the local SQLite Gold source."
        ) from exc

    query = _DATABRICKS_GOLD_QUERY.format(
        customer_features=customer_features_table,
        model_scores=model_scores_table,
    )
    # Not wrapped in try/except: a failure here means the Gold tables could not be
    # read, and publishing a partial or empty batch to Redis would quietly serve
    # stale features to every API replica.
    with databricks_sql.connect(
        server_hostname=host.replace("https://", "").rstrip("/"),
        http_path=http_path,
        access_token=token,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [c[0] for c in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _fetch_gold_rows() -> tuple[str, list[dict]]:
    """Return (source_name, rows), choosing the source from configuration."""
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    if host and token and http_path:
        customer_table = os.getenv("DATABRICKS_GOLD_CUSTOMER_FEATURES_TABLE", "cce.gold.customer_features")
        scores_table = os.getenv("DATABRICKS_GOLD_MODEL_SCORES_TABLE", "cce.gold.customer_model_scores")
        rows = _fetch_gold_rows_databricks(host, token, http_path, customer_table, scores_table)
        return f"databricks:{customer_table}", rows
    return "sqlite:gold_customer_features", _fetch_gold_rows_sqlite()


def export_gold_features_to_online_store(
    store_path: Path | None = None,
    replace: bool = False,
    redis_url: str | None = None,
) -> dict[str, int | str]:
    """Publish T+1 Gold customer features to the online store.

    Writes to Redis when REDIS_URL is available (ElastiCache in production), and
    to the local JSON store otherwise — see online_store.make_online_store for
    the fail-fast rules that stop a deployed environment from degrading silently.
    """
    source, rows = _fetch_gold_rows()

    payloads = {
        row.pop("unified_customer_key"): {
            **row,
            "feature_source": "gold_batch",
        }
        for row in rows
    }
    store = make_online_store(store_path=store_path, redis_url=redis_url)
    upserted = store.bulk_upsert(payloads, replace=replace)
    backend = type(store).__name__
    logger.info(
        "gold export: %d customers from %s into %s (replace=%s)",
        upserted, source, backend, replace,
    )
    return {
        "customers_exported": upserted,
        "source": source,
        "backend": backend,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publish Gold customer features to the online feature store (Redis or local JSON)."
    )
    parser.add_argument("--store-path", type=Path, default=None)
    parser.add_argument("--replace", action="store_true")
    parser.add_argument(
        "--redis-url",
        default=None,
        help="Override REDIS_URL. Omit to use the environment.",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    result = export_gold_features_to_online_store(
        store_path=args.store_path,
        replace=args.replace,
        redis_url=args.redis_url,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
