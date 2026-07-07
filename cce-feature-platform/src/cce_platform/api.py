from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .batch_importer import export_gold_features_to_online_store
from .config import settings
from .db import connect, init_schema
from .online_store import LocalOnlineStore
from .pipeline import run_pipeline


app = FastAPI(
    title="CCE Feature Platform PoC",
    version="0.1.0",
    description="Customer feature and campaign eligibility platform with medallion architecture.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def ensure_data() -> None:
    if not settings.sqlite_path.exists():
        run_pipeline(reset=True)
        return
    with connect() as conn:
        init_schema(conn)
        feature_count = conn.execute("SELECT COUNT(*) FROM gold_customer_features").fetchone()[0]
        policy_feature_count = conn.execute("SELECT COUNT(*) FROM gold_policy_features").fetchone()[0]
        model_run_count = conn.execute("SELECT COUNT(*) FROM ml_model_runs").fetchone()[0]
    if feature_count == 0 or policy_feature_count == 0 or model_run_count == 0:
        run_pipeline(reset=True)


def ensure_online_store() -> None:
    ensure_data()
    if not settings.online_store_path.exists():
        export_gold_features_to_online_store(replace=True)


def rows(query: str, params: tuple = ()) -> list[dict]:
    ensure_data()
    with connect() as conn:
        return [dict(row) for row in conn.execute(query, params).fetchall()]


@app.get("/api/health")
def health() -> dict[str, str]:
    ensure_data()
    return {"status": "ok", "database": str(settings.sqlite_path)}


@app.post("/api/pipeline/run")
def run_pipeline_api() -> dict[str, object]:
    return {"status": "completed", "counts": run_pipeline(reset=True)}


@app.get("/api/summary")
def summary() -> dict[str, object]:
    ensure_data()
    with connect() as conn:
        total_customers = conn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
        total_policies = conn.execute("SELECT COUNT(*) FROM dim_policy").fetchone()[0]
        total_transactions = conn.execute("SELECT COUNT(*) FROM fact_transaction").fetchone()[0]
        eligible = conn.execute(
            "SELECT COUNT(*) FROM gold_campaign_eligibility WHERE is_eligible = 1"
        ).fetchone()[0]
        dq_issues = conn.execute("SELECT COUNT(*) FROM dq_issues").fetchone()[0]
        identity_candidates = conn.execute("SELECT COUNT(*) FROM silver_identity_candidates").fetchone()[0]
        drift_alerts = conn.execute(
            "SELECT COUNT(*) FROM ml_feature_drift WHERE severity IN ('medium', 'high')"
        ).fetchone()[0]
        segment_rows = [
            dict(row)
            for row in conn.execute(
                """
                SELECT segment_name, COUNT(*) AS customers, ROUND(AVG(monetary_30d), 2) AS avg_monetary
                FROM gold_customer_features
                GROUP BY segment_name
                ORDER BY avg_monetary DESC
                """
            ).fetchall()
        ]
    return {
        "total_customers": total_customers,
        "total_policies": total_policies,
        "total_transactions": total_transactions,
        "eligible_customer_campaign_pairs": eligible,
        "data_quality_issues": dq_issues,
        "identity_candidates": identity_candidates,
        "drift_alerts": drift_alerts,
        "segments": segment_rows,
    }


@app.get("/api/features")
def customer_features(segment: str | None = Query(default=None)) -> list[dict]:
    if segment:
        return rows(
            """
            SELECT c.primary_name, f.*, s.propensity_score, s.risk_band
            FROM gold_customer_features f
            JOIN dim_customer c USING (unified_customer_key)
            LEFT JOIN gold_customer_model_scores s
              ON f.unified_customer_key = s.unified_customer_key
            WHERE f.segment_name = ?
            ORDER BY f.monetary_30d DESC
            """,
            (segment,),
        )
    return rows(
        """
        SELECT c.primary_name, f.*, s.propensity_score, s.risk_band
        FROM gold_customer_features f
        JOIN dim_customer c USING (unified_customer_key)
        LEFT JOIN gold_customer_model_scores s
          ON f.unified_customer_key = s.unified_customer_key
        ORDER BY f.monetary_30d DESC
        """
    )


@app.get("/api/customers/{customer_key}/features")
def feature_lookup(customer_key: str) -> dict:
    result = rows(
        """
        SELECT c.primary_name, c.customer_type, f.*, s.propensity_score, s.risk_band
        FROM gold_customer_features f
        JOIN dim_customer c USING (unified_customer_key)
        LEFT JOIN gold_customer_model_scores s
          ON f.unified_customer_key = s.unified_customer_key
        WHERE f.unified_customer_key = ?
        """,
        (customer_key,),
    )
    if not result:
        raise HTTPException(status_code=404, detail="customer not found")
    return result[0]


@app.get("/api/online-features/{customer_key}")
def online_feature_lookup(customer_key: str) -> dict:
    ensure_online_store()
    result = LocalOnlineStore().get(customer_key)
    if not result:
        raise HTTPException(status_code=404, detail="customer not found")
    return {"unified_customer_key": customer_key, **result}


@app.get("/api/policies/features")
def policy_features() -> list[dict]:
    return rows(
        """
        SELECT c.primary_name, p.*
        FROM gold_policy_features p
        JOIN dim_customer c USING (unified_customer_key)
        ORDER BY p.lapse_risk_score DESC, p.renewal_due_days ASC
        """
    )


@app.get("/api/campaigns/{campaign_id}/eligibility")
def campaign_eligibility(campaign_id: str) -> list[dict]:
    return rows(
        """
        SELECT e.campaign_id, e.unified_customer_key, c.primary_name, f.segment_name,
               f.monetary_30d, e.is_eligible, e.reason
        FROM gold_campaign_eligibility e
        JOIN dim_customer c USING (unified_customer_key)
        JOIN gold_customer_features f USING (unified_customer_key)
        WHERE e.campaign_id = ?
        ORDER BY e.is_eligible DESC, f.monetary_30d DESC
        """,
        (campaign_id.upper(),),
    )


@app.get("/api/data-quality/issues")
def data_quality_issues() -> list[dict]:
    return rows("SELECT * FROM dq_issues ORDER BY severity DESC, created_at DESC")


@app.get("/api/identity/candidates")
def identity_candidates() -> list[dict]:
    return rows(
        """
        SELECT *
        FROM silver_identity_candidates
        ORDER BY match_score DESC, created_at DESC
        """
    )


@app.get("/api/mlops/model-runs")
def model_runs() -> list[dict]:
    return rows("SELECT * FROM ml_model_runs ORDER BY created_at DESC")


@app.get("/api/mlops/drift")
def feature_drift() -> list[dict]:
    return rows(
        """
        SELECT *
        FROM ml_feature_drift
        ORDER BY CASE severity WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                 drift_ratio DESC
        """
    )


@app.get("/api/lineage")
def lineage() -> dict[str, object]:
    return {
        "layers": [
            {"name": "Bronze", "asset": "data/bronze/*.jsonl", "purpose": "raw CAS, AJO and CDC event landing"},
            {"name": "Silver", "asset": "identity_crosswalk, silver_identity_candidates, fact_transaction", "purpose": "deterministic and graph-assisted identity resolution"},
            {"name": "Gold", "asset": "gold_customer_features, gold_policy_features, gold_campaign_eligibility", "purpose": "customer and policy feature serving"},
            {"name": "MLOps", "asset": "gold_customer_model_scores, ml_model_runs, ml_feature_drift", "purpose": "model scoring, registry metadata and drift monitoring"},
        ],
        "identity_resolution": "NRIC / FIN / Passport are normalized first; graph-style similarity candidates catch same-person records with missing deterministic IDs.",
        "serving_strategy": "Databricks owns offline features, training and lineage; EKS/Redis owns low-latency online feature serving and request-time authorization.",
    }


@app.get("/")
def index() -> FileResponse:
    ensure_data()
    return FileResponse(settings.frontend_dir / "index.html")


static_path = Path(settings.frontend_dir)
if static_path.exists():
    app.mount("/assets", StaticFiles(directory=static_path), name="assets")
