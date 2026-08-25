from __future__ import annotations

import threading
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .config import settings
from .db import connect
from .metrics import MetricsMiddleware, metrics_response, record_business
from .online_store import LocalOnlineStore, RedisOnlineStore, make_online_store
from .pipeline import run_pipeline

# ---------------------------------------------------------------------------
# Graceful drain state — used by preStop hook
# ---------------------------------------------------------------------------
# _draining=True  → pod is shutting down; reject new CDC/pipeline work
# _draining=False → pod is ready; active batch counter reaches 0 = safe to kill
_draining = False
_active_batches = 0
_drain_lock = threading.Lock()

# Held for the duration of a pipeline run. SQLite allows exactly one writer, so
# a second concurrent rebuild can only wait out busy_timeout and then fail
# halfway through; rejecting it up front with 409 is both faster and honest.
# Per-process is the right granularity: the MVP1 Deployment mounts no shared
# volume, so each pod owns its own SQLite file and has its own single writer.
_pipeline_run_lock = threading.Lock()


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
app.add_middleware(MetricsMiddleware)


def ensure_online_store() -> LocalOnlineStore | RedisOnlineStore:
    """Return the online store, seeding it once if it is still empty.

    The emptiness check is backend-specific: for the local JSON store the file
    either exists or it does not, while for Redis the batch importer (or the
    stream sink) is the writer and this process must not assume ownership of the
    keyspace. A missing key there means the T+1 import has not run yet, which is
    an operational condition, not something to fix by recomputing locally.
    """
    store = make_online_store()
    return store


def rows(query: str, params: tuple = ()) -> list[dict]:
    with connect() as conn:
        return [dict(row) for row in conn.execute(query, params).fetchall()]


@app.get("/metrics", include_in_schema=False)
def metrics():
    return metrics_response()


# ---------------------------------------------------------------------------
# Health endpoints — consumed by K8s liveness / readiness probes
# and the preStop drain script
# ---------------------------------------------------------------------------

@app.get("/health/live", include_in_schema=False)
def health_live() -> dict[str, object]:
    """Liveness probe: returns 200 as long as the process is alive.
    On network-partition chaos tests, if dependencies are unreachable for
    > failureThreshold periods K8s restarts the pod.
    """
    return {"status": "alive"}


@app.get("/health/ready", include_in_schema=False)
def health_ready() -> dict[str, object]:
    """Readiness probe: returns 200 only when the pod is ready to serve traffic.
    Returns 503 while draining (preStop in progress) so K8s removes the pod
    from the Service load balancer before SIGTERM arrives.
    """
    if _draining:
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=503,
            content={"status": "draining", "active_batches": _active_batches},
        )
    return {"status": "ready", "active_batches": _active_batches}


@app.post("/admin/drain", include_in_schema=False)
def admin_drain() -> dict[str, object]:
    """Called by the preStop hook to begin graceful shutdown.
    Sets draining=True so:
      - /health/ready returns 503 (pod removed from LB immediately)
      - new pipeline/CDC requests are rejected with 503
      - preStop script polls /health until active_batches == 0
    """
    global _draining
    with _drain_lock:
        _draining = True
    return {"draining": True, "active_batches": _active_batches}


@app.get("/health", include_in_schema=False)
def health_combined() -> dict[str, object]:
    """Combined health endpoint polled by the preStop shell script.
    The script checks .draining == false to know the pod is safe to terminate.
    """
    return {
        "status": "draining" if _draining else "ok",
        "draining": _draining,
        "active_batches": _active_batches,
        "database": str(settings.sqlite_path),
    }


@app.get("/api/health")
def health() -> dict[str, str]:
    try:
        with connect() as conn:
            conn.execute("SELECT 1")
        return {"status": "ok", "database": str(settings.sqlite_path)}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.post("/api/pipeline/run")
def run_pipeline_api() -> dict[str, object]:
    """Run the medallion pipeline.  Rejected during graceful drain.

    Rejects with 409 if a rebuild is already in flight: SQLite takes a single
    writer, so a second run would block on the write lock and then fail
    mid-rebuild once busy_timeout expires.
    """
    global _active_batches
    if _draining:
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=503,
            content={"error": "pod is draining, retry on another instance"},
        )
    if not _pipeline_run_lock.acquire(blocking=False):
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=409,
            content={"error": "a pipeline run is already in progress on this instance"},
        )
    with _drain_lock:
        _active_batches += 1
    try:
        counts = run_pipeline(reset=True)
        record_business("pipeline_run", "success")
        return {"status": "completed", "counts": counts}
    except Exception:
        record_business("pipeline_run", "failure")
        raise
    finally:
        with _drain_lock:
            _active_batches -= 1
        _pipeline_run_lock.release()


@app.get("/api/summary")
def summary() -> dict[str, object]:
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
    store = ensure_online_store()
    result = store.get(customer_key)
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
    return FileResponse(settings.frontend_dir / "index.html")


static_path = Path(settings.frontend_dir)
if static_path.exists():
    app.mount("/assets", StaticFiles(directory=static_path), name="assets")
