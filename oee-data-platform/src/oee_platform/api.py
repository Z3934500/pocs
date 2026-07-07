from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .config import settings
from .db import connect, init_schema
from .pipeline import run_pipeline


app = FastAPI(
    title="OEE Data Platform PoC",
    version="0.1.0",
    description="Industrial OEE data platform with medallion layers, data quality and anomaly detection.",
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
        count = conn.execute("SELECT COUNT(*) FROM fact_oee_daily").fetchone()[0]
    if count == 0:
        run_pipeline(reset=True)


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
        machines = conn.execute("SELECT COUNT(*) FROM dim_machine").fetchone()[0]
        events = conn.execute("SELECT COUNT(*) FROM fact_machine_event").fetchone()[0]
        dq_issues = conn.execute("SELECT COUNT(*) FROM dq_issues").fetchone()[0]
        anomalies = conn.execute("SELECT COUNT(*) FROM anomaly_alerts").fetchone()[0]
        metrics = dict(
            conn.execute(
                """
                SELECT
                    ROUND(AVG(oee), 4) AS avg_oee,
                    ROUND(AVG(availability), 4) AS avg_availability,
                    ROUND(AVG(performance), 4) AS avg_performance,
                    ROUND(AVG(quality), 4) AS avg_quality
                FROM fact_oee_daily
                """
            ).fetchone()
        )
        sites = [
            dict(row)
            for row in conn.execute(
                """
                SELECT site_code, ROUND(AVG(oee), 4) AS avg_oee, SUM(downtime_minutes) AS downtime_minutes
                FROM fact_oee_daily
                GROUP BY site_code
                ORDER BY avg_oee DESC
                """
            ).fetchall()
        ]
    return {
        "machines": machines,
        "events": events,
        "data_quality_issues": dq_issues,
        "anomalies": anomalies,
        **metrics,
        "sites": sites,
    }


@app.get("/api/oee/daily")
def oee_daily(site: str | None = Query(default=None)) -> list[dict]:
    if site:
        return rows(
            """
            SELECT o.*, m.line_name, m.machine_type
            FROM fact_oee_daily o
            JOIN dim_machine m USING (machine_number)
            WHERE o.site_code = ?
            ORDER BY o.shift_date, o.machine_number
            """,
            (site.upper(),),
        )
    return rows(
        """
        SELECT o.*, m.line_name, m.machine_type
        FROM fact_oee_daily o
        JOIN dim_machine m USING (machine_number)
        ORDER BY o.shift_date, o.machine_number
        """
    )


@app.get("/api/oee/machines")
def machine_ranking() -> list[dict]:
    return rows(
        """
        SELECT o.machine_number, m.site_code, m.line_name, m.machine_type,
               ROUND(AVG(o.oee), 4) AS avg_oee,
               SUM(o.produced_qty) AS produced_qty,
               SUM(o.downtime_minutes) AS downtime_minutes
        FROM fact_oee_daily o
        JOIN dim_machine m USING (machine_number)
        GROUP BY o.machine_number, m.site_code, m.line_name, m.machine_type
        ORDER BY avg_oee ASC
        """
    )


@app.get("/api/downtime/pareto")
def downtime_pareto(site: str | None = Query(default=None)) -> list[dict]:
    if site:
        return rows(
            """
            SELECT downtime_reason, ROUND(SUM(downtime_minutes), 2) AS downtime_minutes
            FROM fact_downtime_reason
            WHERE site_code = ?
            GROUP BY downtime_reason
            ORDER BY downtime_minutes DESC
            """,
            (site.upper(),),
        )
    return rows(
        """
        SELECT downtime_reason, ROUND(SUM(downtime_minutes), 2) AS downtime_minutes
        FROM fact_downtime_reason
        GROUP BY downtime_reason
        ORDER BY downtime_minutes DESC
        """
    )


@app.get("/api/anomalies")
def anomalies() -> list[dict]:
    return rows("SELECT * FROM anomaly_alerts ORDER BY created_at DESC, severity DESC")


@app.get("/api/data-quality/issues")
def data_quality_issues() -> list[dict]:
    return rows("SELECT * FROM dq_issues ORDER BY severity DESC, created_at DESC")


@app.get("/api/lineage")
def lineage() -> dict[str, object]:
    return {
        "layers": [
            {"name": "Bronze", "asset": "data/bronze/*.jsonl", "purpose": "raw site API responses and machine master"},
            {"name": "Silver", "asset": "fact_machine_event", "purpose": "standardized and validated machine events"},
            {"name": "Gold", "asset": "fact_oee_daily, fact_downtime_reason", "purpose": "OEE metrics and operational analytics"},
        ],
        "standardization": "Suzhou and Kunshan API schemas are normalized into SHIFT_DATE, MACHINE_NUMBER, runtime, downtime and quantity fields.",
    }


@app.get("/")
def index() -> FileResponse:
    ensure_data()
    return FileResponse(settings.frontend_dir / "index.html")


static_path = Path(settings.frontend_dir)
if static_path.exists():
    app.mount("/assets", StaticFiles(directory=static_path), name="assets")
