from __future__ import annotations

import time

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from ..L0_configuration import settings
from ..L1_mechanism import connect


SERVICE = "cce-feature-platform"
REQUESTS = Counter(
    "http_requests",
    "HTTP requests handled by the CCE API.",
    ("service", "method", "route", "status"),
)
LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds.",
    ("service", "method", "route"),
)
BUSINESS_OPERATIONS = Counter(
    "business_operations",
    "CCE business operations by outcome.",
    ("service", "operation", "outcome"),
)
DATA_QUALITY_ISSUES = Gauge("cce_data_quality_issues", "Current CCE data quality issue count.")
DRIFT_ALERTS = Gauge("cce_feature_drift_alerts", "Current CCE medium/high feature drift count.")
ELIGIBLE_PAIRS = Gauge(
    "cce_eligible_customer_campaign_pairs",
    "Current eligible customer and campaign pair count.",
)


def route_name(request: Request) -> str:
    route = request.scope.get("route")
    return getattr(route, "path", request.url.path)


class MetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path == "/metrics":
            return await call_next(request)

        started = time.perf_counter()
        status = "500"
        try:
            response = await call_next(request)
            status = str(response.status_code)
            return response
        finally:
            route = route_name(request)
            REQUESTS.labels(SERVICE, request.method, route, status).inc()
            LATENCY.labels(SERVICE, request.method, route).observe(time.perf_counter() - started)


def record_business(operation: str, outcome: str) -> None:
    BUSINESS_OPERATIONS.labels(SERVICE, operation, outcome).inc()


def refresh_business_metrics() -> None:
    if not settings.sqlite_path.exists():
        return
    try:
        with connect() as conn:
            DATA_QUALITY_ISSUES.set(conn.execute("SELECT COUNT(*) FROM dq_issues").fetchone()[0])
            DRIFT_ALERTS.set(
                conn.execute(
                    "SELECT COUNT(*) FROM ml_feature_drift WHERE severity IN ('medium', 'high')"
                ).fetchone()[0]
            )
            ELIGIBLE_PAIRS.set(
                conn.execute(
                    "SELECT COUNT(*) FROM gold_campaign_eligibility WHERE is_eligible = 1"
                ).fetchone()[0]
            )
    except Exception:
        # A scrape must remain available while the local PoC database is initializing.
        return


def metrics_response() -> Response:
    refresh_business_metrics()
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
