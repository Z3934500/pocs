#!/usr/bin/env python3
"""
OMS SRE Inspection Script
Checks payment system health across: Outbox, Payment state, Ledger, Inventory.
Designed to run daily (cron / K8s CronJob) or on-demand before deployments.

Usage:
    python sre_inspect.py [--env staging|prod] [--format text|json] [--fail-on-amber]

Required env vars:
    DB_HOST, DB_PORT (default 5432), DB_NAME, DB_USER, DB_PASSWORD

Optional env vars:
    OUTBOX_DEPTH_WARN      (default 50)   -- AMBER threshold
    OUTBOX_DEPTH_CRIT      (default 500)  -- RED threshold
    OUTBOX_AGE_WARN_SEC    (default 60)   -- AMBER: oldest event older than N seconds
    OUTBOX_AGE_CRIT_SEC    (default 300)  -- RED  (5 min)
    PAYMENT_UNKNOWN_WARN   (default 1)
    PAYMENT_UNKNOWN_CRIT   (default 10)

Exit codes:
    0  All GREEN
    1  At least one P1 RED (COMPENSATION_FAILED, Ledger mismatch, Inventory mismatch)
    2  DB connection failed
    3  At least one non-P1 RED or (if --fail-on-amber) any AMBER
"""
import os
import sys
import json
import argparse
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import List, Optional

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("psycopg2-binary required: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(2)

# ── RAG status constants ──────────────────────────────────────────────────────
RED   = "RED"
AMBER = "AMBER"
GREEN = "GREEN"

RAG_ICON = {RED: "🔴", AMBER: "🟡", GREEN: "🟢"}


@dataclass
class Check:
    name: str
    status: str        # GREEN / AMBER / RED
    value: str
    detail: str = ""
    p1: bool = False   # True = immediate action, fail with exit 1


@dataclass
class InspectionReport:
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    environment: str = "unknown"
    checks: List[Check] = field(default_factory=list)

    def add(self, check: Check) -> None:
        self.checks.append(check)

    def add_all(self, checks: List[Check]) -> None:
        self.checks.extend(checks)

    def has_p1(self) -> bool:
        return any(c.p1 and c.status == RED for c in self.checks)

    def has_red(self) -> bool:
        return any(c.status == RED for c in self.checks)

    def has_amber(self) -> bool:
        return any(c.status == AMBER for c in self.checks)

    def summary_status(self) -> str:
        if self.has_red():
            return RED
        if self.has_amber():
            return AMBER
        return GREEN


# ── DB connection ─────────────────────────────────────────────────────────────
def _cfg(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _cfg_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default


def get_connection():
    return psycopg2.connect(
        host=_cfg("DB_HOST", "localhost"),
        port=_cfg_int("DB_PORT", 5432),
        dbname=_cfg("DB_NAME", "oms_order"),
        user=_cfg("DB_USER", "oms"),
        password=_cfg("DB_PASSWORD", ""),
        connect_timeout=5,
        options="-c statement_timeout=15000",  # 15s query guard
    )


# ── Check functions ───────────────────────────────────────────────────────────
def check_outbox(conn) -> List[Check]:
    depth_warn = _cfg_int("OUTBOX_DEPTH_WARN", 50)
    depth_crit = _cfg_int("OUTBOX_DEPTH_CRIT", 500)
    age_warn   = _cfg_int("OUTBOX_AGE_WARN_SEC", 60)
    age_crit   = _cfg_int("OUTBOX_AGE_CRIT_SEC", 300)
    checks: List[Check] = []

    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM order_outbox_event WHERE status IN ('PENDING','IN_FLIGHT')"
        )
        depth = cur.fetchone()[0]
        if depth == 0:
            st = GREEN
        elif depth < depth_warn:
            st = GREEN
        elif depth < depth_crit:
            st = AMBER
        else:
            st = RED
        checks.append(Check("Outbox pending depth", st, str(depth),
                             f"warn≥{depth_warn} crit≥{depth_crit}"))

        cur.execute("""
            SELECT COALESCE(
                EXTRACT(EPOCH FROM (NOW() - MIN(created_at)))::int, -1
            ) FROM order_outbox_event WHERE status IN ('PENDING','IN_FLIGHT')
        """)
        age_s = cur.fetchone()[0]
        if age_s == -1:
            checks.append(Check("Outbox oldest event age", GREEN, "none pending"))
        else:
            st = GREEN if age_s < age_warn else (AMBER if age_s < age_crit else RED)
            checks.append(Check("Outbox oldest event age", st, f"{age_s}s",
                                 f"warn≥{age_warn}s crit≥{age_crit}s (5min SLA)"))
    return checks


def check_payment(conn) -> List[Check]:
    unk_warn = _cfg_int("PAYMENT_UNKNOWN_WARN", 1)
    unk_crit = _cfg_int("PAYMENT_UNKNOWN_CRIT", 10)
    checks: List[Check] = []

    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM customer_order WHERE status = 'PAYMENT_UNKNOWN'"
        )
        unknown = cur.fetchone()[0]
        st = GREEN if unknown < unk_warn else (AMBER if unknown < unk_crit else RED)
        checks.append(Check("PAYMENT_UNKNOWN orders", st, str(unknown),
                             "capture timed out — run provider status query"))

        cur.execute(
            "SELECT COUNT(*) FROM customer_order WHERE status = 'COMPENSATION_FAILED'"
        )
        comp = cur.fetchone()[0]
        st = RED if comp > 0 else GREEN
        checks.append(Check("COMPENSATION_FAILED orders", st, str(comp),
                             "Saga + compensation both failed — manual review", p1=(comp > 0)))
    return checks


def check_ledger(conn) -> List[Check]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT ledger_txn_id
                FROM payment_ledger_entry
                GROUP BY ledger_txn_id
                HAVING SUM(CASE WHEN direction='DEBIT'  THEN amount_cents ELSE 0 END)
                    != SUM(CASE WHEN direction='CREDIT' THEN amount_cents ELSE 0 END)
            ) t
        """)
        mismatches = cur.fetchone()[0]
        st = RED if mismatches > 0 else GREEN
        return [Check("Ledger balance mismatches", st, str(mismatches),
                      "DEBIT≠CREDIT — financial integrity P1", p1=(mismatches > 0))]


def check_inventory(conn) -> List[Check]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM (
                SELECT s.sku
                FROM inventory_stock s
                LEFT JOIN inventory_reservation r ON r.sku = s.sku
                GROUP BY s.sku, s.reserved_qty
                HAVING s.reserved_qty !=
                    COALESCE(SUM(CASE WHEN r.status='RESERVED' THEN r.qty ELSE 0 END), 0)
            ) t
        """)
        mismatches = cur.fetchone()[0]
        st = RED if mismatches > 0 else GREEN
        return [Check("Inventory stock mismatches", st, str(mismatches),
                      "reserved_qty ≠ sum(RESERVED reservations)", p1=(mismatches > 0))]


# ── Output ────────────────────────────────────────────────────────────────────
def print_report(report: InspectionReport, fmt: str = "text") -> None:
    if fmt == "json":
        print(json.dumps({
            "timestamp": report.timestamp,
            "environment": report.environment,
            "summary": report.summary_status(),
            "p1_alert": report.has_p1(),
            "checks": [
                {"name": c.name, "status": c.status, "value": c.value,
                 "detail": c.detail, "p1": c.p1}
                for c in report.checks
            ],
        }, indent=2, ensure_ascii=False))
        return

    W = 80
    icon = RAG_ICON.get(report.summary_status(), "")
    print("=" * W)
    print(f"  OMS SRE Inspection  |  env={report.environment}")
    print(f"  {report.timestamp}")
    print(f"  Summary: {icon} {report.summary_status()}"
          + ("  ⚠️  P1 — IMMEDIATE ACTION REQUIRED" if report.has_p1() else ""))
    print("=" * W)
    print(f"{'Check':<34} {'Status':<8} {'Value':<14}  Detail")
    print("-" * W)
    for c in report.checks:
        icon_c = RAG_ICON.get(c.status, "")
        p1_tag = "  ← P1" if c.p1 and c.status == RED else ""
        detail = (c.detail[:26] + "…") if len(c.detail) > 27 else c.detail
        print(f"{c.name:<34} {icon_c} {c.status:<6} {c.value:<14}  {detail}{p1_tag}")
    print("=" * W)


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="OMS SRE Inspection")
    parser.add_argument("--env", default=_cfg("ENV", "unknown"),
                        help="Environment label (staging/prod)")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    parser.add_argument("--fail-on-amber", action="store_true",
                        help="Exit 3 if any AMBER check found")
    args = parser.parse_args()

    report = InspectionReport(environment=args.env)

    try:
        conn = get_connection()
        conn.autocommit = True
    except Exception as e:
        print(f"DB connection failed: {e}", file=sys.stderr)
        sys.exit(2)

    try:
        report.add_all(check_outbox(conn))
        report.add_all(check_payment(conn))
        report.add_all(check_ledger(conn))
        report.add_all(check_inventory(conn))
    except Exception as e:
        print(f"Inspection query failed: {e}", file=sys.stderr)
        conn.close()
        sys.exit(2)
    finally:
        conn.close()

    print_report(report, fmt=args.format)

    if report.has_p1():
        sys.exit(1)
    if report.has_red() or (args.fail_on_amber and report.has_amber()):
        sys.exit(3)
    sys.exit(0)


if __name__ == "__main__":
    main()
