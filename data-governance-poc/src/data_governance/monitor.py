from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable


STATUS_OK = "OK"
STATUS_WARN = "WARN"
STATUS_FAIL = "FAIL"


@dataclass(frozen=True)
class CheckResult:
    check: str
    status: str
    severity: str
    metric: float | int | None
    threshold: str
    details: str

    @property
    def is_failure(self) -> bool:
        return self.status == STATUS_FAIL


def load_contract(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


class DataGovernanceMonitor:
    def __init__(self, db_path: Path, contract: dict[str, Any], *, now: datetime | None = None):
        self.db_path = db_path
        self.contract = contract
        self.now = (now or datetime.now(UTC)).astimezone(UTC)

    def run_all(self) -> list[CheckResult]:
        with self._connect() as conn:
            results: list[CheckResult] = []
            results.extend(self.check_schema(conn))
            results.append(self.check_event_contract(conn))
            results.append(self.check_timestamp_validity(conn))
            results.append(self.check_clock_skew(conn))
            results.append(self.check_freshness(conn))
            results.append(self.check_pending_lag(conn))
            results.append(self.check_publish_delay(conn))
            results.append(self.check_event_volume(conn))
            results.append(self.check_duplicate_semantic_events(conn))
            results.append(self.check_inventory_reconciliation(conn))
            return results

    def _connect(self) -> sqlite3.Connection:
        if not self.db_path.exists():
            raise FileNotFoundError(f"SQLite database not found: {self.db_path}")
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def check_schema(self, conn: sqlite3.Connection) -> list[CheckResult]:
        results: list[CheckResult] = []
        for table_name, required_columns in self.contract.get("tables", {}).items():
            try:
                rows = conn.execute(f"PRAGMA table_info({quote_identifier(table_name)})").fetchall()
            except sqlite3.Error as exc:
                results.append(
                    CheckResult(
                        check=f"schema.{table_name}",
                        status=STATUS_FAIL,
                        severity="critical",
                        metric=None,
                        threshold="table exists",
                        details=f"Cannot inspect table: {exc}",
                    )
                )
                continue

            existing = {row["name"] for row in rows}
            missing = sorted(set(required_columns) - existing)
            status = STATUS_FAIL if missing else STATUS_OK
            results.append(
                CheckResult(
                    check=f"schema.{table_name}",
                    status=status,
                    severity="critical",
                    metric=len(missing),
                    threshold="0 missing required columns",
                    details="Missing columns: " + ", ".join(missing) if missing else "Required columns present",
                )
            )
        return results

    def check_event_contract(self, conn: sqlite3.Connection) -> CheckResult:
        allowed_types = set(self.contract.get("event_types", []))
        required_fields = self.contract.get("payload_required_fields", {})
        bad_events: list[str] = []

        rows = conn.execute(
            """
            SELECT event_id, event_type, payload_json
            FROM outbox_events
            ORDER BY created_at, event_id
            """
        ).fetchall()

        for row in rows:
            event_id = row["event_id"]
            event_type = row["event_type"]
            if event_type not in allowed_types:
                bad_events.append(f"{event_id}: unknown event_type={event_type}")
                continue
            try:
                payload = json.loads(row["payload_json"])
            except json.JSONDecodeError as exc:
                bad_events.append(f"{event_id}: invalid JSON ({exc.msg})")
                continue

            missing = sorted(set(required_fields.get(event_type, [])) - set(payload))
            if missing:
                bad_events.append(f"{event_id}: missing {event_type} fields={','.join(missing)}")

        return CheckResult(
            check="event.payload_contract",
            status=STATUS_FAIL if bad_events else STATUS_OK,
            severity="critical",
            metric=len(bad_events),
            threshold="0 invalid events",
            details=summarize_items(bad_events, ok_text=f"{len(rows)} events match payload contract"),
        )

    def check_timestamp_validity(self, conn: sqlite3.Connection) -> CheckResult:
        invalid: list[str] = []
        rows = conn.execute(
            """
            SELECT event_id, created_at, published_at
            FROM outbox_events
            ORDER BY created_at, event_id
            """
        ).fetchall()

        for row in rows:
            for column_name in ("created_at", "published_at"):
                value = row[column_name]
                if value is None:
                    continue
                try:
                    parse_timestamp(value)
                except ValueError:
                    invalid.append(f"{row['event_id']}.{column_name}={value}")

        return CheckResult(
            check="time.timestamp_parse",
            status=STATUS_FAIL if invalid else STATUS_OK,
            severity="critical",
            metric=len(invalid),
            threshold="0 invalid timestamps",
            details=summarize_items(invalid, ok_text=f"{len(rows)} events have parseable timestamps"),
        )

    def check_clock_skew(self, conn: sqlite3.Connection) -> CheckResult:
        warn_seconds = int(self.contract["freshness"].get("future_timestamp_warn_seconds", 60))
        fail_seconds = int(self.contract["freshness"].get("future_timestamp_fail_seconds", 300))
        rows = conn.execute(
            """
            SELECT event_id, created_at
            FROM outbox_events
            ORDER BY created_at DESC
            """
        ).fetchall()
        future_events: list[str] = []
        max_future_seconds = 0.0
        for row in rows:
            created_at = parse_timestamp(row["created_at"])
            if created_at is None:
                continue
            future_seconds = (created_at - self.now).total_seconds()
            if future_seconds > 0:
                max_future_seconds = max(max_future_seconds, future_seconds)
                future_events.append(f"{row['event_id']} is {round(future_seconds, 2)}s in the future")

        status = status_from_threshold(max_future_seconds, warn_seconds, fail_seconds)
        return CheckResult(
            check="time.clock_skew",
            status=status,
            severity="warning",
            metric=round(max_future_seconds, 2),
            threshold=f"WARN > {warn_seconds}s, FAIL > {fail_seconds}s",
            details=summarize_items(future_events, ok_text="No future-dated event timestamps"),
        )

    def check_freshness(self, conn: sqlite3.Connection) -> CheckResult:
        threshold_minutes = int(self.contract["freshness"]["max_event_age_minutes"])
        row = conn.execute("SELECT MAX(created_at) AS latest_created_at FROM outbox_events").fetchone()
        latest = parse_timestamp(row["latest_created_at"] if row else None)
        if latest is None:
            return CheckResult(
                check="freshness.outbox_latest_event",
                status=STATUS_FAIL,
                severity="critical",
                metric=None,
                threshold=f"<= {threshold_minutes} minutes",
                details="No Outbox events found",
            )

        age_minutes = (self.now - latest).total_seconds() / 60
        return CheckResult(
            check="freshness.outbox_latest_event",
            status=STATUS_FAIL if age_minutes > threshold_minutes else STATUS_OK,
            severity="critical",
            metric=round(age_minutes, 2),
            threshold=f"<= {threshold_minutes} minutes",
            details=f"Latest event at {latest.isoformat()}",
        )

    def check_pending_lag(self, conn: sqlite3.Connection) -> CheckResult:
        warn_minutes = int(self.contract["freshness"]["pending_warn_minutes"])
        fail_minutes = int(self.contract["freshness"]["pending_fail_minutes"])
        rows = conn.execute(
            """
            SELECT event_id, created_at
            FROM outbox_events
            WHERE status = 'PENDING'
            ORDER BY created_at
            """
        ).fetchall()
        if not rows:
            return CheckResult(
                check="lag.outbox_pending",
                status=STATUS_OK,
                severity="warning",
                metric=0,
                threshold=f"WARN > {warn_minutes}m, FAIL > {fail_minutes}m",
                details="No pending events",
            )

        oldest = parse_timestamp(rows[0]["created_at"])
        max_lag_minutes = (self.now - oldest).total_seconds() / 60 if oldest else 0
        status = status_from_threshold(max_lag_minutes, warn_minutes, fail_minutes)
        return CheckResult(
            check="lag.outbox_pending",
            status=status,
            severity="warning",
            metric=round(max_lag_minutes, 2),
            threshold=f"WARN > {warn_minutes}m, FAIL > {fail_minutes}m",
            details=f"{len(rows)} pending events; oldest={rows[0]['event_id']}",
        )

    def check_publish_delay(self, conn: sqlite3.Connection) -> CheckResult:
        warn_seconds = int(self.contract["freshness"]["published_delay_warn_seconds"])
        fail_seconds = int(self.contract["freshness"]["published_delay_fail_seconds"])
        rows = conn.execute(
            """
            SELECT event_id, created_at, published_at
            FROM outbox_events
            WHERE published_at IS NOT NULL
            """
        ).fetchall()
        if not rows:
            return CheckResult(
                check="lag.publish_delay",
                status=STATUS_OK,
                severity="warning",
                metric=0,
                threshold=f"WARN > {warn_seconds}s, FAIL > {fail_seconds}s",
                details="No published events yet",
            )

        delays = []
        for row in rows:
            created_at = parse_timestamp(row["created_at"])
            published_at = parse_timestamp(row["published_at"])
            if created_at and published_at:
                delays.append((published_at - created_at).total_seconds())

        max_delay = max(delays) if delays else 0
        status = status_from_threshold(max_delay, warn_seconds, fail_seconds)
        return CheckResult(
            check="lag.publish_delay",
            status=status,
            severity="warning",
            metric=round(max_delay, 2),
            threshold=f"WARN > {warn_seconds}s, FAIL > {fail_seconds}s",
            details=f"{len(rows)} published events checked",
        )

    def check_event_volume(self, conn: sqlite3.Connection) -> CheckResult:
        min_events = int(self.contract["volume"]["min_events_24h"])
        max_events = int(self.contract["volume"]["max_events_24h"])
        since = (self.now - timedelta(hours=24)).isoformat()
        count = conn.execute(
            "SELECT COUNT(*) AS count FROM outbox_events WHERE created_at >= ?",
            (since,),
        ).fetchone()["count"]
        status = STATUS_OK if min_events <= count <= max_events else STATUS_WARN
        return CheckResult(
            check="volume.outbox_events_24h",
            status=status,
            severity="warning",
            metric=count,
            threshold=f"{min_events} <= count <= {max_events}",
            details="Event volume is inside expected range" if status == STATUS_OK else "Event volume is outside expected range",
        )

    def check_duplicate_semantic_events(self, conn: sqlite3.Connection) -> CheckResult:
        rows = conn.execute(
            """
            SELECT event_type, aggregate_id, payload_json, COUNT(*) AS count
            FROM outbox_events
            GROUP BY event_type, aggregate_id, payload_json
            HAVING COUNT(*) > 1
            ORDER BY count DESC
            """
        ).fetchall()
        duplicates = [f"{row['event_type']}:{row['aggregate_id']} count={row['count']}" for row in rows]
        return CheckResult(
            check="idempotency.duplicate_semantic_event",
            status=STATUS_FAIL if duplicates else STATUS_OK,
            severity="critical",
            metric=len(duplicates),
            threshold="0 duplicate semantic events",
            details=summarize_items(duplicates, ok_text="No duplicate semantic events"),
        )

    def check_inventory_reconciliation(self, conn: sqlite3.Connection) -> CheckResult:
        baseline = {sku: int(qty) for sku, qty in self.contract.get("inventory_baseline", {}).items()}
        if not baseline:
            return CheckResult(
                check="reconciliation.inventory_movements",
                status=STATUS_WARN,
                severity="warning",
                metric=None,
                threshold="baseline configured",
                details="No inventory baseline configured",
            )

        movement_rows = conn.execute(
            """
            SELECT
                sku_id,
                SUM(CASE WHEN movement_type = 'RESERVE' THEN quantity ELSE 0 END) AS reserve_qty,
                SUM(CASE WHEN movement_type = 'COMMIT' THEN quantity ELSE 0 END) AS commit_qty,
                SUM(CASE WHEN movement_type = 'RELEASE' THEN quantity ELSE 0 END) AS release_qty
            FROM inventory_movements
            GROUP BY sku_id
            """
        ).fetchall()
        movements = {
            row["sku_id"]: {
                "reserve": int(row["reserve_qty"] or 0),
                "commit": int(row["commit_qty"] or 0),
                "release": int(row["release_qty"] or 0),
            }
            for row in movement_rows
        }
        stock_rows = conn.execute(
            """
            SELECT sku_id, available_stock, reserved_stock, sold_stock
            FROM sku_inventory
            """
        ).fetchall()
        stock = {row["sku_id"]: dict(row) for row in stock_rows}

        mismatches: list[str] = []
        for sku_id, initial_available in baseline.items():
            movement = movements.get(sku_id, {"reserve": 0, "commit": 0, "release": 0})
            expected = {
                "available_stock": initial_available - movement["reserve"] + movement["release"],
                "reserved_stock": movement["reserve"] - movement["commit"] - movement["release"],
                "sold_stock": movement["commit"],
            }
            actual = stock.get(sku_id)
            if actual is None:
                mismatches.append(f"{sku_id}: missing inventory row")
                continue
            for column_name, expected_value in expected.items():
                actual_value = int(actual[column_name])
                if actual_value != expected_value:
                    mismatches.append(f"{sku_id}.{column_name}: actual={actual_value} expected={expected_value}")

        return CheckResult(
            check="reconciliation.inventory_movements",
            status=STATUS_FAIL if mismatches else STATUS_OK,
            severity="critical",
            metric=len(mismatches),
            threshold="0 mismatched stock fields",
            details=summarize_items(mismatches, ok_text=f"{len(baseline)} SKUs reconcile with movement facts"),
        )


def status_from_threshold(metric: float, warn_threshold: float, fail_threshold: float) -> str:
    if metric > fail_threshold:
        return STATUS_FAIL
    if metric > warn_threshold:
        return STATUS_WARN
    return STATUS_OK


def summarize_items(items: Iterable[str], *, ok_text: str, limit: int = 5) -> str:
    values = list(items)
    if not values:
        return ok_text
    preview = "; ".join(values[:limit])
    if len(values) > limit:
        preview += f"; ... +{len(values) - limit} more"
    return preview


def render_table(results: list[CheckResult]) -> str:
    lines = ["check | status | metric | threshold | details", "--- | --- | ---: | --- | ---"]
    for result in results:
        metric = "" if result.metric is None else str(result.metric)
        lines.append(f"{result.check} | {result.status} | {metric} | {result.threshold} | {result.details}")
    return "\n".join(lines)


def render_json(results: list[CheckResult]) -> str:
    return json.dumps([asdict(result) for result in results], indent=2, sort_keys=True)


def render_prometheus(results: list[CheckResult]) -> str:
    status_score = {STATUS_OK: 0, STATUS_WARN: 1, STATUS_FAIL: 2}
    lines = [
        "# HELP data_contract_check_status 0=OK, 1=WARN, 2=FAIL",
        "# TYPE data_contract_check_status gauge",
    ]
    for result in results:
        check = result.check.replace("\\", "\\\\").replace('"', '\\"')
        severity = result.severity.replace("\\", "\\\\").replace('"', '\\"')
        lines.append(
            f'data_contract_check_status{{check="{check}",severity="{severity}"}} {status_score[result.status]}'
        )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run OMS data governance checks.")
    parser.add_argument("--db", type=Path, required=True, help="Path to OMS SQLite database.")
    parser.add_argument("--contract", type=Path, required=True, help="Path to JSON contract file.")
    parser.add_argument("--format", choices=["table", "json", "prometheus"], default="table")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    contract = load_contract(args.contract)
    results = DataGovernanceMonitor(args.db, contract).run_all()

    if args.format == "json":
        print(render_json(results))
    elif args.format == "prometheus":
        print(render_prometheus(results))
    else:
        print(render_table(results))

    return 1 if any(result.is_failure for result in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())