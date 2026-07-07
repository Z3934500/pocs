from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import mean, pstdev
from uuid import NAMESPACE_URL, uuid5

from .config import settings
from .db import connect, init_schema, reset_tables


def stable_id(*parts: str) -> str:
    return str(uuid5(NAMESPACE_URL, "|".join(parts)))


def safe_hash(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:12]


def utc_now() -> str:
    return datetime.now(UTC).replace(tzinfo=None).isoformat(timespec="seconds")


def machine_master() -> list[dict[str, object]]:
    return [
        {
            "site_code": "SZ",
            "site_name": "Suzhou Factory",
            "country": "CN",
            "line_name": "Assembly A",
            "machine_number": "SZ-CNC-01",
            "machine_type": "CNC",
            "ideal_cycle_seconds": 42.0,
            "planned_minutes_per_shift": 480,
        },
        {
            "site_code": "SZ",
            "site_name": "Suzhou Factory",
            "country": "CN",
            "line_name": "Assembly A",
            "machine_number": "SZ-CNC-02",
            "machine_type": "CNC",
            "ideal_cycle_seconds": 45.0,
            "planned_minutes_per_shift": 480,
        },
        {
            "site_code": "KS",
            "site_name": "Kunshan Factory",
            "country": "CN",
            "line_name": "Machining B",
            "machine_number": "KS-PRESS-01",
            "machine_type": "PRESS",
            "ideal_cycle_seconds": 36.0,
            "planned_minutes_per_shift": 480,
        },
        {
            "site_code": "KS",
            "site_name": "Kunshan Factory",
            "country": "CN",
            "line_name": "Machining B",
            "machine_number": "KS-PRESS-02",
            "machine_type": "PRESS",
            "ideal_cycle_seconds": 39.0,
            "planned_minutes_per_shift": 480,
        },
    ]


def suzhou_payloads() -> list[dict[str, object]]:
    start = datetime(2026, 6, 24)
    rows: list[dict[str, object]] = []
    for day in range(5):
        for machine in ["SZ-CNC-01", "SZ-CNC-02"]:
            produced = 520 + day * 8 + (18 if machine.endswith("02") else 0)
            downtime = 38 + day * 2
            if day == 4 and machine == "SZ-CNC-02":
                downtime = 126
                produced = 380
            rows.append(
                {
                    "SHIFT_DATE": (start + timedelta(days=day)).date().isoformat(),
                    "SHIFT_CODE": "D",
                    "MACHINE_NUMBER": machine,
                    "PRODUCED_QTY": produced,
                    "GOOD_QTY": produced - 8 - day,
                    "RUN_TIME_MIN": 480 - downtime,
                    "DOWN_TIME_MIN": downtime,
                    "DOWN_TIME_REASON": "Tool Change" if downtime < 80 else "Hydraulic Fault",
                }
            )
    rows.append(
        {
            "SHIFT_DATE": "",
            "SHIFT_CODE": "N",
            "MACHINE_NUMBER": "SZ-CNC-99",
            "PRODUCED_QTY": 100,
            "GOOD_QTY": 99,
            "RUN_TIME_MIN": 410,
            "DOWN_TIME_MIN": 70,
            "DOWN_TIME_REASON": "Unknown Machine",
        }
    )
    return rows


def kunshan_payloads() -> list[dict[str, object]]:
    start = datetime(2026, 6, 24)
    rows: list[dict[str, object]] = []
    for day in range(5):
        for machine in ["KS-PRESS-01", "KS-PRESS-02"]:
            produced = 600 + day * 10 + (15 if machine.endswith("02") else 0)
            downtime = 30 + day * 3
            if day == 3 and machine == "KS-PRESS-01":
                downtime = 112
                produced = 430
            rows.append(
                {
                    "shiftDate": (start + timedelta(days=day)).date().isoformat(),
                    "shift": "DAY",
                    "machineNo": machine,
                    "outputQty": produced,
                    "okQty": produced - 11,
                    "runtime_minutes": 480 - downtime,
                    "downtime": {"minutes": downtime, "reason": "Material Wait" if downtime < 80 else "Sensor Failure"},
                }
            )
    rows.append(
        {
            "shiftDate": (start + timedelta(days=2)).date().isoformat(),
            "shift": "DAY",
            "machineNo": "KS-PRESS-02",
            "outputQty": 615,
            "okQty": 640,
            "runtime_minutes": 450,
            "downtime": {"minutes": 30, "reason": "Bad Quality Count"},
        }
    )
    return rows


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row, sort_keys=True) + "\n")


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def record_issue(conn, layer: str, entity_key: str, severity: str, issue_type: str, message: str) -> None:
    now = utc_now()
    conn.execute(
        """
        INSERT OR REPLACE INTO dq_issues
        (issue_id, layer, entity_key, severity, issue_type, message, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (stable_id(layer, entity_key, issue_type), layer, entity_key, severity, issue_type, message, now),
    )


def normalize_event(site_code: str, payload: dict[str, object]) -> dict[str, object]:
    if site_code == "SZ":
        return {
            "site_code": "SZ",
            "shift_date": str(payload.get("SHIFT_DATE", "")).strip(),
            "shift_code": str(payload.get("SHIFT_CODE", "")).strip().upper() or "UNKNOWN",
            "machine_number": str(payload.get("MACHINE_NUMBER", "")).strip().upper(),
            "produced_qty": int(payload.get("PRODUCED_QTY") or 0),
            "good_qty": int(payload.get("GOOD_QTY") or 0),
            "runtime_minutes": float(payload.get("RUN_TIME_MIN") or 0),
            "downtime_minutes": float(payload.get("DOWN_TIME_MIN") or 0),
            "downtime_reason": str(payload.get("DOWN_TIME_REASON", "Unknown")).strip() or "Unknown",
        }

    downtime = payload.get("downtime") or {}
    return {
        "site_code": "KS",
        "shift_date": str(payload.get("shiftDate", "")).strip(),
        "shift_code": "D" if str(payload.get("shift", "")).upper().startswith("DAY") else "N",
        "machine_number": str(payload.get("machineNo", "")).strip().upper(),
        "produced_qty": int(payload.get("outputQty") or 0),
        "good_qty": int(payload.get("okQty") or 0),
        "runtime_minutes": float(payload.get("runtime_minutes") or 0),
        "downtime_minutes": float(downtime.get("minutes") or 0),
        "downtime_reason": str(downtime.get("reason", "Unknown")).strip() or "Unknown",
    }


def build_bronze(conn) -> None:
    now = utc_now()
    datasets = [
        ("SZ", "ws_oee_shift_summary", suzhou_payloads()),
        ("KS", "ws_equipment_output", kunshan_payloads()),
    ]
    write_csv(settings.bronze_dir / "machine_master.csv", machine_master())

    for site_code, interface_name, payloads in datasets:
        write_jsonl(settings.bronze_dir / f"{site_code.lower()}_{interface_name}.jsonl", payloads)
        for payload in payloads:
            conn.execute(
                """
                INSERT OR REPLACE INTO bronze_api_events
                (event_id, site, interface_name, payload_json, ingested_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    stable_id(site_code, interface_name, json.dumps(payload, sort_keys=True)),
                    site_code,
                    interface_name,
                    json.dumps(payload, sort_keys=True),
                    now,
                ),
            )
    conn.commit()


def build_silver(conn) -> None:
    seen_sites: set[str] = set()
    machine_numbers = {str(row["machine_number"]) for row in machine_master()}
    for row in machine_master():
        if row["site_code"] not in seen_sites:
            conn.execute(
                "INSERT OR REPLACE INTO dim_site (site_code, site_name, country) VALUES (?, ?, ?)",
                (row["site_code"], row["site_name"], row["country"]),
            )
            seen_sites.add(str(row["site_code"]))

        conn.execute(
            """
            INSERT OR REPLACE INTO dim_machine
            (machine_number, site_code, line_name, machine_type, ideal_cycle_seconds, planned_minutes_per_shift)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                row["machine_number"],
                row["site_code"],
                row["line_name"],
                row["machine_type"],
                row["ideal_cycle_seconds"],
                row["planned_minutes_per_shift"],
            ),
        )

    bronze_rows = conn.execute("SELECT * FROM bronze_api_events ORDER BY ingested_at").fetchall()
    for bronze in bronze_rows:
        payload = json.loads(bronze["payload_json"])
        event = normalize_event(str(bronze["site"]), payload)
        entity = f"{event['site_code']}:{event['machine_number']}:{event['shift_date']}:{safe_hash(bronze['payload_json'])}"

        if not event["shift_date"]:
            record_issue(conn, "silver", entity, "high", "missing_shift_date", "Shift date is required for OEE calculation.")
            continue
        if event["machine_number"] not in machine_numbers:
            record_issue(conn, "silver", entity, "high", "unknown_machine", "Machine number is not found in master data.")
            continue
        if int(event["good_qty"]) > int(event["produced_qty"]):
            record_issue(conn, "silver", entity, "medium", "invalid_good_quantity", "Good quantity cannot exceed produced quantity.")
            continue
        if float(event["runtime_minutes"]) < 0 or float(event["downtime_minutes"]) < 0:
            record_issue(conn, "silver", entity, "high", "negative_duration", "Runtime and downtime must be non-negative.")
            continue

        conn.execute(
            """
            INSERT OR REPLACE INTO fact_machine_event
            (event_id, site_code, shift_date, shift_code, machine_number, produced_qty,
             good_qty, runtime_minutes, downtime_minutes, downtime_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                stable_id(entity),
                event["site_code"],
                event["shift_date"],
                event["shift_code"],
                event["machine_number"],
                event["produced_qty"],
                event["good_qty"],
                event["runtime_minutes"],
                event["downtime_minutes"],
                event["downtime_reason"],
            ),
        )

    conn.commit()
    write_csv(
        settings.silver_dir / "machine_events.csv",
        [dict(row) for row in conn.execute("SELECT * FROM fact_machine_event").fetchall()],
    )


def clamp_ratio(value: float) -> float:
    return max(0.0, min(1.25, value))


def build_gold(conn) -> None:
    now = utc_now()
    event_rows = conn.execute(
        """
        SELECT e.*, m.ideal_cycle_seconds, m.planned_minutes_per_shift
        FROM fact_machine_event e
        JOIN dim_machine m USING (machine_number)
        """
    ).fetchall()
    grouped: dict[tuple[str, str, str], list] = defaultdict(list)
    for row in event_rows:
        grouped[(row["site_code"], row["shift_date"], row["machine_number"])].append(row)

    oee_records: list[dict[str, object]] = []
    for (site_code, shift_date, machine_number), rows in grouped.items():
        produced_qty = sum(int(row["produced_qty"]) for row in rows)
        good_qty = sum(int(row["good_qty"]) for row in rows)
        runtime_minutes = sum(float(row["runtime_minutes"]) for row in rows)
        downtime_minutes = sum(float(row["downtime_minutes"]) for row in rows)
        planned_minutes = max(float(row["planned_minutes_per_shift"]) for row in rows)
        ideal_cycle_seconds = max(float(row["ideal_cycle_seconds"]) for row in rows)

        availability = clamp_ratio(runtime_minutes / planned_minutes if planned_minutes else 0)
        performance = clamp_ratio((produced_qty * ideal_cycle_seconds) / (runtime_minutes * 60) if runtime_minutes else 0)
        quality = clamp_ratio(good_qty / produced_qty if produced_qty else 0)
        oee = availability * min(performance, 1.0) * min(quality, 1.0)

        record = {
            "site_code": site_code,
            "shift_date": shift_date,
            "machine_number": machine_number,
            "availability": round(availability, 4),
            "performance": round(performance, 4),
            "quality": round(quality, 4),
            "oee": round(oee, 4),
            "produced_qty": produced_qty,
            "downtime_minutes": round(downtime_minutes, 2),
            "updated_at": now,
        }
        oee_records.append(record)
        conn.execute(
            """
            INSERT OR REPLACE INTO fact_oee_daily
            (site_code, shift_date, machine_number, availability, performance, quality,
             oee, produced_qty, downtime_minutes, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            tuple(record.values()),
        )

    downtime_rows = conn.execute(
        """
        SELECT site_code, shift_date, downtime_reason, SUM(downtime_minutes) AS downtime_minutes
        FROM fact_machine_event
        GROUP BY site_code, shift_date, downtime_reason
        """
    ).fetchall()
    for row in downtime_rows:
        conn.execute(
            """
            INSERT OR REPLACE INTO fact_downtime_reason
            (site_code, shift_date, downtime_reason, downtime_minutes)
            VALUES (?, ?, ?, ?)
            """,
            (row["site_code"], row["shift_date"], row["downtime_reason"], row["downtime_minutes"]),
        )

    detect_anomalies(conn, oee_records)
    conn.commit()

    write_csv(settings.gold_dir / "oee_daily.csv", oee_records)
    write_csv(
        settings.gold_dir / "downtime_reason.csv",
        [dict(row) for row in conn.execute("SELECT * FROM fact_downtime_reason").fetchall()],
    )


def detect_anomalies(conn, oee_records: list[dict[str, object]]) -> None:
    now = utc_now()
    by_machine: dict[str, list[dict[str, object]]] = defaultdict(list)
    for record in oee_records:
        by_machine[str(record["machine_number"])].append(record)

    for machine_number, records in by_machine.items():
        sorted_records = sorted(records, key=lambda record: str(record["shift_date"]))
        baseline = sorted_records[:-1]
        if len(baseline) < 3:
            continue
        baseline_oee = [float(record["oee"]) for record in baseline]
        baseline_down = [float(record["downtime_minutes"]) for record in baseline]
        avg_oee = mean(baseline_oee)
        std_oee = pstdev(baseline_oee) or 0.01
        avg_down = mean(baseline_down)
        std_down = pstdev(baseline_down) or 1.0
        latest = sorted_records[-1]

        oee_z = (float(latest["oee"]) - avg_oee) / std_oee
        down_z = (float(latest["downtime_minutes"]) - avg_down) / std_down
        if oee_z < -2.0:
            insert_alert(
                conn,
                latest,
                "oee",
                float(latest["oee"]),
                "high",
                f"OEE dropped below historical baseline. z_score={oee_z:.2f}",
                now,
            )
        if down_z > 2.0:
            insert_alert(
                conn,
                latest,
                "downtime_minutes",
                float(latest["downtime_minutes"]),
                "medium",
                f"Downtime exceeded historical baseline. z_score={down_z:.2f}",
                now,
            )


def insert_alert(conn, record: dict[str, object], metric_name: str, metric_value: float, severity: str, message: str, now: str) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO anomaly_alerts
        (alert_id, site_code, machine_number, shift_date, metric_name, metric_value,
         severity, message, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            stable_id(str(record["site_code"]), str(record["machine_number"]), str(record["shift_date"]), metric_name),
            record["site_code"],
            record["machine_number"],
            record["shift_date"],
            metric_name,
            metric_value,
            severity,
            message,
            now,
        ),
    )


def run_pipeline(reset: bool = True) -> dict[str, int]:
    for path in [settings.bronze_dir, settings.silver_dir, settings.gold_dir, settings.sqlite_path.parent]:
        path.mkdir(parents=True, exist_ok=True)

    with connect() as conn:
        init_schema(conn)
        if reset:
            reset_tables(conn)
        build_bronze(conn)
        build_silver(conn)
        build_gold(conn)
        counts = {
            "bronze_events": conn.execute("SELECT COUNT(*) FROM bronze_api_events").fetchone()[0],
            "machine_events": conn.execute("SELECT COUNT(*) FROM fact_machine_event").fetchone()[0],
            "oee_daily": conn.execute("SELECT COUNT(*) FROM fact_oee_daily").fetchone()[0],
            "dq_issues": conn.execute("SELECT COUNT(*) FROM dq_issues").fetchone()[0],
            "anomalies": conn.execute("SELECT COUNT(*) FROM anomaly_alerts").fetchone()[0],
        }
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the OEE data platform pipeline.")
    parser.add_argument("command", choices=["run", "reset"], nargs="?", default="run")
    args = parser.parse_args()
    counts = run_pipeline(reset=True)
    print(json.dumps({"command": args.command, "counts": counts}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
