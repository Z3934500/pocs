from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid5, NAMESPACE_URL

from .config import settings
from .db import connect, init_schema, reset_tables
from .graph_identity import find_identity_candidates
from .mlops import calculate_feature_drift, score_customer_features
from .segmentation import FeaturePoint, assign_segments


IDENTITY_BRIDGE = {
    ("NRIC", "S1234567A"): "U0001",
    ("PASSPORT", "E7788990"): "U0001",
    ("FIN", "G7654321K"): "U0002",
    ("NRIC", "S9988776B"): "U0003",
    ("PASSPORT", "P3344556"): "U0004",
    ("FIN", "F4455667M"): "U0005",
    ("NRIC", "S1112223C"): "U0006",
}


def normalize_identifier(id_type: str, id_value: str) -> tuple[str, str]:
    clean_type = (id_type or "").upper().strip()
    clean_value = "".join(ch for ch in (id_value or "").upper() if ch.isalnum())
    return clean_type, clean_value


def resolve_unified_key(id_type: str, id_value: str) -> str | None:
    return IDENTITY_BRIDGE.get(normalize_identifier(id_type, id_value))


def stable_issue_id(*parts: str) -> str:
    value = "|".join(parts)
    return str(uuid5(NAMESPACE_URL, value))


def utc_now() -> str:
    return datetime.now(UTC).replace(tzinfo=None).isoformat(timespec="seconds")


def hash_identifier(id_type: str, id_value: str) -> str:
    clean_type, clean_value = normalize_identifier(id_type, id_value)
    digest = hashlib.sha1(f"{clean_type}:{clean_value}".encode("utf-8")).hexdigest()[:12]
    return f"{clean_type}_{digest}"


def sample_customers() -> list[dict[str, object]]:
    return [
        {
            "source_system": "CAS",
            "source_customer_ref": "CAS-1001",
            "id_type": "NRIC",
            "id_value": "S1234567A",
            "name": "Alicia Tan",
            "customer_type": "DBS_CLIENT",
            "first_seen_date": "2025-10-03",
            "phone_hash": "PH_001",
            "email_hash": "EM_ALICIA",
            "date_of_birth": "1988-04-11",
            "postal_code": "018956",
        },
        {
            "source_system": "AJO",
            "source_customer_ref": "AJO-9011",
            "id_type": "Passport",
            "id_value": "E7788990",
            "name": "Alicia T.",
            "customer_type": "DBS_CLIENT",
            "first_seen_date": "2025-10-03",
            "phone_hash": "PH_001",
            "email_hash": "EM_ALICIA",
            "date_of_birth": "1988-04-11",
            "postal_code": "018956",
        },
        {
            "source_system": "CAS",
            "source_customer_ref": "CAS-1002",
            "id_type": "FIN",
            "id_value": "G7654321K",
            "name": "Rahul Menon",
            "customer_type": "NEW_TO_INSURANCE",
            "first_seen_date": "2026-01-18",
            "phone_hash": "PH_002",
            "email_hash": "EM_RAHUL",
            "date_of_birth": "1992-08-21",
            "postal_code": "238877",
        },
        {
            "source_system": "CAS",
            "source_customer_ref": "CAS-1003",
            "id_type": "NRIC",
            "id_value": "S9988776B",
            "name": "Mei Ling Koh",
            "customer_type": "DBS_CLIENT",
            "first_seen_date": "2024-08-11",
            "phone_hash": "PH_003",
            "email_hash": "EM_MEILING",
            "date_of_birth": "1984-12-02",
            "postal_code": "569933",
        },
        {
            "source_system": "AJO",
            "source_customer_ref": "AJO-3344",
            "id_type": "TEMP",
            "id_value": "web-meiling-01",
            "name": "Mei L Koh",
            "customer_type": "DBS_CLIENT",
            "first_seen_date": "2026-05-30",
            "phone_hash": "PH_003",
            "email_hash": "EM_MEILING",
            "date_of_birth": "1984-12-02",
            "postal_code": "569933",
        },
        {
            "source_system": "CAS",
            "source_customer_ref": "CAS-1004",
            "id_type": "Passport",
            "id_value": "P3344556",
            "name": "Daniel Ong",
            "customer_type": "NEW_TO_INSURANCE",
            "first_seen_date": "2026-03-06",
            "phone_hash": "PH_004",
            "email_hash": "EM_DANIEL",
            "date_of_birth": "1996-03-09",
            "postal_code": "408600",
        },
        {
            "source_system": "CAS",
            "source_customer_ref": "CAS-1005",
            "id_type": "FIN",
            "id_value": "F4455667M",
            "name": "Priya Raman",
            "customer_type": "DBS_CLIENT",
            "first_seen_date": "2025-05-21",
            "phone_hash": "PH_005",
            "email_hash": "EM_PRIYA",
            "date_of_birth": "1989-06-14",
            "postal_code": "307591",
        },
        {
            "source_system": "CAS",
            "source_customer_ref": "CAS-1006",
            "id_type": "NRIC",
            "id_value": "S1112223C",
            "name": "Hafiz Ismail",
            "customer_type": "NEW_TO_INSURANCE",
            "first_seen_date": "2026-04-13",
            "phone_hash": "PH_006",
            "email_hash": "EM_HAFIZ",
            "date_of_birth": "1994-11-19",
            "postal_code": "529509",
        },
        {
            "source_system": "CAS",
            "source_customer_ref": "CAS-9999",
            "id_type": "TEMP",
            "id_value": "guest-unknown",
            "name": "Unmapped Walk In",
            "customer_type": "UNKNOWN",
            "first_seen_date": "2026-05-17",
            "phone_hash": "",
            "email_hash": "",
            "date_of_birth": "",
            "postal_code": "",
        },
    ]


def sample_transactions() -> list[dict[str, object]]:
    anchor = datetime(2026, 6, 20, 10, 0, 0)
    profiles = [
        ("NRIC", "S1234567A", "CARD", 260.0, "POS", 2),
        ("Passport", "E7788990", "PREMIUM_FINANCING", 1700.0, "BRANCH", 6),
        ("FIN", "G7654321K", "SAVINGS", 180.0, "MOBILE", 4),
        ("FIN", "G7654321K", "INVESTMENT", 950.0, "MOBILE", 13),
        ("NRIC", "S9988776B", "INSURANCE", 420.0, "RM", 3),
        ("NRIC", "S9988776B", "CARD", 110.0, "POS", 8),
        ("Passport", "P3344556", "SAVINGS", 75.0, "MOBILE", 5),
        ("Passport", "P3344556", "CARD", 90.0, "POS", 9),
        ("FIN", "F4455667M", "PREMIUM_FINANCING", 2300.0, "BRANCH", 1),
        ("FIN", "F4455667M", "INVESTMENT", 1200.0, "MOBILE", 18),
        ("NRIC", "S1112223C", "SAVINGS", 55.0, "MOBILE", 2),
        ("NRIC", "S1112223C", "CARD", 60.0, "POS", 3),
    ]
    rows: list[dict[str, object]] = []
    for idx, (id_type, id_value, product, base_amount, channel, day_offset) in enumerate(profiles, start=1):
        for repeat in range(1, 4):
            txn_ts = anchor - timedelta(days=day_offset + repeat, hours=repeat)
            rows.append(
                {
                    "txn_id": f"TXN-{idx:03d}-{repeat}",
                    "id_type": id_type,
                    "id_value": id_value,
                    "txn_ts": txn_ts.isoformat(timespec="seconds"),
                    "product": product,
                    "amount": round(base_amount + repeat * 17.5, 2),
                    "channel": channel,
                }
            )
    rows.append(
        {
            "txn_id": "TXN-BAD-001",
            "id_type": "TEMP",
            "id_value": "guest-unknown",
            "txn_ts": anchor.isoformat(timespec="seconds"),
            "product": "CARD",
            "amount": -10.0,
            "channel": "POS",
        }
    )
    return rows


def sample_policies() -> list[dict[str, object]]:
    return [
        {
            "policy_id": "POL-INS-1001",
            "id_type": "NRIC",
            "id_value": "S1234567A",
            "policy_type": "INSURANCE",
            "policy_status": "ACTIVE",
            "effective_date": "2024-09-01",
            "premium_amount": 1380.0,
            "claim_count_12m": 0,
            "renewal_due_days": 62,
        },
        {
            "policy_id": "POL-PF-2005",
            "id_type": "FIN",
            "id_value": "F4455667M",
            "policy_type": "PREMIUM_FINANCING",
            "policy_status": "ACTIVE",
            "effective_date": "2025-02-15",
            "premium_amount": 4600.0,
            "claim_count_12m": 1,
            "renewal_due_days": 34,
        },
        {
            "policy_id": "POL-INV-3003",
            "id_type": "FIN",
            "id_value": "G7654321K",
            "policy_type": "INVESTMENT_LINKED",
            "policy_status": "PENDING_RENEWAL",
            "effective_date": "2025-12-05",
            "premium_amount": 2100.0,
            "claim_count_12m": 0,
            "renewal_due_days": 14,
        },
        {
            "policy_id": "POL-TMP-9999",
            "id_type": "TEMP",
            "id_value": "guest-unknown",
            "policy_type": "INSURANCE",
            "policy_status": "QUOTE_ONLY",
            "effective_date": "2026-06-01",
            "premium_amount": 700.0,
            "claim_count_12m": 0,
            "renewal_due_days": 120,
        },
    ]


def campaign_rules() -> list[dict[str, object]]:
    return [
        {
            "campaign_id": "INS_NEW",
            "description": "New-to-insurance customer acquisition",
            "min_monetary_30d": 300.0,
            "exclude_existing_product": "INSURANCE",
        },
        {
            "campaign_id": "PF_UPSELL",
            "description": "Premium financing cross-sell",
            "min_monetary_30d": 1500.0,
            "required_product": "PREMIUM_FINANCING",
        },
    ]


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
        (stable_issue_id(layer, entity_key, issue_type), layer, entity_key, severity, issue_type, message, now),
    )


def build_bronze(conn) -> None:
    now = utc_now()
    customers = sample_customers()
    transactions = sample_transactions()
    policies = sample_policies()
    rules = campaign_rules()

    write_jsonl(settings.bronze_dir / "cas_customers.jsonl", customers)
    write_jsonl(settings.bronze_dir / "transactions.jsonl", transactions)
    write_jsonl(settings.bronze_dir / "policies.jsonl", policies)
    write_jsonl(settings.bronze_dir / "campaign_rules.jsonl", rules)

    event_rows = []
    for event_type, source_system, rows in [
        ("customer_identity", "CAS", customers),
        ("transaction", "RDS_MYSQL_CDC", transactions),
        ("policy", "POLICY_ADMIN", policies),
        ("campaign_rule", "AJO", rules),
    ]:
        for idx, payload in enumerate(rows, start=1):
            event_rows.append(
                (
                    stable_issue_id(event_type, str(idx), json.dumps(payload, sort_keys=True)),
                    event_type,
                    source_system,
                    json.dumps(payload, sort_keys=True),
                    now,
                )
            )

    conn.executemany(
        """
        INSERT OR REPLACE INTO bronze_events
        (event_id, event_type, source_system, payload_json, ingested_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        event_rows,
    )
    conn.commit()


def build_silver(conn) -> None:
    customer_rows = conn.execute(
        "SELECT payload_json FROM bronze_events WHERE event_type = 'customer_identity'"
    ).fetchall()
    customer_payloads = [json.loads(row["payload_json"]) for row in customer_rows]
    best_customer: dict[str, dict[str, object]] = {}

    for payload in customer_payloads:
        id_type, id_value = normalize_identifier(str(payload["id_type"]), str(payload["id_value"]))
        unified_key = resolve_unified_key(id_type, id_value)
        safe_identity = hash_identifier(id_type, id_value)

        if not unified_key:
            record_issue(
                conn,
                "silver",
                safe_identity,
                "high",
                "unmapped_identifier",
                f"Identifier type {id_type} is not mapped to a unified customer key.",
            )
            continue

        conn.execute(
            """
            INSERT OR REPLACE INTO identity_crosswalk
            (id_type, id_value, unified_customer_key, source_customer_ref)
            VALUES (?, ?, ?, ?)
            """,
            (id_type, id_value, unified_key, payload.get("source_customer_ref")),
        )

        current = best_customer.get(unified_key)
        if not current or str(payload["first_seen_date"]) < str(current["first_seen_date"]):
            best_customer[unified_key] = payload

    for unified_key, payload in best_customer.items():
        conn.execute(
            """
            INSERT OR REPLACE INTO dim_customer
            (unified_customer_key, primary_name, customer_type, first_seen_date)
            VALUES (?, ?, ?, ?)
            """,
            (
                unified_key,
                payload["name"],
                payload["customer_type"],
                payload["first_seen_date"],
            ),
        )

    for candidate in find_identity_candidates(customer_payloads, normalize_identifier, resolve_unified_key):
        conn.execute(
            """
            INSERT OR REPLACE INTO silver_identity_candidates
            (candidate_id, left_ref, right_ref, left_identity, right_identity,
             left_unified_customer_key, right_unified_customer_key, match_score,
             match_reason, resolution_action, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                stable_issue_id("identity_candidate", candidate.left_ref, candidate.right_ref),
                candidate.left_ref,
                candidate.right_ref,
                candidate.left_identity,
                candidate.right_identity,
                candidate.left_unified_customer_key,
                candidate.right_unified_customer_key,
                candidate.match_score,
                candidate.match_reason,
                candidate.resolution_action,
                utc_now(),
            ),
        )

    transaction_rows = conn.execute(
        "SELECT payload_json FROM bronze_events WHERE event_type = 'transaction'"
    ).fetchall()
    for row in transaction_rows:
        payload = json.loads(row["payload_json"])
        id_type, id_value = normalize_identifier(str(payload["id_type"]), str(payload["id_value"]))
        unified_key = resolve_unified_key(id_type, id_value)
        safe_identity = hash_identifier(id_type, id_value)
        amount = float(payload["amount"])

        if not unified_key:
            record_issue(
                conn,
                "silver",
                safe_identity,
                "high",
                "unmapped_transaction_identifier",
                "Transaction cannot be linked to unified customer key.",
            )
            continue
        if amount <= 0:
            record_issue(
                conn,
                "silver",
                str(payload["txn_id"]),
                "medium",
                "invalid_amount",
                "Transaction amount must be positive.",
            )
            continue

        conn.execute(
            """
            INSERT OR REPLACE INTO fact_transaction
            (txn_id, unified_customer_key, txn_ts, product, amount, channel)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                payload["txn_id"],
                unified_key,
                payload["txn_ts"],
                str(payload["product"]).upper(),
                amount,
                str(payload["channel"]).upper(),
            ),
        )

    policy_rows = conn.execute("SELECT payload_json FROM bronze_events WHERE event_type = 'policy'").fetchall()
    for row in policy_rows:
        payload = json.loads(row["payload_json"])
        id_type, id_value = normalize_identifier(str(payload["id_type"]), str(payload["id_value"]))
        unified_key = resolve_unified_key(id_type, id_value)
        if not unified_key:
            record_issue(
                conn,
                "silver",
                str(payload["policy_id"]),
                "medium",
                "unmapped_policy_holder",
                "Policy cannot be linked to unified customer key.",
            )
            continue

        conn.execute(
            """
            INSERT OR REPLACE INTO dim_policy
            (policy_id, unified_customer_key, policy_type, policy_status, effective_date, premium_amount)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                payload["policy_id"],
                unified_key,
                str(payload["policy_type"]).upper(),
                str(payload["policy_status"]).upper(),
                payload["effective_date"],
                float(payload["premium_amount"]),
            ),
        )

    conn.commit()


def build_gold(conn) -> None:
    tx_rows = conn.execute(
        """
        SELECT unified_customer_key, txn_ts, product, amount
        FROM fact_transaction
        ORDER BY txn_ts
        """
    ).fetchall()
    if not tx_rows:
        return

    anchor = max(datetime.fromisoformat(row["txn_ts"]) for row in tx_rows)
    by_customer: dict[str, list] = defaultdict(list)
    for row in tx_rows:
        by_customer[row["unified_customer_key"]].append(row)

    points: list[FeaturePoint] = []
    products_by_customer: dict[str, set[str]] = {}
    for customer_key, rows in by_customer.items():
        recent_rows = [
            row for row in rows
            if datetime.fromisoformat(row["txn_ts"]) >= anchor - timedelta(days=30)
        ]
        last_7d_rows = [
            row for row in rows
            if datetime.fromisoformat(row["txn_ts"]) >= anchor - timedelta(days=7)
        ]
        last_txn_ts = max(datetime.fromisoformat(row["txn_ts"]) for row in rows)
        products = {row["product"] for row in rows}
        products_by_customer[customer_key] = products
        points.append(
            FeaturePoint(
                customer_key=customer_key,
                recency_days=(anchor.date() - last_txn_ts.date()).days,
                tx_count_30d=len(recent_rows),
                monetary_30d=round(sum(float(row["amount"]) for row in recent_rows), 2),
                product_diversity=len(products),
                velocity_7d=len(last_7d_rows),
            )
        )

    segments = assign_segments(points)
    now = utc_now()
    for point in points:
        cluster_id, segment_name = segments[point.customer_key]
        risk_score = round(
            min(1.0, (point.velocity_7d * 0.08) + (point.product_diversity * 0.05) + (point.monetary_30d / 10000)),
            3,
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO gold_customer_features
            (unified_customer_key, recency_days, tx_count_30d, monetary_30d,
             product_diversity, velocity_7d, cluster_id, segment_name, risk_score, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                point.customer_key,
                point.recency_days,
                point.tx_count_30d,
                point.monetary_30d,
                point.product_diversity,
                point.velocity_7d,
                cluster_id,
                segment_name,
                risk_score,
                now,
            ),
        )

    rules = campaign_rules()
    for rule in rules:
        campaign_id = str(rule["campaign_id"])
        for point in points:
            products = products_by_customer[point.customer_key]
            eligible = point.monetary_30d >= float(rule["min_monetary_30d"])
            reason = "eligible"
            if "exclude_existing_product" in rule and str(rule["exclude_existing_product"]).upper() in products:
                eligible = False
                reason = "excluded_existing_product"
            if "required_product" in rule and str(rule["required_product"]).upper() not in products:
                eligible = False
                reason = "missing_required_product"
            conn.execute(
                """
                INSERT OR REPLACE INTO gold_campaign_eligibility
                (campaign_id, unified_customer_key, is_eligible, reason, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (campaign_id, point.customer_key, int(eligible), reason, now),
            )

    policy_rows = conn.execute(
        """
        SELECT policy_id, unified_customer_key, policy_type, policy_status, effective_date, premium_amount
        FROM dim_policy
        """
    ).fetchall()
    policy_payloads = [
        json.loads(row["payload_json"])
        for row in conn.execute("SELECT payload_json FROM bronze_events WHERE event_type = 'policy'").fetchall()
    ]
    claims_by_policy = {}
    renewal_by_policy = {}
    for payload in policy_payloads:
        if not resolve_unified_key(str(payload["id_type"]), str(payload["id_value"])):
            continue
        policy_id = str(payload["policy_id"])
        claims_by_policy[policy_id] = int(payload["claim_count_12m"])
        renewal_by_policy[policy_id] = int(payload["renewal_due_days"])
    for row in policy_rows:
        effective_date = datetime.fromisoformat(row["effective_date"])
        tenure_days = (anchor.date() - effective_date.date()).days
        claim_count = claims_by_policy.get(str(row["policy_id"]), 0)
        renewal_due_days = renewal_by_policy.get(str(row["policy_id"]), 999)
        lapse_risk_score = round(
            min(1.0, (claim_count * 0.18) + max(0, 60 - renewal_due_days) / 120 + float(row["premium_amount"]) / 20000),
            3,
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO gold_policy_features
            (policy_id, unified_customer_key, policy_type, policy_status, policy_tenure_days,
             premium_amount, claim_count_12m, renewal_due_days, lapse_risk_score, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["policy_id"],
                row["unified_customer_key"],
                row["policy_type"],
                row["policy_status"],
                tenure_days,
                row["premium_amount"],
                claim_count,
                renewal_due_days,
                lapse_risk_score,
                now,
            ),
        )

    write_csv(
        settings.silver_dir / "identity_crosswalk.csv",
        [dict(row) for row in conn.execute("SELECT * FROM identity_crosswalk").fetchall()],
    )
    write_csv(
        settings.gold_dir / "customer_features.csv",
        [dict(row) for row in conn.execute("SELECT * FROM gold_customer_features").fetchall()],
    )
    write_csv(
        settings.gold_dir / "policy_features.csv",
        [dict(row) for row in conn.execute("SELECT * FROM gold_policy_features").fetchall()],
    )
    conn.commit()


def build_mlops(conn) -> None:
    now = utc_now()
    feature_rows = [
        dict(row)
        for row in conn.execute(
            """
            SELECT unified_customer_key, recency_days, tx_count_30d, monetary_30d,
                   product_diversity, velocity_7d, risk_score
            FROM gold_customer_features
            """
        ).fetchall()
    ]
    if not feature_rows:
        return

    for score in score_customer_features(feature_rows):
        conn.execute(
            """
            INSERT OR REPLACE INTO gold_customer_model_scores
            (unified_customer_key, model_name, model_version, propensity_score,
             risk_band, score_explanation, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                score.unified_customer_key,
                score.model_name,
                score.model_version,
                score.propensity_score,
                score.risk_band,
                score.score_explanation,
                now,
            ),
        )

    high_band_count = sum(1 for row in conn.execute("SELECT risk_band FROM gold_customer_model_scores") if row["risk_band"] == "high")
    precision_at_20 = round(high_band_count / max(1, len(feature_rows)), 3)
    conn.execute(
        """
        INSERT OR REPLACE INTO ml_model_runs
        (model_run_id, model_name, model_version, training_rows, feature_table,
         target_definition, auc, precision_at_20, status, artifact_uri, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            stable_issue_id("model_run", "insurance_propensity", "2026.06.demo"),
            "insurance_propensity",
            "2026.06.demo",
            len(feature_rows),
            "gold_customer_features",
            "purchased_insurance_or_requested_quote_next_30d",
            0.81,
            precision_at_20,
            "registered",
            "dbfs:/models/cce/insurance_propensity/2026.06.demo",
            now,
        ),
    )

    for metric in calculate_feature_drift(feature_rows):
        conn.execute(
            """
            INSERT OR REPLACE INTO ml_feature_drift
            (drift_id, feature_name, baseline_mean, current_mean, drift_ratio, severity, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                stable_issue_id("feature_drift", metric.feature_name),
                metric.feature_name,
                metric.baseline_mean,
                metric.current_mean,
                metric.drift_ratio,
                metric.severity,
                now,
            ),
        )
    conn.commit()


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
        build_mlops(conn)
        counts = {
            "bronze_events": conn.execute("SELECT COUNT(*) FROM bronze_events").fetchone()[0],
            "customers": conn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0],
            "policies": conn.execute("SELECT COUNT(*) FROM dim_policy").fetchone()[0],
            "transactions": conn.execute("SELECT COUNT(*) FROM fact_transaction").fetchone()[0],
            "features": conn.execute("SELECT COUNT(*) FROM gold_customer_features").fetchone()[0],
            "policy_features": conn.execute("SELECT COUNT(*) FROM gold_policy_features").fetchone()[0],
            "identity_candidates": conn.execute("SELECT COUNT(*) FROM silver_identity_candidates").fetchone()[0],
            "model_scores": conn.execute("SELECT COUNT(*) FROM gold_customer_model_scores").fetchone()[0],
            "drift_checks": conn.execute("SELECT COUNT(*) FROM ml_feature_drift").fetchone()[0],
            "dq_issues": conn.execute("SELECT COUNT(*) FROM dq_issues").fetchone()[0],
        }
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the CCE medallion pipeline.")
    parser.add_argument("command", choices=["run", "reset"], nargs="?", default="run")
    args = parser.parse_args()
    counts = run_pipeline(reset=True)
    print(json.dumps({"command": args.command, "counts": counts}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
