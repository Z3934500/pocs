from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from .config import settings
from .online_store import LocalOnlineStore
from .pipeline import resolve_unified_key


@dataclass(frozen=True)
class CdcEvent:
    event_id: str
    table: str
    op: str
    event_ts: str
    after: dict[str, Any]


def utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def stable_event_id(*parts: str) -> str:
    return str(uuid5(NAMESPACE_URL, "|".join(parts)))


def resolve_event_customer(payload: dict[str, Any]) -> str | None:
    if payload.get("unified_customer_key"):
        return str(payload["unified_customer_key"])
    return resolve_unified_key(str(payload.get("id_type", "")), str(payload.get("id_value", "")))


def sample_cdc_events() -> list[CdcEvent]:
    anchor = datetime(2026, 6, 20, 12, 0, 0)
    raw_events = [
        ("orders", "c", anchor, {"order_id": "O-1001", "id_type": "NRIC", "id_value": "S1234567A", "amount": 288.0, "product": "INSURANCE"}),
        ("cart_events", "c", anchor + timedelta(seconds=3), {"cart_id": "C-1001", "id_type": "Passport", "id_value": "E7788990", "amount": 110.0, "product": "TRAVEL_INSURANCE"}),
        ("cart_events", "c", anchor + timedelta(seconds=5), {"cart_id": "C-1002", "id_type": "FIN", "id_value": "G7654321K", "amount": 80.0, "product": "SAVINGS"}),
        ("orders", "c", anchor + timedelta(seconds=8), {"order_id": "O-1002", "id_type": "FIN", "id_value": "G7654321K", "amount": 420.0, "product": "INVESTMENT"}),
        ("cart_events", "c", anchor + timedelta(seconds=11), {"cart_id": "C-1003", "id_type": "NRIC", "id_value": "S9988776B", "amount": 70.0, "product": "CARD"}),
        ("orders", "u", anchor + timedelta(seconds=13), {"order_id": "O-1003", "id_type": "FIN", "id_value": "F4455667M", "amount": 1650.0, "product": "PREMIUM_FINANCING"}),
    ]
    events: list[CdcEvent] = []
    for table, op, event_ts, after in raw_events:
        event_key = str(after.get("order_id") or after.get("cart_id"))
        events.append(
            CdcEvent(
                event_id=stable_event_id(table, op, event_key, event_ts.isoformat(timespec="seconds")),
                table=table,
                op=op,
                event_ts=event_ts.isoformat(timespec="seconds"),
                after=after,
            )
        )
    return events


def write_sample_cdc_events(path: Path | None = None) -> dict[str, int | str]:
    target = path or settings.cdc_events_path
    target.parent.mkdir(parents=True, exist_ok=True)
    events = sample_cdc_events()
    with target.open("w", encoding="utf-8") as file:
        for event in events:
            file.write(json.dumps(event.__dict__, sort_keys=True) + "\n")
    return {"events_written": len(events), "path": str(target)}


def read_cdc_events(path: Path) -> list[CdcEvent]:
    if not path.exists():
        return []
    events: list[CdcEvent] = []
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue
            payload = json.loads(line)
            events.append(CdcEvent(**payload))
    return events


def process_cdc_events(
    events_path: Path | None = None,
    store_path: Path | None = None,
) -> dict[str, int]:
    source = events_path or settings.cdc_events_path
    events = read_cdc_events(source)
    aggregates: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "rt_order_count_1d": 0,
            "rt_order_amount_1d": 0.0,
            "rt_cart_add_count_1d": 0,
            "rt_cart_value_1d": 0.0,
            "rt_last_event_ts": None,
            "rt_last_product": None,
        }
    )

    unresolved = 0
    processed_at = utc_now()
    for event in events:
        customer_key = resolve_event_customer(event.after)
        if not customer_key:
            unresolved += 1
            continue
        amount = float(event.after.get("amount", 0) or 0)
        product = str(event.after.get("product", "")).upper()
        aggregate = aggregates[customer_key]

        if event.table == "orders":
            aggregate["rt_order_count_1d"] += 1
            aggregate["rt_order_amount_1d"] = round(float(aggregate["rt_order_amount_1d"]) + amount, 2)
        elif event.table == "cart_events":
            aggregate["rt_cart_add_count_1d"] += 1
            aggregate["rt_cart_value_1d"] = round(float(aggregate["rt_cart_value_1d"]) + amount, 2)

        aggregate["rt_last_event_ts"] = event.event_ts
        aggregate["rt_last_product"] = product

    payloads: dict[str, dict[str, Any]] = {}
    for customer_key, aggregate in aggregates.items():
        intent_score = min(
            1.0,
            aggregate["rt_cart_add_count_1d"] * 0.18
            + aggregate["rt_order_count_1d"] * 0.25
            + aggregate["rt_order_amount_1d"] / 5000,
        )
        payloads[customer_key] = {
            **aggregate,
            "rt_intent_score": round(intent_score, 3),
            "feature_source": "cdc_stream",
            "stream_updated_at": processed_at.isoformat(timespec="seconds"),
        }

    upserted = LocalOnlineStore(store_path).bulk_upsert(payloads)
    return {
        "events_read": len(events),
        "events_processed": len(events) - unresolved,
        "unresolved_events": unresolved,
        "customers_updated": upserted,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local CDC-to-online-feature-store simulation.")
    parser.add_argument("command", choices=["seed", "process", "run"], nargs="?", default="run")
    parser.add_argument("--events-path", type=Path, default=None)
    parser.add_argument("--store-path", type=Path, default=None)
    args = parser.parse_args()

    if args.command in {"seed", "run"}:
        print(json.dumps(write_sample_cdc_events(args.events_path), indent=2, sort_keys=True))
    if args.command in {"process", "run"}:
        print(json.dumps(process_cdc_events(args.events_path, args.store_path), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
