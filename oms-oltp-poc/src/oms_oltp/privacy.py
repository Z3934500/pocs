from __future__ import annotations

import hashlib
import hmac
import json
from enum import StrEnum
from typing import Any


class DataRegion(StrEnum):
    CN = "CN"
    OVERSEAS = "OVERSEAS"


SAFE_CROSS_BORDER_FIELDS = frozenset(
    {
        "order_id",
        "customer_token",
        "order_status",
        "item_count",
        "created_at",
        "source_region",
        "target_region",
        "data_classification",
    }
)


def normalize_region(value: str) -> str:
    normalized = (value or "").strip().upper()
    if normalized not in {region.value for region in DataRegion}:
        raise ValueError(f"unsupported data region: {value}")
    return normalized


def tokenize(value: str, secret: str) -> str:
    """Create a stable, non-reversible identifier for an approved export."""
    digest = hmac.new(secret.encode("utf-8"), value.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"tok_{digest[:24]}"


def canonical_payload_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def sign_payment_callback(
    *,
    callback_id: str,
    provider_ref: str,
    status: str,
    amount_cents: int,
    secret: str,
) -> str:
    message = f"{callback_id}|{provider_ref}|{status.upper()}|{amount_cents}"
    return hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()


def verify_payment_callback(
    *,
    callback_id: str,
    provider_ref: str,
    status: str,
    amount_cents: int,
    signature: str,
    secret: str,
) -> bool:
    expected = sign_payment_callback(
        callback_id=callback_id,
        provider_ref=provider_ref,
        status=status,
        amount_cents=amount_cents,
        secret=secret,
    )
    return hmac.compare_digest(expected, signature or "")


def build_cross_border_order_summary(
    *,
    order_id: str,
    customer_id: str,
    order_status: str,
    item_count: int,
    created_at: str,
    source_region: str,
    target_region: str,
    token_secret: str,
) -> dict[str, Any]:
    """Return the fixed minimised contract used by the cross-border export."""
    return {
        "order_id": order_id,
        "customer_token": tokenize(customer_id, token_secret),
        "order_status": order_status,
        "item_count": item_count,
        "created_at": created_at,
        "source_region": source_region,
        "target_region": target_region,
        "data_classification": "PSEUDONYMIZED_OPERATIONAL",
    }
