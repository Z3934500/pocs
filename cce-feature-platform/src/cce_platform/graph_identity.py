from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
from itertools import combinations
from typing import Callable


@dataclass(frozen=True)
class IdentityCandidate:
    left_ref: str
    right_ref: str
    left_identity: str
    right_identity: str
    left_unified_customer_key: str | None
    right_unified_customer_key: str | None
    match_score: float
    match_reason: str
    resolution_action: str


Normalizer = Callable[[str, str], tuple[str, str]]
Resolver = Callable[[str, str], str | None]


def normalize_name(value: str) -> str:
    return " ".join("".join(ch for ch in value.upper() if ch.isalnum() or ch.isspace()).split())


def normalized_optional(payload: dict[str, object], field_name: str) -> str:
    value = str(payload.get(field_name) or "").strip().upper()
    return "".join(ch for ch in value if ch.isalnum() or ch in {"@", "."})


def identity_label(payload: dict[str, object], normalizer: Normalizer) -> str:
    id_type, id_value = normalizer(str(payload.get("id_type", "")), str(payload.get("id_value", "")))
    return f"{id_type}:{id_value}"


def score_pair(
    left: dict[str, object],
    right: dict[str, object],
    normalizer: Normalizer,
    resolver: Resolver,
) -> tuple[float, list[str], str | None, str | None]:
    left_id_type, left_id_value = normalizer(str(left.get("id_type", "")), str(left.get("id_value", "")))
    right_id_type, right_id_value = normalizer(str(right.get("id_type", "")), str(right.get("id_value", "")))
    left_key = resolver(left_id_type, left_id_value)
    right_key = resolver(right_id_type, right_id_value)

    score = 0.0
    reasons: list[str] = []

    if left_key and right_key and left_key == right_key:
        score += 0.5
        reasons.append("same_unified_customer_key")

    left_name = normalize_name(str(left.get("name") or ""))
    right_name = normalize_name(str(right.get("name") or ""))
    name_similarity = SequenceMatcher(None, left_name, right_name).ratio() if left_name and right_name else 0.0
    if name_similarity >= 0.72:
        score += min(0.25, name_similarity * 0.25)
        reasons.append(f"name_similarity={name_similarity:.2f}")

    for field_name, weight in [
        ("phone_hash", 0.18),
        ("email_hash", 0.18),
        ("date_of_birth", 0.12),
        ("postal_code", 0.08),
    ]:
        left_value = normalized_optional(left, field_name)
        right_value = normalized_optional(right, field_name)
        if left_value and right_value and left_value == right_value:
            score += weight
            reasons.append(f"same_{field_name}")

    return min(round(score, 3), 1.0), reasons, left_key, right_key


def resolution_action(score: float, left_key: str | None, right_key: str | None) -> str:
    if left_key and right_key and left_key == right_key:
        return "deterministic_merge_confirmed"
    if score >= 0.74 and bool(left_key) != bool(right_key):
        return "review_attach_to_known_customer"
    if score >= 0.68:
        return "manual_review"
    return "ignore"


def find_identity_candidates(
    customer_payloads: list[dict[str, object]],
    normalizer: Normalizer,
    resolver: Resolver,
    minimum_score: float = 0.68,
) -> list[IdentityCandidate]:
    candidates: list[IdentityCandidate] = []
    for left, right in combinations(customer_payloads, 2):
        score, reasons, left_key, right_key = score_pair(left, right, normalizer, resolver)
        action = resolution_action(score, left_key, right_key)
        if score < minimum_score or action == "ignore":
            continue
        candidates.append(
            IdentityCandidate(
                left_ref=str(left.get("source_customer_ref") or ""),
                right_ref=str(right.get("source_customer_ref") or ""),
                left_identity=identity_label(left, normalizer),
                right_identity=identity_label(right, normalizer),
                left_unified_customer_key=left_key,
                right_unified_customer_key=right_key,
                match_score=score,
                match_reason=", ".join(reasons),
                resolution_action=action,
            )
        )
    return sorted(candidates, key=lambda item: item.match_score, reverse=True)
