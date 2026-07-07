from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class ModelScore:
    unified_customer_key: str
    model_name: str
    model_version: str
    propensity_score: float
    risk_band: str
    score_explanation: str


@dataclass(frozen=True)
class DriftMetric:
    feature_name: str
    baseline_mean: float
    current_mean: float
    drift_ratio: float
    severity: str


BASELINE_FEATURE_MEANS = {
    "monetary_30d": 900.0,
    "tx_count_30d": 4.0,
    "velocity_7d": 2.0,
    "risk_score": 0.38,
}


def sigmoid(value: float) -> float:
    return 1 / (1 + math.exp(-value))


def score_customer_features(rows: list[dict[str, object]]) -> list[ModelScore]:
    scores: list[ModelScore] = []
    for row in rows:
        monetary = float(row["monetary_30d"])
        velocity = float(row["velocity_7d"])
        diversity = float(row["product_diversity"])
        recency = float(row["recency_days"])
        model_input = -1.2 + (monetary / 2500) + (velocity * 0.18) + (diversity * 0.12) - (recency * 0.04)
        score = round(sigmoid(model_input), 3)
        if score >= 0.72:
            band = "high"
        elif score >= 0.45:
            band = "medium"
        else:
            band = "low"
        scores.append(
            ModelScore(
                unified_customer_key=str(row["unified_customer_key"]),
                model_name="insurance_propensity",
                model_version="2026.06.demo",
                propensity_score=score,
                risk_band=band,
                score_explanation="monetary_30d + velocity_7d + product_diversity - recency_days",
            )
        )
    return scores


def calculate_feature_drift(rows: list[dict[str, object]]) -> list[DriftMetric]:
    if not rows:
        return []

    metrics: list[DriftMetric] = []
    for feature_name, baseline_mean in BASELINE_FEATURE_MEANS.items():
        current_mean = sum(float(row[feature_name]) for row in rows) / len(rows)
        drift_ratio = abs(current_mean - baseline_mean) / baseline_mean if baseline_mean else 0.0
        if drift_ratio >= 0.5:
            severity = "high"
        elif drift_ratio >= 0.25:
            severity = "medium"
        else:
            severity = "low"
        metrics.append(
            DriftMetric(
                feature_name=feature_name,
                baseline_mean=round(baseline_mean, 3),
                current_mean=round(current_mean, 3),
                drift_ratio=round(drift_ratio, 3),
                severity=severity,
            )
        )
    return metrics
