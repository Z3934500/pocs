from __future__ import annotations

from dataclasses import dataclass
from math import sqrt


@dataclass(frozen=True)
class FeaturePoint:
    customer_key: str
    recency_days: int
    tx_count_30d: int
    monetary_30d: float
    product_diversity: int
    velocity_7d: int


def _distance(left: tuple[float, ...], right: tuple[float, ...]) -> float:
    return sqrt(sum((a - b) ** 2 for a, b in zip(left, right)))


def _scale(points: list[FeaturePoint]) -> dict[str, tuple[float, float, float]]:
    max_count = max((p.tx_count_30d for p in points), default=1) or 1
    max_monetary = max((p.monetary_30d for p in points), default=1) or 1
    max_velocity = max((p.velocity_7d for p in points), default=1) or 1
    scaled: dict[str, tuple[float, float, float]] = {}
    for point in points:
        scaled[point.customer_key] = (
            point.tx_count_30d / max_count,
            point.monetary_30d / max_monetary,
            point.velocity_7d / max_velocity,
        )
    return scaled


def assign_segments(points: list[FeaturePoint], cluster_count: int = 3) -> dict[str, tuple[int, str]]:
    """Tiny dependency-free k-means for local interview demo data."""
    if not points:
        return {}

    k = min(cluster_count, len(points))
    vectors = _scale(points)
    seeds = sorted(points, key=lambda point: point.monetary_30d)
    centers = [vectors[point.customer_key] for point in seeds[:: max(1, len(seeds) // k)][:k]]

    assignments: dict[str, int] = {}
    for _ in range(12):
        changed = False
        for point in points:
            vector = vectors[point.customer_key]
            cluster_id = min(range(k), key=lambda idx: _distance(vector, centers[idx]))
            if assignments.get(point.customer_key) != cluster_id:
                changed = True
                assignments[point.customer_key] = cluster_id
        if not changed:
            break

        next_centers: list[tuple[float, float, float]] = []
        for cluster_id in range(k):
            members = [vectors[key] for key, assigned in assignments.items() if assigned == cluster_id]
            if not members:
                next_centers.append(centers[cluster_id])
                continue
            next_centers.append(tuple(sum(values) / len(values) for values in zip(*members)))
        centers = next_centers

    monetary_by_cluster: dict[int, float] = {}
    for point in points:
        cluster_id = assignments[point.customer_key]
        monetary_by_cluster.setdefault(cluster_id, 0.0)
        monetary_by_cluster[cluster_id] += point.monetary_30d

    ranked_clusters = sorted(monetary_by_cluster, key=monetary_by_cluster.get)
    labels = ["Value Watch", "Growth", "Priority"]
    cluster_labels = {
        cluster_id: labels[min(rank, len(labels) - 1)]
        for rank, cluster_id in enumerate(ranked_clusters)
    }

    return {
        customer_key: (cluster_id, cluster_labels[cluster_id])
        for customer_key, cluster_id in assignments.items()
    }
