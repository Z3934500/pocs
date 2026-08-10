"""Algorithm patterns translated into OMS, payment and data-governance examples.

The functions are deliberately pure or local-memory examples. They explain the
algorithmic invariant; production adapters must add persistence, concurrency
control, authentication, audit and failure recovery.
"""

from __future__ import annotations

from bisect import bisect_right
from collections import OrderedDict, deque
from dataclasses import dataclass
import heapq
from typing import Deque, Iterable, Mapping, Sequence


@dataclass(frozen=True)
class RequestEvent:
    """One already time-ordered request observation."""

    key: str
    timestamp_ms: int


def deduplicate_keys(keys: Iterable[str]) -> list[bool]:
    """Return whether each key is the first occurrence.

    This is the HashSet part of an idempotency check.

    Time: O(n) average. Space: O(u), where u is the number of unique keys.
    Production: use a database unique constraint or an atomic Redis operation
    across replicas; a process-local set is only an optimization.
    """

    seen: set[str] = set()
    first_seen: list[bool] = []
    for key in keys:
        first_seen.append(key not in seen)
        seen.add(key)
    return first_seen


def sliding_window_allow(
    events: Sequence[RequestEvent],
    limit: int,
    window_ms: int,
) -> list[bool]:
    """Decide whether each request is allowed in a fixed-size time window.

    The input must be non-decreasing by timestamp for each key. The queue keeps
    only timestamps that can still affect the current decision.

    Time: O(n) amortized because each timestamp enters and leaves once.
    Space: O(n) worst case for timestamps inside active windows.
    Production: implement the same invariant atomically with Redis ZSET + Lua
    or a gateway-native rate limiter.
    """

    if limit <= 0 or window_ms <= 0:
        raise ValueError("limit and window_ms must be positive")

    windows: dict[str, Deque[int]] = {}
    decisions: list[bool] = []

    for event in events:
        queue = windows.setdefault(event.key, deque())
        cutoff = event.timestamp_ms - window_ms
        while queue and queue[0] <= cutoff:
            queue.popleft()

        allowed = len(queue) < limit
        decisions.append(allowed)
        if allowed:
            queue.append(event.timestamp_ms)

    return decisions


@dataclass(frozen=True, order=True)
class SettlementRecord:
    """Minimal provider settlement record used by reconciliation."""

    occurred_at_ms: int
    provider: str
    transaction_id: str
    amount_cents: int
    status: str


def merge_settlement_streams(
    streams: Sequence[Sequence[SettlementRecord]],
) -> list[SettlementRecord]:
    """Merge K individually sorted settlement streams.

    The heap contains at most one head record from each stream. The tie-breaker
    fields make output deterministic, which is important for replay and audit.

    Time: O(n log k), where n is total records and k is stream count.
    Space: O(k) heap space, excluding the returned output.
    """

    heap: list[tuple[SettlementRecord, int, int]] = []
    for stream_index, stream in enumerate(streams):
        if stream:
            heapq.heappush(heap, (stream[0], stream_index, 0))

    merged: list[SettlementRecord] = []
    while heap:
        record, stream_index, record_index = heapq.heappop(heap)
        merged.append(record)

        next_index = record_index + 1
        stream = streams[stream_index]
        if next_index < len(stream):
            heapq.heappush(heap, (stream[next_index], stream_index, next_index))

    return merged


def topological_order(
    nodes: Iterable[str],
    dependencies: Iterable[tuple[str, str]],
) -> list[str]:
    """Return a valid order for edges (prerequisite, dependent).

    Kahn's BFS algorithm repeatedly removes zero-indegree nodes. If not all
    nodes are emitted, the graph contains a cycle.

    Time: O(V + E). Space: O(V + E).
    Production: use this for dependency linting and explainability; Terraform,
    Argo CD and Kubernetes remain the authoritative execution controllers.
    """

    node_set = set(nodes)
    adjacency: dict[str, list[str]] = {node: [] for node in node_set}
    indegree = {node: 0 for node in node_set}

    for prerequisite, dependent in dependencies:
        if prerequisite not in node_set or dependent not in node_set:
            raise ValueError("dependency references an unknown node")
        adjacency[prerequisite].append(dependent)
        indegree[dependent] += 1

    ready = deque(sorted(node for node in node_set if indegree[node] == 0))
    result: list[str] = []

    while ready:
        node = ready.popleft()
        result.append(node)
        for dependent in sorted(adjacency[node]):
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                ready.append(dependent)

    if len(result) != len(node_set):
        raise ValueError("dependency graph contains a cycle")
    return result


@dataclass(frozen=True)
class DataFlowNode:
    """A node in a governed data-flow graph."""

    name: str
    region: str
    data_class: str


def find_cross_region_violation(
    start: str,
    graph: Mapping[str, Sequence[str]],
    nodes: Mapping[str, DataFlowNode],
    allowed_regions: set[str],
) -> list[str] | None:
    """Find one shortest path that leaves the allowed data region.

    This is a BFS because the first violation path is useful in an audit report.
    It detects a path; it does not grant access, approve cross-border transfer,
    or replace policy-as-code.

    Time: O(V + E). Space: O(V).
    """

    if start not in nodes:
        raise ValueError(f"unknown start node: {start}")

    queue: deque[str] = deque([start])
    parent: dict[str, str | None] = {start: None}

    while queue:
        current = queue.popleft()
        for neighbor in graph.get(current, ()):
            if neighbor not in nodes:
                raise ValueError(f"unknown graph node: {neighbor}")
            if neighbor in parent:
                continue

            parent[neighbor] = current
            if nodes[neighbor].region not in allowed_regions:
                path: list[str] = []
                cursor: str | None = neighbor
                while cursor is not None:
                    path.append(cursor)
                    cursor = parent[cursor]
                return list(reversed(path))
            queue.append(neighbor)

    return None


class LruCache:
    """O(1) average get/put cache using a dict plus an order list.

    OrderedDict is used here only to keep the sample compact. The interview
    invariant is still HashMap + doubly linked list. Production stores only a
    non-authoritative local copy of configuration or risk rules.
    """

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        self._items: OrderedDict[str, str] = OrderedDict()

    def get(self, key: str) -> str | None:
        if key not in self._items:
            return None
        self._items.move_to_end(key)
        return self._items[key]

    def put(self, key: str, value: str) -> None:
        self._items[key] = value
        self._items.move_to_end(key)
        if len(self._items) > self._capacity:
            self._items.popitem(last=False)

    def __len__(self) -> int:
        return len(self._items)


@dataclass(frozen=True)
class ChannelOption:
    units: int
    fee_cents: int


def min_channel_cost(target_units: int, options: Sequence[ChannelOption]) -> int | None:
    """Find the minimum fee for an exact discrete amount.

    This is an unbounded DP example for planning package/channel choices.
    It is not a provider authorization algorithm and does not bypass provider
    limits, currency rules, settlement risk or compliance controls.

    Time: O(target_units * number_of_options).
    Space: O(target_units).
    """

    if target_units < 0 or any(
        option.units <= 0 or option.fee_cents < 0 for option in options
    ):
        raise ValueError("target and options must be non-negative and usable")

    infinity = 10**18
    dp = [0] + [infinity] * target_units
    for current in range(1, target_units + 1):
        for option in options:
            if option.units <= current:
                dp[current] = min(
                    dp[current],
                    dp[current - option.units] + option.fee_cents,
                )

    return None if dp[target_units] == infinity else dp[target_units]


def merge_intervals(intervals: Sequence[tuple[int, int]]) -> list[tuple[int, int]]:
    """Merge overlapping deployment freeze or maintenance windows.

    Time: O(n log n) for sorting. Space: O(n) for the result.
    """

    if not intervals:
        return []

    ordered = sorted(intervals)
    merged: list[list[int]] = [[ordered[0][0], ordered[0][1]]]
    for start, end in ordered[1:]:
        if start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return [(start, end) for start, end in merged]


@dataclass(frozen=True)
class RankingEntry:
    """One already sorted member from a leaderboard shard."""

    score: int
    member: str


def merge_sharded_top_k(
    shards: Sequence[Sequence[RankingEntry]],
    limit: int,
) -> list[RankingEntry]:
    """Merge per-shard descending leaderboards and return the global Top-K.

    Each shard must be sorted by ``score`` descending and ``member`` ascending.
    Only one head per shard is kept in the heap, so the complexity is
    O(K log S), where S is the number of shards. This models the live-ranking
    design from the recording: query each Redis ZSET shard for its local Top-K,
    then merge at the application layer.
    """

    if limit <= 0:
        raise ValueError("limit must be positive")

    heap: list[tuple[int, str, int, int]] = []
    for shard_index, shard in enumerate(shards):
        if shard:
            first = shard[0]
            heapq.heappush(heap, (-first.score, first.member, shard_index, 0))

    result: list[RankingEntry] = []
    while heap and len(result) < limit:
        _, _, shard_index, entry_index = heapq.heappop(heap)
        entry = shards[shard_index][entry_index]
        result.append(entry)
        next_index = entry_index + 1
        if next_index < len(shards[shard_index]):
            next_entry = shards[shard_index][next_index]
            heapq.heappush(
                heap,
                (-next_entry.score, next_entry.member, shard_index, next_index),
            )
    return result


@dataclass(frozen=True, order=True)
class CursorRecord:
    """A stable ascending keyset-pagination record."""

    sort_key: int
    record_id: str


def keyset_page(
    records: Sequence[CursorRecord],
    after: CursorRecord | None,
    limit: int,
) -> tuple[list[CursorRecord], CursorRecord | None]:
    """Return one page after a stable ``(sort_key, record_id)`` cursor.

    ``records`` must already be ordered by the cursor fields, as a database
    query with ``ORDER BY sort_key, record_id`` would be. A real repository
    should apply ``WHERE (sort_key, record_id) > (:sort_key, :record_id)``
    against the matching composite index rather than scan and discard rows.
    """

    if limit <= 0:
        raise ValueError("limit must be positive")

    start = 0 if after is None else bisect_right(records, after)
    page = list(records[start : start + limit])
    next_cursor = page[-1] if start + len(page) < len(records) and page else None
    return page, next_cursor


def _demo() -> None:
    assert deduplicate_keys(["a", "a", "b"]) == [True, False, True]
    assert sliding_window_allow(
        [
            RequestEvent("merchant-1", 0),
            RequestEvent("merchant-1", 100),
            RequestEvent("merchant-1", 200),
        ],
        limit=2,
        window_ms=1_000,
    ) == [True, True, False]

    streams = [
        [SettlementRecord(1, "wx", "w-1", 100, "CAPTURED")],
        [SettlementRecord(2, "ali", "a-1", 100, "CAPTURED")],
    ]
    assert [record.transaction_id for record in merge_settlement_streams(streams)] == [
        "w-1",
        "a-1",
    ]

    assert topological_order(
        ["vpc", "eks", "aurora"], [("vpc", "eks"), ("eks", "aurora")]
    ) == ["vpc", "eks", "aurora"]

    graph = {
        "cn-db": ["cn-kafka"],
        "cn-kafka": ["sg-databricks"],
        "sg-databricks": [],
    }
    nodes = {
        "cn-db": DataFlowNode("cn-db", "cn", "raw_personal"),
        "cn-kafka": DataFlowNode("cn-kafka", "cn", "raw_personal"),
        "sg-databricks": DataFlowNode("sg-databricks", "sg", "derived"),
    }
    assert find_cross_region_violation(
        "cn-db", graph, nodes, {"cn"}
    ) == ["cn-db", "cn-kafka", "sg-databricks"]

    cache = LruCache(1)
    cache.put("risk-rule", "v1")
    assert cache.get("risk-rule") == "v1"
    cache.put("other", "v2")
    assert cache.get("risk-rule") is None

    assert min_channel_cost(
        6, [ChannelOption(units=3, fee_cents=5), ChannelOption(units=2, fee_cents=4)]
    ) == 10
    assert merge_intervals([(1, 3), (2, 5), (8, 9)]) == [(1, 5), (8, 9)]

    leaderboard = merge_sharded_top_k(
        [
            [RankingEntry(100, "u-1"), RankingEntry(80, "u-4")],
            [RankingEntry(100, "u-2"), RankingEntry(90, "u-3")],
        ],
        3,
    )
    assert [(entry.score, entry.member) for entry in leaderboard] == [
        (100, "u-1"),
        (100, "u-2"),
        (90, "u-3"),
    ]

    records = [CursorRecord(10, "a"), CursorRecord(10, "b"), CursorRecord(11, "c")]
    page, cursor = keyset_page(records, CursorRecord(10, "a"), 1)
    assert page == [CursorRecord(10, "b")] and cursor == CursorRecord(10, "b")


if __name__ == "__main__":
    _demo()
    print("Python algorithm review demo passed")
