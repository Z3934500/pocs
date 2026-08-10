#include <algorithm>
#include <cassert>
#include <deque>
#include <functional>
#include <iterator>
#include <list>
#include <optional>
#include <queue>
#include <set>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace oms {

struct RequestEvent {
    std::string key;
    long long timestamp_ms;
};

// Hash-set idempotency model: O(n) average time and O(u) space.
std::vector<bool> deduplicate_keys(const std::vector<std::string>& keys) {
    std::unordered_set<std::string> seen;
    std::vector<bool> first_seen;
    first_seen.reserve(keys.size());

    for (const auto& key : keys) {
        const bool is_new = seen.insert(key).second;
        first_seen.push_back(is_new);
    }
    return first_seen;
}

// Each timestamp enters and leaves its deque once: O(n) amortized time.
std::vector<bool> sliding_window_allow(
        const std::vector<RequestEvent>& events,
        int limit,
        long long window_ms) {
    if (limit <= 0 || window_ms <= 0) {
        throw std::invalid_argument("limit and window_ms must be positive");
    }

    std::unordered_map<std::string, std::deque<long long>> windows;
    std::vector<bool> decisions;
    decisions.reserve(events.size());

    for (const auto& event : events) {
        auto& window = windows[event.key];
        const long long cutoff = event.timestamp_ms - window_ms;
        while (!window.empty() && window.front() <= cutoff) {
            window.pop_front();
        }

        const bool allowed = static_cast<int>(window.size()) < limit;
        decisions.push_back(allowed);
        if (allowed) {
            window.push_back(event.timestamp_ms);
        }
    }
    return decisions;
}

struct SettlementRecord {
    long long occurred_at_ms;
    std::string provider;
    std::string transaction_id;
    long long amount_cents;
    std::string status;
};

struct MergeItem {
    SettlementRecord record;
    std::size_t stream_index;
    std::size_t record_index;
};

struct MergeItemCompare {
    bool operator()(const MergeItem& left, const MergeItem& right) const {
        return std::tie(
                left.record.occurred_at_ms,
                left.record.provider,
                left.record.transaction_id)
            > std::tie(
                right.record.occurred_at_ms,
                right.record.provider,
                right.record.transaction_id);
    }
};

// K-way merge: O(n log k) time and O(k) heap space.
std::vector<SettlementRecord> merge_settlement_streams(
        const std::vector<std::vector<SettlementRecord>>& streams) {
    std::priority_queue<
        MergeItem,
        std::vector<MergeItem>,
        MergeItemCompare> queue;

    for (std::size_t stream_index = 0; stream_index < streams.size(); ++stream_index) {
        if (!streams[stream_index].empty()) {
            queue.push({streams[stream_index][0], stream_index, 0});
        }
    }

    std::vector<SettlementRecord> result;
    while (!queue.empty()) {
        MergeItem item = queue.top();
        queue.pop();
        result.push_back(item.record);

        const std::size_t next_index = item.record_index + 1;
        const auto& stream = streams[item.stream_index];
        if (next_index < stream.size()) {
            queue.push({stream[next_index], item.stream_index, next_index});
        }
    }
    return result;
}

struct RankingEntry {
    long long score;
    std::string member;
};

struct RankingItem {
    RankingEntry entry;
    std::size_t shard_index;
    std::size_t entry_index;
};

struct RankingItemCompare {
    bool operator()(const RankingItem& left, const RankingItem& right) const {
        if (left.entry.score != right.entry.score) {
            return left.entry.score < right.entry.score;
        }
        if (left.entry.member != right.entry.member) {
            return left.entry.member > right.entry.member;
        }
        return left.shard_index > right.shard_index;
    }
};

// Merge already sorted per-shard leaderboards: O(K log S).
std::vector<RankingEntry> merge_sharded_top_k(
        const std::vector<std::vector<RankingEntry>>& shards,
        std::size_t limit) {
    std::priority_queue<RankingItem, std::vector<RankingItem>, RankingItemCompare> queue;
    for (std::size_t shard_index = 0; shard_index < shards.size(); ++shard_index) {
        if (!shards[shard_index].empty()) {
            queue.push({shards[shard_index][0], shard_index, 0});
        }
    }

    std::vector<RankingEntry> result;
    result.reserve(limit);
    while (!queue.empty() && result.size() < limit) {
        RankingItem item = queue.top();
        queue.pop();
        result.push_back(item.entry);

        const std::size_t next_index = item.entry_index + 1;
        const auto& shard = shards[item.shard_index];
        if (next_index < shard.size()) {
            queue.push({shard[next_index], item.shard_index, next_index});
        }
    }
    return result;
}

struct CursorRecord {
    long long sort_key;
    std::string record_id;
};

struct CursorPage {
    std::vector<CursorRecord> records;
    std::optional<CursorRecord> next;
};

// Keyset page over records sorted by (sort_key, record_id).
CursorPage keyset_page(
        const std::vector<CursorRecord>& records,
        const std::optional<CursorRecord>& after,
        std::size_t limit) {
    if (limit == 0) {
        throw std::invalid_argument("limit must be positive");
    }

    const auto first = std::lower_bound(
        records.begin(), records.end(), after,
        [](const CursorRecord& record, const std::optional<CursorRecord>& cursor) {
            if (!cursor.has_value()) {
                return false;
            }
            return std::tie(record.sort_key, record.record_id)
                <= std::tie(cursor->sort_key, cursor->record_id);
        });
    const std::size_t start = static_cast<std::size_t>(std::distance(records.begin(), first));
    const std::size_t end = std::min(records.size(), start + limit);

    CursorPage page;
    page.records.assign(records.begin() + static_cast<std::ptrdiff_t>(start),
                        records.begin() + static_cast<std::ptrdiff_t>(end));
    if (end < records.size() && !page.records.empty()) {
        page.next = page.records.back();
    }
    return page;
}

// Kahn topological sort: O(V+E), with cycle detection.
std::vector<std::string> topological_order(
        const std::vector<std::string>& nodes,
        const std::vector<std::pair<std::string, std::string>>& dependencies) {
    std::unordered_set<std::string> known(nodes.begin(), nodes.end());
    std::unordered_map<std::string, std::vector<std::string>> adjacency;
    std::unordered_map<std::string, int> indegree;
    for (const auto& node : nodes) {
        adjacency[node] = {};
        indegree[node] = 0;
    }

    for (const auto& [before, after] : dependencies) {
        if (!known.count(before) || !known.count(after)) {
            throw std::invalid_argument("dependency references an unknown node");
        }
        adjacency[before].push_back(after);
        ++indegree[after];
    }

    std::set<std::string> ready;
    for (const auto& [node, degree] : indegree) {
        if (degree == 0) {
            ready.insert(node);
        }
    }

    std::vector<std::string> result;
    while (!ready.empty()) {
        auto iterator = ready.begin();
        const std::string node = *iterator;
        ready.erase(iterator);
        result.push_back(node);

        auto& dependents = adjacency[node];
        std::sort(dependents.begin(), dependents.end());
        for (const auto& dependent : dependents) {
            if (--indegree[dependent] == 0) {
                ready.insert(dependent);
            }
        }
    }

    if (result.size() != known.size()) {
        throw std::runtime_error("dependency graph contains a cycle");
    }
    return result;
}

struct DataFlowNode {
    std::string name;
    std::string region;
    std::string data_class;
};

// BFS finds one shortest forbidden-region path: O(V+E) time and O(V) space.
std::optional<std::vector<std::string>> find_cross_region_violation(
        const std::string& start,
        const std::unordered_map<std::string, std::vector<std::string>>& graph,
        const std::unordered_map<std::string, DataFlowNode>& nodes,
        const std::unordered_set<std::string>& allowed_regions) {
    if (!nodes.count(start)) {
        throw std::invalid_argument("unknown start node");
    }

    std::queue<std::string> queue;
    std::unordered_map<std::string, std::string> parent;
    queue.push(start);
    parent[start] = "";

    while (!queue.empty()) {
        const std::string current = queue.front();
        queue.pop();

        const auto graph_iterator = graph.find(current);
        if (graph_iterator == graph.end()) {
            continue;
        }
        for (const auto& neighbor : graph_iterator->second) {
            if (!nodes.count(neighbor)) {
                throw std::invalid_argument("unknown graph node");
            }
            if (parent.count(neighbor)) {
                continue;
            }
            parent[neighbor] = current;

            if (!allowed_regions.count(nodes.at(neighbor).region)) {
                std::vector<std::string> path;
                for (std::string cursor = neighbor; !cursor.empty(); cursor = parent.at(cursor)) {
                    path.push_back(cursor);
                }
                std::reverse(path.begin(), path.end());
                return path;
            }
            queue.push(neighbor);
        }
    }
    return std::nullopt;
}

class LruCache {
public:
    explicit LruCache(std::size_t capacity) : capacity_(capacity) {
        if (capacity == 0) {
            throw std::invalid_argument("capacity must be positive");
        }
    }

    // Hash map lookup + list splice are O(1) average.
    std::optional<std::string> get(const std::string& key) {
        const auto iterator = items_.find(key);
        if (iterator == items_.end()) {
            return std::nullopt;
        }
        order_.splice(order_.end(), order_, iterator->second);
        return iterator->second->second;
    }

    void put(const std::string& key, const std::string& value) {
        const auto iterator = items_.find(key);
        if (iterator != items_.end()) {
            iterator->second->second = value;
            order_.splice(order_.end(), order_, iterator->second);
            return;
        }

        order_.emplace_back(key, value);
        auto element = std::prev(order_.end());
        items_[key] = element;

        if (items_.size() > capacity_) {
            items_.erase(order_.front().first);
            order_.pop_front();
        }
    }

private:
    std::size_t capacity_;
    std::list<std::pair<std::string, std::string>> order_;
    std::unordered_map<
        std::string,
        std::list<std::pair<std::string, std::string>>::iterator> items_;
};

struct ChannelOption {
    int units;
    int fee_cents;
};

// Unbounded DP: O(target * options) time and O(target) space.
std::optional<int> min_channel_cost(
        int target,
        const std::vector<ChannelOption>& options) {
    if (target < 0) {
        return std::nullopt;
    }

    constexpr int infinity = 1'000'000'000;
    std::vector<int> dp(target + 1, infinity);
    dp[0] = 0;

    for (int current = 1; current <= target; ++current) {
        for (const auto& option : options) {
            if (option.units <= 0 || option.fee_cents < 0) {
                throw std::invalid_argument("channel option is invalid");
            }
            if (option.units <= current && dp[current - option.units] != infinity) {
                dp[current] = std::min(
                    dp[current],
                    dp[current - option.units] + option.fee_cents);
            }
        }
    }
    return dp[target] == infinity
        ? std::nullopt
        : std::optional<int>(dp[target]);
}

struct Interval {
    int start;
    int end;
};

std::vector<Interval> merge_intervals(std::vector<Interval> intervals) {
    if (intervals.empty()) {
        return {};
    }
    std::sort(intervals.begin(), intervals.end(), [](const Interval& left, const Interval& right) {
        return std::tie(left.start, left.end) < std::tie(right.start, right.end);
    });

    std::vector<Interval> merged{intervals.front()};
    for (const auto& interval : intervals) {
        if (interval.start <= merged.back().end) {
            merged.back().end = std::max(merged.back().end, interval.end);
        } else {
            merged.push_back(interval);
        }
    }
    return merged;
}

}  // namespace oms

int main() {
    using namespace oms;

    assert((deduplicate_keys({"a", "a", "b"}) == std::vector<bool>{true, false, true}));
    assert((sliding_window_allow({
        {"merchant-1", 0},
        {"merchant-1", 100},
        {"merchant-1", 200},
    }, 2, 1000) == std::vector<bool>{true, true, false}));

    const std::vector<std::vector<SettlementRecord>> streams{
        {{1, "wx", "w-1", 100, "CAPTURED"}},
        {{2, "ali", "a-1", 100, "CAPTURED"}},
    };
    const auto merged = merge_settlement_streams(streams);
    assert(merged[0].transaction_id == "w-1");
    assert(merged[1].transaction_id == "a-1");

    const auto order = topological_order(
        {"vpc", "eks", "aurora"},
        {{"vpc", "eks"}, {"eks", "aurora"}});
    assert(order.size() == 3);

    const std::unordered_map<std::string, std::vector<std::string>> graph{
        {"cn-db", {"cn-kafka"}},
        {"cn-kafka", {"sg-databricks"}},
    };
    const std::unordered_map<std::string, DataFlowNode> nodes{
        {"cn-db", {"cn-db", "cn", "raw_personal"}},
        {"cn-kafka", {"cn-kafka", "cn", "raw_personal"}},
        {"sg-databricks", {"sg-databricks", "sg", "derived"}},
    };
    const auto path = find_cross_region_violation(
        "cn-db", graph, nodes, {"cn"});
    assert(path.has_value());
    assert(path->size() == 3);

    LruCache cache(1);
    cache.put("risk-rule", "v1");
    assert(cache.get("risk-rule") == std::optional<std::string>("v1"));
    cache.put("other", "v2");
    assert(!cache.get("risk-rule").has_value());

    assert(min_channel_cost(6, {{3, 5}, {2, 4}}) == std::optional<int>(10));
    const auto intervals = merge_intervals({{1, 3}, {2, 5}, {8, 9}});
    assert(intervals.size() == 2);
    assert(intervals[0].start == 1 && intervals[0].end == 5);

    const auto ranking = merge_sharded_top_k({
        {{100, "u-1"}, {80, "u-4"}},
        {{100, "u-2"}, {90, "u-3"}},
    }, 3);
    assert(ranking[0].member == "u-1");
    assert(ranking[1].member == "u-2");
    assert(ranking[2].member == "u-3");

    const auto page = keyset_page(
        {{10, "a"}, {10, "b"}, {11, "c"}}, CursorRecord{10, "a"}, 1);
    assert(page.records.size() == 1 && page.records[0].record_id == "b");
    assert(page.next.has_value() && page.next->record_id == "b");

    return 0;
}
