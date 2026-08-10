package algorithms

import (
	"container/heap"
	"container/list"
	"errors"
	"sort"
)

// RequestEvent is a time-ordered request observation.
type RequestEvent struct {
	Key         string
	TimestampMs int64
}

// DeduplicateKeys returns true only for the first occurrence of each key.
//
// Time: O(n) average. Space: O(u), where u is the number of unique keys.
func DeduplicateKeys(keys []string) []bool {
	seen := make(map[string]struct{}, len(keys))
	result := make([]bool, 0, len(keys))
	for _, key := range keys {
		_, exists := seen[key]
		result = append(result, !exists)
		seen[key] = struct{}{}
	}
	return result
}

// SlidingWindowAllow applies a per-key fixed-size window.
//
// Events for the same key must be non-decreasing by TimestampMs. Each
// timestamp is appended and removed once, so the amortized time is O(n).
// Production should move the atomic state to Redis or the API gateway.
func SlidingWindowAllow(events []RequestEvent, limit int, windowMs int64) ([]bool, error) {
	if limit <= 0 || windowMs <= 0 {
		return nil, errors.New("limit and windowMs must be positive")
	}

	timestamps := make(map[string][]int64)
	heads := make(map[string]int)
	result := make([]bool, 0, len(events))

	for _, event := range events {
		queue := timestamps[event.Key]
		head := heads[event.Key]
		cutoff := event.TimestampMs - windowMs
		for head < len(queue) && queue[head] <= cutoff {
			head++
		}

		allowed := len(queue)-head < limit
		result = append(result, allowed)
		if allowed {
			queue = append(queue, event.TimestampMs)
		}
		timestamps[event.Key] = queue
		heads[event.Key] = head
	}
	return result, nil
}

// SettlementRecord is the minimum provider record needed for reconciliation.
type SettlementRecord struct {
	OccurredAtMs int64
	Provider     string
	Transaction  string
	AmountCents  int64
	Status       string
}

type mergeItem struct {
	record      SettlementRecord
	streamIndex int
	recordIndex int
}

type settlementHeap []mergeItem

func (h settlementHeap) Len() int { return len(h) }
func (h settlementHeap) Less(i, j int) bool {
	left, right := h[i].record, h[j].record
	if left.OccurredAtMs != right.OccurredAtMs {
		return left.OccurredAtMs < right.OccurredAtMs
	}
	if left.Provider != right.Provider {
		return left.Provider < right.Provider
	}
	return left.Transaction < right.Transaction
}
func (h settlementHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *settlementHeap) Push(value interface{}) {
	*h = append(*h, value.(mergeItem))
}
func (h *settlementHeap) Pop() interface{} {
	old := *h
	last := len(old) - 1
	value := old[last]
	*h = old[:last]
	return value
}

// MergeSettlementStreams merges K individually sorted provider streams.
//
// Time: O(n log k); heap space: O(k).
func MergeSettlementStreams(streams [][]SettlementRecord) []SettlementRecord {
	queue := &settlementHeap{}
	heap.Init(queue)
	for streamIndex, stream := range streams {
		if len(stream) > 0 {
			heap.Push(queue, mergeItem{stream[0], streamIndex, 0})
		}
	}

	result := make([]SettlementRecord, 0)
	for queue.Len() > 0 {
		item := heap.Pop(queue).(mergeItem)
		result = append(result, item.record)

		nextIndex := item.recordIndex + 1
		if nextIndex < len(streams[item.streamIndex]) {
			heap.Push(queue, mergeItem{
				streams[item.streamIndex][nextIndex],
				item.streamIndex,
				nextIndex,
			})
		}
	}
	return result
}

// RankingEntry is one member from a descending leaderboard shard.
type RankingEntry struct {
	Score  int64
	Member string
}

type rankingItem struct {
	entry       RankingEntry
	shardIndex  int
	entryIndex  int
}

type rankingHeap []rankingItem

func (h rankingHeap) Len() int { return len(h) }
func (h rankingHeap) Less(i, j int) bool {
	if h[i].entry.Score != h[j].entry.Score {
		return h[i].entry.Score > h[j].entry.Score
	}
	if h[i].entry.Member != h[j].entry.Member {
		return h[i].entry.Member < h[j].entry.Member
	}
	return h[i].shardIndex < h[j].shardIndex
}
func (h rankingHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *rankingHeap) Push(value interface{}) { *h = append(*h, value.(rankingItem)) }
func (h *rankingHeap) Pop() interface{} {
	old := *h
	last := len(old) - 1
	value := old[last]
	*h = old[:last]
	return value
}

// MergeShardedTopK merges already sorted per-shard leaderboards.
// Time: O(K log S), where S is the number of shards.
func MergeShardedTopK(shards [][]RankingEntry, limit int) []RankingEntry {
	if limit <= 0 {
		return nil
	}
	queue := &rankingHeap{}
	heap.Init(queue)
	for shardIndex, shard := range shards {
		if len(shard) > 0 {
			heap.Push(queue, rankingItem{shard[0], shardIndex, 0})
		}
	}

	result := make([]RankingEntry, 0, limit)
	for queue.Len() > 0 && len(result) < limit {
		item := heap.Pop(queue).(rankingItem)
		result = append(result, item.entry)
		nextIndex := item.entryIndex + 1
		if nextIndex < len(shards[item.shardIndex]) {
			heap.Push(queue, rankingItem{
				shards[item.shardIndex][nextIndex], item.shardIndex, nextIndex,
			})
		}
	}
	return result
}

// CursorRecord is a stable ascending keyset-pagination key.
type CursorRecord struct {
	SortKey  int64
	RecordID string
}

// KeysetPage returns records after a cursor and the next cursor, if any.
// The input must be sorted by SortKey then RecordID. A database adapter should
// apply the same tuple predicate with a matching composite index.
func KeysetPage(records []CursorRecord, after *CursorRecord, limit int) ([]CursorRecord, *CursorRecord, error) {
	if limit <= 0 {
		return nil, nil, errors.New("limit must be positive")
	}
	start := 0
	if after != nil {
		start = sort.Search(len(records), func(index int) bool {
			current := records[index]
			return current.SortKey > after.SortKey ||
				(current.SortKey == after.SortKey && current.RecordID > after.RecordID)
		})
	}
	end := start + limit
	if end > len(records) {
		end = len(records)
	}
	page := append([]CursorRecord(nil), records[start:end]...)
	if end == len(records) || len(page) == 0 {
		return page, nil, nil
	}
	next := page[len(page)-1]
	return page, &next, nil
}

// TopologicalOrder returns an order for edges prerequisite -> dependent.
//
// Time: O(V+E). If not all nodes are emitted, there is a dependency cycle.
func TopologicalOrder(nodes []string, dependencies [][2]string) ([]string, error) {
	nodeSet := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		nodeSet[node] = struct{}{}
	}

	adjacency := make(map[string][]string, len(nodes))
	indegree := make(map[string]int, len(nodes))
	for node := range nodeSet {
		adjacency[node] = []string{}
		indegree[node] = 0
	}
	for _, dependency := range dependencies {
		before, after := dependency[0], dependency[1]
		if _, ok := nodeSet[before]; !ok {
			return nil, errors.New("unknown prerequisite")
		}
		if _, ok := nodeSet[after]; !ok {
			return nil, errors.New("unknown dependent")
		}
		adjacency[before] = append(adjacency[before], after)
		indegree[after]++
	}

	ready := make([]string, 0)
	for node, degree := range indegree {
		if degree == 0 {
			ready = append(ready, node)
		}
	}
	sort.Strings(ready)

	result := make([]string, 0, len(nodes))
	for len(ready) > 0 {
		node := ready[0]
		ready = ready[1:]
		result = append(result, node)
		dependents := adjacency[node]
		sort.Strings(dependents)
		for _, dependent := range dependents {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				ready = append(ready, dependent)
			}
		}
		sort.Strings(ready)
	}

	if len(result) != len(nodeSet) {
		return nil, errors.New("dependency graph contains a cycle")
	}
	return result, nil
}

// DataFlowNode describes governance metadata attached to a graph node.
type DataFlowNode struct {
	Name      string
	Region    string
	DataClass string
}

// FindCrossRegionViolation returns a shortest path to a disallowed region.
//
// Time: O(V+E). Space: O(V). It reports a path; policy code must decide what
// action to take.
func FindCrossRegionViolation(
	start string,
	graph map[string][]string,
	nodes map[string]DataFlowNode,
	allowedRegions map[string]bool,
) ([]string, error) {
	if _, ok := nodes[start]; !ok {
		return nil, errors.New("unknown start node")
	}

	queue := []string{start}
	parent := map[string]string{start: ""}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		for _, neighbor := range graph[current] {
			if _, ok := nodes[neighbor]; !ok {
				return nil, errors.New("unknown graph node")
			}
			if _, seen := parent[neighbor]; seen {
				continue
			}
			parent[neighbor] = current
			if !allowedRegions[nodes[neighbor].Region] {
				path := []string{}
				for cursor := neighbor; cursor != ""; cursor = parent[cursor] {
					path = append(path, cursor)
				}
				for left, right := 0, len(path)-1; left < right; left, right = left+1, right-1 {
					path[left], path[right] = path[right], path[left]
				}
				return path, nil
			}
			queue = append(queue, neighbor)
		}
	}
	return nil, nil
}

type cacheEntry struct {
	key   string
	value string
}

// LruCache is a local, non-authoritative HashMap + doubly linked list cache.
type LruCache struct {
	capacity int
	items    map[string]*list.Element
	order    *list.List
}

// NewLruCache constructs an O(1) average get/put cache.
func NewLruCache(capacity int) (*LruCache, error) {
	if capacity <= 0 {
		return nil, errors.New("capacity must be positive")
	}
	return &LruCache{
		capacity: capacity,
		items:    make(map[string]*list.Element, capacity),
		order:    list.New(),
	}, nil
}

// Get returns a value and moves the key to the most-recent position.
func (c *LruCache) Get(key string) (string, bool) {
	element, ok := c.items[key]
	if !ok {
		return "", false
	}
	c.order.MoveToBack(element)
	return element.Value.(cacheEntry).value, true
}

// Put inserts or replaces a value, evicting the least-recent key.
func (c *LruCache) Put(key, value string) {
	if element, ok := c.items[key]; ok {
		element.Value = cacheEntry{key, value}
		c.order.MoveToBack(element)
		return
	}
	element := c.order.PushBack(cacheEntry{key, value})
	c.items[key] = element
	if c.order.Len() > c.capacity {
		oldest := c.order.Front()
		delete(c.items, oldest.Value.(cacheEntry).key)
		c.order.Remove(oldest)
	}
}

// ChannelOption is a discrete package/channel option for DP planning.
type ChannelOption struct {
	Units    int
	FeeCents int
}

// MinChannelCost computes an exact-target unbounded DP.
//
// Time: O(target * options). Space: O(target). It is a planning example, not
// a payment authorization or provider routing decision.
func MinChannelCost(target int, options []ChannelOption) (int, bool) {
	if target < 0 {
		return 0, false
	}
	infinity := int(^uint(0) >> 1)
	dp := make([]int, target+1)
	for index := 1; index <= target; index++ {
		dp[index] = infinity
		for _, option := range options {
			if option.Units <= 0 || option.FeeCents < 0 {
				return 0, false
			}
			if option.Units <= index && dp[index-option.Units] != infinity {
				candidate := dp[index-option.Units] + option.FeeCents
				if candidate < dp[index] {
					dp[index] = candidate
				}
			}
		}
	}
	if dp[target] == infinity {
		return 0, false
	}
	return dp[target], true
}
