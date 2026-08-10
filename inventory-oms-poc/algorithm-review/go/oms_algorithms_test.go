package algorithms

import "testing"

func TestAlgorithmExamples(t *testing.T) {
	got := DeduplicateKeys([]string{"a", "a", "b"})
	want := []bool{true, false, true}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("dedup[%d] = %v, want %v", index, got[index], want[index])
		}
	}

	window, err := SlidingWindowAllow([]RequestEvent{
		{Key: "merchant-1", TimestampMs: 0},
		{Key: "merchant-1", TimestampMs: 100},
		{Key: "merchant-1", TimestampMs: 200},
	}, 2, 1000)
	if err != nil || window[2] {
		t.Fatalf("unexpected window result: %v, err=%v", window, err)
	}

	streams := [][]SettlementRecord{
		{{OccurredAtMs: 1, Provider: "wx", Transaction: "w-1"}},
		{{OccurredAtMs: 2, Provider: "ali", Transaction: "a-1"}},
	}
	merged := MergeSettlementStreams(streams)
	if merged[0].Transaction != "w-1" || merged[1].Transaction != "a-1" {
		t.Fatalf("unexpected merge result: %+v", merged)
	}

	order, err := TopologicalOrder(
		[]string{"vpc", "eks", "aurora"},
		[][2]string{{"vpc", "eks"}, {"eks", "aurora"}},
	)
	if err != nil || len(order) != 3 {
		t.Fatalf("unexpected topological result: %v, err=%v", order, err)
	}

	graph := map[string][]string{
		"cn-db":    {"cn-kafka"},
		"cn-kafka": {"sg-databricks"},
	}
	nodes := map[string]DataFlowNode{
		"cn-db":        {Name: "cn-db", Region: "cn", DataClass: "raw_personal"},
		"cn-kafka":     {Name: "cn-kafka", Region: "cn", DataClass: "raw_personal"},
		"sg-databricks": {Name: "sg-databricks", Region: "sg", DataClass: "derived"},
	}
	path, err := FindCrossRegionViolation(
		"cn-db", graph, nodes, map[string]bool{"cn": true},
	)
	if err != nil || len(path) != 3 {
		t.Fatalf("unexpected privacy path: %v, err=%v", path, err)
	}

	cache, err := NewLruCache(1)
	if err != nil {
		t.Fatal(err)
	}
	cache.Put("risk-rule", "v1")
	if value, ok := cache.Get("risk-rule"); !ok || value != "v1" {
		t.Fatalf("cache miss")
	}
	cache.Put("other", "v2")
	if _, ok := cache.Get("risk-rule"); ok {
		t.Fatalf("least-recent item was not evicted")
	}

	if cost, ok := MinChannelCost(6, []ChannelOption{{3, 5}, {2, 4}}); !ok || cost != 10 {
		t.Fatalf("unexpected DP result: %d, %v", cost, ok)
	}

	ranking := MergeShardedTopK([][]RankingEntry{
		{{Score: 100, Member: "u-1"}, {Score: 80, Member: "u-4"}},
		{{Score: 100, Member: "u-2"}, {Score: 90, Member: "u-3"}},
	}, 3)
	if ranking[0].Member != "u-1" || ranking[1].Member != "u-2" || ranking[2].Member != "u-3" {
		t.Fatalf("unexpected ranking result: %+v", ranking)
	}

	records := []CursorRecord{{10, "a"}, {10, "b"}, {11, "c"}}
	page, cursor, err := KeysetPage(records, &CursorRecord{10, "a"}, 1)
	if err != nil || len(page) != 1 || page[0].RecordID != "b" || cursor == nil || cursor.RecordID != "b" {
		t.Fatalf("unexpected keyset page: %+v, cursor=%+v, err=%v", page, cursor, err)
	}
}
