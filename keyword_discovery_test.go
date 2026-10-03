package main

import (
	"strings"
	"testing"

	"github.com/statground/Statground_Data_Shopping/internal/lexicon"
)

func TestShoppingKeywordBudgetCannotExpandOrBecomeUnlimited(t *testing.T) {
	for _, test := range []struct {
		scope       string
		count, want int
	}{
		{"gmarket", 0, 0}, {"gmarket", -1, 0}, {"gmarket", 1, 0}, {"gmarket", 5, 4},
		{"gmarket", 50000, 20}, {"kurly", 120, 120}, {"kurly", 121, 120}, {"kurly", 0, 0},
	} {
		if got := shoppingKeywordBudget(test.scope, test.count); got != test.want {
			t.Errorf("%s count=%d got=%d want=%d", test.scope, test.count, got, test.want)
		}
	}
}

func TestShoppingInterleavesRecoveredLedgerBeforeTargetStops(t *testing.T) {
	recovered := []lexicon.Selection{{Keyword: "curated-1", Origin: "curated"}, {Keyword: "curated-2", Origin: "curated"}, {Keyword: "dictionary-1", Origin: "lexicon"}, {Keyword: "dictionary-2", Origin: "lexicon"}}
	selected := shoppingInterleaveSelections(recovered)
	for i, row := range selected {
		want := "curated"
		if i%2 == 1 {
			want = "lexicon"
		}
		if row.Origin != want {
			t.Fatalf("recovered plan is not paired at %d: %+v", i, selected)
		}
	}
}

func TestShoppingInsightGateTargetsOnlyPublishedDataAndStripsProviderKey(t *testing.T) {
	t.Setenv("CLICKHOUSE_PRESSURE_GATE_TARGETS", "local:unrelated.table")
	t.Setenv("ADPICK_BIZ_API_KEY", "fixture-secret")
	tables := insightRefreshTables{snapshot: "Data_Shopping_Service.shopping_price_insight_snapshot", keywordSearch: "Data_Shopping_Service.shopping_keyword_search_mart", publishedBatch: "Data_Shopping_Service.shopping_price_insight_published_batch"}
	var targets string
	for _, entry := range shoppingInsightGateEnv(tables) {
		if strings.HasPrefix(entry, "ADPICK_BIZ_API_KEY=") {
			t.Fatal("affiliate key reached DB gate")
		}
		if strings.HasPrefix(entry, "CLICKHOUSE_PRESSURE_GATE_TARGETS=") {
			if targets != "" {
				t.Fatal("duplicate target override")
			}
			targets = entry
		}
	}
	want := "CLICKHOUSE_PRESSURE_GATE_TARGETS=local:" + tables.snapshot + ",local:" + tables.keywordSearch + ",local:" + tables.publishedBatch
	if targets != want {
		t.Fatalf("gate targets=%s", targets)
	}
}
