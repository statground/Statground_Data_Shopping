package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/statground/Statground_Data_Shopping/internal/lexicon"
)

func TestBrowserlessGmarketUsesRootContextForDictionaryAdmission(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "active"
		if canceled {
			name = "canceled"
		}
		t.Run(name, func(t *testing.T) {
			previousEnabled, previousMode, previousSource, previousCount := GmarketCollectEnabled, CollectMode, ListSourceMode, RandomKeywordCount
			previousEmpty, previousDetails, previousExcel, previousIngest := AllowEmptyResult, CollectDetailsEnabled, SaveExcelEnabled, IngestMode
			t.Cleanup(func() {
				GmarketCollectEnabled, CollectMode, ListSourceMode, RandomKeywordCount = previousEnabled, previousMode, previousSource, previousCount
				AllowEmptyResult, CollectDetailsEnabled, SaveExcelEnabled, IngestMode = previousEmpty, previousDetails, previousExcel, previousIngest
			})
			GmarketCollectEnabled, CollectMode, ListSourceMode, RandomKeywordCount = true, "search_keywords", "gsearch_ajax", 2
			AllowEmptyResult, CollectDetailsEnabled, SaveExcelEnabled, IngestMode = true, false, false, "none"
			var mu sync.Mutex
			queries := []string{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				sql := string(body)
				mu.Lock()
				queries = append(queries, sql)
				mu.Unlock()
				if strings.HasPrefix(sql, "SELECT keyword,normalized_word,language,sources,snapshot_id,scope,argMax(confidence") {
					json.NewEncoder(w).Encode(map[string]any{"data": []lexicon.Candidate{{Keyword: "사과", NormalizedWord: "사과", Language: "ko", Sources: []string{"shopping_gmarket"}, Confidence: .9, SnapshotID: strings.Repeat("a", 64), Scope: "gmarket"}}})
					return
				}
				// Refuse the selection health gate before any merchant or DB write.
				io.WriteString(w, `{"data":[]}`)
			}))
			defer server.Close()
			t.Setenv("LEXICON_CLICKHOUSE_HTTP_URL", server.URL)
			t.Setenv("LEXICON_CLICKHOUSE_USER", "fixture")
			t.Setenv("LEXICON_CLICKHOUSE_PASSWORD", "fixture-password")
			t.Setenv("LEXICON_CANDIDATES_FILE", "")
			t.Setenv("LEXICON_STATE_DIR", t.TempDir())
			t.Setenv("LEXICON_RUN_ID", "fixture-gmarket-browserless")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if canceled {
				cancel()
			}
			RunGmarketCollection(ctx)
			mu.Lock()
			defer mu.Unlock()
			if canceled {
				if len(queries) != 0 {
					t.Fatal("browserless collection detached from canceled root context")
				}
			} else if len(queries) != 3 || !strings.Contains(queries[2], "system.replicas") {
				t.Fatalf("browserless dictionary admission performed %d read queries", len(queries))
			}
			for _, sql := range queries {
				if !strings.HasPrefix(sql, "SELECT ") {
					t.Fatal("unverified dictionary admission wrote to the database")
				}
			}
		})
	}
}

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
