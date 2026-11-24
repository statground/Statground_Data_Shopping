package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/statground/Statground_Data_Shopping/internal/lexicon"
)

func TestAdpickKeywordPlanIsBalancedScopedAndStable(t *testing.T) {
	t.Setenv("LEXICON_STATE_DIR", t.TempDir())
	t.Setenv("LEXICON_RUN_ID", "adpick-fixture-run")
	t.Setenv("LEXICON_CLICKHOUSE_HTTP_URL", "file://fixture")
	path := filepath.Join(t.TempDir(), "published.json")
	t.Setenv("LEXICON_CANDIDATES_FILE", path)
	snapshot := strings.Repeat("a", 64)
	candidate := func(word, scope, source string) lexicon.Candidate {
		return lexicon.Candidate{Keyword: word, NormalizedWord: word, Scope: scope, Sources: []string{source}, Language: "ko", Confidence: .9, SnapshotID: snapshot}
	}
	pool := []lexicon.Candidate{
		candidate("호텔", "adpick-travel", "book_kakao"),
		candidate("항공사", "adpick-travel", "book_kakao"),
		candidate("박물관", "adpick-travel", "book_kakao"),
		candidate("펜션", "adpick-travel", "book_kakao"),
		candidate("프로그래밍", "adpick-services", "inflearn_title"),
		candidate("디자인", "adpick-services", "r_manual"),
		candidate("작문", "adpick-services", "book_kakao"),
		candidate("마케팅", "adpick-services", "shopping_adpick_product"),
		candidate("호텔", "adpick-services", "book_kakao"),
	}
	body, err := json.Marshal(map[string]any{"candidates": pool, "snapshot_id": snapshot, "generated_at": time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	queries, selections, err := buildAdpickKeywordPlan(context.Background(), defaultAdpickQueries)
	if err != nil || len(queries) != 10 || len(selections) != 10 {
		t.Fatalf("daily plan: length %d/%d, error %v", len(queries), len(selections), err)
	}
	counts := map[string]int{}
	for i, q := range queries {
		selection := selections[i]
		if !validAdpickCategory(q.Vertical, q.Category) || selection.Keyword != q.Keyword || q.Keyword == "마케팅" {
			t.Fatalf("wrong category, source or word: %+v %+v", q, selection)
		}
		counts[q.Vertical+"/"+selection.Origin]++
	}
	want := map[string]int{"travel/curated": 3, "travel/lexicon": 3, "services/curated": 2, "services/lexicon": 2}
	if !reflect.DeepEqual(counts, want) {
		t.Fatalf("schedule allocation %v", counts)
	}
	retryQueries, retrySelections, err := buildAdpickKeywordPlan(context.Background(), defaultAdpickQueries)
	if err != nil || !reflect.DeepEqual(retryQueries, queries) || !reflect.DeepEqual(retrySelections, selections) {
		t.Fatalf("retry changed the plan: %v", err)
	}
	t.Setenv("LEXICON_RUN_ID", "adpick-other-fixture-run")
	t.Setenv("LEXICON_CANDIDATES_FILE", filepath.Join(t.TempDir(), "missing.json"))
	if queries, _, err := buildAdpickKeywordPlan(context.Background(), defaultAdpickQueries); err == nil || len(queries) != 0 {
		t.Fatal("enabled plan proceeded without a committed pool")
	}
}

func TestAdpickUsesSelectedWordWithoutCopyingItIntoHarvest(t *testing.T) {
	t.Setenv("ADPICK_HARVEST_FILE", "")
	t.Setenv("ADPICK_COVERAGE_REPORT", "")
	client := adpickFixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		data := any([]adpickMall{{Code: "T", Name: "트립닷컴"}})
		if strings.HasSuffix(r.URL.Path, "/search") {
			if r.URL.Query().Get("q") != "박물관" || r.URL.Query().Get("limit") != "20" {
				t.Error("selected word or provider budget changed")
			}
			data = []adpickOffer{{Code: "T", Name: "Trip.com", Title: "입장 예약", Link: "https://bitl.bz/ticket"}}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": data})
	})
	client.privateKeywords = true
	client.coverage = newAdpickCoverage(1)
	now := NowKST()
	records, err := collectAdpickCatalog(context.Background(), client, []adpickQuery{{"travel", "experiences", "박물관"}}, 20, adpickTestRun, now)
	if err != nil || len(records) != 2 || len(client.coverage.Queries) != 1 {
		t.Fatalf("private collection failed: %v", err)
	}
	if records[1].SearchKeyword != "" || client.coverage.Queries[0].Keyword != "" {
		t.Fatal("private selected word leaked into report or recovery data")
	}
	if err := validateAdpickReplayRecords(records, adpickTestRun, now); err != nil {
		t.Fatalf("recovery rejected private keyword harvest: %v", err)
	}
}
