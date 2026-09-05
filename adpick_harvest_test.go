package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAdpickDirectoryProfileCollectsBothVerticalsWithoutSearching(t *testing.T) {
	t.Setenv("ADPICK_QUERY_PROFILE", "directory")
	t.Setenv("ADPICK_SEARCH_QUERIES_JSON", "")
	queries, err := adpickQueriesFromEnv()
	if err != nil || len(queries) != 0 {
		t.Fatalf("directory profile: %v %v", queries, err)
	}
	requests := 0
	client := adpickFixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		requests++
		if !strings.HasSuffix(r.URL.Path, "/malls") {
			t.Error("directory profile sent a search request")
		}
		fmt.Fprint(w, `{"success":true,"data":[{"cp_code":"T","name":"트립닷컴","commissionlink":"https://bitl.bz/travel"},{"cp_code":"K","name":"크몽","commissionlink":"https://bitl.bz/service"},{"cp_code":"B","name":"교보문고"}]}`)
	})
	client.coverage = newAdpickCoverage(0)
	client.directoryOnly = true
	records, err := collectAdpickCatalog(context.Background(), client, queries, 20, adpickTestRun, NowKST())
	if err != nil || requests != 1 || len(records) != 2 || !client.coverage.DiscoveryComplete || !client.coverage.CollectionComplete || client.coverage.QueriesCompleted != 0 {
		t.Fatalf("directory requests=%d records=%d coverage=%+v err=%v", requests, len(records), client.coverage, err)
	}
	for _, row := range records {
		if row.RecordType != "merchant" || row.CategorySlug != "" || row.PriceKRW != nil {
			t.Fatal("directory row fabricated a product")
		}
	}
	t.Setenv("ADPICK_SEARCH_QUERIES_JSON", `[{"vertical":"travel","category":"stays","keyword":"hotel"}]`)
	if _, err := adpickQueriesFromEnv(); err == nil {
		t.Fatal("directory silently accepted search overrides")
	}
}

func TestAdpickHarvestCheckpointRetainsRecordsBeforePublicationAndRefusesSecret(t *testing.T) {
	path := filepath.Join(t.TempDir(), "harvest.json")
	t.Setenv("ADPICK_HARVEST_FILE", path)
	report := newAdpickCoverage(2)
	report.RunUUID = adpickTestRun
	report.QueriesCompleted = 1
	rows := []adpickCatalogRecord{{RecordType: "merchant", Title: "Public merchant", AffiliateURL: "https://bitl.bz/merchant"}}
	if err := writeAdpickHarvest(rows, report, "private-api-key"); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var saved struct {
		Schema    string                `json:"schema"`
		Completed int                   `json:"queries_completed"`
		Hash      string                `json:"records_sha256"`
		Records   []adpickCatalogRecord `json:"records"`
	}
	if json.Unmarshal(before, &saved) != nil || saved.Schema != "adpick.harvest.v1" || saved.Completed != 1 || len(saved.Records) != 1 || saved.Records[0].AffiliateURL != rows[0].AffiliateURL {
		t.Fatal("validated records were not checkpointed")
	}
	recordsJSON, _ := json.Marshal(rows)
	if saved.Hash != fmt.Sprintf("%x", sha256.Sum256(recordsJSON)) {
		t.Fatal("checkpoint digest mismatched")
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0600 {
		t.Fatal("checkpoint is not private")
	}
	rows[0].Title = "private-api-key"
	if err := writeAdpickHarvest(rows, report, "private-api-key"); err == nil {
		t.Fatal("credential reached checkpoint")
	}
	after, _ := os.ReadFile(path)
	if string(after) != string(before) {
		t.Fatal("failed checkpoint replaced previous valid harvest")
	}
}

func TestAdpickPublicationGateOnlyRechecksPressureAndKeepsBoundedWait(t *testing.T) {
	calls := 0
	waits := []time.Duration{}
	err := waitAdpickPublicationGate(context.Background(), func(context.Context) ([]byte, error) {
		calls++
		if calls < 3 {
			return []byte("::error::ClickHouse write pressure gate deferred this run: iowait_normalized=0.560\n"), errors.New("gate")
		}
		return []byte("ClickHouse write pressure gate ok local_targets=4\n"), nil
	}, func(_ context.Context, d time.Duration) error { waits = append(waits, d); return nil })
	if err != nil || calls != 3 || len(waits) != 2 || waits[0] != 30*time.Second || waits[1] != 60*time.Second {
		t.Fatalf("calls=%d waits=%v err=%v", calls, waits, err)
	}
	calls = 0
	err = waitAdpickPublicationGate(context.Background(), func(context.Context) ([]byte, error) {
		calls++
		return []byte("::error::ClickHouse write pressure gate failed closed: secret https://private.invalid\n"), errors.New("configuration")
	}, func(context.Context, time.Duration) error { t.Fatal("configuration failure retried"); return nil })
	if err == nil || calls != 1 || strings.Contains(err.Error(), "secret") {
		t.Fatal("unsafe gate failure")
	}
	if adpickGateDiagnosticPattern.MatchString("::error::ClickHouse write pressure gate deferred this run: https://secret.invalid") {
		t.Fatal("gate diagnostic accepted URL")
	}
}

func TestAdpickCompletedSearchSurvivesLaterFailureAndRejectsHalfQuery(t *testing.T) {
	client := adpickFixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/malls") {
			fmt.Fprint(w, `{"success":true,"data":[{"cp_code":"T","name":"트립닷컴"}]}`)
			return
		}
		if r.URL.Query().Get("q") == "first" {
			fmt.Fprint(w, `{"success":true,"data":[{"cp_code":"T","cp_name":"트립닷컴","title":"Verified hotel","commissionlink":"https://bitl.bz/one"}]}`)
			return
		}
		fmt.Fprint(w, `{"success":true,"data":[{"cp_code":"T","cp_name":"트립닷컴","title":"Incomplete query hotel","commissionlink":"https://bitl.bz/two"},{"cp_code":"T","cp_name":"쿠팡","title":"Identity mismatch","commissionlink":"https://bitl.bz/three"}]}`)
	})
	client.coverage = newAdpickCoverage(2)
	queries := []adpickQuery{{"travel", "stays", "first"}, {"travel", "stays", "second"}}
	records, err := collectAdpickCatalog(context.Background(), client, queries, 20, adpickTestRun, NowKST())
	if err == nil || len(records) != 2 || records[1].Title != "Verified hotel" || !client.coverage.DiscoveryComplete || client.coverage.CollectionComplete || client.coverage.QueriesCompleted != 1 || client.coverage.OffersByVertical["travel"] != 1 {
		t.Fatalf("harvest=%+v coverage=%+v err=%v", records, client.coverage, err)
	}
}

func TestAdpickSearchDeadlineHasIndependentPublicationBudget(t *testing.T) {
	collect, cancel := context.WithCancel(context.Background())
	cancel()
	report := newAdpickCoverage(2)
	report.DiscoveryComplete = true
	called := false
	err := finishAdpickHarvest(context.Background(), report, []adpickCatalogRecord{{RecordType: "merchant"}}, collect.Err(), func(ctx context.Context, rows []adpickCatalogRecord) error {
		called = true
		deadline, ok := ctx.Deadline()
		if ctx.Err() != nil || !ok || time.Until(deadline) > 10*time.Minute || time.Until(deadline) < 9*time.Minute {
			t.Fatal("search cancellation consumed publication budget")
		}
		return nil
	})
	if !called || !report.PublicationComplete || report.CollectionComplete || !errors.Is(err, context.Canceled) {
		t.Fatalf("partial state hidden: %+v %v", report, err)
	}
	called = false
	if err := finishAdpickHarvest(collect, report, []adpickCatalogRecord{{}}, nil, func(context.Context, []adpickCatalogRecord) error { called = true; return nil }); err == nil || called {
		t.Fatal("cancelled caller was ignored")
	}
}

func TestAdpickEmptyHarvestRetainsVerifiedPriorOffersAndFreshObservationWins(t *testing.T) {
	merchant := adpickCatalogRecord{Source: "adpick_biz", Vertical: "travel", RecordType: "merchant", MerchantCode: "T", MerchantKey: "trip_com", CollectRunUUID: adpickTestRun, Version: 42, CollectedAt: "2026-09-06 01:02:03.004", ItemKey: "merchant"}
	old := merchant
	old.RecordType = "offer"
	old.CategorySlug = "stays"
	old.Title = "Observed hotel"
	old.AffiliateURL = "https://bitl.bz/hotel"
	old.ItemKey = fmt.Sprintf("%x", sha256.Sum256([]byte("T\x1f"+old.AffiliateURL)))
	old.CollectedAt = "2026-09-01 01:02:03"
	old.Version = 1
	merged, n, evicted, err := mergeAdpickHarvest([]adpickCatalogRecord{merchant}, []adpickCatalogRecord{old})
	if err != nil || len(merged) != 2 || n != 1 || evicted != 0 {
		t.Fatalf("prior offer erased: %v %d %d %v", merged, n, evicted, err)
	}
	for _, row := range merged {
		if row.RecordType == "offer" && (row.CollectedAt != "2026-09-01 01:02:03.000" || row.Version != 42) {
			t.Fatal("observation date rewritten")
		}
	}
	fresh := old
	fresh.Title = "Updated hotel"
	fresh.CollectedAt = merchant.CollectedAt
	fresh.Version = 42
	merged, n, _, err = mergeAdpickHarvest([]adpickCatalogRecord{merchant, fresh}, []adpickCatalogRecord{old})
	if err != nil || len(merged) != 2 || n != 0 {
		t.Fatal("fresh identity duplicated")
	}
	old.AffiliateURL = "https://secret@bitl.bz/hotel"
	if _, _, _, err = mergeAdpickHarvest([]adpickCatalogRecord{merchant}, []adpickCatalogRecord{old}); err == nil {
		t.Fatal("unsafe stored offer promoted")
	}
}

func TestAdpickDiagnosticsPreserveFieldTypesAndRedactSecretsAndURLs(t *testing.T) {
	var offer adpickOffer
	if err := json.Unmarshal([]byte(`{"cp_name":"Mall","title":"fixture-secret https://private.invalid/x","product_name":"alternate"}`), &offer); err != nil {
		t.Fatal(err)
	}
	s := adpickQueryCoverage{MerchantCodes: map[string]int{}}
	s.observe(offer, "fixture-secret")
	b, _ := json.Marshal(s)
	if strings.Contains(string(b), "fixture-secret") || strings.Contains(string(b), "private.invalid") || s.MerchantCodes["<missing>"] != 1 || s.Samples[0].Fields["cp_code"] != "missing" || s.Samples[0].Fields["product_name"] != "string" {
		t.Fatalf("unsafe or incomplete diagnostics: %s", b)
	}
	t.Setenv("ADPICK_QUERY_PROFILE", "diagnostic")
	t.Setenv("ADPICK_SEARCH_QUERIES_JSON", "")
	queries, err := adpickQueriesFromEnv()
	if err != nil || len(queries) != 16 || queries[14].Vertical != "diagnostic" {
		t.Fatalf("diagnostic profile: %v %v", queries, err)
	}
	t.Setenv("ADPICK_QUERY_PROFILE", "focused")
	queries, err = adpickQueriesFromEnv()
	if err != nil || len(queries) != 60 {
		t.Fatal("focused profile not bounded at60")
	}
}

func TestAdpickRetailControlNeverBecomesTravelOffer(t *testing.T) {
	client := adpickFixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/malls") {
			fmt.Fprint(w, `{"success":true,"data":[{"cp_code":"T","name":"트립닷컴"}]}`)
			return
		}
		fmt.Fprint(w, `{"success":true,"data":[{"cp_code":"T","cp_name":"트립닷컴","title":"control","commissionlink":"https://bitl.bz/control"}]}`)
	})
	queries := []adpickQuery{{"travel", "stays", "hotel"}, {"diagnostic", "retail_control", "노트북"}}
	client.coverage = newAdpickCoverage(2)
	rows, err := collectAdpickCatalog(context.Background(), client, queries, 20, adpickTestRun, NowKST())
	if err != nil || len(rows) != 2 || client.coverage.Queries[1].NewOffers != 0 || client.coverage.Queries[1].Excluded != 1 {
		t.Fatalf("control became offer: %v %v", rows, err)
	}
}
