package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func adpickFakeClock(client *adpickClient) *time.Time {
	now := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	client.now = func() time.Time { return now }
	client.sleep = func(ctx context.Context, delay time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if delay > 0 {
			now = now.Add(delay)
		}
		return nil
	}
	return &now
}

func TestAdpickRetryAfterAndOneQuotaAcrossEndpoints(t *testing.T) {
	calls := 0
	client := adpickFixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		switch calls {
		case 2:
			w.Header().Set("Retry-After", "12")
			w.WriteHeader(429)
		case 3:
			w.Header().Set("Retry-After", "Sun, 06 Sep 2026 00:00:50 GMT")
			w.WriteHeader(503)
		default:
			fmt.Fprint(w, `{"success":true,"data":[]}`)
		}
	})
	client.interval = adpickSearchInterval
	clock := adpickFakeClock(client)
	if err := client.request(context.Background(), "malls", nil, &[]adpickMall{}); err != nil {
		t.Fatal(err)
	}
	if err := client.request(context.Background(), "search", url.Values{"q": {"호텔"}}, &[]adpickOffer{}); err != nil {
		t.Fatal(err)
	}
	if clock.Sub(time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)) != 50*time.Second || calls != 4 || client.retries != 2 {
		t.Fatalf("pacing/retry=%v calls=%d retries=%d", clock, calls, client.retries)
	}
	if err := client.request(context.Background(), "malls", nil, &[]adpickMall{}); err != nil {
		t.Fatal(err)
	}
	if clock.Sub(time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)) != 50*time.Second+adpickSearchInterval {
		t.Fatal("malls bypassed shared API quota")
	}
}

func TestAdpickRetriesAreBoundedAndErrorsRedacted(t *testing.T) {
	for _, status := range []int{401, 403, 429, 501, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			client := adpickFixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				fmt.Fprint(w, "fixture-secret https://biz.adpick.co.kr/api/fixture-secret")
			})
			adpickFakeClock(client)
			err := client.request(context.Background(), "search", nil, &[]adpickOffer{})
			want := 3
			if status == 401 || status == 403 {
				want = 1
			}
			if err == nil || client.requests != want || strings.Contains(err.Error(), "fixture-secret") || strings.Contains(err.Error(), "https:") {
				t.Fatalf("requests=%d err=%v", client.requests, err)
			}
		})
	}
	client := adpickFixtureClient(t, func(w http.ResponseWriter, r *http.Request) { t.Error("request budget bypassed") })
	client.requests = adpickMaximumRequests
	if err := client.request(context.Background(), "malls", nil, &[]adpickMall{}); err == nil {
		t.Fatal("accepted exhausted request budget")
	}
	for _, value := range []string{"301", "9999999999999999", "-1", "secret"} {
		if _, err := adpickRetryDelay(value, time.Now(), 0); err == nil || strings.Contains(err.Error(), value) {
			t.Fatalf("retry delay=%q err=%v", value, err)
		}
	}
}

func TestExpandedAdpickQueriesAreCompleteBoundedAndValidated(t *testing.T) {
	t.Setenv("ADPICK_QUERY_PROFILE", "expanded")
	t.Setenv("ADPICK_SEARCH_QUERIES_JSON", "")
	queries, err := adpickQueriesFromEnv()
	if err != nil || len(queries) != 240 {
		t.Fatalf("queries=%d err=%v", len(queries), err)
	}
	counts := map[string]int{}
	for _, q := range queries {
		counts[q.Vertical+"/"+q.Category]++
	}
	if len(counts) != 8 {
		t.Fatalf("categories=%v", counts)
	}
	for category, count := range counts {
		if count != 30 {
			t.Fatalf("%s=%d", category, count)
		}
	}
	queries = append(queries, adpickQuery{"travel", "stays", "overflow"})
	raw, _ := json.Marshal(queries)
	t.Setenv("ADPICK_SEARCH_QUERIES_JSON", string(raw))
	if _, err := adpickQueriesFromEnv(); err == nil {
		t.Fatal("accepted oversized query plan")
	}
	if safeAdpickURL("https://link.adpick.co.kr/verified-offer", true) == "" {
		t.Fatal("documented commissionlink rejected")
	}
	for _, link := range []string{"https://link.adpick.co.kr.evil.com/x", "https://secret@link.adpick.co.kr/x", "https://link.adpick.co.kr/x?api_key=secret"} {
		if safeAdpickURL(link, true) != "" {
			t.Fatal("unsafe documented-host lookalike accepted")
		}
	}
}

func TestAdpickCoverageCountsOnlyEligibleUniqueOffersAndSurvivesEmpty(t *testing.T) {
	client := adpickFixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/malls") {
			fmt.Fprint(w, `{"success":true,"data":[{"cp_code":"T","name":"트립닷컴"}]}`)
			return
		}
		fmt.Fprint(w, `{"success":true,"data":[{"cp_code":"T","title":"hotel","commissionlink":"https://link.adpick.co.kr/one"},{"cp_code":"T","title":"duplicate","commissionlink":"https://link.adpick.co.kr/one"},{"cp_code":"X","title":"excluded","commissionlink":"https://bitl.bz/other"},{"cp_code":"T","title":"bad","commissionlink":"https://evil.com/x"}]}`)
	})
	report := newAdpickCoverage(1)
	client.coverage = report
	path := filepath.Join(t.TempDir(), "coverage.json")
	t.Setenv("ADPICK_COVERAGE_REPORT", path)
	records, err := collectAdpickCatalog(context.Background(), client, defaultAdpickQueries[:1], 20, adpickTestRun, NowKST())
	if err != nil || len(records) != 2 {
		t.Fatalf("records=%v err=%v", records, err)
	}
	if err := writeAdpickCoverage(report, client); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "https:") || strings.Contains(string(body), "fixture-secret") {
		t.Fatal("coverage exposed source URL or credential")
	}
	stats := report.Queries[0]
	if stats.NewOffers != 1 || stats.Duplicates != 1 || stats.Excluded != 1 || stats.Invalid != 1 || report.TargetMet || report.PublicationComplete || !report.CollectionComplete || report.Merchants["trip_com"].Offers != 1 {
		t.Fatalf("coverage=%+v stats=%+v", report, stats)
	}
}

func TestAdpickOnlyDedicatedWorkflowOwnsAPICollection(t *testing.T) {
	retail, _ := os.ReadFile(".github/workflows/gmarket-crawl.yml")
	catalog, _ := os.ReadFile(".github/workflows/adpick-catalog.yml")
	if strings.Contains(string(retail), "adpick_catalog") || strings.Contains(string(retail), "ADPICK_BIZ_API_KEY") {
		t.Fatal("duplicate legacy Adpick scheduling owner")
	}
	for _, workflow := range []string{string(retail), string(catalog)} {
		if !strings.Contains(workflow, "group: statground-shopping-clickhouse-writer-replayer\n  cancel-in-progress: false") {
			t.Fatal("shared durable writer concurrency missing")
		}
	}
	if strings.Contains(string(catalog), "secrets.ADPICK_BIZ_API_KEY") || !strings.Contains(string(catalog), "ADPICK_BIZ_API_KEY: ${{ secrets.ADPICK_API }}") {
		t.Fatal("wrong authorized API secret mapping")
	}
}

func TestAdpickPublicationGateUsesExactWriterEndpointAndTargets(t *testing.T) {
	t.Setenv("CLICKHOUSE_HOST", "https://wrong.example")
	t.Setenv("CH_HOST", "https://other.example")
	t.Setenv("ADPICK_BIZ_API_KEY", "fixture-secret")
	t.Setenv("CLICKHOUSE_PRESSURE_GATE_TARGETS", "local:Other.Table")
	pub := &ClickHouseRawPublisher{cfg: ClickHouseRawConfig{URL: "https://fixture.example:8443/", User: "fixture", Password: "db-fixture", DirectEndpointHostname: "clickhouse-s1-r1", OutboxTable: defaultShoppingRawOutboxTable}}
	env := map[string]string{}
	for _, entry := range adpickPublicationGateEnv(pub) {
		key, value, _ := strings.Cut(entry, "=")
		env[key] = value
	}
	if env["CLICKHOUSE_HOST"] != pub.cfg.URL || env["CH_HOST"] != pub.cfg.URL || env["CLICKHOUSE_PORT"] != "" || env["CH_PORT"] != "" || env["ADPICK_BIZ_API_KEY"] != "" {
		t.Fatal("gate endpoint/credential boundary drift")
	}
	for _, table := range []string{adpickRawTable, adpickSnapshotTable, adpickPublishedTable, defaultShoppingRawOutboxTable} {
		if !strings.Contains(env["CLICKHOUSE_PRESSURE_GATE_TARGETS"], "local:"+table) {
			t.Fatal("exact write target missing")
		}
	}
	if strings.Contains(env["CLICKHOUSE_PRESSURE_GATE_TARGETS"], "Other") {
		t.Fatal("unrelated gate target inherited")
	}
}
