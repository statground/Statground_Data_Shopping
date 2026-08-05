package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestTopInsightProductsInterleavesProviders(t *testing.T) {
	products := []insightProduct{}
	for i := 0; i < 20; i++ {
		products = append(products, insightProduct{
			Provider:    "kurly",
			ProductCode: "k",
			ProductName: "kurly",
			PriceKRW:    9000 + i,
			ReviewCount: 1000 - i,
			CollectedAt: "2026-07-04 20:00:00",
		})
	}
	for i := 0; i < 20; i++ {
		products = append(products, insightProduct{
			Provider:    "gmarket",
			ProductCode: "g",
			ProductName: "gmarket",
			PriceKRW:    19000 + i,
			ReviewCount: 10 - i,
			CollectedAt: "2026-07-04 19:00:00",
		})
	}

	got := topInsightProducts(products, 10)
	counts := countInsightProviders(got)
	if counts["gmarket"] != 5 || counts["kurly"] != 5 {
		t.Fatalf("provider counts = %#v, want 5/5", counts)
	}
	for i := 0; i < len(got); i += 2 {
		if got[i].Provider != "gmarket" {
			t.Fatalf("item %d provider = %q, want gmarket", i, got[i].Provider)
		}
		if i+1 < len(got) && got[i+1].Provider != "kurly" {
			t.Fatalf("item %d provider = %q, want kurly", i+1, got[i+1].Provider)
		}
	}
}

func TestBuildInsightDealCandidatesInterleavesProviders(t *testing.T) {
	products := []insightProduct{}
	for i := 0; i < 20; i++ {
		products = append(products, insightProduct{
			Provider:         "kurly",
			ProductCode:      "k",
			ProductName:      "kurly",
			SourceCategory:   "식품",
			PriceKRW:         9000 + i,
			OriginalPriceKRW: 30000,
		})
	}
	for i := 0; i < 20; i++ {
		products = append(products, insightProduct{
			Provider:         "gmarket",
			ProductCode:      "g",
			ProductName:      "gmarket",
			SourceCategory:   "식품",
			PriceKRW:         12000 + i,
			OriginalPriceKRW: 20000,
		})
	}

	got := buildInsightDealCandidates(products, 10)
	counts := countInsightDealProviders(got)
	if counts["gmarket"] != 5 || counts["kurly"] != 5 {
		t.Fatalf("provider counts = %#v, want 5/5", counts)
	}
}

func TestShoppingInsightProductsSQLUsesOverflowSafePrices(t *testing.T) {
	query := shoppingInsightProductsSQL()
	if !strings.Contains(query, "toInt64(round(price_krw))") || !strings.Contains(query, "toInt64(round(original_price_krw))") {
		t.Fatalf("shopping product query does not use Int64 price conversion:\n%s", query)
	}
	if strings.Contains(query, "toInt32(price_krw)") || strings.Contains(query, "toInt32(original_price_krw)") {
		t.Fatalf("shopping product query still contains overflow-prone Int32 price conversion:\n%s", query)
	}
}

func TestInsightClickHouseJSONEmitsUnquotedInt64(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("output_format_json_quote_64bit_integers"); got != "0" {
			t.Errorf("output_format_json_quote_64bit_integers=%q, want 0", got)
		}
		_, _ = w.Write([]byte("{}\n"))
	}))
	defer server.Close()
	client := &insightCHClient{url: server.URL, user: "user", password: "password", client: server.Client()}
	if _, err := client.post(context.Background(), "SELECT 1 FORMAT JSONEachRow"); err != nil {
		t.Fatal(err)
	}
}

func TestInsightClickHouseRetriesExplicitOverload(t *testing.T) {
	t.Setenv("SHOPPING_INSIGHT_OVERLOAD_RETRY_ATTEMPTS", "3")
	t.Setenv("SHOPPING_INSIGHT_OVERLOAD_RETRY_BACKOFF_SECONDS", "0.001")
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("Code: 202. Too many simultaneous queries (TOO_MANY_SIMULTANEOUS_QUERIES)"))
			return
		}
		_, _ = w.Write([]byte("{}\n"))
	}))
	defer server.Close()

	client := &insightCHClient{url: server.URL, user: "user", password: "password", client: server.Client()}
	if _, err := client.post(context.Background(), "SELECT 1 FORMAT JSONEachRow"); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d, want 2", attempts)
	}
}

func TestInsightClickHouseRetriesLiveCode202UntilExhausted(t *testing.T) {
	t.Setenv("SHOPPING_INSIGHT_OVERLOAD_RETRY_ATTEMPTS", "3")
	t.Setenv("SHOPPING_INSIGHT_OVERLOAD_RETRY_BACKOFF_SECONDS", "0.001")
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("Code: 202. DB::Exception: Too many simultaneous queries for user statground_ch_app. Current: 16, maximum: 16. (TOO_MANY_SIMULTANEOUS_QUERIES)"))
	}))
	defer server.Close()

	client := &insightCHClient{url: server.URL, user: "user", password: "password", client: server.Client()}
	_, err := client.post(context.Background(), "SELECT 1 FORMAT JSONEachRow")
	if err == nil || !strings.Contains(err.Error(), "category=too_many_simultaneous_queries") {
		t.Fatalf("error=%v, want sanitized query-slot exhaustion category", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts=%d, want 3", attempts)
	}
}

func TestInsightClickHouseClassifiesCode202WithoutMessageText(t *testing.T) {
	if !insightClickHouseQuerySlotsExhausted(http.StatusInternalServerError, []byte("Code: 202. DB::Exception")) {
		t.Fatal("Code 202 must be classified as exhausted ClickHouse query slots")
	}
	if insightClickHouseQuerySlotsExhausted(http.StatusInternalServerError, []byte("Code: 60. DB::Exception")) {
		t.Fatal("unrelated ClickHouse HTTP 500 must not be retried as query-slot exhaustion")
	}
	if insightClickHouseQuerySlotsExhausted(http.StatusInternalServerError, []byte("Code: 2020. DB::Exception")) {
		t.Fatal("ClickHouse code prefix matches must not be classified as Code 202")
	}
}

func TestInsightClickHouseRetriesReadOnlyEOF(t *testing.T) {
	t.Setenv("SHOPPING_INSIGHT_OVERLOAD_RETRY_ATTEMPTS", "3")
	t.Setenv("SHOPPING_INSIGHT_OVERLOAD_RETRY_BACKOFF_SECONDS", "0.001")
	attempts := 0
	client := &insightCHClient{
		url:      "http://clickhouse.test",
		user:     "user",
		password: "password",
		client: &http.Client{Transport: insightRoundTripFunc(func(*http.Request) (*http.Response, error) {
			attempts++
			if attempts == 1 {
				return nil, io.EOF
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("{}\n")),
				Header:     make(http.Header),
			}, nil
		})},
	}
	if _, err := client.post(context.Background(), "SELECT 1 FORMAT JSONEachRow"); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d, want 2", attempts)
	}
}

func TestInsightClickHouseDoesNotRetryWriteEOF(t *testing.T) {
	t.Setenv("SHOPPING_INSIGHT_OVERLOAD_RETRY_ATTEMPTS", "3")
	t.Setenv("SHOPPING_INSIGHT_OVERLOAD_RETRY_BACKOFF_SECONDS", "0.001")
	attempts := 0
	client := &insightCHClient{
		url:      "http://internal-clickhouse.example",
		user:     "user",
		password: "password",
		client: &http.Client{Transport: insightRoundTripFunc(func(*http.Request) (*http.Response, error) {
			attempts++
			return nil, io.EOF
		})},
	}
	_, err := client.post(context.Background(), "INSERT INTO db.table FORMAT JSONEachRow\n{}\n")
	if err == nil || err.Error() != "clickhouse request transport failed" {
		t.Fatalf("error=%v, want sanitized non-retryable write transport failure", err)
	}
	if attempts != 1 {
		t.Fatalf("write attempts=%d, want exactly 1", attempts)
	}
}

func TestInsightClickHouseClassifiesReadOnlyTransientNetworkErrors(t *testing.T) {
	for _, err := range []error{
		io.ErrUnexpectedEOF,
		fmt.Errorf("read tcp: connection reset by peer"),
		insightTemporaryNetworkError{},
	} {
		if !insightClickHouseReadTransportRetryable(err) {
			t.Errorf("error=%v must be classified as a retryable read-only transport failure", err)
		}
	}
	if insightClickHouseReadTransportRetryable(fmt.Errorf("x509: certificate signed by unknown authority")) {
		t.Fatal("permanent TLS configuration errors must not be retried")
	}
}

func TestInsightPublishedBatchVerificationAvoidsVersionAliasInWhere(t *testing.T) {
	expected := insightPublishedBatchInsert{
		RefreshUUID:          "019fd310-acbe-7259-abe4-884ad30ad9a1",
		Version:              1785952464934,
		SourceMaxCollectedAt: "2026-08-05 23:36:49",
		SourceProductCount:   32652,
		ExpectedScopeCount:   11,
		SnapshotScopeCount:   11,
		KeywordScopeCount:    11,
		KeywordRowCount:      205499,
	}
	query := shoppingInsightPublishedBatchVerificationSQL("Data_Shopping_Service.shopping_price_insight_published_batch", expected)
	if strings.Contains(query, "max(version) AS version") {
		t.Fatalf("marker verification query contains aggregate alias collision:\n%s", query)
	}
	if !strings.Contains(query, "max(version) AS marker_version") || !strings.Contains(query, "AND version = 1785952464934") {
		t.Fatalf("marker verification query lost its exact version filter:\n%s", query)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		if strings.Contains(string(body), "max(version) AS version") {
			t.Errorf("live marker verification request contains aggregate alias collision:\n%s", body)
		}
		_, _ = fmt.Fprintf(w, `{"rows":1,"marker_version":%d,"generated_version":%d,"invalid_published_at":0,"source_max_collected_at":%q,"source_product_count":%d,"expected_scope_count":11,"snapshot_scope_count":11,"keyword_scope_count":11,"keyword_row_count":%d}`+"\n",
			expected.Version,
			expected.Version,
			expected.SourceMaxCollectedAt,
			expected.SourceProductCount,
			expected.KeywordRowCount,
		)
	}))
	defer server.Close()
	client := &insightCHClient{url: server.URL, user: "user", password: "password", client: server.Client()}
	if err := client.verifyInsightPublishedBatch(context.Background(), "Data_Shopping_Service.shopping_price_insight_published_batch", expected); err != nil {
		t.Fatal(err)
	}
}

func TestFetchInsightProductsSkipsUnclassifiedRows(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(
			`{"provider":"gmarket","product_code":"unknown","product_name":"분류 불가 상품","source_category_raw":"기타","price_krw":1000,"collected_at":"2026-08-01 00:00:00"}` + "\n" +
				`{"provider":"gmarket","product_code":"food","product_name":"국산 양배추","source_category_raw":"신선식품","price_krw":2000,"collected_at":"2026-08-01 00:00:00"}` + "\n",
		))
	}))
	defer server.Close()

	client := &insightCHClient{url: server.URL, user: "user", password: "password", client: server.Client()}
	products, err := client.fetchInsightProducts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(products) != 1 || products[0].ProductCode != "food" || products[0].SourceCategory != "식품" {
		t.Fatalf("products=%#v, want only classified food row", products)
	}
}

func TestInsightKeywordInsertUsesBoundedChunks(t *testing.T) {
	t.Setenv("SHOPPING_INSIGHT_INSERT_CHUNK_SIZE", "2")
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		_, _ = w.Write([]byte("Ok.\n"))
	}))
	defer server.Close()

	client := &insightCHClient{url: server.URL, user: "user", password: "password", client: server.Client()}
	rows := make([]insightKeywordSearchMartInsert, 5)
	if err := client.insertInsightKeywordSearchMart(context.Background(), "db.table", rows); err != nil {
		t.Fatal(err)
	}
	if requests != 3 {
		t.Fatalf("requests=%d, want 3", requests)
	}
}

func TestInsightCategoryAliasesCollapseToStandardScopes(t *testing.T) {
	cases := map[string]string{
		"떡·한과":      "식품",
		"논알콜·무알콜":   "식품",
		"식초·소스·드레싱": "식품",
		"프로틴":       "뷰티/헬스",
		"스크럽 대디":    "생활/주방",
		"보조배터리":     "디지털/가전",
		"강아지 주식":    "유아/반려",
		"취미·문구·펫":   "도서/취미/문구",
		"모바일 쿠폰":    "여행/e쿠폰",
	}
	for input, want := range cases {
		if got := insightStandardCategory(input); got != want {
			t.Errorf("insightStandardCategory(%q)=%q, want %q", input, got, want)
		}
	}
}

func TestWorkflowPublishesOneSerializedBatchAfterCrawl(t *testing.T) {
	body, err := os.ReadFile(".github/workflows/gmarket-crawl.yml")
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(body)
	for _, fragment := range []string{
		"refresh_insight_after_crawl:",
		"needs: crawl",
		"Build, verify, and publish Shopping Price Insight batch",
		"group: shopping-price-insight-refresh",
		`SHOPPING_INSIGHT_OVERLOAD_RETRY_ATTEMPTS: "90"`,
		`SHOPPING_INSIGHT_OVERLOAD_RETRY_BACKOFF_SECONDS: "10"`,
	} {
		if !strings.Contains(workflow, fragment) {
			t.Fatalf("workflow missing %q", fragment)
		}
	}
	if strings.Count(workflow, "group: shopping-price-insight-refresh") != 2 || strings.Count(workflow, "cancel-in-progress: false") != 2 {
		t.Fatalf("insight refresh jobs must share one non-cancelling concurrency group")
	}
	if strings.Count(workflow, `SHOPPING_INSIGHT_OVERLOAD_RETRY_ATTEMPTS: "90"`) != 2 ||
		strings.Count(workflow, `SHOPPING_INSIGHT_OVERLOAD_RETRY_BACKOFF_SECONDS: "10"`) != 2 {
		t.Fatalf("both insight refresh jobs must keep the same bounded query-slot retry contract")
	}
}

func TestInsightBuildUsesAllAndTenStandardScopes(t *testing.T) {
	products := testInsightStandardProducts()
	snapshots, generatedAt, version, err := buildInsightSnapshots(products)
	if err != nil {
		t.Fatal(err)
	}
	searchRows, err := buildInsightKeywordSearchMartRows(products, generatedAt, version)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateInsightRefreshBuild(products, snapshots, searchRows, version); err != nil {
		t.Fatal(err)
	}
	scopes := make([]string, 0, len(snapshots))
	for _, row := range snapshots {
		scopes = append(scopes, row.ScopeSlug)
		if row.Version != version {
			t.Fatalf("snapshot version=%d, want %d", row.Version, version)
		}
	}
	if !sameInsightStringSet(scopes, insightExpectedScopeSlugs()) || len(scopes) != 11 {
		t.Fatalf("snapshot scopes=%v, want exact all+10=%v", scopes, insightExpectedScopeSlugs())
	}
}

func TestInsightRefreshBuildsBeforeFirstWriteAndPublishesMarkerLast(t *testing.T) {
	client := &fakeInsightRefreshClient{products: testInsightStandardProducts()}
	tables := insightRefreshTables{snapshot: "snapshot", keywordSearch: "keyword", publishedBatch: "published"}
	if err := runShoppingInsightRefresh(context.Background(), client, tables); err != nil {
		t.Fatal(err)
	}
	want := []string{"preflight", "insert_keyword", "insert_snapshot", "verify_refresh", "insert_published", "verify_published"}
	if !sameInsightStringSet(client.calls, want) {
		t.Fatalf("calls=%v, want members %v", client.calls, want)
	}
	for i := range want {
		if client.calls[i] != want[i] {
			t.Fatalf("calls=%v, want ordered %v", client.calls, want)
		}
	}
	if client.published.Version == 0 || client.published.ExpectedScopeCount != 11 || client.published.SnapshotScopeCount != 11 || client.published.KeywordScopeCount != 11 {
		t.Fatalf("published marker=%+v", client.published)
	}
}

func TestInsightRefreshRejectsIncompleteCategorySetBeforeWrite(t *testing.T) {
	client := &fakeInsightRefreshClient{products: []insightProduct{{
		Provider:       "gmarket",
		ProductCode:    "unknown-1",
		ProductName:    "unknown product",
		SourceCategory: "unclassified",
		PriceKRW:       5000,
		CollectedAt:    "2026-07-31 18:21:46",
		Keywords:       []string{"unknown"},
	}}}
	err := runShoppingInsightRefresh(context.Background(), client, insightRefreshTables{snapshot: "snapshot", keywordSearch: "keyword", publishedBatch: "published"})
	if err == nil || !strings.Contains(err.Error(), "standard") {
		t.Fatalf("error=%v, want standard scope validation failure", err)
	}
	if len(client.calls) != 1 || client.calls[0] != "preflight" {
		t.Fatalf("writes happened before complete build validation: %v", client.calls)
	}
}

func TestInsightRefreshStopsBeforeSourceFetchWhenPublishPreflightFails(t *testing.T) {
	client := &fakeInsightRefreshClient{products: testInsightStandardProducts(), preflightErr: fmt.Errorf("marker missing")}
	err := runShoppingInsightRefresh(context.Background(), client, insightRefreshTables{snapshot: "snapshot", keywordSearch: "keyword", publishedBatch: "published"})
	if err == nil || !strings.Contains(err.Error(), "preflight") {
		t.Fatalf("error=%v, want preflight failure", err)
	}
	if len(client.calls) != 1 || client.calls[0] != "preflight" {
		t.Fatalf("calls=%v, want preflight only", client.calls)
	}
}

func TestInsightRefreshDoesNotPublishWhenPostconditionFails(t *testing.T) {
	bad := insightRefreshVerification{}
	client := &fakeInsightRefreshClient{products: testInsightStandardProducts(), verificationOverride: &bad}
	err := runShoppingInsightRefresh(context.Background(), client, insightRefreshTables{snapshot: "snapshot", keywordSearch: "keyword", publishedBatch: "published"})
	if err == nil || !strings.Contains(err.Error(), "postcondition") {
		t.Fatalf("error=%v, want postcondition failure", err)
	}
	for _, call := range client.calls {
		if call == "insert_published" || call == "verify_published" {
			t.Fatalf("published marker was written after failed verification: %v", client.calls)
		}
	}
}

func TestInsightRefreshDoesNotPublishOnGeneratedVersionMismatch(t *testing.T) {
	client := &fakeInsightRefreshClient{products: testInsightStandardProducts(), versionMismatch: true}
	err := runShoppingInsightRefresh(context.Background(), client, insightRefreshTables{snapshot: "snapshot", keywordSearch: "keyword", publishedBatch: "published"})
	if err == nil || !strings.Contains(err.Error(), "generated-at version mismatch") {
		t.Fatalf("error=%v, want generated-at version mismatch", err)
	}
	for _, call := range client.calls {
		if call == "insert_published" || call == "verify_published" {
			t.Fatalf("published marker was written after version mismatch: %v", client.calls)
		}
	}
}

type fakeInsightRefreshClient struct {
	products             []insightProduct
	calls                []string
	snapshots            []insightSnapshotInsert
	searchRows           []insightKeywordSearchMartInsert
	published            insightPublishedBatchInsert
	verificationOverride *insightRefreshVerification
	versionMismatch      bool
	preflightErr         error
}

type insightRoundTripFunc func(*http.Request) (*http.Response, error)

func (f insightRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type insightTemporaryNetworkError struct{}

func (insightTemporaryNetworkError) Error() string   { return "temporary network failure" }
func (insightTemporaryNetworkError) Timeout() bool   { return false }
func (insightTemporaryNetworkError) Temporary() bool { return true }

func (f *fakeInsightRefreshClient) preflightInsightPublishTargets(context.Context, insightRefreshTables) error {
	f.calls = append(f.calls, "preflight")
	return f.preflightErr
}

func (f *fakeInsightRefreshClient) fetchInsightProducts(context.Context) ([]insightProduct, error) {
	return append([]insightProduct(nil), f.products...), nil
}

func (f *fakeInsightRefreshClient) insertInsightSnapshots(_ context.Context, _ string, rows []insightSnapshotInsert) error {
	f.calls = append(f.calls, "insert_snapshot")
	f.snapshots = append([]insightSnapshotInsert(nil), rows...)
	return nil
}

func (f *fakeInsightRefreshClient) insertInsightKeywordSearchMart(_ context.Context, _ string, rows []insightKeywordSearchMartInsert) error {
	f.calls = append(f.calls, "insert_keyword")
	f.searchRows = append([]insightKeywordSearchMartInsert(nil), rows...)
	return nil
}

func (f *fakeInsightRefreshClient) verifyInsightRefresh(_ context.Context, _ insightRefreshTables, _ uint64) (insightRefreshVerification, error) {
	f.calls = append(f.calls, "verify_refresh")
	if f.verificationOverride != nil {
		return *f.verificationOverride, nil
	}
	snapshotScopes := make([]string, 0, len(f.snapshots))
	for _, row := range f.snapshots {
		snapshotScopes = append(snapshotScopes, row.ScopeSlug)
	}
	keywordSet := map[string]struct{}{}
	for _, row := range f.searchRows {
		keywordSet[row.ScopeSlug] = struct{}{}
	}
	keywordScopes := make([]string, 0, len(keywordSet))
	for scope := range keywordSet {
		keywordScopes = append(keywordScopes, scope)
	}
	verification := insightRefreshVerification{
		SnapshotRows:                 uint64(len(f.snapshots)),
		SnapshotScopes:               snapshotScopes,
		SnapshotSourceMaxCollectedAt: latestInsightCollectedAt(f.products),
		SnapshotSourceProductCount:   uint64(len(f.products)),
		KeywordRows:                  uint64(len(f.searchRows)),
		KeywordScopes:                keywordScopes,
		KeywordSourceMaxCollectedAt:  latestInsightCollectedAt(f.products),
		KeywordSourceProductCount:    uint64(len(f.products)),
	}
	if f.versionMismatch {
		verification.SnapshotVersionMismatches = 1
	}
	return verification, nil
}

func (f *fakeInsightRefreshClient) insertInsightPublishedBatch(_ context.Context, _ string, row insightPublishedBatchInsert) error {
	f.calls = append(f.calls, "insert_published")
	f.published = row
	return nil
}

func (f *fakeInsightRefreshClient) verifyInsightPublishedBatch(_ context.Context, _ string, _ insightPublishedBatchInsert) error {
	f.calls = append(f.calls, "verify_published")
	return nil
}

func testInsightStandardProducts() []insightProduct {
	products := make([]insightProduct, 0, len(insightStandardCategories))
	for i, category := range insightStandardCategories {
		products = append(products, insightProduct{
			Provider:         "gmarket",
			ProductCode:      fmt.Sprintf("product-%02d", i),
			ProductName:      fmt.Sprintf("검증 상품 %02d", i),
			SourceCategory:   category,
			PriceKRW:         10000 + i*1000,
			OriginalPriceKRW: 12000 + i*1000,
			Seller:           "seller",
			Brand:            "brand",
			ReviewCount:      i + 1,
			OrderCount:       i,
			CollectedAt:      "2026-07-31 18:21:46",
			Keywords:         []string{fmt.Sprintf("키워드%02d", i)},
		})
	}
	return products
}

func countInsightProviders(items []insightProduct) map[string]int {
	counts := map[string]int{}
	for _, item := range items {
		counts[item.Provider]++
	}
	return counts
}

func countInsightDealProviders(items []insightDealCandidate) map[string]int {
	counts := map[string]int{}
	for _, item := range items {
		counts[item.Provider]++
	}
	return counts
}
