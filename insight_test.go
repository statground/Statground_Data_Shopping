package main

import (
	"context"
	"fmt"
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
	} {
		if !strings.Contains(workflow, fragment) {
			t.Fatalf("workflow missing %q", fragment)
		}
	}
	if strings.Count(workflow, "group: shopping-price-insight-refresh") != 2 || strings.Count(workflow, "cancel-in-progress: false") != 2 {
		t.Fatalf("insight refresh jobs must share one non-cancelling concurrency group")
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
	want := []string{"insert_keyword", "insert_snapshot", "verify_refresh", "insert_published", "verify_published"}
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
	if len(client.calls) != 0 {
		t.Fatalf("writes happened before complete build validation: %v", client.calls)
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
