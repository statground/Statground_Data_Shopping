package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

const defaultShoppingInsightSnapshotTable = "Data_Shopping_Service.shopping_price_insight_snapshot"
const defaultShoppingKeywordSearchMartTable = "Data_Shopping_Service.shopping_keyword_search_mart"
const defaultShoppingInsightPublishedBatchTable = "Data_Shopping_Service.shopping_price_insight_published_batch"
const insightKeywordPayloadLimit = 240
const insightCategoryKeywordPayloadLimit = 800
const insightCategoryKeywordPerCategoryLimit = 40
const insightEvidenceProductLimit = 24
const insightEvidenceDealCandidateLimit = 24
const insightRangeEvidenceProductLimit = 80
const insightRangeEvidenceDealCandidateLimit = 60

var insightSplitRe = regexp.MustCompile(`[^0-9a-zA-Z가-힣]+`)
var insightDigitRe = regexp.MustCompile(`[0-9]`)
var insightUUIDRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

var insightStandardCategories = []string{
	"식품",
	"생활/주방",
	"뷰티/헬스",
	"패션/잡화",
	"디지털/가전",
	"가구/홈",
	"스포츠/레저",
	"유아/반려",
	"도서/취미/문구",
	"여행/e쿠폰",
}

type insightCHClient struct {
	url      string
	user     string
	password string
	client   *http.Client
}

type insightProduct struct {
	Provider         string   `json:"provider"`
	ProviderLabel    string   `json:"provider_label,omitempty"`
	ProductCode      string   `json:"product_code"`
	ProductName      string   `json:"product_name"`
	ProductLabel     string   `json:"product_label,omitempty"`
	SourceCategory   string   `json:"source_category"`
	GroupCode        string   `json:"group_code,omitempty"`
	SearchKeyword    string   `json:"search_keyword,omitempty"`
	ImageURL         string   `json:"image_url,omitempty"`
	ProductURL       string   `json:"product_url,omitempty"`
	RawProductURL    string   `json:"raw_product_url,omitempty"`
	PriceKRW         int      `json:"price_krw,omitempty"`
	PriceBasis       string   `json:"price_basis,omitempty"`
	OriginalPriceKRW int      `json:"original_price_krw,omitempty"`
	Brand            string   `json:"brand,omitempty"`
	Seller           string   `json:"seller,omitempty"`
	CategoryPath     string   `json:"category_path,omitempty"`
	ReviewCount      int      `json:"review_count,omitempty"`
	OrderCount       int      `json:"order_count,omitempty"`
	CollectedAt      string   `json:"collected_at,omitempty"`
	UpdatedAt        string   `json:"updated_at,omitempty"`
	Keywords         []string `json:"-"`
}

type insightRadar struct {
	Provider         string                          `json:"provider"`
	ScopeLabel       string                          `json:"scope_label"`
	ScopeCategory    string                          `json:"scope_category,omitempty"`
	GeneratedAt      string                          `json:"generated_at"`
	Summary          insightRadarSummary             `json:"summary"`
	PriceBands       []insightPriceBand              `json:"price_bands"`
	Categories       []insightCategoryBenchmark      `json:"categories"`
	CategoryOptions  []insightCategoryBenchmark      `json:"category_options"`
	Keywords         []insightKeywordBenchmark       `json:"keywords"`
	CategoryKeywords []insightCategoryKeywordInsight `json:"category_keywords"`
	PriceRangeSlices []insightPriceRangeSlice        `json:"price_range_slices,omitempty"`
	Products         []insightProduct                `json:"products"`
	DealCandidates   []insightDealCandidate          `json:"deal_candidates"`
	PriceDrops       []map[string]any                `json:"price_drop_candidates"`
	SellerInsights   []insightSellerInsight          `json:"seller_insights"`
	PolicyNotes      []insightPolicyNote             `json:"policy_notes"`
}

type insightRadarSummary struct {
	ProductCount       int     `json:"product_count"`
	CategoryCount      int     `json:"category_count"`
	DiscountedCount    int     `json:"discounted_count"`
	DiscountedPercent  float64 `json:"discounted_percent"`
	LowPriceCount      int     `json:"low_price_count"`
	LowPricePercent    float64 `json:"low_price_percent"`
	MinPriceKRW        int     `json:"min_price_krw"`
	MedianPriceKRW     int     `json:"median_price_krw"`
	MaxPriceKRW        int     `json:"max_price_krw"`
	FirstCollectedAt   string  `json:"first_collected_at,omitempty"`
	LatestCollectedAt  string  `json:"latest_collected_at,omitempty"`
	HistoryProductRuns int     `json:"history_product_runs"`
}

type insightCategoryBenchmark struct {
	SourceCategory    string  `json:"source_category"`
	ProductCount      int     `json:"product_count"`
	SellerCount       int     `json:"seller_count"`
	BrandCount        int     `json:"brand_count"`
	ReviewSum         int     `json:"review_sum"`
	OrderSum          int     `json:"order_sum"`
	MinPriceKRW       int     `json:"min_price_krw"`
	P25PriceKRW       int     `json:"p25_price_krw"`
	MedianPriceKRW    int     `json:"median_price_krw"`
	P75PriceKRW       int     `json:"p75_price_krw"`
	MaxPriceKRW       int     `json:"max_price_krw"`
	IQRPriceKRW       int     `json:"iqr_price_krw"`
	DemandScore       float64 `json:"demand_score"`
	CompetitionScore  float64 `json:"competition_score"`
	OpportunityScore  float64 `json:"opportunity_score"`
	DiscountedCount   int     `json:"discounted_count"`
	DiscountedPercent float64 `json:"discounted_percent"`
	LowPriceCount     int     `json:"low_price_count"`
	LowPricePercent   float64 `json:"low_price_percent"`
	Interpretation    string  `json:"interpretation,omitempty"`
	LatestCollectedAt string  `json:"latest_collected_at,omitempty"`
}

type insightPriceBand struct {
	Label           string  `json:"label"`
	MinPriceKRW     int     `json:"min_price_krw"`
	MaxPriceKRW     int     `json:"max_price_krw,omitempty"`
	ProductCount    int     `json:"product_count"`
	ProductPercent  float64 `json:"product_percent"`
	ReviewSum       int     `json:"review_sum"`
	OrderSum        int     `json:"order_sum"`
	ReactionPercent float64 `json:"reaction_percent"`
	Interpretation  string  `json:"interpretation,omitempty"`
}

type insightKeywordBenchmark struct {
	Keyword          string  `json:"keyword"`
	ProductCount     int     `json:"product_count"`
	CategoryCount    int     `json:"category_count"`
	SellerCount      int     `json:"seller_count"`
	BrandCount       int     `json:"brand_count"`
	ReviewSum        int     `json:"review_sum"`
	OrderSum         int     `json:"order_sum"`
	MedianPriceKRW   int     `json:"median_price_krw"`
	P25PriceKRW      int     `json:"p25_price_krw"`
	P75PriceKRW      int     `json:"p75_price_krw"`
	DemandScore      float64 `json:"demand_score"`
	CompetitionScore float64 `json:"competition_score"`
	SaturationScore  float64 `json:"saturation_score"`
	OpportunityScore float64 `json:"opportunity_score"`
	Interpretation   string  `json:"interpretation,omitempty"`
}

type insightCategoryKeywordInsight struct {
	SourceCategory   string  `json:"source_category"`
	Keyword          string  `json:"keyword"`
	ClusterLabel     string  `json:"cluster_label"`
	ProductCount     int     `json:"product_count"`
	SellerCount      int     `json:"seller_count"`
	BrandCount       int     `json:"brand_count"`
	ReviewSum        int     `json:"review_sum"`
	OrderSum         int     `json:"order_sum"`
	P25PriceKRW      int     `json:"p25_price_krw"`
	MedianPriceKRW   int     `json:"median_price_krw"`
	P75PriceKRW      int     `json:"p75_price_krw"`
	IQRPriceKRW      int     `json:"iqr_price_krw"`
	DemandScore      float64 `json:"demand_score"`
	CompetitionScore float64 `json:"competition_score"`
	PriceGapScore    float64 `json:"price_gap_score"`
	OpportunityScore float64 `json:"opportunity_score"`
	Interpretation   string  `json:"interpretation,omitempty"`
}

type insightDealCandidate struct {
	insightProduct
	DiscountPercent            float64 `json:"discount_percent"`
	CategoryMedianPriceKRW     int     `json:"category_median_price_krw"`
	BelowCategoryMedianPercent float64 `json:"below_category_median_percent"`
	RadarScore                 float64 `json:"radar_score"`
	DealConfidenceScore        float64 `json:"deal_confidence_score"`
	Reason                     string  `json:"reason,omitempty"`
}

type insightSellerInsight struct {
	SourceCategory    string  `json:"source_category"`
	ProductCount      int     `json:"product_count"`
	MedianPriceKRW    int     `json:"median_price_krw"`
	LowPricePercent   float64 `json:"low_price_percent"`
	DiscountedPercent float64 `json:"discounted_percent"`
	CompetitionLevel  string  `json:"competition_level"`
	RecommendedAction string  `json:"recommended_action"`
}

type insightPolicyNote struct {
	Code   string `json:"code"`
	Label  string `json:"label"`
	Status string `json:"status"`
	Detail string `json:"detail"`
}

type insightPriceRangeSlice struct {
	Key              string                          `json:"key"`
	Label            string                          `json:"label"`
	MinPriceKRW      int                             `json:"min_price_krw"`
	MaxPriceKRW      int                             `json:"max_price_krw,omitempty"`
	Summary          insightRadarSummary             `json:"summary"`
	PriceBands       []insightPriceBand              `json:"price_bands"`
	Categories       []insightCategoryBenchmark      `json:"categories"`
	Keywords         []insightKeywordBenchmark       `json:"keywords"`
	CategoryKeywords []insightCategoryKeywordInsight `json:"category_keywords"`
	Products         []insightProduct                `json:"products"`
	DealCandidates   []insightDealCandidate          `json:"deal_candidates"`
	SellerInsights   []insightSellerInsight          `json:"seller_insights"`
}

type insightSnapshotInsert struct {
	SnapshotUUID         string `json:"snapshot_uuid"`
	ScopeSlug            string `json:"scope_slug"`
	ScopeCategory        string `json:"scope_category"`
	Version              uint64 `json:"version"`
	GeneratedAt          string `json:"generated_at"`
	SourceMaxCollectedAt string `json:"source_max_collected_at"`
	SourceProductCount   uint64 `json:"source_product_count"`
	PayloadJSON          string `json:"payload_json"`
	CreatedAt            string `json:"created_at"`
}

type insightKeywordSearchMartInsert struct {
	KeywordUUID          string  `json:"keyword_uuid"`
	SnapshotUUID         string  `json:"snapshot_uuid"`
	ScopeSlug            string  `json:"scope_slug"`
	ScopeCategory        string  `json:"scope_category"`
	Version              uint64  `json:"version"`
	GeneratedAt          string  `json:"generated_at"`
	SourceMaxCollectedAt string  `json:"source_max_collected_at"`
	SourceProductCount   uint64  `json:"source_product_count"`
	Keyword              string  `json:"keyword"`
	KeywordKey           string  `json:"keyword_key"`
	SearchText           string  `json:"search_text"`
	ProductCount         uint64  `json:"product_count"`
	CategoryCount        uint64  `json:"category_count"`
	SellerCount          uint64  `json:"seller_count"`
	BrandCount           uint64  `json:"brand_count"`
	ReviewSum            uint64  `json:"review_sum"`
	OrderSum             uint64  `json:"order_sum"`
	MinPriceKRW          int     `json:"min_price_krw"`
	P25PriceKRW          int     `json:"p25_price_krw"`
	MedianPriceKRW       int     `json:"median_price_krw"`
	P75PriceKRW          int     `json:"p75_price_krw"`
	MaxPriceKRW          int     `json:"max_price_krw"`
	DemandScore          float64 `json:"demand_score"`
	CompetitionScore     float64 `json:"competition_score"`
	SaturationScore      float64 `json:"saturation_score"`
	OpportunityScore     float64 `json:"opportunity_score"`
	CategoriesJSON       string  `json:"categories_json"`
	ProductsJSON         string  `json:"products_json"`
	CreatedAt            string  `json:"created_at"`
}

type insightPublishedBatchInsert struct {
	RefreshUUID          string `json:"refresh_uuid"`
	Version              uint64 `json:"version"`
	GeneratedAt          string `json:"generated_at"`
	SourceMaxCollectedAt string `json:"source_max_collected_at"`
	SourceProductCount   uint64 `json:"source_product_count"`
	ExpectedScopeCount   uint16 `json:"expected_scope_count"`
	SnapshotScopeCount   uint16 `json:"snapshot_scope_count"`
	KeywordScopeCount    uint16 `json:"keyword_scope_count"`
	KeywordRowCount      uint64 `json:"keyword_row_count"`
	PublishedAt          string `json:"published_at"`
}

type insightRefreshTables struct {
	snapshot       string
	keywordSearch  string
	publishedBatch string
}

type insightRefreshVerification struct {
	SnapshotRows                 uint64   `json:"snapshot_rows"`
	SnapshotScopes               []string `json:"snapshot_scopes"`
	SnapshotInvalidPayloads      uint64   `json:"snapshot_invalid_payloads"`
	SnapshotVersionMismatches    uint64   `json:"snapshot_version_mismatches"`
	SnapshotSourceMaxCollectedAt string   `json:"snapshot_source_max_collected_at"`
	SnapshotSourceProductCount   uint64   `json:"snapshot_source_product_count"`
	KeywordRows                  uint64   `json:"keyword_rows"`
	KeywordScopes                []string `json:"keyword_scopes"`
	KeywordInvalidPayloads       uint64   `json:"keyword_invalid_payloads"`
	KeywordVersionMismatches     uint64   `json:"keyword_version_mismatches"`
	KeywordSourceMaxCollectedAt  string   `json:"keyword_source_max_collected_at"`
	KeywordSourceProductCount    uint64   `json:"keyword_source_product_count"`
}

type shoppingInsightRefreshClient interface {
	preflightInsightPublishTargets(context.Context, insightRefreshTables) error
	fetchInsightProducts(context.Context) ([]insightProduct, error)
	insertInsightSnapshots(context.Context, string, []insightSnapshotInsert) error
	insertInsightKeywordSearchMart(context.Context, string, []insightKeywordSearchMartInsert) error
	verifyInsightRefresh(context.Context, insightRefreshTables, uint64) (insightRefreshVerification, error)
	insertInsightPublishedBatch(context.Context, string, insightPublishedBatchInsert) error
	verifyInsightPublishedBatch(context.Context, string, insightPublishedBatchInsert) error
}

func RunShoppingInsightRefreshFromEnv(ctx context.Context) error {
	client, err := newInsightCHClientFromEnv()
	if err != nil {
		return err
	}
	tables, err := insightRefreshTablesFromEnv()
	if err != nil {
		return err
	}
	return runShoppingInsightRefresh(ctx, client, tables)
}

func insightRefreshTablesFromEnv() (insightRefreshTables, error) {
	publishedBatchTable := firstNonEmptyEnv("SHOPPING_PRICE_INSIGHT_PUBLISHED_BATCH_TABLE", "SHOPPING_INSIGHT_PUBLISHED_BATCH_TABLE")
	if publishedBatchTable == "" {
		publishedBatchTable = defaultShoppingInsightPublishedBatchTable
	}
	tables := insightRefreshTables{
		snapshot:       safeInsightIdentifierPath(envString("SHOPPING_INSIGHT_SNAPSHOT_TABLE", defaultShoppingInsightSnapshotTable)),
		keywordSearch:  safeInsightIdentifierPath(envString("SHOPPING_KEYWORD_SEARCH_MART_TABLE", defaultShoppingKeywordSearchMartTable)),
		publishedBatch: safeInsightIdentifierPath(publishedBatchTable),
	}
	if tables.snapshot == "" {
		return insightRefreshTables{}, fmt.Errorf("invalid SHOPPING_INSIGHT_SNAPSHOT_TABLE")
	}
	if tables.keywordSearch == "" {
		return insightRefreshTables{}, fmt.Errorf("invalid SHOPPING_KEYWORD_SEARCH_MART_TABLE")
	}
	if tables.publishedBatch == "" {
		return insightRefreshTables{}, fmt.Errorf("invalid SHOPPING_PRICE_INSIGHT_PUBLISHED_BATCH_TABLE")
	}
	return tables, nil
}

func runShoppingInsightRefresh(ctx context.Context, client shoppingInsightRefreshClient, tables insightRefreshTables) error {
	if err := client.preflightInsightPublishTargets(ctx, tables); err != nil {
		return fmt.Errorf("shopping insight publish preflight failed: %w", err)
	}
	products, err := client.fetchInsightProducts(ctx)
	if err != nil {
		return fmt.Errorf("shopping insight source fetch failed: %w", err)
	}
	if len(products) == 0 {
		return fmt.Errorf("shopping insight refresh skipped: no current shopping products")
	}
	snapshots, generatedAt, version, err := buildInsightSnapshots(products)
	if err != nil {
		return err
	}
	searchRows, err := buildInsightKeywordSearchMartRows(products, generatedAt, version)
	if err != nil {
		return err
	}
	if err := validateInsightRefreshBuild(products, snapshots, searchRows, version); err != nil {
		return err
	}

	// Build both products completely before the first write. Keyword rows are
	// written first and snapshots second; neither becomes serving-authoritative
	// until the completed-batch marker is appended after verification below.
	if err := client.insertInsightKeywordSearchMart(ctx, tables.keywordSearch, searchRows); err != nil {
		return fmt.Errorf("shopping insight keyword write failed: %w", err)
	}
	if err := client.insertInsightSnapshots(ctx, tables.snapshot, snapshots); err != nil {
		return fmt.Errorf("shopping insight snapshot write failed: %w", err)
	}
	verification, err := client.verifyInsightRefresh(ctx, tables, version)
	if err != nil {
		return fmt.Errorf("shopping insight postcondition query failed: %w", err)
	}
	if err := validateInsightRefreshVerification(products, snapshots, searchRows, version, verification); err != nil {
		return err
	}

	generatedText := FormatCHDateTime64Millis(generatedAt)
	batch := insightPublishedBatchInsert{
		RefreshUUID:          NewUUIDv7(),
		Version:              version,
		GeneratedAt:          generatedText,
		SourceMaxCollectedAt: latestInsightCollectedAt(products),
		SourceProductCount:   uint64(len(products)),
		ExpectedScopeCount:   uint16(len(insightExpectedScopeSlugs())),
		SnapshotScopeCount:   uint16(len(verification.SnapshotScopes)),
		KeywordScopeCount:    uint16(len(verification.KeywordScopes)),
		KeywordRowCount:      verification.KeywordRows,
		PublishedAt:          FormatCHDateTime64Millis(NowKST()),
	}
	if err := client.insertInsightPublishedBatch(ctx, tables.publishedBatch, batch); err != nil {
		return fmt.Errorf("shopping insight publish marker write failed: %w", err)
	}
	if err := client.verifyInsightPublishedBatch(ctx, tables.publishedBatch, batch); err != nil {
		return fmt.Errorf("shopping insight publish marker verification failed: %w", err)
	}
	fmt.Printf("Shopping Price Insight complete batch published version=%d scopes=%d products=%d table=%s keyword_search_rows=%d keyword_search_table=%s published_batch_table=%s\n", version, len(snapshots), len(products), tables.snapshot, len(searchRows), tables.keywordSearch, tables.publishedBatch)
	return nil
}

func (c *insightCHClient) preflightInsightPublishTargets(ctx context.Context, tables insightRefreshTables) error {
	checks := []struct {
		name  string
		query string
		grant bool
	}{
		{name: "snapshot read", query: "SELECT scope_slug FROM " + tables.snapshot + " LIMIT 0 FORMAT JSONEachRow"},
		{name: "keyword read", query: "SELECT scope_slug FROM " + tables.keywordSearch + " LIMIT 0 FORMAT JSONEachRow"},
		{name: "published marker read", query: "SELECT version FROM " + tables.publishedBatch + " LIMIT 0 FORMAT JSONEachRow"},
		{name: "snapshot insert grant", query: "CHECK GRANT INSERT ON " + tables.snapshot, grant: true},
		{name: "keyword insert grant", query: "CHECK GRANT INSERT ON " + tables.keywordSearch, grant: true},
		{name: "published marker insert grant", query: "CHECK GRANT INSERT ON " + tables.publishedBatch, grant: true},
	}
	for _, check := range checks {
		body, err := c.post(ctx, check.query)
		if err != nil {
			return fmt.Errorf("%s unavailable: %w", check.name, err)
		}
		if check.grant && strings.TrimSpace(string(body)) != "1" {
			return fmt.Errorf("%s denied", check.name)
		}
	}
	return nil
}

func newInsightCHClientFromEnv() (*insightCHClient, error) {
	host := firstNonEmptyEnv("CLICKHOUSE_HOST", "CH_HOST")
	port := firstNonEmptyEnv("CLICKHOUSE_PORT", "CH_PORT")
	user := firstNonEmptyEnv("CLICKHOUSE_USER", "CH_USER")
	password := firstNonEmptyEnv("CLICKHOUSE_PASSWORD", "CH_PASSWORD")
	protocol := firstNonEmptyEnv("CLICKHOUSE_PROTOCOL", "CH_PROTOCOL")
	if protocol == "" {
		protocol = "http"
	}
	path := firstNonEmptyEnv("CLICKHOUSE_HTTP_URL_PATH", "CH_HTTP_URL_PATH")
	if path == "" {
		path = "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if host == "" || port == "" || user == "" || password == "" {
		return nil, fmt.Errorf("missing ClickHouse env: CLICKHOUSE_HOST/PORT/USER/PASSWORD")
	}
	timeout := secondsDefault(envString("SHOPPING_INSIGHT_HTTP_TIMEOUT_SECONDS", envString("CLICKHOUSE_REQUEST_TIMEOUT_SECONDS", "240")), 240*time.Second)
	return &insightCHClient{
		url:      fmt.Sprintf("%s://%s:%s%s", protocol, host, port, path),
		user:     user,
		password: password,
		client:   &http.Client{Timeout: timeout},
	}, nil
}

func firstNonEmptyEnv(names ...string) string {
	for _, name := range names {
		if v := strings.TrimSpace(os.Getenv(name)); v != "" {
			return v
		}
	}
	return ""
}

func (c *insightCHClient) post(ctx context.Context, sql string) ([]byte, error) {
	const maxOverloadAttempts = 6
	trimmedSQL := strings.TrimSpace(strings.ToUpper(sql))
	readOnly := strings.HasPrefix(trimmedSQL, "SELECT") || strings.HasPrefix(trimmedSQL, "WITH")
	for attempt := 1; attempt <= maxOverloadAttempts; attempt++ {
		body, retry, err := c.postOnce(ctx, sql, readOnly)
		if err == nil {
			return body, nil
		}
		if !retry || attempt == maxOverloadAttempts {
			return nil, err
		}
		backoff := time.Duration(attempt*2) * time.Second
		fmt.Printf("Shopping Price Insight ClickHouse temporarily unavailable; retrying attempt=%d/%d wait=%s\n", attempt+1, maxOverloadAttempts, backoff)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, fmt.Errorf("clickhouse request exhausted overload retries")
}

func (c *insightCHClient) postOnce(ctx context.Context, sql string, readOnly bool) ([]byte, bool, error) {
	endpoint, err := url.Parse(c.url)
	if err != nil {
		return nil, false, err
	}
	query := endpoint.Query()
	// Price fields and verification counters are Int64/UInt64. Force JSONEachRow
	// to emit JSON numbers instead of quoted values so overflow-safe decoding is
	// deterministic for every ClickHouse server profile.
	query.Set("output_format_json_quote_64bit_integers", "0")
	endpoint.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), strings.NewReader(sql))
	if err != nil {
		return nil, false, err
	}
	req.SetBasicAuth(c.user, c.password)
	resp, err := c.client.Do(req)
	if err != nil {
		// A write may have reached ClickHouse even when the response was lost.
		// Do not retry ambiguous transport failures here.
		return nil, false, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		message := strings.ToLower(string(body))
		if (resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusInternalServerError) &&
			(strings.Contains(message, "too many simultaneous queries") || strings.Contains(message, "too_many_simultaneous_queries")) {
			return nil, true, fmt.Errorf("clickhouse overloaded status=%d", resp.StatusCode)
		}
		if readOnly && resp.StatusCode == http.StatusRequestTimeout {
			return nil, true, fmt.Errorf("clickhouse readonly query timed out status=%d", resp.StatusCode)
		}
		return nil, false, fmt.Errorf("clickhouse request failed status=%d", resp.StatusCode)
	}
	return body, false, nil
}

func (c *insightCHClient) fetchInsightProducts(ctx context.Context) ([]insightProduct, error) {
	body, err := c.post(ctx, shoppingInsightProductsSQL())
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 1024), 1024*1024*10)
	products := []insightProduct{}
	skippedUnclassified := 0
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var raw struct {
			Provider          string `json:"provider"`
			ProductCode       string `json:"product_code"`
			ProductName       string `json:"product_name"`
			SourceCategoryRaw string `json:"source_category_raw"`
			GroupCode         string `json:"group_code"`
			SearchKeyword     string `json:"search_keyword"`
			ImageURL          string `json:"image_url"`
			ProductURL        string `json:"product_url"`
			PriceKRW          int    `json:"price_krw"`
			OriginalPriceKRW  int    `json:"original_price_krw"`
			Brand             string `json:"brand"`
			Seller            string `json:"seller"`
			CategoryPath      string `json:"category_path"`
			ReviewCount       int    `json:"review_count"`
			OrderCount        int    `json:"order_count"`
			CollectedAt       string `json:"collected_at"`
			UpdatedAt         string `json:"updated_at"`
		}
		if err := json.Unmarshal(line, &raw); err != nil {
			return nil, err
		}
		category := normalizeInsightCategory(raw.Provider, raw.SourceCategoryRaw, raw.CategoryPath, raw.SearchKeyword, raw.ProductName)
		category = insightStandardCategory(category)
		if category == "" {
			// New or malformed upstream category labels must not invalidate an
			// otherwise complete all+10 publish. Exclude those rows before the
			// build; the strict coverage and postcondition checks below still
			// prevent publication unless every standard scope is present.
			skippedUnclassified++
			continue
		}
		p := insightProduct{
			Provider:         raw.Provider,
			ProductCode:      raw.ProductCode,
			ProductName:      cleanInsightText(raw.ProductName),
			SourceCategory:   category,
			GroupCode:        raw.GroupCode,
			SearchKeyword:    raw.SearchKeyword,
			ImageURL:         raw.ImageURL,
			RawProductURL:    raw.ProductURL,
			PriceKRW:         raw.PriceKRW,
			OriginalPriceKRW: raw.OriginalPriceKRW,
			Brand:            cleanInsightText(raw.Brand),
			Seller:           cleanInsightText(raw.Seller),
			CategoryPath:     raw.CategoryPath,
			ReviewCount:      raw.ReviewCount,
			OrderCount:       raw.OrderCount,
			CollectedAt:      raw.CollectedAt,
			UpdatedAt:        raw.UpdatedAt,
		}
		decorateInsightProduct(&p)
		p.Keywords = preprocessInsightKeywords(p)
		products = append(products, p)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if skippedUnclassified > 0 {
		fmt.Printf("Shopping Price Insight skipped unclassified source rows=%d\n", skippedUnclassified)
	}
	return products, nil
}

func shoppingInsightProductsSQL() string {
	return `
SELECT
    provider,
    product_code,
    product_name,
    source_category_raw,
    group_code,
    search_keyword,
    image_url,
    product_url,
    toInt64(round(price_krw)) AS price_krw,
    toInt64(round(original_price_krw)) AS original_price_krw,
    brand,
    seller,
    category_path,
    toInt32(review_count) AS review_count,
    toInt32(order_count) AS order_count,
    collected_at,
    updated_at
FROM
(
    SELECT
        'gmarket' AS provider,
        ifNull(product_code, '') AS product_code,
        ifNull(product_name, '') AS product_name,
        trim(BOTH ' ' FROM ifNull(source_category, '')) AS source_category_raw,
        trim(BOTH ' ' FROM ifNull(group_code, '')) AS group_code,
        trim(BOTH ' ' FROM ifNull(search_keyword, '')) AS search_keyword,
        trim(BOTH ' ' FROM ifNull(image_url, '')) AS image_url,
        coalesce(nullIf(domestic_url, ''), nullIf(global_url, ''), '') AS product_url,
        toFloat64(coalesce(domestic_price_krw, list_price_krw, global_price_krw)) AS price_krw,
        toFloat64(ifNull(list_original_price_krw, 0)) AS original_price_krw,
        ifNull(brand, '') AS brand,
        ifNull(seller, '') AS seller,
        ifNull(category_path, '') AS category_path,
        toUInt64(ifNull(review_count, 0)) AS review_count,
        toUInt64(ifNull(order_count, 0)) AS order_count,
        formatDateTime(collected_at, '%Y-%m-%d %H:%i:%S', 'Asia/Seoul') AS collected_at,
        formatDateTime(updated_at, '%Y-%m-%d %H:%i:%S', 'Asia/Seoul') AS updated_at
    FROM Data_Shopping_Service.gmarket_product_latest
    WHERE notEmpty(product_code)
      AND notEmpty(product_name)
      AND coalesce(domestic_price_krw, list_price_krw, global_price_krw) > 0
    UNION ALL
    SELECT
        'kurly' AS provider,
        ifNull(product_code, '') AS product_code,
        ifNull(product_name, '') AS product_name,
        trim(BOTH ' ' FROM ifNull(source_category, '')) AS source_category_raw,
        trim(BOTH ' ' FROM ifNull(group_code, '')) AS group_code,
        trim(BOTH ' ' FROM ifNull(search_keyword, '')) AS search_keyword,
        trim(BOTH ' ' FROM ifNull(image_url, '')) AS image_url,
        ifNull(product_url, '') AS product_url,
        toFloat64(coalesce(detail_price_krw, list_price_krw)) AS price_krw,
        toFloat64(ifNull(list_original_price_krw, 0)) AS original_price_krw,
        ifNull(brand, '') AS brand,
        ifNull(seller, '') AS seller,
        ifNull(category_path, '') AS category_path,
        toUInt64(ifNull(review_count, 0)) AS review_count,
        toUInt64(0) AS order_count,
        formatDateTime(collected_at, '%Y-%m-%d %H:%i:%S', 'Asia/Seoul') AS collected_at,
        formatDateTime(updated_at, '%Y-%m-%d %H:%i:%S', 'Asia/Seoul') AS updated_at
    FROM Data_Shopping_Service.kurly_product_latest
    WHERE notEmpty(product_code)
      AND notEmpty(product_name)
      AND coalesce(detail_price_krw, list_price_krw) > 0
)
ORDER BY provider ASC, product_code ASC
FORMAT JSONEachRow`
}

func (c *insightCHClient) insertInsightSnapshots(ctx context.Context, table string, rows []insightSnapshotInsert) error {
	var body strings.Builder
	body.WriteString("INSERT INTO ")
	body.WriteString(table)
	body.WriteString(" FORMAT JSONEachRow\n")
	for _, row := range rows {
		b, err := json.Marshal(row)
		if err != nil {
			return err
		}
		body.Write(b)
		body.WriteByte('\n')
	}
	_, err := c.post(ctx, body.String())
	return err
}

func (c *insightCHClient) insertInsightKeywordSearchMart(ctx context.Context, table string, rows []insightKeywordSearchMartInsert) error {
	if len(rows) == 0 {
		return nil
	}
	chunkSize := positiveInt(envString("SHOPPING_INSIGHT_INSERT_CHUNK_SIZE", "1000"), 1000)
	for start := 0; start < len(rows); start += chunkSize {
		end := start + chunkSize
		if end > len(rows) {
			end = len(rows)
		}
		var body strings.Builder
		body.WriteString("INSERT INTO ")
		body.WriteString(table)
		body.WriteString(" FORMAT JSONEachRow\n")
		for _, row := range rows[start:end] {
			b, err := json.Marshal(row)
			if err != nil {
				return err
			}
			body.Write(b)
			body.WriteByte('\n')
		}
		if _, err := c.post(ctx, body.String()); err != nil {
			return fmt.Errorf("keyword chunk %d-%d of %d: %w", start+1, end, len(rows), err)
		}
	}
	return nil
}

func (c *insightCHClient) verifyInsightRefresh(ctx context.Context, tables insightRefreshTables, version uint64) (insightRefreshVerification, error) {
	query := fmt.Sprintf(`
SELECT
    (SELECT count() FROM %s WHERE version = %d) AS snapshot_rows,
    (SELECT arraySort(groupUniqArray(scope_slug)) FROM %s WHERE version = %d) AS snapshot_scopes,
    (SELECT countIf(NOT isValidJSON(payload_json)) FROM %s WHERE version = %d) AS snapshot_invalid_payloads,
    (SELECT countIf(toUInt64(toUnixTimestamp64Milli(generated_at)) != version) FROM %s WHERE version = %d) AS snapshot_version_mismatches,
    (SELECT formatDateTime(maxIf(source_max_collected_at, scope_slug = 'all'), '%%Y-%%m-%%d %%H:%%i:%%S', 'Asia/Seoul') FROM %s WHERE version = %d) AS snapshot_source_max_collected_at,
    (SELECT maxIf(source_product_count, scope_slug = 'all') FROM %s WHERE version = %d) AS snapshot_source_product_count,
    (SELECT count() FROM %s WHERE version = %d) AS keyword_rows,
    (SELECT arraySort(groupUniqArray(scope_slug)) FROM %s WHERE version = %d) AS keyword_scopes,
    (SELECT countIf(NOT isValidJSON(categories_json) OR NOT isValidJSON(products_json)) FROM %s WHERE version = %d) AS keyword_invalid_payloads,
    (SELECT countIf(toUInt64(toUnixTimestamp64Milli(generated_at)) != version) FROM %s WHERE version = %d) AS keyword_version_mismatches,
    (SELECT formatDateTime(maxIf(source_max_collected_at, scope_slug = 'all'), '%%Y-%%m-%%d %%H:%%i:%%S', 'Asia/Seoul') FROM %s WHERE version = %d) AS keyword_source_max_collected_at,
    (SELECT maxIf(source_product_count, scope_slug = 'all') FROM %s WHERE version = %d) AS keyword_source_product_count
FORMAT JSONEachRow`,
		tables.snapshot, version,
		tables.snapshot, version,
		tables.snapshot, version,
		tables.snapshot, version,
		tables.snapshot, version,
		tables.snapshot, version,
		tables.keywordSearch, version,
		tables.keywordSearch, version,
		tables.keywordSearch, version,
		tables.keywordSearch, version,
		tables.keywordSearch, version,
		tables.keywordSearch, version,
	)
	body, err := c.post(ctx, query)
	if err != nil {
		return insightRefreshVerification{}, err
	}
	var verification insightRefreshVerification
	if err := decodeInsightJSONEachRow(body, &verification); err != nil {
		return insightRefreshVerification{}, fmt.Errorf("decode shopping insight refresh verification: %w", err)
	}
	return verification, nil
}

func (c *insightCHClient) insertInsightPublishedBatch(ctx context.Context, table string, row insightPublishedBatchInsert) error {
	body, err := json.Marshal(row)
	if err != nil {
		return err
	}
	query := "INSERT INTO " + table + " FORMAT JSONEachRow\n" + string(body) + "\n"
	_, err = c.post(ctx, query)
	return err
}

func (c *insightCHClient) verifyInsightPublishedBatch(ctx context.Context, table string, expected insightPublishedBatchInsert) error {
	if !insightUUIDRe.MatchString(expected.RefreshUUID) {
		return fmt.Errorf("invalid shopping insight refresh UUID")
	}
	type markerVerification struct {
		Rows                 uint64 `json:"rows"`
		Version              uint64 `json:"version"`
		GeneratedVersion     uint64 `json:"generated_version"`
		InvalidPublishedAt   uint64 `json:"invalid_published_at"`
		SourceMaxCollectedAt string `json:"source_max_collected_at"`
		SourceProductCount   uint64 `json:"source_product_count"`
		ExpectedScopeCount   uint16 `json:"expected_scope_count"`
		SnapshotScopeCount   uint16 `json:"snapshot_scope_count"`
		KeywordScopeCount    uint16 `json:"keyword_scope_count"`
		KeywordRowCount      uint64 `json:"keyword_row_count"`
	}
	query := fmt.Sprintf(`
SELECT
    count() AS rows,
    max(version) AS version,
    toUInt64(toUnixTimestamp64Milli(max(generated_at))) AS generated_version,
    countIf(published_at < generated_at) AS invalid_published_at,
    formatDateTime(max(source_max_collected_at), '%%Y-%%m-%%d %%H:%%i:%%S', 'Asia/Seoul') AS source_max_collected_at,
    max(source_product_count) AS source_product_count,
    max(expected_scope_count) AS expected_scope_count,
    max(snapshot_scope_count) AS snapshot_scope_count,
    max(keyword_scope_count) AS keyword_scope_count,
    max(keyword_row_count) AS keyword_row_count
FROM %s
WHERE refresh_uuid = toUUID('%s')
  AND version = %d
FORMAT JSONEachRow`, table, expected.RefreshUUID, expected.Version)
	body, err := c.post(ctx, query)
	if err != nil {
		return err
	}
	var got markerVerification
	if err := decodeInsightJSONEachRow(body, &got); err != nil {
		return fmt.Errorf("decode shopping insight published-batch verification: %w", err)
	}
	if got.Rows != 1 || got.Version != expected.Version || got.GeneratedVersion != expected.Version || got.InvalidPublishedAt != 0 ||
		got.SourceMaxCollectedAt != expected.SourceMaxCollectedAt ||
		got.SourceProductCount != expected.SourceProductCount ||
		got.ExpectedScopeCount != expected.ExpectedScopeCount ||
		got.SnapshotScopeCount != expected.SnapshotScopeCount ||
		got.KeywordScopeCount != expected.KeywordScopeCount ||
		got.KeywordRowCount != expected.KeywordRowCount {
		return fmt.Errorf("shopping insight published-batch marker verification failed")
	}
	return nil
}

func decodeInsightJSONEachRow(body []byte, target any) error {
	line := bytes.TrimSpace(body)
	if len(line) == 0 {
		return fmt.Errorf("empty ClickHouse response")
	}
	if index := bytes.IndexByte(line, '\n'); index >= 0 {
		line = bytes.TrimSpace(line[:index])
	}
	return json.Unmarshal(line, target)
}

func buildInsightSnapshots(products []insightProduct) ([]insightSnapshotInsert, time.Time, uint64, error) {
	generatedAt := NowKST()
	generatedText := FormatCHDateTime64Millis(generatedAt)
	version := uint64(generatedAt.UnixMilli())
	allCategories := buildInsightCategoryBenchmarks(products, 40)
	categoryProducts := map[string][]insightProduct{}
	for _, p := range products {
		categoryProducts[p.SourceCategory] = append(categoryProducts[p.SourceCategory], p)
	}
	rows := []insightSnapshotInsert{}
	allRadar := buildInsightRadar(products, "", allCategories, allCategories, generatedAt)
	allPayload, err := json.Marshal(allRadar)
	if err != nil {
		return nil, time.Time{}, 0, err
	}
	rows = append(rows, insightSnapshotInsert{
		SnapshotUUID:         NewUUIDv7(),
		ScopeSlug:            "all",
		ScopeCategory:        "",
		Version:              version,
		GeneratedAt:          generatedText,
		SourceMaxCollectedAt: latestInsightCollectedAt(products),
		SourceProductCount:   uint64(len(products)),
		PayloadJSON:          string(allPayload),
		CreatedAt:            generatedText,
	})
	benchmarksByCategory := map[string]insightCategoryBenchmark{}
	for _, category := range allCategories {
		benchmarksByCategory[category.SourceCategory] = category
	}
	for _, categoryName := range insightStandardCategories {
		scoped := categoryProducts[categoryName]
		if len(scoped) == 0 {
			continue
		}
		category := benchmarksByCategory[categoryName]
		radar := buildInsightRadar(scoped, categoryName, allCategories, []insightCategoryBenchmark{category}, generatedAt)
		payload, err := json.Marshal(radar)
		if err != nil {
			return nil, time.Time{}, 0, err
		}
		rows = append(rows, insightSnapshotInsert{
			SnapshotUUID:         NewUUIDv7(),
			ScopeSlug:            insightCategorySlug(categoryName),
			ScopeCategory:        categoryName,
			Version:              version,
			GeneratedAt:          generatedText,
			SourceMaxCollectedAt: latestInsightCollectedAt(scoped),
			SourceProductCount:   uint64(len(scoped)),
			PayloadJSON:          string(payload),
			CreatedAt:            generatedText,
		})
	}
	return rows, generatedAt, version, nil
}

func buildInsightKeywordSearchMartRows(products []insightProduct, generatedAt time.Time, version uint64) ([]insightKeywordSearchMartInsert, error) {
	generatedText := FormatCHDateTime64Millis(generatedAt)
	categoryProducts := map[string][]insightProduct{}
	for _, p := range products {
		categoryProducts[p.SourceCategory] = append(categoryProducts[p.SourceCategory], p)
	}
	rows := []insightKeywordSearchMartInsert{}
	rows = append(rows, buildInsightKeywordSearchMartScope(products, "all", "", NewUUIDv7(), generatedText, version)...)
	for _, category := range insightStandardCategories {
		scoped := categoryProducts[category]
		if len(scoped) == 0 {
			continue
		}
		rows = append(rows, buildInsightKeywordSearchMartScope(scoped, insightCategorySlug(category), category, NewUUIDv7(), generatedText, version)...)
	}
	return rows, nil
}

func validateInsightRefreshBuild(products []insightProduct, snapshots []insightSnapshotInsert, searchRows []insightKeywordSearchMartInsert, version uint64) error {
	if len(products) == 0 || version == 0 {
		return fmt.Errorf("shopping insight build validation failed: empty source or version")
	}
	standardSet := make(map[string]struct{}, len(insightStandardCategories))
	categoryCounts := make(map[string]int, len(insightStandardCategories))
	for _, category := range insightStandardCategories {
		standardSet[category] = struct{}{}
	}
	for _, product := range products {
		if product.PriceKRW <= 0 {
			return fmt.Errorf("shopping insight build validation failed: non-positive price")
		}
		if _, ok := standardSet[product.SourceCategory]; !ok {
			return fmt.Errorf("shopping insight build validation failed: category outside the standard scope set")
		}
		categoryCounts[product.SourceCategory]++
	}
	for _, category := range insightStandardCategories {
		if categoryCounts[category] == 0 {
			return fmt.Errorf("shopping insight build validation failed: standard category coverage is incomplete")
		}
	}

	expectedScopes := insightExpectedScopeSlugs()
	snapshotScopes := make([]string, 0, len(snapshots))
	seenSnapshotScopes := map[string]struct{}{}
	globalSourceMax := latestInsightCollectedAt(products)
	for _, row := range snapshots {
		if row.Version != version || row.SnapshotUUID == "" || row.PayloadJSON == "" || !json.Valid([]byte(row.PayloadJSON)) {
			return fmt.Errorf("shopping insight build validation failed: invalid snapshot row")
		}
		if _, exists := seenSnapshotScopes[row.ScopeSlug]; exists {
			return fmt.Errorf("shopping insight build validation failed: duplicate snapshot scope")
		}
		seenSnapshotScopes[row.ScopeSlug] = struct{}{}
		snapshotScopes = append(snapshotScopes, row.ScopeSlug)
		if row.ScopeSlug == "all" && (row.SourceProductCount != uint64(len(products)) || row.SourceMaxCollectedAt != globalSourceMax) {
			return fmt.Errorf("shopping insight build validation failed: all-scope snapshot source parity")
		}
	}
	if !sameInsightStringSet(snapshotScopes, expectedScopes) {
		return fmt.Errorf("shopping insight build validation failed: snapshot scopes are not all plus the ten standard categories")
	}

	if len(searchRows) == 0 {
		return fmt.Errorf("shopping insight build validation failed: empty keyword mart")
	}
	keywordScopes := make([]string, 0, len(expectedScopes))
	seenKeywordScopes := map[string]struct{}{}
	for _, row := range searchRows {
		if row.Version != version || row.KeywordUUID == "" || row.KeywordKey == "" ||
			!json.Valid([]byte(row.CategoriesJSON)) || !json.Valid([]byte(row.ProductsJSON)) {
			return fmt.Errorf("shopping insight build validation failed: invalid keyword mart row")
		}
		if _, exists := seenKeywordScopes[row.ScopeSlug]; !exists {
			seenKeywordScopes[row.ScopeSlug] = struct{}{}
			keywordScopes = append(keywordScopes, row.ScopeSlug)
		}
		if row.ScopeSlug == "all" && (row.SourceProductCount != uint64(len(products)) || row.SourceMaxCollectedAt != globalSourceMax) {
			return fmt.Errorf("shopping insight build validation failed: all-scope keyword source parity")
		}
	}
	if !sameInsightStringSet(keywordScopes, expectedScopes) {
		return fmt.Errorf("shopping insight build validation failed: keyword scopes are not all plus the ten standard categories")
	}
	return nil
}

func validateInsightRefreshVerification(products []insightProduct, snapshots []insightSnapshotInsert, searchRows []insightKeywordSearchMartInsert, version uint64, got insightRefreshVerification) error {
	if version == 0 || got.SnapshotRows != uint64(len(snapshots)) || got.KeywordRows != uint64(len(searchRows)) {
		return fmt.Errorf("shopping insight postcondition failed: row count mismatch")
	}
	expectedScopes := insightExpectedScopeSlugs()
	if !sameInsightStringSet(got.SnapshotScopes, expectedScopes) || !sameInsightStringSet(got.KeywordScopes, expectedScopes) {
		return fmt.Errorf("shopping insight postcondition failed: scope set mismatch")
	}
	if got.SnapshotInvalidPayloads != 0 || got.KeywordInvalidPayloads != 0 {
		return fmt.Errorf("shopping insight postcondition failed: invalid JSON payload")
	}
	if got.SnapshotVersionMismatches != 0 || got.KeywordVersionMismatches != 0 {
		return fmt.Errorf("shopping insight postcondition failed: generated-at version mismatch")
	}
	globalSourceMax := latestInsightCollectedAt(products)
	productCount := uint64(len(products))
	if got.SnapshotSourceMaxCollectedAt != globalSourceMax || got.KeywordSourceMaxCollectedAt != globalSourceMax ||
		got.SnapshotSourceProductCount != productCount || got.KeywordSourceProductCount != productCount {
		return fmt.Errorf("shopping insight postcondition failed: source version parity mismatch")
	}
	return nil
}

func insightExpectedScopeSlugs() []string {
	scopes := make([]string, 0, len(insightStandardCategories)+1)
	scopes = append(scopes, "all")
	for _, category := range insightStandardCategories {
		scopes = append(scopes, insightCategorySlug(category))
	}
	sort.Strings(scopes)
	return scopes
}

func sameInsightStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	leftCopy := append([]string(nil), left...)
	rightCopy := append([]string(nil), right...)
	sort.Strings(leftCopy)
	sort.Strings(rightCopy)
	for i := range leftCopy {
		if leftCopy[i] != rightCopy[i] {
			return false
		}
	}
	return true
}

func buildInsightKeywordSearchMartScope(products []insightProduct, scopeSlug, scopeCategory, snapshotUUID, generatedText string, version uint64) []insightKeywordSearchMartInsert {
	benchmarks := buildInsightKeywordBenchmarks(products, 0)
	productsByKeyword := map[string][]insightProduct{}
	for _, p := range products {
		for _, keyword := range p.Keywords {
			productsByKeyword[keyword] = append(productsByKeyword[keyword], p)
		}
	}
	rows := make([]insightKeywordSearchMartInsert, 0, len(benchmarks))
	sourceMaxCollectedAt := latestInsightCollectedAt(products)
	for _, row := range benchmarks {
		key := insightKeywordKey(row.Keyword)
		if key == "" {
			continue
		}
		keywordProducts := productsByKeyword[row.Keyword]
		if len(keywordProducts) == 0 {
			continue
		}
		categories := uniqueInsightCategories(keywordProducts)
		categoriesJSON, _ := json.Marshal(categories)
		evidence := topInsightProducts(keywordProducts, insightEvidenceProductLimit)
		productsJSON, _ := json.Marshal(evidence)
		minPrice, maxPrice := insightProductPriceBounds(keywordProducts)
		rows = append(rows, insightKeywordSearchMartInsert{
			KeywordUUID:          NewUUIDv7(),
			SnapshotUUID:         snapshotUUID,
			ScopeSlug:            scopeSlug,
			ScopeCategory:        scopeCategory,
			Version:              version,
			GeneratedAt:          generatedText,
			SourceMaxCollectedAt: sourceMaxCollectedAt,
			SourceProductCount:   uint64(len(products)),
			Keyword:              row.Keyword,
			KeywordKey:           key,
			SearchText:           insightKeywordSearchText(row.Keyword, categories, keywordProducts),
			ProductCount:         uint64(row.ProductCount),
			CategoryCount:        uint64(row.CategoryCount),
			SellerCount:          uint64(row.SellerCount),
			BrandCount:           uint64(row.BrandCount),
			ReviewSum:            uint64(row.ReviewSum),
			OrderSum:             uint64(row.OrderSum),
			MinPriceKRW:          minPrice,
			P25PriceKRW:          row.P25PriceKRW,
			MedianPriceKRW:       row.MedianPriceKRW,
			P75PriceKRW:          row.P75PriceKRW,
			MaxPriceKRW:          maxPrice,
			DemandScore:          row.DemandScore,
			CompetitionScore:     row.CompetitionScore,
			SaturationScore:      row.SaturationScore,
			OpportunityScore:     row.OpportunityScore,
			CategoriesJSON:       string(categoriesJSON),
			ProductsJSON:         string(productsJSON),
			CreatedAt:            generatedText,
		})
	}
	return rows
}

func insightKeywordKey(keyword string) string {
	tokens := insightTokenizeBasic(keyword)
	if len(tokens) == 0 {
		return strings.ToLower(strings.TrimSpace(norm.NFKC.String(keyword)))
	}
	return strings.Join(tokens, " ")
}

func uniqueInsightCategories(products []insightProduct) []string {
	seen := map[string]struct{}{}
	categories := make([]string, 0, 4)
	for _, p := range products {
		category := strings.TrimSpace(p.SourceCategory)
		if category == "" {
			continue
		}
		if _, ok := seen[category]; ok {
			continue
		}
		seen[category] = struct{}{}
		categories = append(categories, category)
	}
	sort.Strings(categories)
	return categories
}

func insightProductPriceBounds(products []insightProduct) (int, int) {
	minPrice := 0
	maxPrice := 0
	for _, p := range products {
		if p.PriceKRW <= 0 {
			continue
		}
		if minPrice == 0 || p.PriceKRW < minPrice {
			minPrice = p.PriceKRW
		}
		if p.PriceKRW > maxPrice {
			maxPrice = p.PriceKRW
		}
	}
	return minPrice, maxPrice
}

func insightKeywordSearchText(keyword string, categories []string, products []insightProduct) string {
	parts := make([]string, 0, 8+len(products)*6)
	parts = append(parts, keyword)
	parts = append(parts, categories...)
	productLimit := minInsightInt(len(products), 160)
	for _, p := range products[:productLimit] {
		parts = append(parts,
			p.ProductName,
			p.ProductLabel,
			p.Brand,
			p.Seller,
			p.SourceCategory,
			p.CategoryPath,
			p.SearchKeyword,
			p.ProductCode,
		)
	}
	text := strings.ToLower(norm.NFKC.String(strings.Join(parts, " ")))
	text = strings.Join(strings.Fields(text), " ")
	runes := []rune(text)
	if len(runes) > 12000 {
		return string(runes[:12000])
	}
	return text
}

func buildInsightRadar(products []insightProduct, scopeCategory string, categoryOptions, categories []insightCategoryBenchmark, generatedAt time.Time) insightRadar {
	scopeLabel := "수집 기준 파생 가격/카테고리 인텔리전스"
	if scopeCategory != "" {
		scopeLabel = scopeCategory + " 집중 가격/키워드 인텔리전스"
	}
	return insightRadar{
		Provider:         "shopping",
		ScopeLabel:       scopeLabel,
		ScopeCategory:    scopeCategory,
		GeneratedAt:      generatedAt.Format("2006-01-02 15:04:05"),
		Summary:          buildInsightSummary(products),
		PriceBands:       buildInsightPriceBands(products),
		Categories:       categories,
		CategoryOptions:  categoryOptions,
		Keywords:         buildInsightKeywordBenchmarks(products, insightKeywordPayloadLimit),
		CategoryKeywords: buildInsightCategoryKeywords(products, insightCategoryKeywordPayloadLimit),
		PriceRangeSlices: buildInsightPriceRangeSlices(products),
		Products:         topInsightProducts(products, insightEvidenceProductLimit),
		DealCandidates:   buildInsightDealCandidates(products, insightEvidenceDealCandidateLimit),
		PriceDrops:       []map[string]any{},
		SellerInsights:   buildInsightSellerInsights(categories, len(categories)),
		PolicyNotes:      insightPolicyNotes(),
	}
}

func buildInsightSummary(products []insightProduct) insightRadarSummary {
	summary := insightRadarSummary{ProductCount: len(products)}
	if len(products) == 0 {
		return summary
	}
	categories := map[string]struct{}{}
	prices := make([]int, 0, len(products))
	for _, p := range products {
		categories[p.SourceCategory] = struct{}{}
		prices = append(prices, p.PriceKRW)
		if p.OriginalPriceKRW > p.PriceKRW {
			summary.DiscountedCount++
		}
		if p.PriceKRW <= 10000 {
			summary.LowPriceCount++
		}
		if summary.FirstCollectedAt == "" || (p.CollectedAt != "" && p.CollectedAt < summary.FirstCollectedAt) {
			summary.FirstCollectedAt = p.CollectedAt
		}
		if p.CollectedAt > summary.LatestCollectedAt {
			summary.LatestCollectedAt = p.CollectedAt
		}
	}
	sort.Ints(prices)
	summary.CategoryCount = len(categories)
	summary.MinPriceKRW = prices[0]
	summary.MedianPriceKRW = percentileInt(prices, 0.5)
	summary.MaxPriceKRW = prices[len(prices)-1]
	summary.DiscountedPercent = percent(summary.DiscountedCount, len(products))
	summary.LowPricePercent = percent(summary.LowPriceCount, len(products))
	return summary
}

func buildInsightCategoryBenchmarks(products []insightProduct, limit int) []insightCategoryBenchmark {
	groups := map[string]*insightAgg{}
	for _, p := range products {
		agg := ensureInsightAgg(groups, p.SourceCategory)
		agg.add(p)
	}
	out := make([]insightCategoryBenchmark, 0, len(groups))
	for category, agg := range groups {
		row := insightCategoryBenchmark{
			SourceCategory:    category,
			ProductCount:      agg.count,
			SellerCount:       len(agg.sellers),
			BrandCount:        len(agg.brands),
			ReviewSum:         agg.reviewSum,
			OrderSum:          agg.orderSum,
			MinPriceKRW:       minSorted(agg.prices),
			P25PriceKRW:       percentileInt(agg.prices, 0.25),
			MedianPriceKRW:    percentileInt(agg.prices, 0.5),
			P75PriceKRW:       percentileInt(agg.prices, 0.75),
			MaxPriceKRW:       maxSorted(agg.prices),
			DiscountedCount:   agg.discountedCount,
			DiscountedPercent: percent(agg.discountedCount, agg.count),
			LowPriceCount:     agg.lowPriceCount,
			LowPricePercent:   percent(agg.lowPriceCount, agg.count),
			LatestCollectedAt: agg.latestCollectedAt,
		}
		row.IQRPriceKRW = row.P75PriceKRW - row.P25PriceKRW
		decorateInsightCategory(&row)
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].LowPriceCount != out[j].LowPriceCount {
			return out[i].LowPriceCount > out[j].LowPriceCount
		}
		if out[i].DiscountedCount != out[j].DiscountedCount {
			return out[i].DiscountedCount > out[j].DiscountedCount
		}
		if out[i].ProductCount != out[j].ProductCount {
			return out[i].ProductCount > out[j].ProductCount
		}
		return out[i].SourceCategory < out[j].SourceCategory
	})
	if limit > 0 && len(out) > limit {
		return out[:limit]
	}
	return out
}

func buildInsightPriceBands(products []insightProduct) []insightPriceBand {
	bands := []insightPriceBand{
		{Label: "<=10k", MinPriceKRW: 0, MaxPriceKRW: 10000},
		{Label: "10k-30k", MinPriceKRW: 10001, MaxPriceKRW: 30000},
		{Label: "30k-50k", MinPriceKRW: 30001, MaxPriceKRW: 50000},
		{Label: "50k-100k", MinPriceKRW: 50001, MaxPriceKRW: 100000},
		{Label: "100k+", MinPriceKRW: 100001},
	}
	totalReaction := 0
	for _, p := range products {
		totalReaction += p.ReviewCount + p.OrderCount*3
		for i := range bands {
			max := bands[i].MaxPriceKRW
			if p.PriceKRW >= bands[i].MinPriceKRW && (max == 0 || p.PriceKRW <= max) {
				bands[i].ProductCount++
				bands[i].ReviewSum += p.ReviewCount
				bands[i].OrderSum += p.OrderCount
				break
			}
		}
	}
	for i := range bands {
		reaction := bands[i].ReviewSum + bands[i].OrderSum*3
		bands[i].ProductPercent = percent(bands[i].ProductCount, len(products))
		bands[i].ReactionPercent = percent(reaction, totalReaction)
		bands[i].Interpretation = insightPriceBandInterpretation(bands[i])
	}
	return bands
}

func buildInsightPriceRangeSlices(products []insightProduct) []insightPriceRangeSlice {
	boundaries := insightPriceRangeBoundaries(products)
	if len(boundaries) < 2 {
		return nil
	}
	out := []insightPriceRangeSlice{}
	for i := 1; i < len(boundaries); i++ {
		minPrice := boundaries[i-1]
		if i > 1 {
			minPrice++
		}
		maxPrice := boundaries[i]
		if maxPrice < minPrice {
			continue
		}
		scoped := filterInsightProductsByPriceRange(products, minPrice, maxPrice)
		if len(scoped) == 0 {
			continue
		}
		categories := buildInsightCategoryBenchmarks(scoped, 40)
		out = append(out, insightPriceRangeSlice{
			Key:              insightPriceRangeKey(minPrice, maxPrice),
			Label:            insightPriceRangeLabel(minPrice, maxPrice),
			MinPriceKRW:      minPrice,
			MaxPriceKRW:      maxPrice,
			Summary:          buildInsightSummary(scoped),
			PriceBands:       buildInsightPriceBands(scoped),
			Categories:       categories,
			Keywords:         buildInsightKeywordBenchmarks(scoped, insightKeywordPayloadLimit),
			CategoryKeywords: buildInsightCategoryKeywords(scoped, insightCategoryKeywordPayloadLimit),
			Products:         topInsightProducts(scoped, insightRangeEvidenceProductLimit),
			DealCandidates:   buildInsightDealCandidates(scoped, insightRangeEvidenceDealCandidateLimit),
			SellerInsights:   buildInsightSellerInsights(categories, len(categories)),
		})
	}
	return out
}

func insightPriceRangeBoundaries(products []insightProduct) []int {
	prices := make([]int, 0, len(products))
	for _, p := range products {
		if p.PriceKRW > 0 {
			prices = append(prices, p.PriceKRW)
		}
	}
	sort.Ints(prices)
	if len(prices) == 0 {
		return nil
	}
	minPrice := prices[0]
	maxPrice := prices[len(prices)-1]
	if minPrice == maxPrice {
		return []int{minPrice, maxPrice}
	}
	seen := map[int]struct{}{minPrice: {}}
	out := []int{minPrice}
	span := maxPrice - minPrice
	for i := 1; i < 5; i++ {
		boundary := roundInsightPriceBoundary(minPrice + (span*i)/5)
		if boundary <= minPrice || boundary >= maxPrice {
			continue
		}
		if _, ok := seen[boundary]; ok {
			continue
		}
		seen[boundary] = struct{}{}
		out = append(out, boundary)
	}
	out = append(out, maxPrice)
	sort.Ints(out)
	return out
}

func roundInsightPriceBoundary(value int) int {
	unit := insightPriceRangeRoundUnit(value)
	if unit <= 0 {
		return value
	}
	return int(math.Round(float64(value)/float64(unit))) * unit
}

func insightPriceRangeRoundUnit(value int) int {
	n := value
	if n < 0 {
		n = -n
	}
	switch {
	case n >= 1000000:
		return 100000
	case n >= 100000:
		return 10000
	case n >= 10000:
		return 1000
	default:
		return 100
	}
}

func insightPriceRangeKey(minPrice, maxPrice int) string {
	return fmt.Sprintf("range:%d-%d", maxInsightInt(0, minPrice), maxInsightInt(0, maxPrice))
}

func insightPriceRangeLabel(minPrice, maxPrice int) string {
	if minPrice <= 0 {
		return fmt.Sprintf("~₩%s", formatInsightNumber(maxPrice))
	}
	return fmt.Sprintf("₩%s~₩%s", formatInsightNumber(minPrice), formatInsightNumber(maxPrice))
}

func formatInsightNumber(value int) string {
	text := fmt.Sprintf("%d", value)
	n := len(text)
	if n <= 3 {
		return text
	}
	var out strings.Builder
	first := n % 3
	if first == 0 {
		first = 3
	}
	out.WriteString(text[:first])
	for i := first; i < n; i += 3 {
		out.WriteByte(',')
		out.WriteString(text[i : i+3])
	}
	return out.String()
}

func filterInsightProductsByPriceRange(products []insightProduct, minPrice, maxPrice int) []insightProduct {
	out := make([]insightProduct, 0, len(products))
	for _, p := range products {
		if p.PriceKRW >= minPrice && p.PriceKRW <= maxPrice {
			out = append(out, p)
		}
	}
	return out
}

func buildInsightKeywordBenchmarks(products []insightProduct, limit int) []insightKeywordBenchmark {
	groups := map[string]*insightAgg{}
	for _, p := range products {
		for _, keyword := range p.Keywords {
			agg := ensureInsightAgg(groups, keyword)
			agg.add(p)
			agg.categories[p.SourceCategory] = struct{}{}
		}
	}
	out := make([]insightKeywordBenchmark, 0, len(groups))
	for keyword, agg := range groups {
		row := insightKeywordBenchmark{
			Keyword:        keyword,
			ProductCount:   agg.count,
			CategoryCount:  len(agg.categories),
			SellerCount:    len(agg.sellers),
			BrandCount:     len(agg.brands),
			ReviewSum:      agg.reviewSum,
			OrderSum:       agg.orderSum,
			P25PriceKRW:    percentileInt(agg.prices, 0.25),
			MedianPriceKRW: percentileInt(agg.prices, 0.5),
			P75PriceKRW:    percentileInt(agg.prices, 0.75),
		}
		decorateInsightKeyword(&row)
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		ri := out[i].ReviewSum + out[i].OrderSum*3
		rj := out[j].ReviewSum + out[j].OrderSum*3
		if ri != rj {
			return ri > rj
		}
		if out[i].ProductCount != out[j].ProductCount {
			return out[i].ProductCount > out[j].ProductCount
		}
		return out[i].Keyword < out[j].Keyword
	})
	if limit > 0 && len(out) > limit {
		return out[:limit]
	}
	return out
}

func buildInsightCategoryKeywords(products []insightProduct, limit int) []insightCategoryKeywordInsight {
	groups := map[string]*insightAgg{}
	for _, p := range products {
		for _, keyword := range p.Keywords {
			key := p.SourceCategory + "\x00" + keyword
			agg := ensureInsightAgg(groups, key)
			agg.add(p)
		}
	}
	byCategory := map[string][]insightCategoryKeywordInsight{}
	for key, agg := range groups {
		parts := strings.SplitN(key, "\x00", 2)
		if len(parts) != 2 {
			continue
		}
		row := insightCategoryKeywordInsight{
			SourceCategory: parts[0],
			Keyword:        parts[1],
			ProductCount:   agg.count,
			SellerCount:    len(agg.sellers),
			BrandCount:     len(agg.brands),
			ReviewSum:      agg.reviewSum,
			OrderSum:       agg.orderSum,
			P25PriceKRW:    percentileInt(agg.prices, 0.25),
			MedianPriceKRW: percentileInt(agg.prices, 0.5),
			P75PriceKRW:    percentileInt(agg.prices, 0.75),
		}
		row.IQRPriceKRW = row.P75PriceKRW - row.P25PriceKRW
		decorateInsightCategoryKeyword(&row)
		byCategory[row.SourceCategory] = append(byCategory[row.SourceCategory], row)
	}
	out := []insightCategoryKeywordInsight{}
	categories := make([]string, 0, len(byCategory))
	for category := range byCategory {
		categories = append(categories, category)
	}
	sort.Strings(categories)
	for _, category := range categories {
		rows := byCategory[category]
		sort.Slice(rows, func(i, j int) bool {
			ri := rows[i].ReviewSum + rows[i].OrderSum*3
			rj := rows[j].ReviewSum + rows[j].OrderSum*3
			if ri != rj {
				return ri > rj
			}
			if rows[i].ProductCount != rows[j].ProductCount {
				return rows[i].ProductCount > rows[j].ProductCount
			}
			return rows[i].Keyword < rows[j].Keyword
		})
		if len(rows) > insightCategoryKeywordPerCategoryLimit {
			rows = rows[:insightCategoryKeywordPerCategoryLimit]
		}
		out = append(out, rows...)
	}
	if limit > 0 && len(out) > limit {
		return out[:limit]
	}
	return out
}

func buildInsightDealCandidates(products []insightProduct, limit int) []insightDealCandidate {
	categoryMedian := map[string]int{}
	categoryPrices := map[string][]int{}
	for _, p := range products {
		categoryPrices[p.SourceCategory] = append(categoryPrices[p.SourceCategory], p.PriceKRW)
	}
	for category, prices := range categoryPrices {
		sort.Ints(prices)
		categoryMedian[category] = percentileInt(prices, 0.5)
	}
	out := []insightDealCandidate{}
	for _, p := range products {
		median := categoryMedian[p.SourceCategory]
		discount := 0.0
		if p.OriginalPriceKRW > p.PriceKRW && p.OriginalPriceKRW > 0 {
			discount = round2(100 * float64(p.OriginalPriceKRW-p.PriceKRW) / float64(p.OriginalPriceKRW))
		}
		below := 0.0
		if median > p.PriceKRW && median > 0 {
			below = round2(100 * float64(median-p.PriceKRW) / float64(median))
		}
		if p.PriceKRW > 30000 && discount < 20 && below < 20 {
			continue
		}
		score := clampInsightScore(
			math.Min(45, math.Max(0, below*0.45)) +
				math.Min(35, math.Max(0, discount*0.35)) +
				func() float64 {
					if p.PriceKRW <= 10000 {
						return 20
					}
					if p.PriceKRW <= 30000 {
						return 10
					}
					return 0
				}(),
		)
		item := insightDealCandidate{
			insightProduct:             p,
			DiscountPercent:            discount,
			CategoryMedianPriceKRW:     median,
			BelowCategoryMedianPercent: below,
			RadarScore:                 score,
			DealConfidenceScore:        score,
			Reason:                     insightDealReason(discount, below, p.PriceKRW),
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RadarScore != out[j].RadarScore {
			return out[i].RadarScore > out[j].RadarScore
		}
		if out[i].DiscountPercent != out[j].DiscountPercent {
			return out[i].DiscountPercent > out[j].DiscountPercent
		}
		if out[i].BelowCategoryMedianPercent != out[j].BelowCategoryMedianPercent {
			return out[i].BelowCategoryMedianPercent > out[j].BelowCategoryMedianPercent
		}
		return out[i].PriceKRW < out[j].PriceKRW
	})
	return interleaveInsightDealCandidatesByProvider(out, limit)
}

func topInsightProducts(products []insightProduct, limit int) []insightProduct {
	out := append([]insightProduct(nil), products...)
	sort.Slice(out, func(i, j int) bool {
		ri := out[i].ReviewCount + out[i].OrderCount*3
		rj := out[j].ReviewCount + out[j].OrderCount*3
		if ri != rj {
			return ri > rj
		}
		if out[i].CollectedAt != out[j].CollectedAt {
			return out[i].CollectedAt > out[j].CollectedAt
		}
		return out[i].PriceKRW < out[j].PriceKRW
	})
	return interleaveInsightProductsByProvider(out, limit)
}

func interleaveInsightProductsByProvider(items []insightProduct, limit int) []insightProduct {
	if limit <= 0 {
		limit = len(items)
	}
	if len(items) <= 1 || limit <= 0 {
		return items
	}
	buckets := map[string][]insightProduct{}
	for _, item := range items {
		provider := normalizeInsightProvider(item.Provider)
		buckets[provider] = append(buckets[provider], item)
	}
	providers := orderedInsightProviders(buckets)
	if len(providers) < 2 {
		if len(items) > limit {
			return append([]insightProduct(nil), items[:limit]...)
		}
		return items
	}
	out := make([]insightProduct, 0, minInsightInt(limit, len(items)))
	for len(out) < limit && len(out) < len(items) {
		progress := false
		for _, provider := range providers {
			bucket := buckets[provider]
			if len(bucket) == 0 {
				continue
			}
			out = append(out, bucket[0])
			buckets[provider] = bucket[1:]
			progress = true
			if len(out) >= limit {
				break
			}
		}
		if !progress {
			break
		}
	}
	return out
}

func interleaveInsightDealCandidatesByProvider(items []insightDealCandidate, limit int) []insightDealCandidate {
	if limit <= 0 {
		limit = len(items)
	}
	if len(items) <= 1 || limit <= 0 {
		return items
	}
	buckets := map[string][]insightDealCandidate{}
	for _, item := range items {
		provider := normalizeInsightProvider(item.Provider)
		buckets[provider] = append(buckets[provider], item)
	}
	providers := orderedInsightProviders(buckets)
	if len(providers) < 2 {
		if len(items) > limit {
			return append([]insightDealCandidate(nil), items[:limit]...)
		}
		return items
	}
	out := make([]insightDealCandidate, 0, minInsightInt(limit, len(items)))
	for len(out) < limit && len(out) < len(items) {
		progress := false
		for _, provider := range providers {
			bucket := buckets[provider]
			if len(bucket) == 0 {
				continue
			}
			out = append(out, bucket[0])
			buckets[provider] = bucket[1:]
			progress = true
			if len(out) >= limit {
				break
			}
		}
		if !progress {
			break
		}
	}
	return out
}

func orderedInsightProviders[T any](buckets map[string][]T) []string {
	out := make([]string, 0, len(buckets))
	for _, provider := range []string{"gmarket", "kurly"} {
		if len(buckets[provider]) > 0 {
			out = append(out, provider)
		}
	}
	extras := make([]string, 0, len(buckets))
	for provider, items := range buckets {
		if len(items) == 0 || provider == "gmarket" || provider == "kurly" {
			continue
		}
		extras = append(extras, provider)
	}
	sort.Strings(extras)
	return append(out, extras...)
}

func normalizeInsightProvider(provider string) string {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "kurly":
		return "kurly"
	case "gmarket":
		return "gmarket"
	default:
		if strings.TrimSpace(provider) != "" {
			return strings.ToLower(strings.TrimSpace(provider))
		}
		return "gmarket"
	}
}

func minInsightInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInsightInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

type insightAgg struct {
	count             int
	sellers           map[string]struct{}
	brands            map[string]struct{}
	categories        map[string]struct{}
	prices            []int
	reviewSum         int
	orderSum          int
	discountedCount   int
	lowPriceCount     int
	latestCollectedAt string
}

func ensureInsightAgg(groups map[string]*insightAgg, key string) *insightAgg {
	if agg := groups[key]; agg != nil {
		return agg
	}
	agg := &insightAgg{
		sellers:    map[string]struct{}{},
		brands:     map[string]struct{}{},
		categories: map[string]struct{}{},
	}
	groups[key] = agg
	return agg
}

func (a *insightAgg) add(p insightProduct) {
	a.count++
	if p.Seller != "" {
		a.sellers[p.Seller] = struct{}{}
	}
	if p.Brand != "" {
		a.brands[p.Brand] = struct{}{}
	}
	a.prices = append(a.prices, p.PriceKRW)
	a.reviewSum += p.ReviewCount
	a.orderSum += p.OrderCount
	if p.OriginalPriceKRW > p.PriceKRW {
		a.discountedCount++
	}
	if p.PriceKRW <= 10000 {
		a.lowPriceCount++
	}
	if p.CollectedAt > a.latestCollectedAt {
		a.latestCollectedAt = p.CollectedAt
	}
}

func preprocessInsightKeywords(p insightProduct) []string {
	categoryStops := insightCategoryStopSet(p.SourceCategory + " " + p.SearchKeyword + " " + p.Brand)
	rawTokens := insightTokenizeBasic(strings.Join([]string{p.ProductName, p.CategoryPath, p.SearchKeyword}, " "))
	normalized := make([]string, 0, len(rawTokens))
	for _, token := range rawTokens {
		token = normalizeInsightToken(token, categoryStops)
		if token == "" {
			continue
		}
		normalized = append(normalized, token)
	}
	out := []string{}
	seen := map[string]struct{}{}
	add := func(keyword string) {
		if keyword == "" {
			return
		}
		if _, ok := seen[keyword]; ok {
			return
		}
		seen[keyword] = struct{}{}
		out = append(out, keyword)
	}
	for i := 0; i+1 < len(normalized); i++ {
		a, b := normalized[i], normalized[i+1]
		if a == b || insightKeywordTooGeneric(a) || insightKeywordTooGeneric(b) {
			continue
		}
		add(a + " " + b)
	}
	for _, token := range normalized {
		add(token)
	}
	if len(out) > 8 {
		out = out[:8]
	}
	return out
}

func insightTokenizeBasic(text string) []string {
	text = strings.ToLower(norm.NFKC.String(text))
	parts := insightSplitRe.Split(text, -1)
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func normalizeInsightToken(token string, categoryStops map[string]struct{}) string {
	token = strings.Trim(strings.ToLower(norm.NFKC.String(token)), "_- ")
	if token == "" || insightDigitRe.MatchString(token) {
		return ""
	}
	if _, ok := insightStopwords[token]; ok {
		return ""
	}
	if _, ok := categoryStops[token]; ok {
		return ""
	}
	if isASCIIWord(token) {
		token = stemInsightEnglish(token)
		if !validInsightKeywordToken(token) {
			return ""
		}
		if _, ok := insightStopwords[token]; ok {
			return ""
		}
		if _, ok := categoryStops[token]; ok {
			return ""
		}
		return token
	}
	token = normalizeInsightKoreanToken(token)
	if !validInsightKeywordToken(token) {
		return ""
	}
	if _, ok := categoryStops[token]; ok {
		return ""
	}
	if _, ok := insightStopwords[token]; ok {
		return ""
	}
	return token
}

func normalizeInsightKoreanToken(token string) string {
	token = strings.TrimSuffix(token, "전용")
	token = strings.TrimSuffix(token, "용")
	token = strings.TrimSuffix(token, "형")
	token = strings.TrimSuffix(token, "식")
	token = stripInsightKoreanParticle(token)
	if v, ok := insightCanonicalKorean[token]; ok {
		return v
	}
	return token
}

func stripInsightKoreanParticle(token string) string {
	if len([]rune(token)) <= 2 {
		return token
	}
	for _, suffix := range []string{"으로", "에서", "부터", "까지", "에게", "하고", "처럼", "보다", "과", "와", "을", "를", "이", "가", "은", "는", "로", "에", "의", "도", "만"} {
		if strings.HasSuffix(token, suffix) && len([]rune(token)) > len([]rune(suffix))+1 {
			return strings.TrimSuffix(token, suffix)
		}
	}
	return token
}

func stemInsightEnglish(token string) string {
	if v, ok := insightCanonicalEnglish[token]; ok {
		return v
	}
	switch {
	case strings.HasSuffix(token, "ational") && len(token) > 8:
		token = strings.TrimSuffix(token, "ational") + "ate"
	case strings.HasSuffix(token, "ies") && len(token) > 5:
		token = strings.TrimSuffix(token, "ies") + "y"
	case strings.HasSuffix(token, "ing") && len(token) > 6:
		token = strings.TrimSuffix(token, "ing")
	case strings.HasSuffix(token, "ers") && len(token) > 6:
		token = strings.TrimSuffix(token, "ers")
	case strings.HasSuffix(token, "er") && len(token) > 5:
		token = strings.TrimSuffix(token, "er")
	case strings.HasSuffix(token, "ed") && len(token) > 5:
		token = strings.TrimSuffix(token, "ed")
	case strings.HasSuffix(token, "ly") && len(token) > 5:
		token = strings.TrimSuffix(token, "ly")
	case strings.HasSuffix(token, "es") && len(token) > 5:
		token = strings.TrimSuffix(token, "es")
	case strings.HasSuffix(token, "s") && len(token) > 4 && !strings.HasSuffix(token, "ss"):
		token = strings.TrimSuffix(token, "s")
	}
	if v, ok := insightCanonicalEnglish[token]; ok {
		return v
	}
	return token
}

func validInsightKeywordToken(token string) bool {
	runeCount := len([]rune(token))
	if runeCount < 2 || runeCount > 24 {
		return false
	}
	if isASCIIWord(token) && runeCount < 3 {
		return false
	}
	return true
}

func isASCIIWord(token string) bool {
	for _, r := range token {
		if r < 'a' || r > 'z' {
			return false
		}
	}
	return token != ""
}

func insightCategoryStopSet(text string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, token := range insightTokenizeBasic(text) {
		for _, candidate := range []string{token, stemInsightEnglish(token), normalizeInsightKoreanToken(token)} {
			if candidate != "" {
				out[candidate] = struct{}{}
			}
		}
	}
	return out
}

func insightKeywordTooGeneric(token string) bool {
	_, ok := insightGenericPhraseBlock[token]
	return ok
}

var insightCanonicalEnglish = map[string]string{
	"bag":        "가방",
	"bags":       "가방",
	"babies":     "유아",
	"baby":       "유아",
	"ankle":      "발목",
	"backpack":   "백팩",
	"band":       "밴드",
	"beanie":     "비니",
	"bath":       "욕실",
	"bathroom":   "욕실",
	"beauty":     "뷰티",
	"bedding":    "침구",
	"box":        "박스",
	"bracelet":   "팔찌",
	"cap":        "모자",
	"carri":      "캐리어",
	"butter":     "버터",
	"cabinet":    "수납장",
	"cabinets":   "수납장",
	"calculator": "계산기",
	"cat":        "고양이",
	"cats":       "고양이",
	"cheese":     "치즈",
	"chicken":    "닭고기",
	"children":   "유아",
	"collagen":   "콜라겐",
	"coffee":     "커피",
	"cosmetic":   "화장품",
	"cosmetics":  "화장품",
	"desk":       "책상",
	"detergent":  "세제",
	"diap":       "기저귀",
	"digital":    "디지털",
	"dog":        "강아지",
	"dogs":       "강아지",
	"dress":      "의류",
	"dried":      "건조",
	"electronic": "전자",
	"eyebrow":    "눈썹",
	"eyelash":    "속눈썹",
	"fashion":    "패션",
	"fish":       "생선",
	"food":       "식품",
	"frying":     "프라이팬",
	"gifticon":   "기프티콘",
	"fresh":      "신선",
	"fruit":      "과일",
	"fruits":     "과일",
	"germanium":  "게르마늄",
	"grocery":    "식품",
	"guard":      "보호대",
	"health":     "건강",
	"hobby":      "취미",
	"insol":      "깔창",
	"insole":     "깔창",
	"keyboard":   "키보드",
	"kitchen":    "주방",
	"laptop":     "노트북",
	"laver":      "김",
	"luggage":    "여행가방",
	"lunch":      "도시락",
	"mask":       "마스크",
	"men":        "남성",
	"milk":       "우유",
	"neck":       "목",
	"necktie":    "넥타이",
	"noodle":     "라면",
	"noodles":    "라면",
	"oil":        "오일",
	"pan":        "프라이팬",
	"pet":        "펫",
	"pets":       "펫",
	"puppies":    "강아지",
	"puppy":      "강아지",
	"rack":       "선반",
	"rice":       "쌀",
	"razor":      "면도기",
	"shampoo":    "샴푸",
	"shelf":      "선반",
	"shoe":       "신발",
	"shoes":      "신발",
	"snack":      "간식",
	"snacks":     "간식",
	"sport":      "스포츠",
	"stationery": "문구",
	"storage":    "수납",
	"sunscreen":  "선크림",
	"silicon":    "실리콘",
	"tissue":     "티슈",
	"toe":        "발가락",
	"towel":      "수건",
	"travel":     "여행",
	"vegetable":  "채소",
	"vegetables": "채소",
	"warm":       "워머",
	"water":      "생수",
	"women":      "여성",
	"yogurt":     "요거트",
}

var insightCanonicalKorean = map[string]string{
	"강아지사료": "강아지",
	"고양이사료": "고양이",
	"반려견":   "강아지",
	"반려묘":   "고양이",
	"욕실장":   "수납장",
	"키친":    "주방",
}

var insightGenericPhraseBlock = map[string]struct{}{
	"best": {}, "new": {}, "sale": {}, "hot": {}, "made": {}, "korea": {}, "korean": {},
	"무료": {}, "배송": {}, "상품": {}, "정품": {}, "공식": {}, "국내": {}, "국산": {},
}

var insightStopwords = map[string]struct{}{
	"a": {}, "about": {}, "above": {}, "after": {}, "again": {}, "all": {}, "also": {}, "am": {}, "an": {}, "and": {}, "any": {}, "are": {}, "as": {}, "at": {},
	"be": {}, "because": {}, "been": {}, "before": {}, "being": {}, "best": {}, "between": {}, "both": {}, "but": {}, "by": {},
	"automatic": {}, "basic": {}, "black": {}, "can": {}, "capacity": {}, "casio": {}, "certification": {}, "certificat": {}, "cks": {}, "cm": {}, "comfort": {}, "could": {}, "day": {}, "diamond": {}, "each": {}, "eas": {}, "edition": {}, "exclusive": {}, "for": {}, "from": {}, "functional": {}, "futuro": {}, "gmarket": {}, "goods": {}, "had": {}, "has": {}, "have": {}, "he": {}, "her": {}, "here": {}, "high": {}, "him": {}, "his": {}, "hot": {}, "how": {},
	"giveaway": {}, "haccp": {}, "in": {}, "into": {}, "invitate": {}, "invitation": {}, "invitational": {}, "invite": {}, "is": {}, "it": {}, "item": {}, "its": {}, "kookmin": {}, "korea": {}, "korean": {}, "kurly": {}, "limit": {}, "limited": {}, "made": {}, "market": {}, "marketkurly": {}, "military": {}, "mm": {}, "more": {}, "most": {}, "multi": {},
	"manual": {}, "medicine": {}, "natural": {}, "new": {}, "no": {}, "not": {}, "of": {}, "office": {}, "official": {}, "olbaan": {}, "on": {}, "one": {}, "only": {}, "or": {}, "other": {}, "our": {}, "out": {}, "over": {}, "pack": {}, "per": {}, "plus": {}, "portable": {}, "premium": {}, "product": {}, "products": {}, "purpose": {},
	"sale": {}, "seban": {}, "set": {}, "she": {}, "slim": {}, "sneak": {}, "so": {}, "some": {}, "starbuck": {}, "sticky": {}, "such": {}, "supplies": {}, "than": {}, "that": {}, "the": {}, "their": {}, "them": {}, "then": {}, "there": {}, "these": {}, "they": {}, "this": {}, "to": {}, "up": {}, "use": {}, "using": {}, "wando": {},
	"was": {}, "we": {}, "were": {}, "what": {}, "when": {}, "where": {}, "which": {}, "who": {}, "will": {}, "with": {}, "you": {}, "your": {},
	"가격": {}, "기획": {}, "골라": {}, "골라담기": {}, "담기": {}, "단독": {}, "대용량": {}, "마켓컬리": {}, "묶음": {}, "무료": {}, "무료배송": {}, "배송": {}, "베스트": {}, "상품": {}, "선택": {}, "세일": {}, "세트": {}, "신상": {}, "옵션": {}, "전용": {}, "정품": {}, "추가": {}, "컬리": {}, "특가": {}, "할인": {},
	"공식": {}, "국내": {}, "국산": {}, "모음": {}, "수입": {}, "신선": {}, "예약": {}, "인기": {}, "증정": {}, "직구": {}, "카시오": {}, "해외": {},
}

func insightStandardCategory(category string) string {
	category = cleanInsightText(category)
	if category == "" {
		return ""
	}
	slug := insightCategorySlug(category)
	for _, standard := range insightStandardCategories {
		if slug == insightCategorySlug(standard) {
			return standard
		}
	}
	switch slug {
	case "신선식품", "가공식품", "식품-신선", "식품-가공", "간식", "간식빵", "선식-시리얼", "식용유-참기름-오일", "브로콜리-파프리카-양배추", "소시지-베이컨-하몽", "달걀-가공란", "달걀", "명란", "디저트", "오징어-낙지-문어", "국", "치즈", "코코아-밀크티-기타-차", "수입산-돼지고기-양고기", "신선하게-받아보는", "닭고기", "닭가슴살", "닭-오리고기", "밀가루-가루-믹스", "김-미역-해조류", "치킨-피자-핫도그-만두", "잡곡", "멸치-황태-다시팩", "떡볶이", "떡-한과", "초콜릿-젤리-캔디", "증류주-약주-청주", "콩나물-버섯", "두부-어묵-부침개", "아이스크림", "이유식-재료", "분유-간편-이유식", "짜장-짬뽕-파스타-면류", "피자", "친환경", "6월신상품", "7월신상품", "조미료", "소금-설탕-향신료", "회-탕류", "탕", "양념-액젓-장류", "죽-스프-카레", "구수한-집밥", "풍성하게-담은", "논알콜-무알콜", "햄-통조림-병조림", "식초-소스-드레싱", "간단히-맛보는", "찌개", "식단관리용-가공육", "집밥의발견":
		return "식품"
	case "생활", "주방", "생필품", "생필품-육아", "수건", "스크럽-대디":
		return "생활/주방"
	case "뷰티", "화장품", "립메이크업", "건강", "헬스", "여성-위생용품", "구강-면도", "프로틴":
		return "뷰티/헬스"
	case "패션", "패션-의류", "잡화", "가방", "신발", "운동화":
		return "패션/잡화"
	case "디지털", "가전", "컴퓨터", "보조배터리", "키보드", "선풍기":
		return "디지털/가전"
	case "가구", "홈", "홈패브릭", "책상", "테이블-식탁-책상":
		return "가구/홈"
	case "스포츠", "스포츠-건강", "수영":
		return "스포츠/레저"
	case "유아", "육아", "반려", "펫", "강아지-주식", "장난감":
		return "유아/반려"
	case "도서", "도서-음반", "취미-문구-펫", "문구", "취미":
		return "도서/취미/문구"
	case "여행", "e쿠폰", "이쿠폰", "쿠폰":
		return "여행/e쿠폰"
	}
	lower := strings.ToLower(category)
	switch {
	case containsAny(lower, "식품", "간식", "고기", "닭", "돼지", "소고", "수산", "해조", "면류", "떡", "빵", "치즈", "우유", "분유", "이유식", "아이스크림", "초콜릿", "피자", "만두", "시리얼", "친환경", "조미료", "소스", "장류", "찌개", "탕류", "통조림"):
		return "식품"
	case containsAny(lower, "생활", "주방", "수건", "세제", "휴지", "스크럽"):
		return "생활/주방"
	case containsAny(lower, "뷰티", "헬스", "화장", "위생", "구강", "면도", "프로틴"):
		return "뷰티/헬스"
	case containsAny(lower, "패션", "잡화", "의류", "신발", "운동화", "가방"):
		return "패션/잡화"
	case containsAny(lower, "디지털", "가전", "컴퓨터", "선풍기", "키보드"):
		return "디지털/가전"
	case containsAny(lower, "가구", "홈", "침구", "테이블", "책상"):
		return "가구/홈"
	case containsAny(lower, "스포츠", "레저", "수영"):
		return "스포츠/레저"
	case containsAny(lower, "유아", "육아", "반려", "펫", "강아지", "고양이"):
		return "유아/반려"
	case containsAny(lower, "도서", "취미", "문구", "음반"):
		return "도서/취미/문구"
	case containsAny(lower, "여행", "쿠폰", "e쿠폰"):
		return "여행/e쿠폰"
	default:
		return ""
	}
}

func normalizeInsightCategory(provider, raw, categoryPath, searchKeyword, productName string) string {
	raw = cleanInsightText(raw)
	rawContext := strings.ToLower(norm.NFKC.String(strings.Join([]string{raw, categoryPath, searchKeyword}, " ")))
	productContext := strings.ToLower(norm.NFKC.String(productName))
	switch {
	case containsAny(productContext, "도서", "책", "문구", "노트", "펜", "연필", "필기구", "음반", "앨범", "취미", "피규어", "퍼즐"):
		return "도서/취미/문구"
	case containsAny(rawContext, "유아", "육아", "출산", "아기", "베이비", "키즈", "기저귀", "장난감", "강아지", "고양이", "반려", "펫", "사료", "주식") || containsAny(productContext, "유아", "육아", "출산", "아기", "베이비", "키즈", "기저귀", "장난감", "강아지", "고양이", "반려", "펫", "사료", "간식", "주식"):
		return "유아/반려"
	case containsAny(rawContext, "e쿠폰", "이쿠폰", "쿠폰", "기프티콘", "상품권", "교환권", "모바일쿠폰", "여행", "항공", "숙박", "호텔", "렌터카", "티켓"):
		return "여행/e쿠폰"
	case containsAny(rawContext, "도서", "음반", "문구", "취미") || containsAny(productContext, "도서", "책", "문구", "노트", "펜", "연필", "음반", "앨범", "취미", "피규어", "퍼즐"):
		return "도서/취미/문구"
	case containsAny(rawContext, "패션", "잡화", "의류", "신발", "가방", "쥬얼리", "주얼리", "시계", "모자") || containsAny(productContext, "의류", "티셔츠", "셔츠", "팬츠", "원피스", "신발", "운동화", "가방", "백팩", "모자", "양말"):
		return "패션/잡화"
	case containsAny(rawContext, "디지털", "가전", "컴퓨터", "모바일", "노트북", "전자", "보조배터리", "키보드", "마우스", "모니터", "충전기", "케이블", "이어폰", "헤드셋") || containsAny(productContext, "노트북", "키보드", "마우스", "모니터", "충전기", "케이블", "이어폰", "헤드셋", "보조배터리", "가전"):
		return "디지털/가전"
	case containsAny(rawContext, "가구", "홈", "침구", "인테리어", "수납", "조명", "책상", "테이블", "식탁", "홈패브릭") || containsAny(productContext, "침대", "소파", "책상", "의자", "수납장", "선반", "매트리스", "커튼", "러그", "이불", "베개", "테이블", "식탁", "홈패브릭"):
		return "가구/홈"
	case containsAny(rawContext, "스포츠", "레저", "캠핑", "골프", "등산", "낚시", "자동차", "공구", "수영") || containsAny(productContext, "운동", "스포츠", "골프", "캠핑", "등산", "자전거", "헬멧", "보호대", "낚시", "수영"):
		return "스포츠/레저"
	case containsAny(rawContext, "뷰티", "화장", "헬스", "건강", "미용", "립메이크업", "메이크업") || containsAny(productContext, "화장품", "선크림", "마스크팩", "샴푸", "바디워시", "스킨", "로션", "크림", "콜라겐", "비타민", "영양제", "립스틱", "립메이크업", "메이크업"):
		return "뷰티/헬스"
	case containsAny(productContext, "채소", "과일", "정육", "닭고기", "돼지고기", "소고기", "수산", "생선", "새우", "계란", "달걀", "샐러드", "쌀", "잡곡", "간편식", "밀키트", "반찬", "김치", "라면", "떡볶이", "만두", "피자", "냉동", "과자", "초콜릿", "커피", "차", "주스", "생수", "베이커리", "우유", "요거트", "치즈", "버터", "식품", "푸드", "고기", "음료", "양배추", "브로콜리", "파프리카", "오이", "당근", "양파", "감자", "고구마", "토마토", "사과", "배", "바나나", "포도", "귤", "딸기", "한우", "소시지", "베이컨", "하몽", "명란", "오징어", "낙지", "문어", "디저트", "코코아", "밀크티", "국"):
		return "식품"
	case containsAny(rawContext, "신선식품", "가공식품", "식품", "푸드", "채소", "과일", "정육", "수산", "계란", "달걀", "쌀", "간편식", "밀키트", "반찬", "김치", "라면", "냉동", "과자", "커피", "음료", "생수", "베이커리", "우유", "양배추", "브로콜리", "파프리카", "오이", "당근", "양파", "감자", "고구마", "토마토", "사과", "바나나", "소시지", "베이컨", "하몽", "명란", "오징어", "낙지", "문어", "디저트", "코코아", "밀크티", "기타 차", "치즈", "국", "신선하게 받아보는", "돼지고기", "양고기"):
		return "식품"
	case containsAny(rawContext, "생활", "주방", "생필품", "세제", "휴지", "청소", "욕실", "세탁", "수건") || containsAny(productContext, "세제", "휴지", "티슈", "칫솔", "치약", "수건", "주방", "프라이팬", "냄비", "그릇", "청소", "욕실", "세탁"):
		return "생활/주방"
	case insightStandardCategory(raw) != "":
		return insightStandardCategory(raw)
	case insightStandardCategory(searchKeyword) != "":
		return insightStandardCategory(searchKeyword)
	default:
		return ""
	}
}

func containsAny(s string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(s, needle) {
			return true
		}
	}
	return false
}

func decorateInsightProduct(p *insightProduct) {
	p.Provider = strings.ToLower(strings.TrimSpace(p.Provider))
	switch p.Provider {
	case "kurly":
		p.ProviderLabel = "Kurly"
	default:
		p.Provider = "gmarket"
		p.ProviderLabel = "Gmarket"
	}
	p.ProductLabel = p.ProductName
	if p.ProductLabel == "" {
		p.ProductLabel = p.ProductCode
	}
	p.PriceBasis = "수집 시점 관측가"
	if p.ProductCode != "" {
		p.ProductURL = "/workbench/shopping/out/" + url.PathEscape(p.Provider) + "/" + url.PathEscape(p.ProductCode) + "/"
	}
}

func buildInsightSellerInsights(categories []insightCategoryBenchmark, limit int) []insightSellerInsight {
	if limit <= 0 || limit > len(categories) {
		limit = len(categories)
	}
	out := []insightSellerInsight{}
	for _, category := range categories[:limit] {
		level := "watch"
		action := "가격 분위수와 키워드 반응을 추가 관찰하세요."
		if category.LowPricePercent >= 45 || (category.LowPricePercent >= 30 && category.ProductCount >= 12) {
			level = "high_price_pressure"
			action = "배송비와 옵션 추가금까지 포함한 총액 경쟁력을 먼저 점검하세요."
		} else if category.DiscountedPercent >= 35 {
			level = "promotion_sensitive"
			action = "표시 할인보다 실제 최종가와 프로모션 타이밍을 비교하세요."
		} else if category.ProductCount <= 3 {
			level = "thin_sample"
			action = "표본이 적으므로 추가 수집이나 셀러 업로드 데이터로 보강하세요."
		}
		out = append(out, insightSellerInsight{
			SourceCategory:    category.SourceCategory,
			ProductCount:      category.ProductCount,
			MedianPriceKRW:    category.MedianPriceKRW,
			LowPricePercent:   category.LowPricePercent,
			DiscountedPercent: category.DiscountedPercent,
			CompetitionLevel:  level,
			RecommendedAction: action,
		})
	}
	return out
}

func decorateInsightCategory(item *insightCategoryBenchmark) {
	item.DemandScore = insightDemandScore(item.ProductCount, item.ReviewSum, item.OrderSum)
	item.CompetitionScore = insightCompetitionScore(item.ProductCount, item.SellerCount, item.BrandCount)
	item.OpportunityScore = clampInsightScore(item.DemandScore*0.45 + (100-item.CompetitionScore)*0.25 + item.LowPricePercent*0.15 + item.DiscountedPercent*0.15)
	switch {
	case item.OpportunityScore >= 70:
		item.Interpretation = "수요 신호 대비 경쟁 부담이 낮아 우선 검토할 시장입니다."
	case item.CompetitionScore >= 70:
		item.Interpretation = "상품·셀러 밀도가 높아 가격 외 차별화가 필요합니다."
	case item.IQRPriceKRW > item.MedianPriceKRW:
		item.Interpretation = "가격 분산이 커서 하위 가격군이나 세그먼트별 비교가 필요합니다."
	default:
		item.Interpretation = "가격 분위수와 키워드 반응을 함께 관찰할 시장입니다."
	}
}

func decorateInsightKeyword(item *insightKeywordBenchmark) {
	item.DemandScore = insightDemandScore(item.ProductCount, item.ReviewSum, item.OrderSum)
	item.CompetitionScore = insightCompetitionScore(item.ProductCount, item.SellerCount, item.BrandCount)
	item.SaturationScore = clampInsightScore(item.CompetitionScore*0.65 - item.DemandScore*0.25 + math.Log1p(float64(item.CategoryCount))*8)
	item.OpportunityScore = clampInsightScore(item.DemandScore*0.50 + (100-item.CompetitionScore)*0.30 + math.Log1p(float64(item.CategoryCount))*5)
	switch {
	case item.OpportunityScore >= 70:
		item.Interpretation = "반응 신호가 높고 경쟁 부담이 비교적 낮은 키워드입니다."
	case item.SaturationScore >= 70:
		item.Interpretation = "노출 상품이 많은 포화 키워드라 롱테일 세분화가 필요합니다."
	default:
		item.Interpretation = "카테고리와 가격군을 함께 좁혀 확인할 키워드입니다."
	}
}

func decorateInsightCategoryKeyword(item *insightCategoryKeywordInsight) {
	item.ClusterLabel = insightPriceClusterLabel(item.MedianPriceKRW)
	item.DemandScore = insightDemandScore(item.ProductCount, item.ReviewSum, item.OrderSum)
	item.CompetitionScore = insightCompetitionScore(item.ProductCount, item.SellerCount, item.BrandCount)
	iqrRatio := 0.0
	if item.MedianPriceKRW > 0 {
		iqrRatio = float64(item.IQRPriceKRW) / float64(item.MedianPriceKRW)
	}
	item.PriceGapScore = clampInsightScore((1-math.Min(iqrRatio, 1))*35 + (100-item.CompetitionScore)*0.35 + item.DemandScore*0.30)
	item.OpportunityScore = clampInsightScore(item.DemandScore*0.40 + item.PriceGapScore*0.25 + (100-item.CompetitionScore)*0.25)
	switch {
	case item.OpportunityScore >= 75:
		item.Interpretation = "반응 대비 공급 밀도가 낮아 진입 후보로 볼 수 있습니다."
	case item.CompetitionScore >= 75:
		item.Interpretation = "경쟁 밀도가 높아 브랜드·구성 차별화가 필요합니다."
	case item.PriceGapScore >= 65:
		item.Interpretation = "가격군 공백 신호가 있어 포지셔닝 검토 가치가 있습니다."
	default:
		item.Interpretation = "추가 기간 데이터로 추세를 확인할 조합입니다."
	}
}

func insightPolicyNotes() []insightPolicyNote {
	return []insightPolicyNote{
		{Code: "derived_only", Label: "파생 지표만 노출", Status: "active", Detail: "기본 화면은 카테고리, 키워드, 가격군, 기회점수 같은 집계·파생 지표를 중심으로 제공합니다."},
		{Code: "affiliate_asset_boundary", Label: "제휴 자산 경계", Status: "active", Detail: "상품명, 썸네일, 가격, 링크는 제휴 또는 공식 API가 허용한 범위에서만 제한적으로 노출합니다."},
		{Code: "price_basis", Label: "가격 기준 고지", Status: "partial", Detail: "현재 가격은 수집 시점 관측가이며 배송비와 옵션 총액은 별도 데이터 연결 후 확정합니다."},
		{Code: "affiliate_notice", Label: "외부 이동 고지", Status: "active", Detail: "외부몰 이동 전 가격/판매 여부 확인과 파트너 링크 가능성을 가까운 위치에 고지합니다."},
	}
}

func insightDealReason(discount, below float64, price int) string {
	switch {
	case discount >= 20 && below >= 20:
		return "표시 정가와 카테고리 중앙값 모두 대비 낮은 관측가입니다."
	case discount >= 20:
		return "표시 정가 대비 할인 신호가 있습니다."
	case below >= 20:
		return "카테고리 중앙값 대비 낮은 가격대입니다."
	case price <= 10000:
		return "1만원 이하 저가 후보입니다."
	default:
		return "수집 기준 가격 후보입니다."
	}
}

func insightDemandScore(productCount, reviewSum, orderSum int) float64 {
	return clampInsightScore(math.Log1p(float64(productCount)+float64(reviewSum)+float64(orderSum)*3) * 11)
}

func insightCompetitionScore(productCount, sellerCount, brandCount int) float64 {
	return clampInsightScore(math.Log1p(float64(productCount))*18 + math.Log1p(float64(sellerCount))*10 + math.Log1p(float64(brandCount))*8)
}

func clampInsightScore(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 100 {
		return 100
	}
	return round2(value)
}

func round2(value float64) float64 {
	return math.Round(value*100) / 100
}

func percent(numerator, denominator int) float64 {
	if denominator <= 0 {
		return 0
	}
	return round2(100 * float64(numerator) / float64(denominator))
}

func percentileInt(values []int, p float64) int {
	if len(values) == 0 {
		return 0
	}
	sort.Ints(values)
	idx := int(math.Round(p * float64(len(values)-1)))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

func minSorted(values []int) int {
	if len(values) == 0 {
		return 0
	}
	sort.Ints(values)
	return values[0]
}

func maxSorted(values []int) int {
	if len(values) == 0 {
		return 0
	}
	sort.Ints(values)
	return values[len(values)-1]
}

func insightPriceClusterLabel(price int) string {
	switch {
	case price <= 0:
		return "가격 미확정"
	case price < 10000:
		return "초저가형"
	case price < 30000:
		return "저가형"
	case price < 70000:
		return "중가형"
	case price < 150000:
		return "중상가형"
	default:
		return "프리미엄형"
	}
}

func insightPriceBandInterpretation(item insightPriceBand) string {
	switch {
	case item.ProductPercent >= 30 && item.ReactionPercent < item.ProductPercent:
		return "상품 수 대비 반응이 낮아 공급 과밀 가능성이 있습니다."
	case item.ReactionPercent >= item.ProductPercent+8:
		return "상품 수보다 반응 비중이 높아 수요가 몰리는 가격군입니다."
	case item.ProductCount <= 3 && item.ReactionPercent > 0:
		return "표본은 얇지만 반응이 있어 가격공백 후보로 볼 수 있습니다."
	default:
		return "상품 수와 반응을 함께 관찰할 가격군입니다."
	}
}

func latestInsightCollectedAt(products []insightProduct) string {
	latest := ""
	for _, p := range products {
		if p.CollectedAt > latest {
			latest = p.CollectedAt
		}
	}
	return latest
}

func cleanInsightText(s string) string {
	return CleanText(htmlEntityUnescape(s))
}

func htmlEntityUnescape(s string) string {
	return strings.NewReplacer("&amp;", "&", "&lt;", "<", "&gt;", ">", "&quot;", `"`, "&#39;", "'").Replace(s)
}

func insightCategorySlug(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		dash := unicode.IsSpace(r) || r == '/' || r == '\\'
		allowed := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || (r >= '가' && r <= '힣') || r == '_' || r == '-'
		if dash || !allowed {
			if !lastDash && b.Len() > 0 {
				b.WriteRune('-')
				lastDash = true
			}
			continue
		}
		b.WriteRune(r)
		lastDash = false
	}
	return strings.Trim(b.String(), "-")
}

func safeInsightIdentifierPath(value string) string {
	parts := strings.Split(strings.TrimSpace(value), ".")
	if len(parts) != 2 {
		return ""
	}
	for _, part := range parts {
		if part == "" {
			return ""
		}
		for _, r := range part {
			if !(r == '_' || (r >= '0' && r <= '9') || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z')) {
				return ""
			}
		}
	}
	return "`" + parts[0] + "`.`" + parts[1] + "`"
}
