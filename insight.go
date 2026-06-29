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

var insightSplitRe = regexp.MustCompile(`[^0-9a-zA-Z가-힣]+`)
var insightDigitRe = regexp.MustCompile(`[0-9]`)

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

func RunShoppingInsightRefreshFromEnv(ctx context.Context) error {
	client, err := newInsightCHClientFromEnv()
	if err != nil {
		return err
	}
	products, err := client.fetchInsightProducts(ctx)
	if err != nil {
		return err
	}
	if len(products) == 0 {
		return fmt.Errorf("shopping insight refresh skipped: no current shopping products")
	}
	table := safeInsightIdentifierPath(envString("SHOPPING_INSIGHT_SNAPSHOT_TABLE", defaultShoppingInsightSnapshotTable))
	if table == "" {
		return fmt.Errorf("invalid SHOPPING_INSIGHT_SNAPSHOT_TABLE")
	}
	snapshots, err := buildInsightSnapshots(products)
	if err != nil {
		return err
	}
	if err := client.insertInsightSnapshots(ctx, table, snapshots); err != nil {
		return err
	}
	fmt.Printf("Shopping Price Insight snapshot refreshed scopes=%d products=%d table=%s\n", len(snapshots), len(products), table)
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
	return &insightCHClient{
		url:      fmt.Sprintf("%s://%s:%s%s", protocol, host, port, path),
		user:     user,
		password: password,
		client:   &http.Client{Timeout: 90 * time.Second},
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, strings.NewReader(sql))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.user, c.password)
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("clickhouse status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

func (c *insightCHClient) fetchInsightProducts(ctx context.Context) ([]insightProduct, error) {
	sql := `
SELECT
    provider,
    product_code,
    product_name,
    source_category_raw,
    group_code,
    search_keyword,
    image_url,
    product_url,
    toInt32(price_krw) AS price_krw,
    toInt32(original_price_krw) AS original_price_krw,
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
	body, err := c.post(ctx, sql)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 1024), 1024*1024*10)
	products := []insightProduct{}
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
		p := insightProduct{
			Provider:         raw.Provider,
			ProductCode:      raw.ProductCode,
			ProductName:      cleanInsightText(raw.ProductName),
			SourceCategory:   normalizeInsightCategory(raw.Provider, raw.SourceCategoryRaw, raw.CategoryPath, raw.SearchKeyword, raw.ProductName),
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
	return products, nil
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

func buildInsightSnapshots(products []insightProduct) ([]insightSnapshotInsert, error) {
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
		return nil, err
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
	for _, category := range allCategories {
		scoped := categoryProducts[category.SourceCategory]
		if len(scoped) == 0 {
			continue
		}
		radar := buildInsightRadar(scoped, category.SourceCategory, allCategories, []insightCategoryBenchmark{category}, generatedAt)
		payload, err := json.Marshal(radar)
		if err != nil {
			return nil, err
		}
		rows = append(rows, insightSnapshotInsert{
			SnapshotUUID:         NewUUIDv7(),
			ScopeSlug:            insightCategorySlug(category.SourceCategory),
			ScopeCategory:        category.SourceCategory,
			Version:              version,
			GeneratedAt:          generatedText,
			SourceMaxCollectedAt: latestInsightCollectedAt(scoped),
			SourceProductCount:   uint64(len(scoped)),
			PayloadJSON:          string(payload),
			CreatedAt:            generatedText,
		})
	}
	return rows, nil
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
		Keywords:         buildInsightKeywordBenchmarks(products, 30),
		CategoryKeywords: buildInsightCategoryKeywords(products, 320),
		Products:         topInsightProducts(products, 24),
		DealCandidates:   buildInsightDealCandidates(products, 24),
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
		if len(rows) > 8 {
			rows = rows[:8]
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
	if limit > 0 && len(out) > limit {
		return out[:limit]
	}
	return out
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
	if limit > 0 && len(out) > limit {
		return out[:limit]
	}
	return out
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
	"가격": {}, "기획": {}, "단독": {}, "대용량": {}, "묶음": {}, "무료": {}, "무료배송": {}, "배송": {}, "베스트": {}, "상품": {}, "선택": {}, "세일": {}, "세트": {}, "신상": {}, "옵션": {}, "전용": {}, "정품": {}, "추가": {}, "컬리": {}, "특가": {}, "할인": {},
	"공식": {}, "국내": {}, "국산": {}, "모음": {}, "수입": {}, "신선": {}, "예약": {}, "인기": {}, "증정": {}, "직구": {}, "카시오": {}, "해외": {},
}

func normalizeInsightCategory(provider, raw, categoryPath, searchKeyword, productName string) string {
	raw = cleanInsightText(raw)
	if provider == "gmarket" && raw != "" {
		return raw
	}
	key := strings.ToLower(raw + " " + categoryPath + " " + searchKeyword + " " + productName)
	switch {
	case containsAny(key, "채소", "과일", "정육", "닭고기", "돼지고기", "소고기", "수산", "생선", "새우", "계란", "샐러드"):
		return "신선식품"
	case containsAny(key, "쌀", "잡곡", "간편식", "밀키트", "반찬", "김치", "라면", "떡볶이", "만두", "피자", "냉동", "과자", "초콜릿", "커피", "주스", "생수", "베이커리", "우유", "요거트", "치즈", "버터"):
		return "가공식품"
	case containsAny(key, "세제", "휴지", "칫솔", "주방", "생활"):
		return "생활/주방"
	case containsAny(key, "샴푸", "바디워시", "화장품", "선크림", "마스크팩", "뷰티"):
		return "뷰티"
	case containsAny(key, "강아지", "고양이", "문구", "펫"):
		return "취미/문구/펫"
	case raw != "":
		return raw
	case searchKeyword != "":
		return cleanInsightText(searchKeyword)
	default:
		return "미분류"
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
