package main

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	htmlpkg "html"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
	"github.com/xuri/excelize/v2"
)

// ============================================================
// 1. 설정값
// ============================================================

var CollectMode = "random_best_categories"

// "random_best_categories" : 여러 베스트 카테고리에서 랜덤 수집
// "search_keywords"        : 여러 검색어에서 랜덤 수집
// "from_excel"             : 기존 엑셀 기준으로 상세 재수집

var InputFile = "gmarket_products_global_detail_improved.xlsx"
var OutputFile = "outputs/gmarket_go_validated_parallel_detail.xlsx"

var TotalTargetProducts = 60

var RandomCategoryCount = 10
var ProductsPerCategory = 10

// 0이면 매번 랜덤, 숫자를 넣으면 같은 결과 재현
var RandomSeed int64 = 0

var SearchKeywords = []string{
	"노트북", "생수", "키보드", "샴푸", "운동화",
	"커피", "마스크", "이어폰", "캠핑", "책상",
	"간식", "수건", "화장품", "강아지사료", "가방",
	"청소기", "보조배터리", "선풍기", "침구", "쌀",
}

var SearchPagesPerKeyword = 1
var SearchPageSize = 60

// 0이면 전부 상세 수집
var DetailLimit = 0
var ShardCount = 1
var ShardIndex = 0

var DetailSourceMode = "both"

// "both"   : 국내 + 글로벌 모두 수집
// "korean" : 국내만
// "global" : 글로벌만

var ListSourceMode = "gsearch_ajax"

// "gsearch_ajax" : gsearch.gmarket.co.kr 목록 AJAX endpoint를 직접 호출
// "browser"      : 브라우저로 목록 페이지를 렌더링
// "auto"         : 브라우저를 먼저 시도하고 실패하면 gsearch AJAX로 보충

// ============================================================
// 병렬/안정성 설정
// ============================================================

// 차단 페이지가 자주 뜨면 2 / 2 권장
var DetailProductConcurrency = 2
var DetailPageConcurrency = 2
var ListPageConcurrency = 2

var Headless = true

var PageTimeout = 70 * time.Second
var ListScrollCount = 5
var DetailScrollCount = 8
var PageDelay = 1200 * time.Millisecond

var DetailRetryCount = 3
var DetailRetryBaseSleep = 5 * time.Second
var DetailValidationWait = 35 * time.Second
var DetailStartJitter = 1500 * time.Millisecond
var PostNavigationSleep = 2500 * time.Millisecond
var ValidationPollSleep = 3000 * time.Millisecond
var DetailClickSleep = 900 * time.Millisecond
var DetailScrollSleep = 800 * time.Millisecond
var DetailFinalSleep = 1500 * time.Millisecond

// 차단이 잦으면 false 권장.
// true로 하면 이미지/폰트/미디어를 차단해 빨라지지만, 일부 상세 이미지 URL 수집률이 낮아질 수 있음.
var BlockHeavyResources = false

var DescriptionTextMaxChars = 6000
var FullTextMaxChars = 7000
var AllImageMax = 40
var DescriptionImageMax = 30

var SaveDebugHTML = false
var AllowEmptyResult = false
var CollectDetailsEnabled = true
var SaveExcelEnabled = true
var IngestMode = "excel"

func ApplyEnvConfig() {
	setStringFromEnv("INGEST_MODE", &IngestMode)
	setStringFromEnv("GMARKET_COLLECT_MODE", &CollectMode)
	setStringFromEnv("GMARKET_INPUT_FILE", &InputFile)
	setStringFromEnv("GMARKET_OUTPUT_FILE", &OutputFile)
	setStringFromEnv("GMARKET_DETAIL_SOURCE_MODE", &DetailSourceMode)
	setStringFromEnv("GMARKET_LIST_SOURCE_MODE", &ListSourceMode)

	setIntFromEnv("GMARKET_TOTAL_TARGET_PRODUCTS", &TotalTargetProducts)
	setIntFromEnv("GMARKET_RANDOM_CATEGORY_COUNT", &RandomCategoryCount)
	setIntFromEnv("GMARKET_PRODUCTS_PER_CATEGORY", &ProductsPerCategory)
	setIntFromEnv("GMARKET_SEARCH_PAGES_PER_KEYWORD", &SearchPagesPerKeyword)
	setIntFromEnv("GMARKET_SEARCH_PAGE_SIZE", &SearchPageSize)
	setIntFromEnv("GMARKET_DETAIL_LIMIT", &DetailLimit)
	setIntFromEnv("GMARKET_SHARD_COUNT", &ShardCount)
	setIntFromEnv("GMARKET_SHARD_INDEX", &ShardIndex)
	setIntFromEnv("GMARKET_DETAIL_PRODUCT_CONCURRENCY", &DetailProductConcurrency)
	setIntFromEnv("GMARKET_DETAIL_PAGE_CONCURRENCY", &DetailPageConcurrency)
	setIntFromEnv("GMARKET_LIST_SCROLL_COUNT", &ListScrollCount)
	setIntFromEnv("GMARKET_DETAIL_SCROLL_COUNT", &DetailScrollCount)
	setIntFromEnv("GMARKET_DETAIL_RETRY_COUNT", &DetailRetryCount)
	setIntFromEnv("GMARKET_LIST_PAGE_CONCURRENCY", &ListPageConcurrency)

	setInt64FromEnv("GMARKET_RANDOM_SEED", &RandomSeed)
	setDurationSecondsFromEnv("GMARKET_PAGE_TIMEOUT_SECONDS", &PageTimeout)
	setDurationSecondsFromEnv("GMARKET_DETAIL_VALIDATION_WAIT_SECONDS", &DetailValidationWait)
	setDurationSecondsFromEnv("GMARKET_DETAIL_RETRY_BASE_SLEEP_SECONDS", &DetailRetryBaseSleep)
	setDurationMillisFromEnv("GMARKET_PAGE_DELAY_MS", &PageDelay)
	setDurationMillisFromEnv("GMARKET_DETAIL_START_JITTER_MS", &DetailStartJitter)
	setDurationMillisFromEnv("GMARKET_POST_NAVIGATION_SLEEP_MS", &PostNavigationSleep)
	setDurationMillisFromEnv("GMARKET_VALIDATION_POLL_SLEEP_MS", &ValidationPollSleep)
	setDurationMillisFromEnv("GMARKET_DETAIL_CLICK_SLEEP_MS", &DetailClickSleep)
	setDurationMillisFromEnv("GMARKET_DETAIL_SCROLL_SLEEP_MS", &DetailScrollSleep)
	setDurationMillisFromEnv("GMARKET_DETAIL_FINAL_SLEEP_MS", &DetailFinalSleep)

	setBoolFromEnv("GMARKET_HEADLESS", &Headless)
	setBoolFromEnv("GMARKET_BLOCK_HEAVY_RESOURCES", &BlockHeavyResources)
	setBoolFromEnv("GMARKET_SAVE_DEBUG_HTML", &SaveDebugHTML)
	setBoolFromEnv("GMARKET_ALLOW_EMPTY_RESULT", &AllowEmptyResult)
	setBoolFromEnv("GMARKET_COLLECT_DETAILS", &CollectDetailsEnabled)
	setBoolFromEnv("GMARKET_SAVE_EXCEL", &SaveExcelEnabled)

	if v := strings.TrimSpace(os.Getenv("GMARKET_SEARCH_KEYWORDS")); v != "" {
		parts := strings.Split(v, ",")
		keywords := []string{}
		for _, part := range parts {
			if keyword := CleanText(part); keyword != "" {
				keywords = append(keywords, keyword)
			}
		}
		if len(keywords) > 0 {
			SearchKeywords = keywords
		}
	}

	if ShardCount <= 0 {
		panic(fmt.Sprintf("GMARKET_SHARD_COUNT must be positive: %d", ShardCount))
	}
	if ShardIndex < 0 || ShardIndex >= ShardCount {
		panic(fmt.Sprintf("GMARKET_SHARD_INDEX must be between 0 and GMARKET_SHARD_COUNT-1: index=%d count=%d", ShardIndex, ShardCount))
	}
}

func setStringFromEnv(name string, target *string) {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		*target = v
	}
}

func setIntFromEnv(name string, target *int) {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			panic(fmt.Sprintf("%s must be an integer: %q", name, v))
		}
		*target = parsed
	}
}

func setInt64FromEnv(name string, target *int64) {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			panic(fmt.Sprintf("%s must be an integer: %q", name, v))
		}
		*target = parsed
	}
}

func setDurationSecondsFromEnv(name string, target *time.Duration) {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil || parsed < 0 {
			panic(fmt.Sprintf("%s must be a non-negative number of seconds: %q", name, v))
		}
		*target = time.Duration(parsed * float64(time.Second))
	}
}

func setDurationMillisFromEnv(name string, target *time.Duration) {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil || parsed < 0 {
			panic(fmt.Sprintf("%s must be a non-negative number of milliseconds: %q", name, v))
		}
		*target = time.Duration(parsed * float64(time.Millisecond))
	}
}

func setBoolFromEnv(name string, target *bool) {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		parsed, err := strconv.ParseBool(v)
		if err != nil {
			panic(fmt.Sprintf("%s must be true or false: %q", name, v))
		}
		*target = parsed
	}
}

// ============================================================
// 2. 자료형
// ============================================================

type Row map[string]string

type Category struct {
	Name      string
	GroupCode string
	URL       string
}

type ListJob struct {
	Label          string
	Keyword        string
	SourceURL      string
	SourceCategory string
	GroupCode      string
	SourcePage     string
	Limit          int
}

type gmarketSearchAjaxResponse struct {
	Success    bool   `json:"success"`
	Message    string `json:"message"`
	TotalCount int    `json:"totalCount"`
}

var gmarketHTTPClient = &http.Client{
	Timeout: 25 * time.Second,
}

// ============================================================
// 3. 기본 카테고리
// ============================================================

var DefaultBestCategories = []Category{
	{"신선식품", "100000006", "https://www.gmarket.co.kr/n/best?viewType=G&groupCode=100000006"},
	{"가공식품", "100000005", "https://www.gmarket.co.kr/n/best?viewType=G&groupCode=100000005"},
	{"생필품/육아", "100000007", "https://www.gmarket.co.kr/n/best?viewType=G&groupCode=100000007"},
	{"생활/주방", "100001001", "https://www.gmarket.co.kr/n/best?viewType=G&groupCode=100001001"},
	{"패션/잡화", "100000001", "https://www.gmarket.co.kr/n/best?viewType=G&groupCode=100000001"},
	{"뷰티", "100000003", "https://www.gmarket.co.kr/n/best?viewType=G&groupCode=100000003"},
	{"디지털/가전", "100001007", "https://www.gmarket.co.kr/n/best?viewType=G&groupCode=100001007"},
	{"가구/홈", "100001004", "https://www.gmarket.co.kr/n/best?viewType=G&groupCode=100001004"},
	{"스포츠/건강", "100001002", "https://www.gmarket.co.kr/n/best?viewType=G&groupCode=100001002"},
	{"취미/문구/펫", "100001003", "https://www.gmarket.co.kr/n/best?viewType=G&groupCode=100001003"},
	{"도서/음반", "100001009", "https://www.gmarket.co.kr/n/best?viewType=G&groupCode=100001009"},
	{"e쿠폰", "100001011", "https://www.gmarket.co.kr/n/best?viewType=G&groupCode=100001011"},
	{"여행", "100001005", "https://www.gmarket.co.kr/n/best?viewType=G&groupCode=100001005"},
}

// ============================================================
// 4. 기본 유틸
// ============================================================

var spaceRe = regexp.MustCompile(`\s+`)

func CleanText(s string) string {
	return strings.TrimSpace(spaceRe.ReplaceAllString(s, " "))
}

func CleanMultilineText(s string) string {
	s = strings.ReplaceAll(s, "\r", "\n")

	lines := []string{}
	for _, line := range strings.Split(s, "\n") {
		line = CleanText(line)
		if line != "" {
			lines = append(lines, line)
		}
	}

	return strings.Join(lines, "\n")
}

func Truncate(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}

func UniqueKeepOrder(values []string) []string {
	seen := map[string]bool{}
	result := []string{}

	for _, v := range values {
		v = CleanText(v)
		if v == "" {
			continue
		}
		if seen[v] {
			continue
		}

		seen[v] = true
		result = append(result, v)
	}

	return result
}

func NormalizeURL(raw string, base string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}

	raw = htmlpkg.UnescapeString(raw)
	raw = strings.ReplaceAll(raw, `\/`, `/`)

	if decoded, err := url.QueryUnescape(raw); err == nil {
		raw = decoded
	}

	if strings.HasPrefix(raw, "//") {
		raw = "https:" + raw
	}

	baseURL, err := url.Parse(base)
	if err != nil {
		return raw
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}

	return baseURL.ResolveReference(parsed).String()
}

func ExtractProductCode(s string) string {
	patterns := []string{
		`goodscode=(\d+)`,
		`goodsCode=(\d+)`,
		`goodsNo=(\d+)`,
		`itemNo=(\d+)`,
		`Item\s*No\.\s*:\s*(\d+)`,
		`/(\d{8,})`,
	}

	for _, p := range patterns {
		re := regexp.MustCompile(`(?i)` + p)
		m := re.FindStringSubmatch(s)
		if len(m) >= 2 {
			return m[1]
		}
	}

	return ""
}

func KoreanItemURL(code string) string {
	if code == "" {
		return ""
	}
	return "https://item.gmarket.co.kr/Item?goodscode=" + code
}

func GlobalItemURL(code string) string {
	if code == "" {
		return ""
	}
	return "https://global.gmarket.co.kr/item?goodscode=" + code
}

func ExtractURLsFromRaw(raw string, productCode string) (string, string, string) {
	raw = htmlpkg.UnescapeString(raw)
	raw = strings.ReplaceAll(raw, `\/`, `/`)

	if decoded, err := url.QueryUnescape(raw); err == nil {
		raw = decoded
	}

	globalURL := ""
	domesticURL := ""

	globalPatterns := []string{
		`SearchListPage\.viewUrl\(\s*'([^']+)'`,
		`SearchListPage\.viewUrl\(\s*"([^"]+)"`,
		`'(https?://global\.gmarket\.co\.kr/[^']+)'`,
		`"(https?://global\.gmarket\.co\.kr/[^"]+)"`,
		`(https?://global\.gmarket\.co\.kr/[^\s'")]+)`,
	}

	for _, p := range globalPatterns {
		re := regexp.MustCompile(`(?i)` + p)
		m := re.FindStringSubmatch(raw)
		if len(m) >= 2 {
			globalURL = NormalizeURL(m[1], "https://global.gmarket.co.kr")
			break
		}
	}

	domesticPatterns := []string{
		`(https?://item\.gmarket\.co\.kr/[^\s'")]+)`,
		`(https?://www\.gmarket\.co\.kr/[^\s'")]+)`,
	}

	for _, p := range domesticPatterns {
		re := regexp.MustCompile(`(?i)` + p)
		m := re.FindStringSubmatch(raw)
		if len(m) >= 2 {
			domesticURL = NormalizeURL(m[1], "https://www.gmarket.co.kr")
			break
		}
	}

	code := ExtractProductCode(raw)
	if code == "" {
		code = ExtractProductCode(globalURL)
	}
	if code == "" {
		code = ExtractProductCode(domesticURL)
	}
	if code == "" {
		code = productCode
	}

	if code != "" {
		if domesticURL == "" {
			domesticURL = KoreanItemURL(code)
		}
		if globalURL == "" {
			globalURL = GlobalItemURL(code)
		}
	}

	domesticURL = strings.Replace(domesticURL, "http://item.gmarket.co.kr", "https://item.gmarket.co.kr", 1)
	globalURL = strings.Replace(globalURL, "http://global.gmarket.co.kr", "https://global.gmarket.co.kr", 1)

	return code, domesticURL, globalURL
}

func ExtractPriceKRW(text string) string {
	text = CleanText(text)
	if text == "" {
		return ""
	}

	patterns := []string{
		`[￦₩]\s*([\d,]+)`,
		`KRW\s*([\d,]+)`,
		`([\d,]+)\s*원`,
		`\(\s*[￦₩]\s*([\d,]+)\s*\)`,
		`판매가\s*([\d,]+)`,
		`쿠폰적용가\s*([\d,]+)`,
	}

	for _, p := range patterns {
		re := regexp.MustCompile(`(?i)` + p)
		m := re.FindStringSubmatch(text)
		if len(m) >= 2 {
			n, err := strconv.Atoi(strings.ReplaceAll(m[1], ",", ""))
			if err == nil && n >= 100 {
				return strconv.Itoa(n)
			}
		}
	}

	re := regexp.MustCompile(`\d{1,3}(?:,\d{3})+`)
	matches := re.FindAllString(text, -1)

	maxVal := 0
	for _, m := range matches {
		n, err := strconv.Atoi(strings.ReplaceAll(m, ",", ""))
		if err == nil && n > maxVal {
			maxVal = n
		}
	}

	if maxVal > 0 {
		return strconv.Itoa(maxVal)
	}

	return ""
}

func ExtractPriceUSD(text string) string {
	text = CleanText(text)
	if text == "" {
		return ""
	}

	re := regexp.MustCompile(`\$\s*([\d,]+(?:\.\d+)?)`)
	m := re.FindStringSubmatch(text)
	if len(m) >= 2 {
		return strings.ReplaceAll(m[1], ",", "")
	}

	return ""
}

func ExtractFirstNumber(text string) string {
	text = CleanText(text)
	if text == "" {
		return ""
	}

	re := regexp.MustCompile(`(\d{1,3}(?:,\d{3})+|\d+)`)
	m := re.FindStringSubmatch(text)

	if len(m) >= 2 {
		return strings.ReplaceAll(m[1], ",", "")
	}

	return ""
}

func GetMetaContent(doc *goquery.Document, key string) string {
	selectors := []string{
		fmt.Sprintf(`meta[property="%s"]`, key),
		fmt.Sprintf(`meta[name="%s"]`, key),
	}

	for _, sel := range selectors {
		content, ok := doc.Find(sel).First().Attr("content")
		if ok && CleanText(content) != "" {
			return CleanText(content)
		}
	}

	return ""
}

func LinesFromHTML(htmlStr string) []string {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlStr))
	if err != nil {
		return nil
	}

	doc.Find("script, style, noscript").Remove()

	text := doc.Text()
	lines := []string{}

	for _, line := range strings.Split(text, "\n") {
		line = CleanText(line)
		if line == "" {
			continue
		}

		lower := strings.ToLower(line)
		if lower == "open" || lower == "close" || lower == "prev" || lower == "next" {
			continue
		}

		lines = append(lines, line)
	}

	return UniqueKeepOrder(lines)
}

func ExtractSectionText(lines []string, starts []string, ends []string, maxChars int) string {
	startIdx := -1

	for i, line := range lines {
		lower := strings.ToLower(line)

		for _, kw := range starts {
			if strings.Contains(lower, strings.ToLower(kw)) {
				startIdx = i
				break
			}
		}

		if startIdx >= 0 {
			break
		}
	}

	if startIdx < 0 {
		return ""
	}

	endIdx := len(lines)

	for j := startIdx + 1; j < len(lines); j++ {
		lower := strings.ToLower(lines[j])

		for _, kw := range ends {
			if strings.Contains(lower, strings.ToLower(kw)) {
				endIdx = j
				break
			}
		}

		if endIdx != len(lines) {
			break
		}
	}

	skip := map[string]bool{
		"interest":      true,
		"Added on your": true,
		"Wish List":     true,
		"Quantity":      true,
		"Confirm":       true,
		"Add to cart":   true,
		"Buy now":       true,
		"수량증가 수량감소":     true,
	}

	selected := []string{}
	for _, line := range lines[startIdx:endIdx] {
		if skip[line] {
			continue
		}
		if len([]rune(line)) <= 1 {
			continue
		}
		selected = append(selected, line)
	}

	return Truncate(CleanMultilineText(strings.Join(selected, "\n")), maxChars)
}

// ============================================================
// 5. 차단/오류 페이지 검증
// ============================================================

var BadPageKeywords = []string{
	"ERROR: The request could not be satisfied",
	"The request could not be satisfied",
	"504 Gateway Timeout",
	"Generated by cloudfront",
	"CloudFront",
	"Access Denied",
	"Forbidden",
	"Service Unavailable",
	"잠시만 기다리십시오",
	"원활한 서비스 이용을 위한 간단한 확인 안내",
	"Checking your Browser",
	"봇 확인",
	"captcha",
	"CAPTCHA",
}

func DetectPlatform(targetURL string) string {
	lower := strings.ToLower(targetURL)

	if strings.Contains(lower, "item.gmarket.co.kr") {
		return "korean"
	}

	if strings.Contains(lower, "global.gmarket.co.kr") {
		return "global"
	}

	return "list"
}

func ClassifyHTMLPage(htmlStr string, finalURL string, platform string) (bool, string) {
	htmlStr = strings.TrimSpace(htmlStr)

	if len(htmlStr) < 800 {
		return false, fmt.Sprintf("HTML too short: %d chars", len(htmlStr))
	}

	lower := strings.ToLower(htmlStr)

	for _, keyword := range BadPageKeywords {
		if strings.Contains(lower, strings.ToLower(keyword)) {
			return false, "blocked_or_error_page: " + keyword
		}
	}

	if platform == "korean" {
		signals := []string{
			"og:title",
			"goodscode",
			"goodsCode",
			"상품상세정보",
			"상품 상세정보",
			"상품정보제공고시",
			"판매가",
			"구매하기",
			"장바구니",
			"배송비",
			"판매자",
		}

		for _, s := range signals {
			if strings.Contains(lower, strings.ToLower(s)) {
				return true, ""
			}
		}

		return false, "korean_product_signal_missing"
	}

	if platform == "global" {
		signals := []string{
			"Item No.",
			"Shipping fee",
			"Specific Item Info",
			"Seller",
			"Gmarket -",
			"Item Weight",
			"Country of manufacture",
			"Product name & Model number",
		}

		for _, s := range signals {
			if strings.Contains(lower, strings.ToLower(s)) {
				return true, ""
			}
		}

		return false, "global_product_signal_missing"
	}

	return true, ""
}

func ShouldWaitForPage(reason string) bool {
	lower := strings.ToLower(reason)

	waitKeywords := []string{
		"잠시만",
		"checking your browser",
		"signal_missing",
	}

	for _, k := range waitKeywords {
		if strings.Contains(lower, k) {
			return true
		}
	}

	return false
}

func IsHardBlock(reason string) bool {
	lower := strings.ToLower(reason)

	hardKeywords := []string{
		"cloudfront",
		"504",
		"access denied",
		"forbidden",
	}

	for _, k := range hardKeywords {
		if strings.Contains(lower, k) {
			return true
		}
	}

	return false
}

// ============================================================
// 6. Chrome / chromedp
// ============================================================

func NewBrowserContext() (context.Context, context.CancelFunc, error) {
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", Headless),
		chromedp.Flag("no-sandbox", true),
		chromedp.Flag("disable-dev-shm-usage", true),
		chromedp.Flag("disable-gpu", true),
		chromedp.Flag("disable-extensions", true),
		chromedp.Flag("disable-blink-features", "AutomationControlled"),
		chromedp.Flag("lang", "ko-KR"),
		chromedp.UserAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
	)
	if chromePath := strings.TrimSpace(os.Getenv("CHROME_BIN")); chromePath != "" {
		opts = append(opts, chromedp.ExecPath(chromePath))
	}

	allocCtx, cancelAlloc := chromedp.NewExecAllocator(context.Background(), opts...)
	browserCtx, cancelBrowser := chromedp.NewContext(allocCtx)

	cancel := func() {
		cancelBrowser()
		cancelAlloc()
	}

	err := chromedp.Run(browserCtx)
	if err != nil {
		cancel()
		return nil, nil, err
	}

	return browserCtx, cancel, nil
}

func SetupNetwork(ctx context.Context) {
	actions := []chromedp.Action{
		network.Enable(),
		network.SetExtraHTTPHeaders(network.Headers{
			"Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
		}),
	}

	if BlockHeavyResources {
		blockPatterns := []*network.BlockPattern{}
		for _, pattern := range []string{
			"*://*/*.woff", "*://*/*.woff2", "*://*/*.ttf", "*://*/*.otf",
			"*://*/*.mp4", "*://*/*.mp3", "*://*/*.avi", "*://*/*.mov",
		} {
			blockPatterns = append(blockPatterns, &network.BlockPattern{
				URLPattern: pattern,
				Block:      true,
			})
		}
		actions = append(actions,
			network.SetBlockedURLs().WithURLPatterns(blockPatterns),
		)
	}

	_ = chromedp.Run(ctx, actions...)
}

func FetchRenderedHTMLValidated(browserCtx context.Context, targetURL string, scrollCount int, clickTexts []string) (string, string, string) {
	platform := DetectPlatform(targetURL)

	retryCount := DetailRetryCount
	if platform == "list" {
		retryCount = 2
	}

	lastError := ""
	finalURL := targetURL

	for attempt := 1; attempt <= retryCount; attempt++ {
		tabCtx, cancelTab := chromedp.NewContext(browserCtx)

		totalTimeout := PageTimeout + time.Duration(scrollCount)*2*time.Second + DetailValidationWait + 30*time.Second
		ctx, cancel := context.WithTimeout(tabCtx, totalTimeout)

		SetupNetwork(ctx)

		err := chromedp.Run(ctx, chromedp.Navigate(targetURL))
		if err != nil {
			lastError = err.Error()
		}

		_ = chromedp.Run(ctx, chromedp.Sleep(PostNavigationSleep))

		validationStart := time.Now()
		pageLooksOK := false

		for {
			var currentHTML string
			var currentURL string

			_ = chromedp.Run(ctx,
				chromedp.OuterHTML("html", &currentHTML, chromedp.ByQuery),
				chromedp.Evaluate(`location.href`, &currentURL),
			)

			finalURL = currentURL

			ok, reason := ClassifyHTMLPage(currentHTML, currentURL, platform)

			if ok {
				pageLooksOK = true
				break
			}

			lastError = reason

			if IsHardBlock(reason) {
				break
			}

			if !ShouldWaitForPage(reason) {
				break
			}

			if time.Since(validationStart) >= DetailValidationWait {
				break
			}

			_ = chromedp.Run(ctx, chromedp.Sleep(ValidationPollSleep))
		}

		if platform != "list" && !pageLooksOK {
			cancel()
			cancelTab()

			sleep := DetailRetryBaseSleep*time.Duration(attempt) + time.Duration(rand.Intn(3000))*time.Millisecond
			time.Sleep(sleep)
			continue
		}

		for _, text := range clickTexts {
			clickJS := fmt.Sprintf(`
				(function(txt){
					const nodes = Array.from(document.querySelectorAll('a, button, div, span, li'));
					const el = nodes.find(e => e && e.innerText && e.innerText.includes(txt));
					if (el) {
						el.click();
						return true;
					}
					return false;
				})(%s);
			`, strconv.Quote(text))

			var clicked bool
			_ = chromedp.Run(ctx,
				chromedp.Evaluate(clickJS, &clicked),
				chromedp.Sleep(DetailClickSleep),
			)
		}

		for i := 0; i < scrollCount; i++ {
			var ok bool
			_ = chromedp.Run(ctx,
				chromedp.Evaluate(`window.scrollBy(0, 2500); true;`, &ok),
				chromedp.Sleep(DetailScrollSleep),
			)
		}

		_ = chromedp.Run(ctx, chromedp.Sleep(DetailFinalSleep))

		var htmlCombined string

		htmlJS := `
			(function(){
				const parts = [document.documentElement.outerHTML];

				for (let i = 0; i < window.frames.length; i++) {
					try {
						const d = window.frames[i].document;
						if (d && d.documentElement) {
							parts.push(d.documentElement.outerHTML);
						}
					} catch(e) {}
				}

				return parts.join("\n");
			})();
		`

		err = chromedp.Run(ctx,
			chromedp.Evaluate(htmlJS, &htmlCombined),
			chromedp.Evaluate(`location.href`, &finalURL),
		)

		cancel()
		cancelTab()

		if err != nil {
			lastError = err.Error()

			sleep := DetailRetryBaseSleep*time.Duration(attempt) + time.Duration(rand.Intn(3000))*time.Millisecond
			time.Sleep(sleep)
			continue
		}

		ok, reason := ClassifyHTMLPage(htmlCombined, finalURL, platform)

		if platform != "list" && !ok {
			lastError = fmt.Sprintf("%s / attempt=%d", reason, attempt)

			sleep := DetailRetryBaseSleep*time.Duration(attempt) + time.Duration(rand.Intn(3000))*time.Millisecond
			time.Sleep(sleep)
			continue
		}

		if platform == "list" && !ok {
			lastError = fmt.Sprintf("%s / attempt=%d", reason, attempt)

			sleep := 2*time.Second + time.Duration(rand.Intn(2000))*time.Millisecond
			time.Sleep(sleep)
			continue
		}

		return htmlCombined, finalURL, ""
	}

	return "", finalURL, lastError
}

func SleepRandomUpTo(maxDelay time.Duration) {
	if maxDelay <= 0 {
		return
	}
	time.Sleep(time.Duration(rand.Int63n(int64(maxDelay))))
}

// ============================================================
// 7. 목록 파싱
// ============================================================

func GetFirstText(parent *goquery.Selection, selectors []string) string {
	for _, selector := range selectors {
		found := ""

		parent.Find(selector).Each(func(i int, s *goquery.Selection) {
			if found != "" {
				return
			}

			txt := CleanText(s.Text())
			if txt != "" {
				found = txt
			}
		})

		if found != "" {
			return found
		}
	}

	return ""
}

func GetFirstAttr(parent *goquery.Selection, selectors []string, attrs []string) string {
	for _, selector := range selectors {
		found := ""

		parent.Find(selector).Each(func(i int, s *goquery.Selection) {
			if found != "" {
				return
			}

			for _, attr := range attrs {
				if val, ok := s.Attr(attr); ok && val != "" {
					found = val
					return
				}
			}
		})

		if found != "" {
			return found
		}
	}

	return ""
}

func FindProductContainer(anchor *goquery.Selection) *goquery.Selection {
	node := anchor

	for i := 0; i < 12; i++ {
		if node == nil || node.Length() == 0 {
			break
		}

		text := CleanText(node.Text())

		if (strings.Contains(text, "원") ||
			strings.Contains(text, "￦") ||
			strings.Contains(text, "₩") ||
			strings.Contains(text, "$")) &&
			len([]rune(text)) < 9000 {
			return node
		}

		node = node.Parent()
	}

	node = anchor

	for i := 0; i < 12; i++ {
		if node == nil || node.Length() == 0 {
			break
		}

		name := goquery.NodeName(node)
		if name == "li" || name == "div" || name == "article" {
			return node
		}

		node = node.Parent()
	}

	return anchor
}

func ParseListProducts(htmlStr string, sourceURL string, sourceCategory string, groupCode string, sourceKeyword string, sourcePage string) []Row {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlStr))
	if err != nil {
		return nil
	}

	productSelector := strings.Join([]string{
		"a.itemname",
		"a.item_name",
		"a.link__item",
		`a[href*="goodscode="]`,
		`a[href*="goodsCode="]`,
		`a[href*="item.gmarket.co.kr/Item"]`,
		`a[href*="global.gmarket.co.kr/item"]`,
	}, ", ")

	nameSelectors := []string{
		"a.itemname",
		"a.item_name",
		"a.link__item",
		".itemname",
		".item_name",
		".link__item",
		".box__item-title",
		".text__item",
		".text__title",
		`[class*="itemname"]`,
		`[class*="item-title"]`,
		`[class*="title"]`,
	}

	priceSelectors := []string{
		"div.s-price strong",
		"div.s-price",
		".discount_price",
		".price_cont",
		".s-price strong",
		".s-price",
		".box__price-seller strong",
		".box__price-seller",
		".price strong",
		".price",
		".text__price-seller",
		".box__price",
		`[class*="price"] strong`,
		`[class*="price"]`,
	}

	originalPriceSelectors := []string{
		"div.o-price",
		".o-price",
		".orgin_price",
		".box__price-original",
		".box__price-coupon",
		".text__price-original",
		`[class*="price-original"]`,
		`[class*="original"]`,
	}

	rows := []Row{}
	seen := map[string]bool{}

	doc.Find(productSelector).Each(func(i int, anchor *goquery.Selection) {
		rawParts := []string{}

		for _, attr := range []string{"href", "onclick", "data-url", "data-href"} {
			if v, ok := anchor.Attr(attr); ok && v != "" {
				rawParts = append(rawParts, v)
			}
		}

		rawHref := strings.Join(rawParts, " ")
		productCode, domesticURL, globalURL := ExtractURLsFromRaw(rawHref, "")

		if productCode == "" {
			return
		}

		card := FindProductContainer(anchor)

		name := CleanText(anchor.Text())

		if name == "" {
			if v, ok := anchor.Attr("title"); ok {
				name = CleanText(v)
			}
		}

		if name == "" {
			if v, ok := anchor.Attr("aria-label"); ok {
				name = CleanText(v)
			}
		}

		if name == "" {
			img := anchor.Find("img").First()
			if img.Length() > 0 {
				if v, ok := img.Attr("alt"); ok {
					name = CleanText(v)
				}
				if name == "" {
					if v, ok := img.Attr("title"); ok {
						name = CleanText(v)
					}
				}
			}
		}

		if name == "" {
			name = GetFirstText(card, nameSelectors)
		}

		if name == "" || len([]rune(name)) < 2 {
			return
		}

		badNames := map[string]bool{
			"cart":         true,
			"wishlist":     true,
			"viewed items": true,
			"image":        true,
			"장바구니":         true,
			"찜하기":          true,
			"최근본상품":        true,
			"검색":           true,
			"닫기":           true,
		}

		if badNames[strings.ToLower(name)] {
			return
		}

		priceRaw := GetFirstText(card, priceSelectors)
		originalRaw := GetFirstText(card, originalPriceSelectors)

		if priceRaw == "" {
			cardText := CleanText(card.Text())
			if strings.Contains(cardText, "원") ||
				strings.Contains(cardText, "￦") ||
				strings.Contains(cardText, "$") {
				priceRaw = cardText
			}
		}

		price := ExtractPriceKRW(priceRaw)
		originalPrice := ExtractPriceKRW(originalRaw)

		if originalPrice == "" {
			originalPrice = price
		}

		imageURL := GetFirstAttr(
			card,
			[]string{"img"},
			[]string{"src", "data-src", "data-original", "data-lazy", "data-original-src"},
		)
		imageURL = NormalizeURL(imageURL, sourceURL)

		if seen[productCode] {
			return
		}
		seen[productCode] = true

		row := Row{
			"상품명":        name,
			"상품코드":       productCode,
			"상품URL_국내":   domesticURL,
			"상품URL_글로벌":  globalURL,
			"상품URL_raw":  rawHref,
			"목록_이미지URL":  imageURL,
			"목록_가격_raw":  priceRaw,
			"목록_판매가_KRW": price,
			"목록_정가_raw":  originalRaw,
			"목록_정가_KRW":  originalPrice,
			"수집URL":      sourceURL,
			"수집카테고리":     sourceCategory,
			"groupCode":  groupCode,
			"검색어":        sourceKeyword,
			"페이지":        sourcePage,
		}

		rows = append(rows, row)
	})

	return rows
}

func BuildSearchURL(keyword string, page int, pageSize int) string {
	q := url.Values{}
	q.Set("keyword", keyword)
	q.Set("page", strconv.Itoa(page))
	q.Set("pagesize", strconv.Itoa(pageSize))
	q.Set("type", "IMG")
	q.Set("IsGmarketBest", "True")
	q.Set("IsGlobalSearch", "True")

	return "https://gsearch.gmarket.co.kr/Listview/Search?" + q.Encode()
}

func FetchGmarketSearchAjaxHTML(keyword string, page int, pageSize int, sourceURL string) (string, int, error) {
	form := url.Values{}
	form.Set("type", "IMG")
	form.Set("page", strconv.Itoa(page))
	form.Set("pageSize", strconv.Itoa(pageSize))
	form.Set("keyword", keyword)
	form.Set("GdlcCd", "")
	form.Set("GdmcCd", "")
	form.Set("GdscCd", "")
	form.Set("priceStart", "")
	form.Set("priceEnd", "")
	form.Set("searchType", "IMG")
	form.Set("IsOversea", "False")
	form.Set("isDeliveryFeeFree", "")
	form.Set("isDiscount", "False")
	form.Set("isGmileage", "False")
	form.Set("isGStamp", "False")
	form.Set("isGmarketBest", "True")
	form.Set("orderType", "")
	form.Set("listType", "IMG")
	form.Set("IsBookCash", "False")
	form.Set("IsGlobalSort", "True")
	form.Set("DelFee", "")
	form.Set("CurrPage", "srp")
	form.Set("isGlobalSite", "true")
	form.Set("isBigSmileItem", "false")

	req, err := http.NewRequest("POST", "https://gsearch.gmarket.co.kr/SearchService/SeachListTemplateAjax", strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Accept", "application/json, text/javascript, */*; q=0.01")
	req.Header.Set("Accept-Language", "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7")
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	if sourceURL != "" {
		req.Header.Set("Referer", sourceURL)
	} else {
		req.Header.Set("Referer", BuildSearchURL(keyword, page, pageSize))
	}

	resp, err := gmarketHTTPClient.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", 0, fmt.Errorf("gsearch ajax http status %d", resp.StatusCode)
	}

	var payload gmarketSearchAjaxResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", 0, err
	}
	if !payload.Success {
		return "", payload.TotalCount, fmt.Errorf("gsearch ajax returned success=false")
	}

	return payload.Message, payload.TotalCount, nil
}

func CollectListJobDirect(job ListJob) []Row {
	pageNum, err := strconv.Atoi(job.SourcePage)
	if err != nil || pageNum <= 0 {
		pageNum = 1
	}
	pageSize := SearchPageSize
	if pageSize <= 0 {
		pageSize = 60
	}
	sourceURL := job.SourceURL
	if sourceURL == "" {
		sourceURL = BuildSearchURL(job.Keyword, pageNum, pageSize)
	}

	fmt.Printf("\n[gsearch AJAX 목록 수집] %s\n%s\n", job.Label, sourceURL)

	htmlStr, totalCount, err := FetchGmarketSearchAjaxHTML(job.Keyword, pageNum, pageSize, sourceURL)
	if err != nil {
		fmt.Println("gsearch AJAX 목록 오류:", err)
		return nil
	}

	rows := ParseListProducts(htmlStr, sourceURL, job.SourceCategory, job.GroupCode, job.Keyword, job.SourcePage)
	for i := range rows {
		rows[i]["목록_수집방식"] = "gsearch_ajax"
		rows[i]["목록_총검색수"] = strconv.Itoa(totalCount)
	}

	rand.Shuffle(len(rows), func(i, j int) {
		rows[i], rows[j] = rows[j], rows[i]
	})

	if job.Limit > 0 && len(rows) > job.Limit {
		rows = rows[:job.Limit]
	}

	fmt.Printf("현재 목록 상품 수: %d / totalCount=%d\n", len(rows), totalCount)
	return rows
}

func CollectListJobsDirect(jobs []ListJob) []Row {
	if len(jobs) == 0 {
		return nil
	}

	concurrency := ListPageConcurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > len(jobs) {
		concurrency = len(jobs)
	}

	fmt.Println("목록 수집 방식: gsearch_ajax")
	fmt.Println("목록 페이지 동시 처리 수:", concurrency)

	sem := make(chan struct{}, concurrency)
	resultCh := make(chan []Row, len(jobs))
	var wg sync.WaitGroup

	for _, job := range jobs {
		jobCopy := job
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() {
				<-sem
			}()
			time.Sleep(time.Duration(rand.Intn(700)) * time.Millisecond)
			resultCh <- CollectListJobDirect(jobCopy)
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	allRows := []Row{}
	for rows := range resultCh {
		allRows = append(allRows, rows...)
	}

	return allRows
}

func BalancedLimitRows(rows []Row, total int) []Row {
	if total <= 0 || len(rows) <= total {
		return rows
	}

	rand.Shuffle(len(rows), func(i, j int) {
		rows[i], rows[j] = rows[j], rows[i]
	})

	groups := map[string][]Row{}

	for _, row := range rows {
		cat := row["수집카테고리"]
		if cat == "" {
			cat = "기타"
		}
		groups[cat] = append(groups[cat], row)
	}

	keys := []string{}
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	selected := []Row{}

	for len(selected) < total {
		added := false

		for _, k := range keys {
			if len(groups[k]) == 0 {
				continue
			}

			selected = append(selected, groups[k][0])
			groups[k] = groups[k][1:]
			added = true

			if len(selected) >= total {
				break
			}
		}

		if !added {
			break
		}
	}

	return selected
}

// ============================================================
// 8. 이미지 추출
// ============================================================

func ExtractImageURLsFromHTML(htmlStr string, doc *goquery.Document, baseURL string, productCode string) ([]string, []string) {
	imageURLs := []string{}

	ogImage := GetMetaContent(doc, "og:image")
	if ogImage != "" {
		imageURLs = append(imageURLs, NormalizeURL(ogImage, baseURL))
	}

	doc.Find("img").Each(func(i int, s *goquery.Selection) {
		for _, attr := range []string{
			"src", "data-src", "data-original", "data-lazy",
			"data-original-src", "data-url", "data-img",
		} {
			if v, ok := s.Attr(attr); ok && v != "" {
				imageURLs = append(imageURLs, NormalizeURL(v, baseURL))
			}
		}
	})

	doc.Find("[style]").Each(func(i int, s *goquery.Selection) {
		style, ok := s.Attr("style")
		if !ok {
			return
		}

		re := regexp.MustCompile(`url\((.*?)\)`)
		matches := re.FindAllStringSubmatch(style, -1)

		for _, m := range matches {
			if len(m) >= 2 {
				v := strings.Trim(m[1], `"' `)
				if v != "" {
					imageURLs = append(imageURLs, NormalizeURL(v, baseURL))
				}
			}
		}
	})

	rawHTML := strings.ReplaceAll(htmlStr, `\/`, `/`)

	imagePattern := regexp.MustCompile(`(?i)(https?:)?//[^'"\s<>]+?\.(?:jpg|jpeg|png|gif|webp)(?:\?[^'"\s<>]*)?`)
	for _, m := range imagePattern.FindAllString(rawHTML, -1) {
		imageURLs = append(imageURLs, NormalizeURL(m, baseURL))
	}

	specialPattern := regexp.MustCompile(`(?i)(https?:)?//(?:gdimg|gi\.esmplus|image\.gmarket)[^'"\s<>\\]+`)
	for _, m := range specialPattern.FindAllString(rawHTML, -1) {
		imageURLs = append(imageURLs, NormalizeURL(m, baseURL))
	}

	cleaned := []string{}

	badKeywords := []string{
		"sprite", "blank", "logo", "button", "btn", "icon",
		"loading", "pixel", "transparent", "common", "banner_ad",
		"facebook", "twitter",
	}

	for _, u := range imageURLs {
		u = NormalizeURL(u, baseURL)
		lower := strings.ToLower(u)

		if !strings.HasPrefix(lower, "http") {
			continue
		}

		bad := false
		for _, b := range badKeywords {
			if strings.Contains(lower, b) {
				bad = true
				break
			}
		}

		if bad {
			continue
		}

		cleaned = append(cleaned, u)

		if strings.Contains(u, "/still/80") {
			cleaned = append(cleaned, strings.Replace(u, "/still/80", "/still/600", 1))
		} else if strings.Contains(u, "/still/160") {
			cleaned = append(cleaned, strings.Replace(u, "/still/160", "/still/600", 1))
		} else if strings.Contains(u, "/still/300") {
			cleaned = append(cleaned, strings.Replace(u, "/still/300", "/still/600", 1))
		}
	}

	cleaned = UniqueKeepOrder(cleaned)

	allImages := cleaned
	if len(allImages) > AllImageMax {
		allImages = allImages[:AllImageMax]
	}

	descImages := []string{}

	for _, u := range cleaned {
		lower := strings.ToLower(u)
		score := 0

		if productCode != "" && strings.Contains(u, productCode) {
			score += 2
		}

		if strings.Contains(lower, "gi.esmplus.com") {
			score += 5
		}

		if strings.Contains(lower, "gdimg") {
			score += 1
		}

		for _, word := range []string{
			"detail", "desc", "description", "prd", "contents", "content", "goods_image2",
		} {
			if strings.Contains(lower, word) {
				score += 3
				break
			}
		}

		if strings.Contains(lower, "/exlarge") ||
			strings.Contains(lower, "/shop_moreimg") ||
			strings.Contains(lower, "/middle_moreimg") {
			score += 2
		}

		if strings.Contains(lower, "/still/600") ||
			strings.Contains(lower, "/still/400") {
			score += 2
		}

		if strings.Contains(lower, "/still/80") ||
			strings.Contains(lower, "/still/160") {
			score -= 1
		}

		if score >= 2 {
			descImages = append(descImages, u)
		}
	}

	descImages = UniqueKeepOrder(descImages)
	if len(descImages) > DescriptionImageMax {
		descImages = descImages[:DescriptionImageMax]
	}

	return allImages, descImages
}

// ============================================================
// 9. 국내 상세 파싱
// ============================================================

func ExtractKVPairs(doc *goquery.Document) map[string]string {
	kv := map[string]string{}

	addPair := func(k string, v string) {
		k = strings.Trim(CleanText(k), ":：")
		v = CleanText(v)

		if k == "" || v == "" {
			return
		}

		if k == v {
			return
		}

		if len([]rune(k)) > 80 {
			return
		}

		v = Truncate(v, 1500)

		if _, ok := kv[k]; !ok {
			kv[k] = v
		}
	}

	doc.Find("tr").Each(func(i int, tr *goquery.Selection) {
		texts := []string{}

		tr.Find("th, td").Each(func(j int, cell *goquery.Selection) {
			t := CleanText(cell.Text())
			if t != "" {
				texts = append(texts, t)
			}
		})

		if len(texts) == 2 {
			addPair(texts[0], texts[1])
		} else if len(texts) >= 4 {
			for i := 0; i < len(texts)-1; i += 2 {
				addPair(texts[i], texts[i+1])
			}
		}
	})

	doc.Find("dt").Each(func(i int, dt *goquery.Selection) {
		dd := dt.NextFiltered("dd")
		if dd.Length() > 0 {
			addPair(dt.Text(), dd.Text())
		}
	})

	doc.Find("li").Each(func(i int, li *goquery.Selection) {
		spans := li.ChildrenFiltered("span, strong, em")

		if spans.Length() >= 2 {
			texts := []string{}

			spans.Each(func(j int, s *goquery.Selection) {
				t := CleanText(s.Text())
				if t != "" {
					texts = append(texts, t)
				}
			})

			if len(texts) >= 2 {
				addPair(texts[0], texts[1])
			}
		}
	})

	return kv
}

func KVGet(kv map[string]string, labels []string) string {
	for k, v := range kv {
		for _, label := range labels {
			if strings.Contains(strings.ToLower(k), strings.ToLower(label)) {
				return v
			}
		}
	}

	return ""
}

func RegexFirst(text string, patterns []string) string {
	for _, p := range patterns {
		re := regexp.MustCompile(`(?is)` + p)
		m := re.FindStringSubmatch(text)

		if len(m) >= 2 {
			v := htmlpkg.UnescapeString(m[1])
			v = strings.ReplaceAll(v, `\/`, `/`)
			return CleanText(v)
		}
	}

	return ""
}

func ParseKoreanDetail(htmlStr string, detailURL string, finalURL string, fetchMethod string) Row {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlStr))
	if err != nil {
		return Row{
			"국내_파싱오류": err.Error(),
		}
	}

	scripts := []string{}
	doc.Find("script").Each(func(i int, s *goquery.Selection) {
		scripts = append(scripts, s.Text())
	})
	scriptText := strings.Join(scripts, "\n")

	lines := LinesFromHTML(htmlStr)
	fullText := strings.Join(lines, "\n")

	productCode := ExtractProductCode(detailURL)
	if productCode == "" {
		productCode = ExtractProductCode(fullText)
	}

	titleTag := ""
	if doc.Find("title").Length() > 0 {
		titleTag = CleanText(doc.Find("title").First().Text())
	}

	ogTitle := GetMetaContent(doc, "og:title")
	metaDesc := GetMetaContent(doc, "description")
	kv := ExtractKVPairs(doc)

	productName := ogTitle
	if productName == "" {
		productName = GetFirstText(doc.Selection, []string{
			"h1",
			".itemtit",
			".item-title",
			".box__item-title",
			".text__item-title",
			`[class*="item-title"]`,
			`[class*="goods-title"]`,
			`[class*="product-title"]`,
		})
	}
	if productName == "" {
		productName = titleTag
	}

	priceRaw := GetFirstText(doc.Selection, []string{
		".price",
		".s-price",
		".box__price-seller",
		".text__price-seller",
		`[class*="price"]`,
		`[class*="Price"]`,
	})

	if priceRaw == "" {
		priceRaw = RegexFirst(scriptText+"\n"+fullText, []string{
			`"price"\s*:\s*"?([\d,]+)"?`,
			`"sellPrice"\s*:\s*"?([\d,]+)"?`,
			`"salePrice"\s*:\s*"?([\d,]+)"?`,
		})
	}

	detail := Row{
		"국내_상세URL":            detailURL,
		"국내_최종URL":            finalURL,
		"국내_수집방식":             fetchMethod,
		"국내_상품코드":             productCode,
		"국내_상품명":              productName,
		"국내_페이지제목":            titleTag,
		"국내_meta_description": metaDesc,
		"국내_가격_raw":           priceRaw,
		"국내_가격_KRW":           ExtractPriceKRW(priceRaw),
	}

	if detail["국내_가격_KRW"] == "" {
		detail["국내_가격_KRW"] = ExtractPriceKRW(fullText)
	}

	categoryCandidates := []string{}

	doc.Find(`a[href*="category"], a[href*="Category"], .location a, .breadcrumb a, nav a`).Each(func(i int, a *goquery.Selection) {
		txt := CleanText(a.Text())

		if txt != "" &&
			txt != "홈" &&
			txt != "Home" &&
			txt != "G마켓" &&
			txt != "전체보기" &&
			len([]rune(txt)) > 1 &&
			len([]rune(txt)) <= 60 {
			categoryCandidates = append(categoryCandidates, txt)
		}
	})

	detail["국내_카테고리"] = strings.Join(UniqueKeepOrder(categoryCandidates), " > ")

	detail["국내_브랜드"] = KVGet(kv, []string{"브랜드"})
	if detail["국내_브랜드"] == "" {
		detail["국내_브랜드"] = RegexFirst(scriptText, []string{
			`"brandName"\s*:\s*"([^"]{1,120})"`,
			`"brand"\s*:\s*"([^"]{1,120})"`,
		})
	}

	detail["국내_판매자"] = KVGet(kv, []string{"판매자", "상호", "업체명", "스토어"})
	if detail["국내_판매자"] == "" {
		detail["국내_판매자"] = RegexFirst(scriptText, []string{
			`"sellerName"\s*:\s*"([^"]{1,150})"`,
			`"sellerNickName"\s*:\s*"([^"]{1,150})"`,
			`"shopName"\s*:\s*"([^"]{1,150})"`,
			`"miniShopName"\s*:\s*"([^"]{1,150})"`,
		})
	}

	fieldMap := map[string][]string{
		"국내_상품상태":      {"상품상태"},
		"국내_원산지":       {"원산지"},
		"국내_제조국":       {"제조국"},
		"국내_제조사":       {"제조사", "제조자"},
		"국내_제조자_수입자":   {"제조자", "수입자", "제조업자"},
		"국내_모델명":       {"모델명", "모델"},
		"국내_품명":        {"품명"},
		"국내_인증정보":      {"인증", "KC"},
		"국내_정격전압_소비전력": {"정격전압", "소비전력"},
		"국내_출시년월":      {"출시년월", "출시일"},
		"국내_크기":        {"크기", "사이즈", "치수"},
		"국내_중량":        {"중량", "무게"},
		"국내_색상":        {"색상", "컬러"},
		"국내_재질":        {"재질"},
		"국내_구성품":       {"구성품"},
		"국내_배송비":       {"배송비", "배송료"},
		"국내_반품교환정보":    {"반품", "교환"},
		"국내_품질보증기준":    {"품질보증", "보증"},
		"국내_AS_연락처":    {"A/S", "AS", "소비자상담", "고객센터"},
		"국내_사업자번호":     {"사업자등록번호", "사업자번호"},
		"국내_통신판매업번호":   {"통신판매업"},
		"국내_판매자연락처":    {"전화번호", "연락처", "고객센터"},
		"국내_판매자주소":     {"주소"},
		"국내_판매자이메일":    {"이메일", "e-mail"},
	}

	for col, labels := range fieldMap {
		if v := KVGet(kv, labels); v != "" {
			detail[col] = v
		}
	}

	detail["국내_리뷰수"] = ExtractFirstNumber(RegexFirst(fullText+"\n"+scriptText, []string{
		`상품평\s*([\d,]+)`,
		`리뷰\s*([\d,]+)`,
		`후기\s*([\d,]+)`,
		`"reviewCount"\s*:\s*"?([\d,]+)"?`,
	}))

	detail["국내_주문수"] = ExtractFirstNumber(RegexFirst(fullText+"\n"+scriptText, []string{
		`구매\s*([\d,]+)\s*건`,
		`판매\s*([\d,]+)\s*개`,
		`"buyCount"\s*:\s*"?([\d,]+)"?`,
		`"orderCount"\s*:\s*"?([\d,]+)"?`,
	}))

	descriptionText := ExtractSectionText(
		lines,
		[]string{"상품상세정보", "상품 상세정보", "상세정보", "상품 설명", "상품설명", "제품상세"},
		[]string{"상품정보제공고시", "배송", "교환", "반품", "상품평", "판매자정보", "판매자 정보"},
		DescriptionTextMaxChars,
	)

	if descriptionText == "" {
		descriptionText = metaDesc
	}

	detail["국내_상세설명_텍스트"] = descriptionText

	allImages, descImages := ExtractImageURLsFromHTML(htmlStr, doc, detailURL, productCode)

	detail["국내_상세이미지_전체개수"] = strconv.Itoa(len(allImages))
	detail["국내_상세설명_이미지개수"] = strconv.Itoa(len(descImages))
	detail["국내_상세설명_이미지URL목록"] = strings.Join(descImages, " | ")

	for i := 0; i < len(allImages) && i < 10; i++ {
		detail[fmt.Sprintf("국내_상세이미지_URL_%d", i+1)] = allImages[i]
	}

	for i := 0; i < len(descImages) && i < 10; i++ {
		detail[fmt.Sprintf("국내_상세설명이미지_URL_%d", i+1)] = descImages[i]
	}

	detail["국내_본문텍스트_일부"] = Truncate(strings.Join(lines, "\n"), FullTextMaxChars)

	return detail
}

// ============================================================
// 10. 글로벌 상세 파싱
// ============================================================

var GlobalLabels = []string{
	"Shipping fee",
	"Item Weight",
	"Item condition",
	"VAT exemption",
	"Receipt issurance",
	"Business type",
	"Brand",
	"Country of manufacture",
	"Product name & Model number",
	"Rated voltage / Power consumption",
	"Energy efficiency rating",
	"Release date of the model",
	"Manufacturer / Importer",
	"Additional charges for installation",
	"Warranty policy",
	"Customer service and contact",
	"Estimated delivery time(in Korea)",
	"Seller",
	"Shop Name/Representative",
	"Business No.",
	"Contact No.",
	"Address",
	"Mail-order selling registration No.",
	"e-mail",
}

func GlobalLabelsLowerMap() map[string]bool {
	m := map[string]bool{}
	for _, label := range GlobalLabels {
		m[strings.ToLower(label)] = true
	}
	return m
}

func FindLabelValue(lines []string, label string, known map[string]bool) string {
	labelLower := strings.ToLower(label)
	values := []string{}

	for i, line := range lines {
		lineClean := CleanText(line)
		lineLower := strings.ToLower(lineClean)

		if lineLower != labelLower && !strings.HasPrefix(lineLower, labelLower+" ") {
			continue
		}

		sameLine := regexp.MustCompile(`(?i)`+regexp.QuoteMeta(label)).ReplaceAllString(lineClean, "")
		sameLine = strings.Trim(sameLine, " :：-")
		sameLine = strings.ReplaceAll(sameLine, "Copy URL copy", "")
		sameLine = CleanText(sameLine)

		if sameLine != "" && !known[strings.ToLower(sameLine)] {
			values = append(values, sameLine)
		}

		for j := i + 1; j < len(lines) && j < i+6; j++ {
			cand := CleanText(lines[j])
			candLower := strings.ToLower(cand)

			if cand == "" {
				continue
			}

			if known[candLower] {
				break
			}

			if candLower == "open" ||
				candLower == "close" ||
				candLower == "prev" ||
				candLower == "next" ||
				candLower == "열기" ||
				candLower == "닫기" {
				continue
			}

			cand = strings.ReplaceAll(cand, "Copy URL copy", "")
			cand = CleanText(cand)

			if cand != "" {
				values = append(values, cand)
				break
			}
		}
	}

	return strings.Join(UniqueKeepOrder(values), " | ")
}

func ExtractGlobalTitleFromLines(lines []string) string {
	known := GlobalLabelsLowerMap()

	for i, line := range lines {
		if strings.Contains(line, "Item No.") {
			for j := i + 1; j < len(lines) && j < i+10; j++ {
				cand := CleanText(lines[j])

				if cand == "" {
					continue
				}

				if known[strings.ToLower(cand)] {
					continue
				}

				if strings.Contains(cand, "$") ||
					strings.Contains(cand, "￦") ||
					strings.Contains(cand, "₩") {
					continue
				}

				if cand == "add/remove on wish list" ||
					cand == "Added on your" ||
					cand == "Wish List" {
					continue
				}

				if len([]rune(cand)) >= 3 {
					return cand
				}
			}
		}
	}

	return ""
}

func ExtractMainPriceLine(lines []string) string {
	for _, line := range lines {
		if strings.Contains(line, "$") &&
			(strings.Contains(line, "￦") ||
				strings.Contains(line, "₩") ||
				strings.Contains(line, "KRW")) {
			return line
		}
	}

	for _, line := range lines {
		if strings.Contains(line, "$") {
			return line
		}
	}

	for _, line := range lines {
		if strings.Contains(line, "￦") ||
			strings.Contains(line, "₩") ||
			strings.Contains(line, "원") {
			return line
		}
	}

	return ""
}

func ExtractGlobalOptions(lines []string) []string {
	options := []string{}
	re := regexp.MustCompile(`(?i)^Item\s+\d{1,3}`)

	for i, line := range lines {
		line = CleanText(line)

		if !re.MatchString(line) {
			continue
		}

		pieces := []string{line}

		for j := i + 1; j < len(lines) && j < i+4; j++ {
			nxt := CleanText(lines[j])

			if nxt == "" {
				continue
			}

			if nxt == "interest" ||
				nxt == "Description Select Item" ||
				nxt == "Added on your" ||
				nxt == "Wish List" ||
				nxt == "Quantity" ||
				nxt == "Confirm" {
				continue
			}

			if re.MatchString(nxt) {
				break
			}

			pieces = append(pieces, nxt)
		}

		option := strings.Join(pieces, " ")
		if len([]rune(option)) >= 5 && len([]rune(option)) <= 300 {
			options = append(options, option)
		}
	}

	return UniqueKeepOrder(options)
}

func ParseGlobalDetail(htmlStr string, detailURL string, finalURL string, fetchMethod string) Row {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlStr))
	if err != nil {
		return Row{
			"글로벌_파싱오류": err.Error(),
		}
	}

	lines := LinesFromHTML(htmlStr)
	fullText := strings.Join(lines, "\n")

	productCode := ExtractProductCode(detailURL)
	if productCode == "" {
		productCode = ExtractProductCode(fullText)
	}

	titleTag := ""
	if doc.Find("title").Length() > 0 {
		titleTag = CleanText(doc.Find("title").First().Text())
	}

	ogTitle := GetMetaContent(doc, "og:title")
	priceLine := ExtractMainPriceLine(lines)

	productName := ogTitle
	if productName == "" {
		productName = ExtractGlobalTitleFromLines(lines)
	}
	if productName == "" {
		productName = strings.TrimSpace(strings.Replace(titleTag, "Gmarket -", "", 1))
	}

	detail := Row{
		"글로벌_상세URL":  detailURL,
		"글로벌_최종URL":  finalURL,
		"글로벌_수집방식":   fetchMethod,
		"글로벌_상품코드":   productCode,
		"글로벌_상품명":    productName,
		"글로벌_페이지제목":  titleTag,
		"글로벌_가격_raw": priceLine,
		"글로벌_가격_USD": ExtractPriceUSD(priceLine),
		"글로벌_가격_KRW": ExtractPriceKRW(priceLine),
	}

	if detail["글로벌_가격_KRW"] == "" {
		detail["글로벌_가격_KRW"] = ExtractPriceKRW(fullText)
	}

	if detail["글로벌_가격_USD"] == "" {
		detail["글로벌_가격_USD"] = ExtractPriceUSD(fullText)
	}

	known := GlobalLabelsLowerMap()

	labelMap := map[string]string{
		"Shipping fee":                        "글로벌_배송비",
		"Item Weight":                         "글로벌_상품중량",
		"Item condition":                      "글로벌_상품상태",
		"VAT exemption":                       "글로벌_과세여부",
		"Receipt issurance":                   "글로벌_영수증발행",
		"Business type":                       "글로벌_판매자유형",
		"Brand":                               "글로벌_브랜드",
		"Country of manufacture":              "글로벌_제조국",
		"Product name & Model number":         "글로벌_모델명",
		"Rated voltage / Power consumption":   "글로벌_정격전압_소비전력",
		"Energy efficiency rating":            "글로벌_에너지효율등급",
		"Release date of the model":           "글로벌_출시년월",
		"Manufacturer / Importer":             "글로벌_제조자_수입자",
		"Additional charges for installation": "글로벌_설치추가비용",
		"Warranty policy":                     "글로벌_품질보증기준",
		"Customer service and contact":        "글로벌_AS_연락처",
		"Estimated delivery time(in Korea)":   "글로벌_국내예상배송기간",
		"Seller":                              "글로벌_판매자",
		"Shop Name/Representative":            "글로벌_상호_대표자",
		"Business No.":                        "글로벌_사업자번호",
		"Contact No.":                         "글로벌_판매자연락처",
		"Address":                             "글로벌_판매자주소",
		"Mail-order selling registration No.": "글로벌_통신판매업번호",
		"e-mail":                              "글로벌_판매자이메일",
	}

	for label, col := range labelMap {
		v := FindLabelValue(lines, label, known)
		if v != "" {
			detail[col] = v
		}
	}

	categoryCandidates := []string{}

	doc.Find(`a[href*="glistings.gmarket.co.kr"], a[href*="category"]`).Each(func(i int, a *goquery.Selection) {
		txt := CleanText(a.Text())
		if txt != "" && txt != "Home" && txt != "열기" && len([]rune(txt)) <= 60 {
			categoryCandidates = append(categoryCandidates, txt)
		}
	})

	detail["글로벌_카테고리"] = strings.Join(UniqueKeepOrder(categoryCandidates), " > ")

	reReview := regexp.MustCompile(`(?i)\bReview\s+([\d,]+)`)
	reOrder := regexp.MustCompile(`(?i)\bOrder\s+([\d,]+)`)

	if m := reReview.FindStringSubmatch(fullText); len(m) >= 2 {
		detail["글로벌_리뷰수"] = ExtractFirstNumber(m[1])
	}

	if m := reOrder.FindStringSubmatch(fullText); len(m) >= 2 {
		detail["글로벌_주문수"] = ExtractFirstNumber(m[1])
	}

	shippingFlags := []string{}

	for _, word := range []string{
		"Korea Domestic Shipping Only",
		"Within Korea Free",
		"Free Shipping",
		"International Delivery",
		"EMS",
		"SF Express",
		"aviation Shipment",
	} {
		if strings.Contains(strings.ToLower(fullText), strings.ToLower(word)) {
			shippingFlags = append(shippingFlags, word)
		}
	}

	detail["글로벌_배송태그"] = strings.Join(UniqueKeepOrder(shippingFlags), " | ")

	options := ExtractGlobalOptions(lines)
	detail["글로벌_옵션개수"] = strconv.Itoa(len(options))
	detail["글로벌_옵션목록"] = strings.Join(options, " | ")

	descriptionText := ExtractSectionText(
		lines,
		[]string{"See Description", "Description Select Item", "You can see more description"},
		[]string{"Specific Item Info", "Item Review", "Cancel / Return / Exchange Info", "Seller Info", "Notice"},
		DescriptionTextMaxChars,
	)

	if descriptionText == "" {
		descriptionText = ExtractSectionText(
			lines,
			[]string{"Item 01", "Item 02"},
			[]string{"Specific Item Info", "Premium Review", "Item Review"},
			DescriptionTextMaxChars,
		)
	}

	detail["글로벌_상세설명_텍스트"] = descriptionText

	allImages, descImages := ExtractImageURLsFromHTML(htmlStr, doc, detailURL, productCode)

	detail["글로벌_상세이미지_전체개수"] = strconv.Itoa(len(allImages))
	detail["글로벌_상세설명_이미지개수"] = strconv.Itoa(len(descImages))
	detail["글로벌_상세설명_이미지URL목록"] = strings.Join(descImages, " | ")

	for i := 0; i < len(allImages) && i < 10; i++ {
		detail[fmt.Sprintf("글로벌_상세이미지_URL_%d", i+1)] = allImages[i]
	}

	for i := 0; i < len(descImages) && i < 10; i++ {
		detail[fmt.Sprintf("글로벌_상세설명이미지_URL_%d", i+1)] = descImages[i]
	}

	detail["글로벌_본문텍스트_일부"] = Truncate(strings.Join(lines, "\n"), FullTextMaxChars)

	return detail
}

// ============================================================
// 11. 목록 수집
// ============================================================

func CollectFromBestCategories(ctx context.Context) []Row {
	allRows := []Row{}

	categories := make([]Category, len(DefaultBestCategories))
	copy(categories, DefaultBestCategories)

	rand.Shuffle(len(categories), func(i, j int) {
		categories[i], categories[j] = categories[j], categories[i]
	})

	if RandomCategoryCount > 0 && RandomCategoryCount < len(categories) {
		categories = categories[:RandomCategoryCount]
	}

	fmt.Println("선택된 카테고리:")
	for _, c := range categories {
		fmt.Printf("- %s / %s\n", c.Name, c.URL)
	}

	if ListSourceMode == "gsearch_ajax" {
		jobs := []ListJob{}
		for _, c := range categories {
			keyword := strings.ReplaceAll(c.Name, "/", " ")
			jobs = append(jobs, ListJob{
				Label:          c.Name,
				Keyword:        keyword,
				SourceURL:      BuildSearchURL(keyword, 1, SearchPageSize),
				SourceCategory: c.Name,
				GroupCode:      c.GroupCode,
				SourcePage:     "1",
				Limit:          ProductsPerCategory,
			})
		}
		return CollectListJobsDirect(jobs)
	}

	for _, c := range categories {
		fmt.Printf("\n[목록 수집] %s / %s\n", c.Name, c.URL)

		htmlStr, _, errMsg := FetchRenderedHTMLValidated(ctx, c.URL, ListScrollCount, nil)
		if errMsg != "" {
			fmt.Println("목록 페이지 오류:", errMsg)
		}

		rows := ParseListProducts(htmlStr, c.URL, c.Name, c.GroupCode, "", "")

		rand.Shuffle(len(rows), func(i, j int) {
			rows[i], rows[j] = rows[j], rows[i]
		})

		if ProductsPerCategory > 0 && len(rows) > ProductsPerCategory {
			rows = rows[:ProductsPerCategory]
		}

		if len(rows) == 0 && ListSourceMode == "auto" {
			keyword := strings.ReplaceAll(c.Name, "/", " ")
			fmt.Println("브라우저 목록이 비어 gsearch AJAX 목록으로 보충합니다:", keyword)
			rows = CollectListJobDirect(ListJob{
				Label:          c.Name,
				Keyword:        keyword,
				SourceURL:      BuildSearchURL(keyword, 1, SearchPageSize),
				SourceCategory: c.Name,
				GroupCode:      c.GroupCode,
				SourcePage:     "1",
				Limit:          ProductsPerCategory,
			})
		}

		fmt.Println("현재 카테고리 상품 수:", len(rows))

		allRows = append(allRows, rows...)
		time.Sleep(PageDelay + time.Duration(rand.Intn(900))*time.Millisecond)
	}

	return allRows
}

func CollectFromSearchKeywords(ctx context.Context) []Row {
	allRows := []Row{}

	keywords := make([]string, len(SearchKeywords))
	copy(keywords, SearchKeywords)

	rand.Shuffle(len(keywords), func(i, j int) {
		keywords[i], keywords[j] = keywords[j], keywords[i]
	})

	if ListSourceMode == "gsearch_ajax" {
		jobs := []ListJob{}
		for _, keyword := range keywords {
			for page := 1; page <= SearchPagesPerKeyword; page++ {
				sourceURL := BuildSearchURL(keyword, page, SearchPageSize)
				jobs = append(jobs, ListJob{
					Label:          fmt.Sprintf("%s / %d페이지", keyword, page),
					Keyword:        keyword,
					SourceURL:      sourceURL,
					SourceCategory: "",
					GroupCode:      "",
					SourcePage:     strconv.Itoa(page),
					Limit:          0,
				})
			}
		}
		return CollectListJobsDirect(jobs)
	}

	for _, keyword := range keywords {
		for page := 1; page <= SearchPagesPerKeyword; page++ {
			searchURL := BuildSearchURL(keyword, page, SearchPageSize)

			fmt.Printf("\n[검색 목록 수집] %s / %d페이지\n%s\n", keyword, page, searchURL)

			htmlStr, _, errMsg := FetchRenderedHTMLValidated(ctx, searchURL, ListScrollCount, nil)
			if errMsg != "" {
				fmt.Println("검색 페이지 오류:", errMsg)
			}

			rows := ParseListProducts(htmlStr, searchURL, "", "", keyword, strconv.Itoa(page))

			rand.Shuffle(len(rows), func(i, j int) {
				rows[i], rows[j] = rows[j], rows[i]
			})

			if len(rows) == 0 && ListSourceMode == "auto" {
				fmt.Println("브라우저 검색 목록이 비어 gsearch AJAX 목록으로 보충합니다:", keyword)
				rows = CollectListJobDirect(ListJob{
					Label:          fmt.Sprintf("%s / %d페이지", keyword, page),
					Keyword:        keyword,
					SourceURL:      searchURL,
					SourceCategory: "",
					GroupCode:      "",
					SourcePage:     strconv.Itoa(page),
					Limit:          0,
				})
			}

			fmt.Println("현재 검색 페이지 상품 수:", len(rows))

			allRows = append(allRows, rows...)

			if len(allRows) >= TotalTargetProducts*2 {
				return allRows
			}

			time.Sleep(PageDelay + time.Duration(rand.Intn(900))*time.Millisecond)
		}
	}

	return allRows
}

func FirstNonEmpty(row Row, cols []string) string {
	for _, col := range cols {
		v := CleanText(row[col])
		if v != "" {
			return v
		}
	}
	return ""
}

func FirstOr(a string, b string) string {
	if CleanText(a) != "" {
		return a
	}
	return b
}

func CollectFromExcel() []Row {
	f, err := excelize.OpenFile(InputFile)
	if err != nil {
		fmt.Println("엑셀 파일을 열 수 없습니다:", err)
		return nil
	}
	defer f.Close()

	sheets := f.GetSheetList()

	preferred := []string{
		"목록_국내_글로벌_상세",
		"목록_상세_병합",
		"상품_목록_상세",
		"URL보정_목록",
		"목록만",
		"목록",
	}

	sheetName := ""
	for _, p := range preferred {
		for _, s := range sheets {
			if s == p {
				sheetName = s
				break
			}
		}
		if sheetName != "" {
			break
		}
	}

	if sheetName == "" && len(sheets) > 0 {
		sheetName = sheets[0]
	}

	if sheetName == "" {
		return nil
	}

	rows, err := f.GetRows(sheetName)
	if err != nil || len(rows) == 0 {
		return nil
	}

	headers := rows[0]
	result := []Row{}

	for _, rawRow := range rows[1:] {
		source := Row{}

		for i, h := range headers {
			if i < len(rawRow) {
				source[h] = rawRow[i]
			}
		}

		raw := FirstNonEmpty(source, []string{
			"상품URL",
			"상품URL_raw",
			"상품URL_original",
			"상품URL_글로벌",
			"상품URL_국내",
			"상세URL",
		})

		code, domesticURL, globalURL := ExtractURLsFromRaw(raw, source["상품코드"])

		if code == "" {
			code = ExtractProductCode(domesticURL)
		}
		if code == "" {
			code = ExtractProductCode(globalURL)
		}
		if code == "" {
			continue
		}

		row := Row{
			"상품명":        FirstNonEmpty(source, []string{"상품명", "상품명_목록"}),
			"상품코드":       code,
			"상품URL_국내":   FirstOr(domesticURL, KoreanItemURL(code)),
			"상품URL_글로벌":  FirstOr(globalURL, GlobalItemURL(code)),
			"상품URL_raw":  raw,
			"목록_이미지URL":  FirstNonEmpty(source, []string{"목록_이미지URL", "이미지URL"}),
			"목록_가격_raw":  FirstNonEmpty(source, []string{"목록_가격_raw", "판매가_raw"}),
			"목록_판매가_KRW": FirstNonEmpty(source, []string{"목록_판매가_KRW", "판매가"}),
			"목록_정가_raw":  FirstNonEmpty(source, []string{"목록_정가_raw", "정가_raw"}),
			"목록_정가_KRW":  FirstNonEmpty(source, []string{"목록_정가_KRW", "정가"}),
			"수집URL":      source["수집URL"],
			"수집카테고리":     source["수집카테고리"],
			"groupCode":  source["groupCode"],
			"검색어":        source["검색어"],
			"페이지":        source["페이지"],
		}

		result = append(result, row)
	}

	fmt.Println("엑셀에서 읽은 상품 수:", len(result))

	return result
}

func CollectListProducts(ctx context.Context) []Row {
	var rows []Row

	switch CollectMode {
	case "random_best_categories":
		rows = CollectFromBestCategories(ctx)

		if len(rows) < TotalTargetProducts {
			fmt.Println("\n베스트 카테고리 수집량이 부족해서 검색어 방식으로 보충합니다.")
			extra := CollectFromSearchKeywords(ctx)
			rows = append(rows, extra...)
		}

	case "search_keywords":
		rows = CollectFromSearchKeywords(ctx)

	case "from_excel":
		rows = CollectFromExcel()

	default:
		fmt.Println("알 수 없는 CollectMode:", CollectMode)
		return nil
	}

	seen := map[string]bool{}
	unique := []Row{}

	for _, row := range rows {
		code := row["상품코드"]
		if code == "" {
			continue
		}

		if seen[code] {
			continue
		}

		seen[code] = true
		unique = append(unique, row)
	}

	unique = BalancedLimitRows(unique, TotalTargetProducts)

	for i := range unique {
		unique[i]["순번"] = strconv.Itoa(i + 1)
	}

	return unique
}

func ApplyShardFilter(rows []Row) []Row {
	if ShardCount <= 1 {
		return rows
	}
	filtered := make([]Row, 0, len(rows)/ShardCount+1)
	for _, row := range rows {
		code := CleanText(row["상품코드"])
		if code == "" {
			continue
		}
		if ProductShard(code, ShardCount) == ShardIndex {
			row["shard_count"] = strconv.Itoa(ShardCount)
			row["shard_index"] = strconv.Itoa(ShardIndex)
			filtered = append(filtered, row)
		}
	}
	fmt.Printf("분산 shard 적용: index=%d count=%d before=%d after=%d\n", ShardIndex, ShardCount, len(rows), len(filtered))
	return filtered
}

func ProductShard(productCode string, shardCount int) int {
	if shardCount <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(productCode))
	return int(h.Sum32() % uint32(shardCount))
}

func NeedsBrowserForList() bool {
	return ListSourceMode == "browser" || ListSourceMode == "auto"
}

// ============================================================
// 12. 상세 병렬 수집
// ============================================================

func FetchAndParseKoreanDetail(ctx context.Context, productCode string, domesticURL string, pageSem chan struct{}) Row {
	result := Row{}

	pageSem <- struct{}{}
	SleepRandomUpTo(DetailStartJitter)

	htmlStr, finalURL, errMsg := FetchRenderedHTMLValidated(
		ctx,
		domesticURL,
		DetailScrollCount,
		[]string{
			"상품상세정보",
			"상품정보",
			"상품 정보",
			"상세정보",
		},
	)
	<-pageSem

	if htmlStr == "" {
		result["국내_수집오류"] = errMsg
		result["국내_수집성공"] = "false"
		return result
	}

	ok, reason := ClassifyHTMLPage(htmlStr, finalURL, "korean")
	if !ok {
		result["국내_수집오류"] = reason
		result["국내_수집성공"] = "false"
		return result
	}

	parsed := ParseKoreanDetail(htmlStr, domesticURL, finalURL, "chromedp_parallel_validated")

	suspect := parsed["국내_상품명"] + "\n" + parsed["국내_페이지제목"] + "\n" + Truncate(parsed["국내_본문텍스트_일부"], 500)
	ok2, reason2 := ClassifyHTMLPage(suspect+"\n"+Truncate(htmlStr, 1000), finalURL, "korean")

	if !ok2 {
		result["국내_수집오류"] = reason2
		result["국내_수집성공"] = "false"
		return result
	}

	for k, v := range parsed {
		result[k] = v
	}

	result["국내_수집성공"] = "true"

	return result
}

func FetchAndParseGlobalDetail(ctx context.Context, productCode string, globalURL string, pageSem chan struct{}) Row {
	result := Row{}

	pageSem <- struct{}{}
	SleepRandomUpTo(DetailStartJitter)

	htmlStr, finalURL, errMsg := FetchRenderedHTMLValidated(
		ctx,
		globalURL,
		DetailScrollCount,
		[]string{
			"Item Info",
			"Description Select Item",
			"See Description",
			"Specific Item Info",
		},
	)
	<-pageSem

	if htmlStr == "" {
		result["글로벌_수집오류"] = errMsg
		result["글로벌_수집성공"] = "false"
		return result
	}

	ok, reason := ClassifyHTMLPage(htmlStr, finalURL, "global")
	if !ok {
		result["글로벌_수집오류"] = reason
		result["글로벌_수집성공"] = "false"
		return result
	}

	parsed := ParseGlobalDetail(htmlStr, globalURL, finalURL, "chromedp_parallel_validated")

	suspect := parsed["글로벌_상품명"] + "\n" + parsed["글로벌_페이지제목"] + "\n" + Truncate(parsed["글로벌_본문텍스트_일부"], 500)
	ok2, reason2 := ClassifyHTMLPage(suspect+"\n"+Truncate(htmlStr, 1000), finalURL, "global")

	if !ok2 {
		result["글로벌_수집오류"] = reason2
		result["글로벌_수집성공"] = "false"
		return result
	}

	for k, v := range parsed {
		result[k] = v
	}

	result["글로벌_수집성공"] = "true"

	return result
}

func CollectOneProductDetail(ctx context.Context, row Row, productSem chan struct{}, pageSem chan struct{}) Row {
	productSem <- struct{}{}
	defer func() {
		<-productSem
	}()

	productCode := row["상품코드"]
	productName := row["상품명"]

	domesticURL := row["상품URL_국내"]
	if domesticURL == "" {
		domesticURL = KoreanItemURL(productCode)
	}

	globalURL := row["상품URL_글로벌"]
	if globalURL == "" {
		globalURL = GlobalItemURL(productCode)
	}

	detail := Row{
		"상품코드":      productCode,
		"상품명_목록":    productName,
		"상품URL_국내":  domesticURL,
		"상품URL_글로벌": globalURL,
	}

	resultCh := make(chan Row, 2)
	var wg sync.WaitGroup

	if (DetailSourceMode == "both" || DetailSourceMode == "korean") && domesticURL != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resultCh <- FetchAndParseKoreanDetail(ctx, productCode, domesticURL, pageSem)
		}()
	}

	if (DetailSourceMode == "both" || DetailSourceMode == "global") && globalURL != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resultCh <- FetchAndParseGlobalDetail(ctx, productCode, globalURL, pageSem)
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for partial := range resultCh {
		for k, v := range partial {
			detail[k] = v
		}
	}

	return detail
}

func CollectDetails(ctx context.Context, listRows []Row) []Row {
	workRows := listRows

	if DetailLimit > 0 && DetailLimit < len(workRows) {
		workRows = workRows[:DetailLimit]
	}

	fmt.Println("\n상세 수집 대상 상품 수:", len(workRows))
	fmt.Println("상세 수집 모드:", DetailSourceMode)
	fmt.Println("상품 동시 처리 수:", DetailProductConcurrency)
	fmt.Println("페이지 동시 처리 수:", DetailPageConcurrency)

	productSem := make(chan struct{}, DetailProductConcurrency)
	pageSem := make(chan struct{}, DetailPageConcurrency)

	resultCh := make(chan Row, len(workRows))
	var wg sync.WaitGroup

	for _, row := range workRows {
		rowCopy := row

		wg.Add(1)
		go func() {
			defer wg.Done()

			detail := CollectOneProductDetail(ctx, rowCopy, productSem, pageSem)
			resultCh <- detail

			fmt.Printf(
				"완료: %s / %s / 국내:%s / 글로벌:%s\n",
				detail["상품코드"],
				Truncate(detail["상품명_목록"], 40),
				detail["국내_수집성공"],
				detail["글로벌_수집성공"],
			)
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	detailRows := []Row{}

	for row := range resultCh {
		detailRows = append(detailRows, row)
	}

	return detailRows
}

// ============================================================
// 13. 병합 / 엑셀 저장
// ============================================================

func MergeRows(listRows []Row, detailRows []Row) []Row {
	detailMap := map[string]Row{}

	for _, row := range detailRows {
		code := row["상품코드"]
		if code != "" {
			detailMap[code] = row
		}
	}

	result := []Row{}

	for _, row := range listRows {
		merged := Row{}

		for k, v := range row {
			merged[k] = v
		}

		if d, ok := detailMap[row["상품코드"]]; ok {
			for k, v := range d {
				merged[k] = v
			}
		}

		AddDiscountRate(merged)

		result = append(result, merged)
	}

	return result
}

func AddDiscountRate(row Row) {
	price, err1 := strconv.Atoi(row["목록_판매가_KRW"])
	original, err2 := strconv.Atoi(row["목록_정가_KRW"])

	if err1 != nil || err2 != nil {
		return
	}

	if original <= 0 || original <= price {
		return
	}

	rate := (1.0 - float64(price)/float64(original)) * 100
	row["목록_할인율_계산"] = fmt.Sprintf("%.1f", rate)
}

func BuildColumns(rows []Row, preferred []string) []string {
	exists := map[string]bool{}

	for _, row := range rows {
		for k := range row {
			exists[k] = true
		}
	}

	columns := []string{}
	used := map[string]bool{}

	for _, col := range preferred {
		if exists[col] {
			columns = append(columns, col)
			used[col] = true
		}
	}

	others := []string{}
	for col := range exists {
		if !used[col] {
			others = append(others, col)
		}
	}

	sort.Strings(others)

	columns = append(columns, others...)

	return columns
}

func WriteSheet(f *excelize.File, sheetName string, rows []Row, preferred []string) error {
	if len(rows) == 0 {
		return nil
	}

	_, err := f.NewSheet(sheetName)
	if err != nil {
		return err
	}

	columns := BuildColumns(rows, preferred)

	for c, col := range columns {
		cell, _ := excelize.CoordinatesToCellName(c+1, 1)
		_ = f.SetCellValue(sheetName, cell, col)
	}

	for r, row := range rows {
		for c, col := range columns {
			cell, _ := excelize.CoordinatesToCellName(c+1, r+2)
			_ = f.SetCellValue(sheetName, cell, row[col])
		}
	}

	for i := 1; i <= len(columns); i++ {
		colName, _ := excelize.ColumnNumberToName(i)
		_ = f.SetColWidth(sheetName, colName, colName, 18)
	}

	return nil
}

func CategoryCountRows(listRows []Row) []Row {
	counts := map[string]int{}

	for _, row := range listRows {
		cat := row["수집카테고리"]
		if cat == "" {
			cat = "기타"
		}
		counts[cat]++
	}

	type kv struct {
		Key string
		Val int
	}

	arr := []kv{}
	for k, v := range counts {
		arr = append(arr, kv{k, v})
	}

	sort.Slice(arr, func(i, j int) bool {
		return arr[i].Val > arr[j].Val
	})

	rows := []Row{}
	for _, item := range arr {
		rows = append(rows, Row{
			"수집카테고리": item.Key,
			"상품수":    strconv.Itoa(item.Val),
		})
	}

	return rows
}

func SaveExcel(resultRows []Row, listRows []Row, detailRows []Row) error {
	if dir := filepath.Dir(OutputFile); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	f := excelize.NewFile()

	preferred := []string{
		"순번",
		"수집카테고리",
		"groupCode",
		"검색어",
		"페이지",
		"상품코드",
		"상품명",
		"상품명_목록",
		"상품URL_국내",
		"상품URL_글로벌",
		"상품URL_raw",
		"목록_이미지URL",
		"목록_가격_raw",
		"목록_판매가_KRW",
		"목록_정가_KRW",
		"목록_할인율_계산",

		"국내_상품명",
		"국내_가격_KRW",
		"국내_브랜드",
		"국내_카테고리",
		"국내_판매자",
		"국내_모델명",
		"국내_제조국",
		"국내_원산지",
		"국내_배송비",
		"국내_리뷰수",
		"국내_주문수",
		"국내_상세설명_텍스트",
		"국내_상세설명_이미지개수",
		"국내_상세설명_이미지URL목록",
		"국내_수집방식",
		"국내_수집성공",
		"국내_수집오류",
		"국내_파싱오류",

		"글로벌_상품명",
		"글로벌_가격_KRW",
		"글로벌_가격_USD",
		"글로벌_브랜드",
		"글로벌_카테고리",
		"글로벌_판매자",
		"글로벌_모델명",
		"글로벌_제조국",
		"글로벌_배송비",
		"글로벌_상품중량",
		"글로벌_리뷰수",
		"글로벌_주문수",
		"글로벌_상세설명_텍스트",
		"글로벌_상세설명_이미지개수",
		"글로벌_상세설명_이미지URL목록",
		"글로벌_수집방식",
		"글로벌_수집성공",
		"글로벌_수집오류",
		"글로벌_파싱오류",
	}

	summaryRows := []Row{
		{
			"항목": "목록 상품 수",
			"값":  strconv.Itoa(len(listRows)),
		},
		{
			"항목": "상세 수집 상품 수",
			"값":  strconv.Itoa(len(detailRows)),
		},
		{
			"항목": "수집 모드",
			"값":  CollectMode,
		},
		{
			"항목": "목록 수집 방식",
			"값":  ListSourceMode,
		},
		{
			"항목": "상세 수집 모드",
			"값":  DetailSourceMode,
		},
		{
			"항목": "목록 페이지 동시 처리 수",
			"값":  strconv.Itoa(ListPageConcurrency),
		},
		{
			"항목": "상품 동시 처리 수",
			"값":  strconv.Itoa(DetailProductConcurrency),
		},
		{
			"항목": "페이지 동시 처리 수",
			"값":  strconv.Itoa(DetailPageConcurrency),
		},
		{
			"항목": "검증 로직",
			"값":  "차단/대기/CloudFront 오류 페이지 제외",
		},
	}

	err := WriteSheet(f, "목록_국내_글로벌_상세", resultRows, preferred)
	if err != nil {
		return err
	}

	err = WriteSheet(f, "목록", listRows, nil)
	if err != nil {
		return err
	}

	err = WriteSheet(f, "상세", detailRows, nil)
	if err != nil {
		return err
	}

	err = WriteSheet(f, "요약", summaryRows, []string{"항목", "값"})
	if err != nil {
		return err
	}

	categoryRows := CategoryCountRows(listRows)
	if len(categoryRows) > 0 {
		err = WriteSheet(f, "카테고리별_수집수", categoryRows, []string{"수집카테고리", "상품수"})
		if err != nil {
			return err
		}
	}

	f.DeleteSheet("Sheet1")

	return f.SaveAs(OutputFile)
}

func FinalizeCollection(start time.Time, resultRows []Row, listRows []Row, detailRows []Row) {
	if ShouldPublishKafka() {
		if err := PublishGmarketRowsFromEnv(resultRows); err != nil {
			panic(err)
		}
	}

	if SaveExcelEnabled {
		if err := SaveExcel(resultRows, listRows, detailRows); err != nil {
			panic(err)
		}
		fmt.Println("저장 완료:", OutputFile)
	} else {
		fmt.Println("엑셀 저장 건너뜀: GMARKET_SAVE_EXCEL=false")
	}

	fmt.Println("최종 행 수:", len(resultRows))
	fmt.Println("소요 시간:", time.Since(start))
}

// ============================================================
// 14. main
// ============================================================

func main() {
	ApplyEnvConfig()

	if RandomSeed == 0 {
		rand.Seed(time.Now().UnixNano())
	} else {
		rand.Seed(RandomSeed)
	}

	start := time.Now()

	fmt.Println("Gmarket Go 크롤러 시작")
	fmt.Println("수집 모드:", CollectMode)
	fmt.Println("저장/적재 모드:", IngestMode)
	fmt.Println("목록 수집 방식:", ListSourceMode)
	fmt.Println("상세 수집 모드:", DetailSourceMode)
	fmt.Println("목표 상품 수:", TotalTargetProducts)
	fmt.Printf("분산 shard: %d/%d\n", ShardIndex, ShardCount)
	fmt.Println("목록 페이지 동시 처리:", ListPageConcurrency)
	fmt.Println("상세 상품 동시 처리:", DetailProductConcurrency)
	fmt.Println("상세 페이지 동시 처리:", DetailPageConcurrency)

	if ShouldPublishKafka() {
		fmt.Println("Kafka 사전 점검 시작")
		if err := PreflightKafkaFromEnv(context.Background()); err != nil {
			fmt.Println("Kafka 사전 점검 실패:", err)
			fmt.Println("조치 필요: Docker 권한이 있는 host에서 Statground_SQL/docker-compose/50004_Kafka_Platform/recreate_with_public_host.sh를 실행해 Kafka advertised listener를 현재 public host로 맞춰야 합니다.")
			os.Exit(1)
		}
		fmt.Println("Kafka 사전 점검 완료")
	}

	var ctx context.Context
	var cancel context.CancelFunc
	var err error

	if NeedsBrowserForList() {
		ctx, cancel, err = NewBrowserContext()
		if err != nil {
			panic(err)
		}
		defer cancel()
	}

	listRowsAll := CollectListProducts(ctx)
	listRows := ApplyShardFilter(listRowsAll)

	fmt.Println("\n====================================")
	fmt.Println("목록 수집 완료")
	fmt.Println("전체 목록 상품 수:", len(listRowsAll))
	fmt.Println("현재 shard 상품 수:", len(listRows))
	fmt.Println("====================================")

	if len(listRows) == 0 {
		if len(listRowsAll) > 0 && ShardCount > 1 {
			fmt.Println("현재 shard에 배정된 상품이 없습니다. 작업을 정상 종료합니다.")
			return
		}
		fmt.Println("목록 상품이 수집되지 않았습니다.")
		fmt.Println("CollectMode를 search_keywords로 바꿔 다시 시도해보세요.")
		if !AllowEmptyResult {
			os.Exit(1)
		}
	}

	if !CollectDetailsEnabled {
		fmt.Println("\n상세 수집을 건너뜁니다: GMARKET_COLLECT_DETAILS=false")
		resultRows := MergeRows(listRows, nil)
		FinalizeCollection(start, resultRows, listRows, nil)
		return
	}

	if ctx == nil {
		ctx, cancel, err = NewBrowserContext()
		if err != nil {
			panic(err)
		}
		defer cancel()
	}

	detailRows := CollectDetails(ctx, listRows)

	fmt.Println("\n====================================")
	fmt.Println("상세 수집 완료")
	fmt.Println("상세 수집 상품 수:", len(detailRows))
	fmt.Println("====================================")

	resultRows := MergeRows(listRows, detailRows)

	fmt.Println("\n====================================")
	FinalizeCollection(start, resultRows, listRows, detailRows)
	fmt.Println("====================================")
}
