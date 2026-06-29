package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

var KurlyCollectEnabled = false
var KurlyCollectMode = "search_api_keywords"
var KurlyTotalTargetProducts = 1200
var KurlyRandomKeywordCount = 120
var KurlySearchPagesPerKeyword = 3
var KurlyProductsPerKeyword = 16
var KurlyDetailLimit = 0
var KurlyDetailConcurrency = 8
var KurlyCollectDetailsEnabled = true
var KurlyAllowEmptyResult = false
var KurlyRequestSleepMin = 200 * time.Millisecond
var KurlyRequestSleepMax = 800 * time.Millisecond
var KurlyCategoryGroupsURL = "https://api.kurly.com/collection/v2/home/sites/market/category-groups"
var KurlySites = []string{"market"}
var KurlySearchKeywordsOverride []string

var KurlyDefaultDiverseKeywords = []string{
	"채소", "과일", "쌀", "잡곡", "정육", "닭고기", "돼지고기", "소고기",
	"수산", "생선", "새우", "계란", "우유", "요거트", "치즈", "버터",
	"샐러드", "간편식", "밀키트", "국", "탕", "찌개", "반찬", "김치",
	"라면", "떡볶이", "만두", "피자", "냉동식품", "닭가슴살",
	"과자", "초콜릿", "커피", "차", "주스", "생수", "베이커리",
	"세제", "휴지", "샴푸", "바디워시", "칫솔", "주방용품",
	"화장품", "선크림", "마스크팩", "강아지사료", "고양이간식",
}

var kurlyHTTPClient = &http.Client{
	Timeout: 35 * time.Second,
}

func ApplyKurlyEnvConfig() {
	setBoolFromEnv("KURLY_COLLECT_ENABLED", &KurlyCollectEnabled)
	setBoolFromEnv("KURLY_COLLECT_DETAILS", &KurlyCollectDetailsEnabled)
	setBoolFromEnv("KURLY_ALLOW_EMPTY_RESULT", &KurlyAllowEmptyResult)
	setStringFromEnv("KURLY_COLLECT_MODE", &KurlyCollectMode)
	setStringFromEnv("KURLY_CATEGORY_GROUPS_URL", &KurlyCategoryGroupsURL)
	setIntFromEnv("KURLY_TOTAL_TARGET_PRODUCTS", &KurlyTotalTargetProducts)
	setIntFromEnv("KURLY_RANDOM_KEYWORD_COUNT", &KurlyRandomKeywordCount)
	setIntFromEnv("KURLY_SEARCH_PAGES_PER_KEYWORD", &KurlySearchPagesPerKeyword)
	setIntFromEnv("KURLY_PRODUCTS_PER_KEYWORD", &KurlyProductsPerKeyword)
	setIntFromEnv("KURLY_DETAIL_LIMIT", &KurlyDetailLimit)
	setIntFromEnv("KURLY_DETAIL_CONCURRENCY", &KurlyDetailConcurrency)
	setDurationMillisFromEnv("KURLY_REQUEST_SLEEP_MIN_MS", &KurlyRequestSleepMin)
	setDurationMillisFromEnv("KURLY_REQUEST_SLEEP_MAX_MS", &KurlyRequestSleepMax)

	if v := strings.TrimSpace(os.Getenv("KURLY_SITES")); v != "" {
		sites := splitCSV(v)
		if len(sites) > 0 {
			KurlySites = sites
		}
	}

	if v := strings.TrimSpace(os.Getenv("KURLY_SEARCH_KEYWORDS")); v != "" {
		keywords := splitCSV(v)
		if len(keywords) > 0 {
			KurlySearchKeywordsOverride = keywords
		}
	}

	if KurlySearchPagesPerKeyword <= 0 {
		KurlySearchPagesPerKeyword = 1
	}
	if KurlyProductsPerKeyword < 0 {
		KurlyProductsPerKeyword = 0
	}
	if KurlyDetailConcurrency <= 0 {
		KurlyDetailConcurrency = 1
	}
	if KurlyRequestSleepMax < KurlyRequestSleepMin {
		KurlyRequestSleepMax = KurlyRequestSleepMin
	}
}

func RunKurlyCollection(ctx context.Context) {
	start := time.Now()

	fmt.Println("\nKurly 크롤러 시작")
	fmt.Println("수집 모드:", KurlyCollectMode)
	fmt.Println("대상 사이트:", strings.Join(KurlySites, ", "))
	fmt.Println("목표 상품 수:", KurlyTotalTargetProducts)
	fmt.Println("검색어 사용 수:", KurlyRandomKeywordCount)
	fmt.Println("검색어당 페이지 수:", KurlySearchPagesPerKeyword)
	fmt.Println("검색어당 상품 수:", KurlyProductsPerKeyword)
	fmt.Println("상세 수집:", KurlyCollectDetailsEnabled)
	fmt.Println("상세 동시 처리:", KurlyDetailConcurrency)

	listRows := KurlyCollectListProducts(ctx)

	fmt.Println("\n====================================")
	fmt.Println("Kurly 목록 수집 완료")
	fmt.Println("목록 상품 수:", len(listRows))
	fmt.Println("====================================")

	if len(listRows) == 0 {
		fmt.Println("Kurly 목록 상품이 수집되지 않았습니다.")
		if !KurlyAllowEmptyResult {
			os.Exit(1)
		}
	}

	var detailRows []Row
	var streamDetailPublish func(Row) error
	if KurlyCollectDetailsEnabled {
		if ShouldPublishKafka() {
			publisher, err := NewKurlyRowPublisherFromEnv()
			if err != nil {
				panic(err)
			}
			var publishMu sync.Mutex
			streamDetailPublish = func(row Row) error {
				publishMu.Lock()
				defer publishMu.Unlock()
				return publisher.Publish([]Row{row})
			}
			fmt.Println("Kurly 상세 완료 행 즉시 Kafka publish 활성화")
		}
		detailRows = KurlyCollectDetails(ctx, listRows, streamDetailPublish)
		fmt.Println("\n====================================")
		fmt.Println("Kurly 상세 수집 완료")
		fmt.Println("상세 수집 상품 수:", len(detailRows))
		fmt.Println("====================================")
	} else {
		fmt.Println("Kurly 상세 수집을 건너뜁니다: KURLY_COLLECT_DETAILS=false")
	}

	resultRows := MergeRows(listRows, detailRows)
	if ShouldPublishKafka() && streamDetailPublish == nil {
		if err := PublishKurlyRowsFromEnv(resultRows); err != nil {
			panic(err)
		}
	} else if ShouldPublishKafka() {
		fmt.Println("Kurly Kafka 최종 일괄 publish 건너뜀: 상세 수집 완료 행을 즉시 publish했습니다.")
	}

	fmt.Println("Kurly 최종 행 수:", len(resultRows))
	fmt.Println("Kurly 소요 시간:", time.Since(start))
}

func KurlyCollectListProducts(ctx context.Context) []Row {
	keywords := KurlyDiverseKeywords(ctx)
	fmt.Println("Kurly 사용할 검색어:", strings.Join(keywords, ", "))

	allRows := []Row{}
	for _, site := range KurlySites {
		for _, keyword := range keywords {
			keywordRows := []Row{}
			for page := 1; page <= KurlySearchPagesPerKeyword; page++ {
				rows, err := KurlySearchAPIProducts(ctx, keyword, page, site)
				if err != nil {
					fmt.Printf("[kurly] search_api error site=%s keyword=%s page=%d error=%s\n", site, keyword, page, err)
				}
				if len(rows) == 0 && page == 1 {
					fallbackRows, fallbackErr := KurlySearchHTMLFallback(ctx, keyword)
					if fallbackErr != nil {
						fmt.Printf("[kurly] search_html_fallback error keyword=%s error=%s\n", keyword, fallbackErr)
					}
					rows = fallbackRows
				}
				keywordRows = append(keywordRows, rows...)
				KurlySleepRandom(ctx)
			}

			rand.Shuffle(len(keywordRows), func(i, j int) {
				keywordRows[i], keywordRows[j] = keywordRows[j], keywordRows[i]
			})
			if KurlyProductsPerKeyword > 0 && len(keywordRows) > KurlyProductsPerKeyword {
				keywordRows = keywordRows[:KurlyProductsPerKeyword]
			}
			allRows = append(allRows, keywordRows...)

			if KurlyTotalTargetProducts > 0 && len(allRows) >= KurlyTotalTargetProducts*2 {
				break
			}
		}
		if KurlyTotalTargetProducts > 0 && len(allRows) >= KurlyTotalTargetProducts*2 {
			break
		}
	}

	unique := dedupeRowsByCode(allRows)
	selected := KurlyBalancedLimitRows(unique, KurlyTotalTargetProducts)
	for i := range selected {
		selected[i]["순번"] = strconv.Itoa(i + 1)
	}
	return selected
}

func KurlyDiverseKeywords(ctx context.Context) []string {
	if len(KurlySearchKeywordsOverride) > 0 {
		keywords := append([]string{}, KurlySearchKeywordsOverride...)
		rand.Shuffle(len(keywords), func(i, j int) {
			keywords[i], keywords[j] = keywords[j], keywords[i]
		})
		if KurlyRandomKeywordCount > 0 && len(keywords) > KurlyRandomKeywordCount {
			keywords = keywords[:KurlyRandomKeywordCount]
		}
		return keywords
	}

	keywords := []string{}
	if KurlyCategoryGroupsURL != "" {
		data, _, err := KurlyRequestJSON(ctx, KurlyCategoryGroupsURL, nil)
		if err != nil {
			fmt.Println("[kurly] category-groups 키워드 추출 실패. 기본 키워드를 사용합니다:", err)
		} else {
			keywords = KurlyExtractCategoryKeywords(data)
			if len(keywords) > 0 {
				fmt.Println("[kurly] category-groups 추출 키워드 수:", len(keywords))
			}
		}
	}

	keywords = append(keywords, KurlyDefaultDiverseKeywords...)
	keywords = UniqueKeepOrder(keywords)
	rand.Shuffle(len(keywords), func(i, j int) {
		keywords[i], keywords[j] = keywords[j], keywords[i]
	})
	if KurlyRandomKeywordCount > 0 && len(keywords) > KurlyRandomKeywordCount {
		keywords = keywords[:KurlyRandomKeywordCount]
	}
	return keywords
}

func KurlySearchAPIProducts(ctx context.Context, keyword string, page int, site string) ([]Row, error) {
	apiURL := fmt.Sprintf("https://api.kurly.com/search/v4/sites/%s/normal-search", url.PathEscape(site))
	params := url.Values{}
	params.Set("keyword", keyword)
	params.Set("page", strconv.Itoa(page))

	data, finalURL, err := KurlyRequestJSON(ctx, apiURL, params)
	if err != nil {
		return nil, err
	}
	return KurlyExtractProductRowsFromSearchJSON(data, keyword, strconv.Itoa(page), site, finalURL), nil
}

func KurlySearchHTMLFallback(ctx context.Context, keyword string) ([]Row, error) {
	candidates := []string{
		"https://www.kurly.com/search?sword=" + url.QueryEscape(keyword),
		"https://www.kurly.com/search?keyword=" + url.QueryEscape(keyword),
	}
	rows := []Row{}
	seen := map[string]bool{}
	var lastErr error

	for _, targetURL := range candidates {
		htmlStr, finalURL, err := KurlyRequestHTML(ctx, targetURL, 3, 35*time.Second)
		if err != nil {
			lastErr = err
			continue
		}
		doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlStr))
		if err != nil {
			lastErr = err
			continue
		}
		doc.Find(`a[href*="/goods/"]`).Each(func(i int, a *goquery.Selection) {
			href, _ := a.Attr("href")
			goodsURL := NormalizeURL(href, finalURL)
			productNo := KurlyProductNoFromURL(goodsURL)
			if productNo == "" || seen[productNo] {
				return
			}
			seen[productNo] = true

			name := CleanText(a.Text())
			imageURL := ""
			if img := a.Find("img").First(); img.Length() > 0 {
				if name == "" {
					if alt, ok := img.Attr("alt"); ok {
						name = CleanText(alt)
					}
				}
				for _, attr := range []string{"src", "data-src", "data-original", "data-lazy"} {
					if v, ok := img.Attr(attr); ok && v != "" {
						imageURL = NormalizeURL(v, finalURL)
						break
					}
				}
			}

			rows = append(rows, Row{
				"상품코드":      productNo,
				"상품번호":      productNo,
				"상품명":       name,
				"상품URL":     "https://www.kurly.com/goods/" + productNo,
				"이미지URL_목록": imageURL,
				"수집검색어":     keyword,
				"검색어":       keyword,
				"검색페이지":     "1",
				"페이지":       "1",
				"사이트":       "market",
				"수집방식":      "search_html_fallback",
				"수집URL":     targetURL,
				"수집카테고리":    keyword,
				"목록_이미지URL": imageURL,
				"상품URL_국내":  "https://www.kurly.com/goods/" + productNo,
				"상품URL_raw": goodsURL,
			})
		})
	}

	return rows, lastErr
}

func KurlyExtractProductRowsFromSearchJSON(data any, keyword string, pageNo string, site string, finalURL string) []Row {
	rows := []Row{}
	seen := map[string]bool{}

	productNoKeys := []string{"no", "productNo", "goodsNo", "goodsId", "productId", "contentNo"}
	nameKeys := []string{"name", "productName", "goodsName", "title", "displayName"}
	priceKeys := []string{"discountedPrice", "salesPrice", "salePrice", "price", "sellingPrice", "originalPrice"}
	imageKeys := []string{"thumbnail_image_url", "thumbnailImageUrl", "thumbnailUrl", "imageUrl", "listImageUrl", "mainImageUrl", "image"}
	signalKeys := append(append([]string{}, priceKeys...), append(imageKeys, "isSoldOut", "soldOut", "deliveryTypeNames", "deliveryTypes", "discountRate")...)

	for _, d := range iterJSONMaps(data) {
		productNo := KurlyValueToString(firstJSONValue(d, productNoKeys))
		name := KurlyValueToString(firstJSONValue(d, nameKeys))
		if productNo == "" || name == "" || !regexp.MustCompile(`^\d+$`).MatchString(productNo) {
			continue
		}
		if seen[productNo] || !hasAnyJSONKey(d, signalKeys) {
			continue
		}
		seen[productNo] = true

		discountedPrice := KurlyIntString(firstJSONValue(d, []string{"discountedPrice", "salePrice", "sellingPrice"}))
		salesPrice := KurlyIntString(firstJSONValue(d, []string{"salesPrice", "originalPrice", "price"}))
		currentPrice := discountedPrice
		if currentPrice == "" {
			currentPrice = salesPrice
		}
		imageURL := NormalizeURL(KurlyValueToString(firstJSONValue(d, imageKeys)), "https://www.kurly.com")
		productURL := "https://www.kurly.com/goods/" + productNo

		rows = append(rows, Row{
			"상품코드":       productNo,
			"상품번호":       productNo,
			"상품명":        name,
			"브랜드_목록":     KurlyValueToString(firstJSONValue(d, []string{"brandName", "brand"})),
			"짧은설명_목록":    KurlyValueToString(firstJSONValue(d, []string{"shortdesc", "shortDesc", "shortDescription", "subtitle", "description"})),
			"판매가_목록":     salesPrice,
			"할인가_목록":     discountedPrice,
			"목록_판매가_KRW": currentPrice,
			"목록_정가_KRW":  salesPrice,
			"할인율_목록":     KurlyValueToString(firstJSONValue(d, []string{"discountRate", "discountPercent"})),
			"품절여부_목록":    KurlyValueToString(firstJSONValue(d, []string{"isSoldOut", "soldOut"})),
			"배송유형_목록":    KurlyValueToString(firstJSONValue(d, []string{"deliveryTypeNames", "deliveryTypes", "deliveryTypeName"})),
			"이미지URL_목록":  imageURL,
			"목록_이미지URL":  imageURL,
			"상품URL":      productURL,
			"상품URL_국내":   productURL,
			"상품URL_raw":  productURL,
			"수집검색어":      keyword,
			"검색어":        keyword,
			"검색페이지":      pageNo,
			"페이지":        pageNo,
			"사이트":        site,
			"groupCode":  site,
			"수집방식":       "search_api",
			"수집URL":      finalURL,
			"수집카테고리":     keyword,
		})
	}
	return rows
}

func KurlyCollectDetails(ctx context.Context, listRows []Row, onDetailReady func(Row) error) []Row {
	workRows := listRows
	if KurlyDetailLimit > 0 && KurlyDetailLimit < len(workRows) {
		workRows = workRows[:KurlyDetailLimit]
	}

	fmt.Println("\nKurly 상세 수집 대상 상품 수:", len(workRows))
	resultCh := make(chan Row, len(workRows))
	sem := make(chan struct{}, KurlyDetailConcurrency)
	var wg sync.WaitGroup
	var publishMu sync.Mutex
	var publishErr error

	for _, row := range workRows {
		rowCopy := row
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() {
				<-sem
			}()
			detail := KurlyFetchOneDetail(ctx, rowCopy)
			resultCh <- detail
			if onDetailReady != nil {
				merged := MergeRows([]Row{rowCopy}, []Row{detail})
				if len(merged) > 0 {
					publishMu.Lock()
					if publishErr == nil {
						publishErr = onDetailReady(merged[0])
					}
					publishMu.Unlock()
				}
			}
			fmt.Printf("Kurly 완료: %s / %s / 상세:%s\n", detail["상품코드"], Truncate(detail["상품명_목록"], 40), detail["상세_수집성공"])
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	details := []Row{}
	for row := range resultCh {
		details = append(details, row)
	}
	if publishErr != nil {
		panic(publishErr)
	}
	return details
}

func KurlyFetchOneDetail(ctx context.Context, row Row) Row {
	productNo := FirstNonEmpty(row, []string{"상품코드", "상품번호"})
	productURL := FirstNonEmpty(row, []string{"상품URL", "상품URL_국내"})
	if productURL == "" && productNo != "" {
		productURL = "https://www.kurly.com/goods/" + productNo
	}

	KurlySleepRandom(ctx)
	htmlStr, finalURL, err := KurlyRequestHTML(ctx, productURL, 3, 40*time.Second)
	if err != nil {
		return Row{
			"상품코드":    productNo,
			"상품명_목록":  row["상품명"],
			"상세URL":   productURL,
			"상세_수집성공": "false",
			"상세_수집오류": err.Error(),
		}
	}
	if ok, reason := KurlyClassifyHTML(htmlStr, finalURL); !ok {
		return Row{
			"상품코드":    productNo,
			"상품명_목록":  row["상품명"],
			"상세URL":   finalURL,
			"상세_수집성공": "false",
			"상세_수집오류": reason,
		}
	}
	return KurlyParseDetailPage(htmlStr, productNo, finalURL, row["상품명"])
}

func KurlyParseDetailPage(htmlStr string, productNo string, finalURL string, listName string) Row {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlStr))
	if err != nil {
		return Row{
			"상품코드":    productNo,
			"상품명_목록":  listName,
			"상세URL":   finalURL,
			"상세_수집성공": "false",
			"상세_수집오류": "parse_error: " + err.Error(),
		}
	}

	lines := LinesFromHTML(htmlStr)
	fullText := strings.Join(lines, "\n")
	kv := ExtractKVPairs(doc)
	knownLabels := map[string]bool{
		"배송": true, "판매자": true, "단위 당 가격": true, "포장타입": true, "판매단위": true,
		"중량/용량": true, "알레르기정보": true, "소비기한(또는 유통기한)정보": true, "원산지": true,
	}

	title := CleanText(doc.Find("title").First().Text())
	ogTitle := GetMetaContent(doc, "og:title")
	metaDesc := GetMetaContent(doc, "description")
	h1 := CleanText(doc.Find("h1").First().Text())
	productName := FirstOr(ogTitle, FirstOr(h1, FirstOr(title, listName)))
	price := ExtractPriceKRW(fullText)
	reviewCount := ExtractFirstNumber(RegexFirst(fullText, []string{`후기\s*([\d,]+)\s*건`, `상품\s*후기\s*([\d,]+)`}))
	descriptionText := ExtractSectionText(lines, []string{"상품설명"}, []string{"상품고시정보", "WHY KURLY", "상품 후기"}, DescriptionTextMaxChars)
	if descriptionText == "" {
		descriptionText = metaDesc
	}
	noticeText := ExtractSectionText(lines, []string{"상품고시정보"}, []string{"WHY KURLY", "상품 후기", "고객행복센터"}, FullTextMaxChars)
	allImages, descriptionImages := ExtractImageURLsFromHTML(htmlStr, doc, finalURL, productNo)

	detail := Row{
		"상품코드":                productNo,
		"상품명_목록":              listName,
		"상세URL":               finalURL,
		"상세_상품명":              productName,
		"상세_페이지제목":            title,
		"상세_meta_description": metaDesc,
		"상세_가격_KRW":           price,
		"상세_후기수":              reviewCount,
		"상세_원산지":              FirstOr(KVGet(kv, []string{"원산지"}), RegexFirst(fullText, []string{`원산지\s*:?\s*([^\n]+)`})),
		"상세_배송":               kurlyFindLineValue(lines, "배송", knownLabels, 5),
		"상세_판매자":              FirstOr(kurlyFindLineValue(lines, "판매자", knownLabels, 5), KVGet(kv, []string{"판매자"})),
		"상세_단위당가격":            kurlyFindLineValue(lines, "단위 당 가격", knownLabels, 5),
		"상세_포장타입":             FirstOr(kurlyFindLineValue(lines, "포장타입", knownLabels, 5), KVGet(kv, []string{"포장타입"})),
		"상세_판매단위":             FirstOr(kurlyFindLineValue(lines, "판매단위", knownLabels, 5), KVGet(kv, []string{"판매단위"})),
		"상세_중량용량":             FirstOr(kurlyFindLineValue(lines, "중량/용량", knownLabels, 5), KVGet(kv, []string{"중량", "용량"})),
		"상세_알레르기정보":           FirstOr(kurlyFindLineValue(lines, "알레르기정보", knownLabels, 8), KVGet(kv, []string{"알레르기"})),
		"상세_소비기한_유통기한":        FirstOr(kurlyFindLineValue(lines, "소비기한(또는 유통기한)정보", knownLabels, 8), KVGet(kv, []string{"소비기한", "유통기한"})),
		"상세설명_텍스트":            descriptionText,
		"상품고시_텍스트":            noticeText,
		"상세이미지_개수":            strconv.Itoa(len(allImages)),
		"상세설명이미지_개수":          strconv.Itoa(len(descriptionImages)),
		"상세이미지_URL목록":         strings.Join(allImages, " | "),
		"상세설명이미지_URL목록":       strings.Join(descriptionImages, " | "),
		"상세_본문텍스트_일부":         Truncate(fullText, FullTextMaxChars),
		"상세_NEXT_DATA_존재":     strconv.FormatBool(strings.Contains(htmlStr, `id="__NEXT_DATA__"`)),
		"상세_수집성공":             "true",
		"상세_수집오류":             "",
	}

	for i, imgURL := range allImages {
		if i >= 10 {
			break
		}
		detail[fmt.Sprintf("상세이미지_URL_%d", i+1)] = imgURL
	}
	for i, imgURL := range descriptionImages {
		if i >= 10 {
			break
		}
		detail[fmt.Sprintf("상세설명이미지_URL_%d", i+1)] = imgURL
	}
	for key, value := range kv {
		if value == "" {
			continue
		}
		safeKey := regexp.MustCompile(`[\[\]:*?/\\]`).ReplaceAllString(key, "_")
		detail["고시_"+Truncate(safeKey, 40)] = value
	}

	return detail
}

func KurlyRequestJSON(ctx context.Context, targetURL string, params url.Values) (any, string, error) {
	if params != nil {
		if strings.Contains(targetURL, "?") {
			targetURL += "&" + params.Encode()
		} else {
			targetURL += "?" + params.Encode()
		}
	}
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
		if err != nil {
			return nil, targetURL, err
		}
		applyKurlyHeaders(req, true)
		resp, err := kurlyHTTPClient.Do(req)
		if err != nil {
			lastErr = err
			KurlySleepBackoff(ctx, attempt)
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			KurlySleepBackoff(ctx, attempt)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			lastErr = fmt.Errorf("http_status=%d", resp.StatusCode)
			KurlySleepBackoff(ctx, attempt)
			continue
		}
		var data any
		dec := json.NewDecoder(strings.NewReader(string(body)))
		dec.UseNumber()
		if err := dec.Decode(&data); err != nil {
			lastErr = err
			KurlySleepBackoff(ctx, attempt)
			continue
		}
		return data, resp.Request.URL.String(), nil
	}
	return nil, targetURL, lastErr
}

func KurlyRequestHTML(ctx context.Context, targetURL string, retries int, timeout time.Duration) (string, string, error) {
	var lastErr error
	client := *kurlyHTTPClient
	client.Timeout = timeout
	for attempt := 1; attempt <= retries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
		if err != nil {
			return "", targetURL, err
		}
		applyKurlyHeaders(req, false)
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			KurlySleepBackoff(ctx, attempt)
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			KurlySleepBackoff(ctx, attempt)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			lastErr = fmt.Errorf("http_status=%d", resp.StatusCode)
			KurlySleepBackoff(ctx, attempt)
			continue
		}
		htmlStr := string(body)
		if len(strings.TrimSpace(htmlStr)) < 500 {
			lastErr = fmt.Errorf("html_too_short=%d", len(htmlStr))
			KurlySleepBackoff(ctx, attempt)
			continue
		}
		return htmlStr, resp.Request.URL.String(), nil
	}
	return "", targetURL, lastErr
}

func applyKurlyHeaders(req *http.Request, jsonPreferred bool) {
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept-Language", "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7")
	if jsonPreferred {
		req.Header.Set("Accept", "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	} else {
		req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	}
	req.Header.Set("Origin", "https://www.kurly.com")
	req.Header.Set("Referer", "https://www.kurly.com/")
	req.Header.Set("Connection", "close")
}

func KurlyClassifyHTML(htmlStr string, finalURL string) (bool, string) {
	if len(strings.TrimSpace(htmlStr)) < 500 {
		return false, fmt.Sprintf("HTML too short: %d", len(htmlStr))
	}
	lower := strings.ToLower(htmlStr)
	for _, keyword := range []string{
		"Access Denied", "Forbidden", "Service Unavailable", "ERROR: The request could not be satisfied",
		"The request could not be satisfied", "CloudFront", "잠시만 기다리십시오", "Checking your Browser", "captcha", "CAPTCHA",
	} {
		if strings.Contains(lower, strings.ToLower(keyword)) {
			return false, "blocked_or_error_page: " + keyword
		}
	}
	if strings.Contains(finalURL, "/goods/") {
		ok := false
		for _, signal := range []string{"상품설명", "상품고시정보", "장바구니", "구매하기", "판매자", "포장타입", "중량/용량", "/goods/", "후기"} {
			if strings.Contains(lower, strings.ToLower(signal)) {
				ok = true
				break
			}
		}
		if !ok {
			return false, "product_signal_missing"
		}
	}
	return true, ""
}

func KurlySleepRandom(ctx context.Context) {
	delay := KurlyRequestSleepMin
	if KurlyRequestSleepMax > KurlyRequestSleepMin {
		delay += time.Duration(rand.Int63n(int64(KurlyRequestSleepMax - KurlyRequestSleepMin)))
	}
	if delay <= 0 {
		return
	}
	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
}

func KurlySleepBackoff(ctx context.Context, attempt int) {
	delay := time.Duration(1200*attempt+rand.Intn(700)) * time.Millisecond
	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
}

func KurlyProductNoFromURL(raw string) string {
	re := regexp.MustCompile(`/goods/(\d+)`)
	m := re.FindStringSubmatch(raw)
	if len(m) >= 2 {
		return m[1]
	}
	return ""
}

func KurlyExtractCategoryKeywords(data any) []string {
	nameKeys := map[string]bool{
		"name": true, "title": true, "label": true, "displayName": true,
		"categoryName": true, "groupName": true, "mainCategoryName": true, "subCategoryName": true,
	}
	badWords := map[string]bool{
		"전체": true, "홈": true, "Home": true, "Market": true, "마켓": true,
		"컬리": true, "Kurly": true, "더보기": true, "추천": true, "베스트": true,
		"이벤트": true, "기획전": true,
	}
	candidates := []string{}
	var walk func(any)
	walk = func(obj any) {
		switch v := obj.(type) {
		case map[string]any:
			for key, value := range v {
				if nameKeys[key] {
					name := KurlyNestedName(value)
					if name != "" {
						candidates = append(candidates, name)
					}
				}
				walk(value)
			}
		case []any:
			for _, item := range v {
				walk(item)
			}
		}
	}
	walk(data)

	cleaned := []string{}
	for _, name := range candidates {
		name = CleanText(name)
		if name == "" || badWords[name] {
			continue
		}
		if l := len([]rune(name)); l < 2 || l > 20 {
			continue
		}
		if regexp.MustCompile(`[{}\[\]<>]`).MatchString(name) {
			continue
		}
		cleaned = append(cleaned, name)
	}
	return UniqueKeepOrder(cleaned)
}

func iterJSONMaps(obj any) []map[string]any {
	out := []map[string]any{}
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			out = append(out, x)
			for _, child := range x {
				walk(child)
			}
		case []any:
			for _, child := range x {
				walk(child)
			}
		}
	}
	walk(obj)
	return out
}

func firstJSONValue(data map[string]any, keys []string) any {
	for _, key := range keys {
		if v, ok := data[key]; ok && KurlyValueToString(v) != "" {
			return v
		}
	}
	return nil
}

func hasAnyJSONKey(data map[string]any, keys []string) bool {
	for _, key := range keys {
		if _, ok := data[key]; ok {
			return true
		}
	}
	return false
}

func KurlyNestedName(value any) string {
	switch v := value.(type) {
	case string:
		return CleanText(v)
	case map[string]any:
		for _, key := range []string{"name", "title", "label", "text"} {
			if val, ok := v[key]; ok {
				if name := KurlyValueToString(val); name != "" {
					return name
				}
			}
		}
	}
	return ""
}

func KurlyValueToString(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return CleanText(html.UnescapeString(v))
	case bool:
		return strconv.FormatBool(v)
	case json.Number:
		return v.String()
	case float64:
		if v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case []any:
		parts := []string{}
		for _, item := range v {
			if s := KurlyValueToString(item); s != "" {
				parts = append(parts, s)
			}
		}
		return strings.Join(parts, " | ")
	case map[string]any:
		if name := KurlyNestedName(v); name != "" {
			return name
		}
		b, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		return string(b)
	default:
		return CleanText(fmt.Sprint(v))
	}
}

func KurlyIntString(value any) string {
	text := strings.ReplaceAll(KurlyValueToString(value), ",", "")
	if text == "" {
		return ""
	}
	if n, err := strconv.ParseFloat(text, 64); err == nil {
		if n >= 10 {
			return strconv.FormatInt(int64(n), 10)
		}
	}
	return ExtractPriceKRW(text)
}

func KurlyBalancedLimitRows(rows []Row, total int) []Row {
	if total <= 0 || len(rows) <= total {
		return rows
	}
	grouped := map[string][]Row{}
	keys := []string{}
	for _, row := range rows {
		key := row["수집검색어"]
		if key == "" {
			key = "기타"
		}
		if _, ok := grouped[key]; !ok {
			keys = append(keys, key)
		}
		grouped[key] = append(grouped[key], row)
	}
	sort.Strings(keys)
	selected := []Row{}
	for len(selected) < total && len(keys) > 0 {
		nextKeys := []string{}
		for _, key := range keys {
			bucket := grouped[key]
			if len(bucket) == 0 {
				continue
			}
			selected = append(selected, bucket[0])
			grouped[key] = bucket[1:]
			if len(grouped[key]) > 0 {
				nextKeys = append(nextKeys, key)
			}
			if len(selected) >= total {
				break
			}
		}
		keys = nextKeys
	}
	return selected
}

func dedupeRowsByCode(rows []Row) []Row {
	seen := map[string]bool{}
	out := []Row{}
	for _, row := range rows {
		code := FirstNonEmpty(row, []string{"상품코드", "상품번호"})
		if code == "" || seen[code] {
			continue
		}
		seen[code] = true
		out = append(out, row)
	}
	return out
}

func kurlyFindLineValue(lines []string, label string, knownLabels map[string]bool, searchWindow int) string {
	values := []string{}
	for i, line := range lines {
		if CleanText(line) != label {
			continue
		}
		limit := i + searchWindow
		if limit > len(lines) {
			limit = len(lines)
		}
		for j := i + 1; j < limit; j++ {
			candidate := CleanText(lines[j])
			if candidate == "" {
				continue
			}
			if knownLabels[candidate] || candidate == "상품설명" || candidate == "상세정보" || candidate == "후기" || candidate == "문의" {
				break
			}
			values = append(values, candidate)
			break
		}
	}
	return strings.Join(UniqueKeepOrder(values), " | ")
}
