package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	adpickAPIBase        = "https://biz.adpick.co.kr/api/"
	adpickResponseLimit  = 4 << 20
	adpickSearchInterval = 6100 * time.Millisecond
	adpickMaximumQueries = 32
)

type adpickQuery struct {
	Vertical string `json:"vertical"`
	Category string `json:"category"`
	Keyword  string `json:"keyword"`
}

var defaultAdpickQueries = []adpickQuery{
	{"travel", "stays", "제주 호텔"}, {"travel", "stays", "서울 호텔"},
	{"travel", "flights", "항공권"}, {"travel", "experiences", "일본 입장권"},
	{"travel", "experiences", "제주 투어"}, {"travel", "packages", "해외 패키지 여행"},
	{"services", "design", "로고 디자인"}, {"services", "development", "웹사이트 제작"},
	{"services", "marketing", "광고 마케팅"}, {"services", "writing", "번역"},
}

type adpickMerchantIdentity struct{ Key, Vertical string }

// These are exact display-name aliases, never guesses at provider cp_code values.
var adpickMerchantNames = map[string]adpickMerchantIdentity{
	"트립닷컴": {"trip_com", "travel"}, "tripcom": {"trip_com", "travel"},
	"야놀자": {"nol", "travel"}, "nol": {"nol", "travel"}, "nol야놀자": {"nol", "travel"},
	"마이리얼트립": {"myrealtrip", "travel"}, "myrealtrip": {"myrealtrip", "travel"},
	"kkday": {"kkday", "travel"}, "클룩": {"klook", "travel"}, "klook": {"klook", "travel"},
	"트래블로카": {"traveloka", "travel"}, "traveloka": {"traveloka", "travel"},
	"호텔스닷컴": {"hotels_com", "travel"}, "hotelscom": {"hotels_com", "travel"},
	"크몽": {"kmong", "services"}, "kmong": {"kmong", "services"},
}

type adpickMall struct {
	Code  string `json:"cp_code"`
	Name  string `json:"name"`
	Icon  string `json:"icon"`
	Title string `json:"title"`
	Link  string `json:"commissionlink"`
}

type adpickOffer struct {
	Code  string `json:"cp_code"`
	Name  string `json:"cp_name"`
	Title string `json:"title"`
	Price string `json:"price"`
	Photo string `json:"photo"`
	Link  string `json:"commissionlink"`
}

type adpickCatalogRecord struct {
	Source         string  `json:"source"`
	Vertical       string  `json:"vertical"`
	RecordType     string  `json:"record_type"`
	MerchantCode   string  `json:"merchant_code"`
	MerchantKey    string  `json:"merchant_key"`
	MerchantName   string  `json:"merchant_name"`
	ItemKey        string  `json:"item_key"`
	CategorySlug   string  `json:"category_slug"`
	Title          string  `json:"title"`
	Description    string  `json:"description"`
	ImageURL       string  `json:"image_url"`
	AffiliateURL   string  `json:"affiliate_url"`
	PriceText      string  `json:"price_text"`
	PriceKRW       *uint64 `json:"price_krw"`
	SearchKeyword  string  `json:"search_keyword"`
	CollectedAt    string  `json:"collected_at"`
	CollectRunUUID string  `json:"collect_run_uuid"`
	Version        uint64  `json:"version"`
}

type adpickClient struct {
	key        string
	baseURL    string
	client     *http.Client
	interval   time.Duration
	lastSearch time.Time
}

func newAdpickClient(key string) (*adpickClient, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, fmt.Errorf("ADPICK_BIZ_API_KEY is required")
	}
	if len(key) > 512 || strings.ContainsAny(key, "/?#\\\r\n\t ") {
		return nil, fmt.Errorf("invalid ADPICK_BIZ_API_KEY")
	}
	return &adpickClient{key: key, baseURL: adpickAPIBase, interval: adpickSearchInterval,
		client: &http.Client{Timeout: 25 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (c *adpickClient) request(ctx context.Context, endpoint string, params url.Values, target any) error {
	if endpoint != "malls" && endpoint != "search" {
		return fmt.Errorf("unsupported Adpick read endpoint")
	}
	if endpoint == "search" && !c.lastSearch.IsZero() {
		delay := time.Until(c.lastSearch.Add(c.interval))
		if delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	requestURL := c.baseURL + url.PathEscape(c.key) + "/" + endpoint + "?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return fmt.Errorf("Adpick request configuration rejected")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Statground-Data-Shopping/1.0")
	if endpoint == "search" {
		c.lastSearch = time.Now()
	}
	resp, err := c.client.Do(req)
	// The API credential is in the URL: never propagate transport errors or response bodies.
	if err != nil {
		return fmt.Errorf("Adpick %s transport failure", endpoint)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Adpick %s HTTP status %d", endpoint, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, adpickResponseLimit+1))
	if err != nil || len(body) > adpickResponseLimit {
		return fmt.Errorf("Adpick %s response unavailable or oversized", endpoint)
	}
	var envelope struct {
		Success bool            `json:"success"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || !envelope.Success || len(envelope.Data) == 0 || envelope.Data[0] != '[' {
		return fmt.Errorf("Adpick %s unsuccessful response", endpoint)
	}
	if err := json.Unmarshal(envelope.Data, target); err != nil {
		return fmt.Errorf("Adpick %s invalid data", endpoint)
	}
	return nil
}

func adpickQueriesFromEnv() ([]adpickQuery, error) {
	raw := envString("ADPICK_SEARCH_QUERIES_JSON", "")
	queries := append([]adpickQuery(nil), defaultAdpickQueries...)
	if raw != "" {
		if len(raw) > 16*1024 || json.Unmarshal([]byte(raw), &queries) != nil {
			return nil, fmt.Errorf("invalid ADPICK_SEARCH_QUERIES_JSON")
		}
	}
	if len(queries) < 1 || len(queries) > adpickMaximumQueries {
		return nil, fmt.Errorf("Adpick query count must be between 1 and %d", adpickMaximumQueries)
	}
	seen := map[string]bool{}
	for i := range queries {
		q := &queries[i]
		q.Keyword = strings.TrimSpace(q.Keyword)
		if !validAdpickCategory(q.Vertical, q.Category) || q.Keyword == "" || len([]rune(q.Keyword)) > 100 || strings.ContainsAny(q.Keyword, "\r\n\t") {
			return nil, fmt.Errorf("invalid Adpick query at index %d", i)
		}
		id := q.Vertical + ":" + q.Category + ":" + q.Keyword
		if seen[id] {
			return nil, fmt.Errorf("duplicate Adpick query at index %d", i)
		}
		seen[id] = true
	}
	return queries, nil
}

func validAdpickCategory(vertical, category string) bool {
	switch vertical {
	case "travel":
		return category == "stays" || category == "flights" || category == "experiences" || category == "packages"
	case "services":
		return category == "design" || category == "development" || category == "marketing" || category == "writing"
	default:
		return false
	}
}

func normalizedAdpickName(name string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return unicode.ToLower(r)
		}
		return -1
	}, strings.TrimSpace(name))
}

var adpickCodePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)
var adpickPricePattern = regexp.MustCompile(`^(?:[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+)(?:\s*원)?$`)

func adpickPrice(raw string) *uint64 {
	raw = strings.TrimSpace(raw)
	if !adpickPricePattern.MatchString(raw) {
		return nil
	}
	raw = strings.TrimSpace(strings.TrimSuffix(raw, "원"))
	n, err := strconv.ParseUint(strings.ReplaceAll(raw, ",", ""), 10, 64)
	if err != nil || n == 0 {
		return nil
	}
	return &n
}

func adpickText(raw string, maximum int) string {
	value := strings.Join(strings.Fields(html.UnescapeString(raw)), " ")
	runes := []rune(value)
	if len(runes) > maximum {
		return string(runes[:maximum])
	}
	return value
}

func safeAdpickURL(raw string, affiliate bool) string {
	if len(raw) > 2048 || strings.TrimSpace(raw) != raw {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Port() != "" || u.Fragment != "" {
		return ""
	}
	host := strings.ToLower(u.Hostname())
	if net.ParseIP(host) != nil || !strings.Contains(host, ".") || strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".localhost") {
		return ""
	}
	if host == "biz.adpick.co.kr" || strings.HasPrefix(u.Path, "/api/") {
		return ""
	}
	for key := range u.Query() {
		normalized := strings.ToLower(key)
		if strings.Contains(normalized, "key") || strings.Contains(normalized, "token") || strings.Contains(normalized, "secret") {
			return ""
		}
	}
	if affiliate {
		if host != "bitl.bz" && host != "deg.kr" && host != "adpick.co.kr" && host != "www.adpick.co.kr" {
			return ""
		}
	}
	return u.String()
}

func collectAdpickCatalog(ctx context.Context, client *adpickClient, queries []adpickQuery, limit int, runUUID string, now time.Time) ([]adpickCatalogRecord, error) {
	if limit < 1 || limit > 20 {
		return nil, fmt.Errorf("ADPICK_SEARCH_LIMIT must be between 1 and 20")
	}
	if !rawOutboxUUIDPattern.MatchString(runUUID) || len(queries) == 0 || len(queries) > adpickMaximumQueries {
		return nil, fmt.Errorf("invalid Adpick collection bounds")
	}
	var malls []adpickMall
	if err := client.request(ctx, "malls", url.Values{"rewardmalls": {"true"}, "order": {"popular"}}, &malls); err != nil {
		return nil, err
	}
	if len(malls) > 1000 {
		return nil, fmt.Errorf("Adpick mall list exceeds bounded limit")
	}
	requested := map[string]bool{}
	for _, q := range queries {
		if !validAdpickCategory(q.Vertical, q.Category) || strings.TrimSpace(q.Keyword) == "" {
			return nil, fmt.Errorf("invalid Adpick category")
		}
		requested[q.Vertical] = true
	}
	records := []adpickCatalogRecord{}
	byCode := map[string]adpickCatalogRecord{}
	seenMerchant := map[string]bool{}
	for _, mall := range malls {
		identity, eligible := adpickMerchantNames[normalizedAdpickName(mall.Name)]
		if !eligible || !requested[identity.Vertical] {
			continue
		}
		if !adpickCodePattern.MatchString(mall.Code) || strings.EqualFold(mall.Code, "COUPANG") {
			return nil, fmt.Errorf("Adpick merchant identity rejected")
		}
		if _, exists := byCode[mall.Code]; exists || seenMerchant[identity.Key] {
			return nil, fmt.Errorf("Adpick duplicate merchant identity")
		}
		seenMerchant[identity.Key] = true
		record := adpickCatalogRecord{Source: "adpick_biz", Vertical: identity.Vertical, RecordType: "merchant", MerchantCode: mall.Code,
			MerchantKey: identity.Key, MerchantName: adpickText(mall.Name, 100), ItemKey: fmt.Sprintf("%x", sha256.Sum256([]byte("merchant\x1f"+mall.Code))),
			Title: adpickText(mall.Name, 100), Description: adpickText(mall.Title, 500), ImageURL: safeAdpickURL(mall.Icon, false),
			AffiliateURL: safeAdpickURL(mall.Link, true), CollectedAt: FormatCHDateTime64Millis(now), CollectRunUUID: runUUID, Version: uint64(now.UnixMilli())}
		if mall.Link != "" && record.AffiliateURL == "" {
			return nil, fmt.Errorf("Adpick merchant link rejected")
		}
		byCode[mall.Code] = record
		records = append(records, record)
	}
	seenOffers := map[string]bool{}
	for _, q := range queries {
		var offers []adpickOffer
		if err := client.request(ctx, "search", url.Values{"q": {q.Keyword}, "limit": {strconv.Itoa(limit)}}, &offers); err != nil {
			return nil, err
		}
		if len(offers) > limit {
			return nil, fmt.Errorf("Adpick search exceeds requested limit")
		}
		for _, offer := range offers {
			merchant, eligible := byCode[offer.Code]
			if !eligible || merchant.Vertical != q.Vertical {
				continue
			}
			if offer.Name != "" {
				identity, found := adpickMerchantNames[normalizedAdpickName(offer.Name)]
				if !found || identity.Key != merchant.MerchantKey {
					return nil, fmt.Errorf("Adpick offer merchant mismatch")
				}
			}
			title, link := adpickText(offer.Title, 300), safeAdpickURL(offer.Link, true)
			if title == "" || link == "" {
				continue
			}
			// This is an affiliate-link identity, not an original product ID or comparable booking quote.
			key := fmt.Sprintf("%x", sha256.Sum256([]byte(offer.Code+"\x1f"+link)))
			if seenOffers[key] {
				continue
			}
			seenOffers[key] = true
			record := merchant
			record.RecordType, record.ItemKey, record.CategorySlug = "offer", key, q.Category
			record.Title, record.Description, record.ImageURL, record.AffiliateURL = title, "", safeAdpickURL(offer.Photo, false), link
			record.PriceText, record.PriceKRW, record.SearchKeyword = adpickText(offer.Price, 100), adpickPrice(offer.Price), q.Keyword
			records = append(records, record)
		}
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].Vertical+records[i].ItemKey < records[j].Vertical+records[j].ItemKey
	})
	return records, nil
}
