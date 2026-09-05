package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"
)

func finishAdpickHarvest(parent context.Context, report *adpickCoverage, records []adpickCatalogRecord, searchErr error, publish func(context.Context, []adpickCatalogRecord) error) error {
	if !report.DiscoveryComplete {
		if searchErr != nil {
			return searchErr
		}
		return fmt.Errorf("Adpick verified discovery required")
	}
	if len(records) == 0 {
		return fmt.Errorf("Adpick returned no eligible merchants; existing catalogs retained")
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Minute)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := publish(ctx, records); err != nil {
		return err
	}
	report.PublicationComplete = true
	if searchErr != nil {
		return fmt.Errorf("Adpick verified harvest published with incomplete search: %w", searchErr)
	}
	return nil
}

func validAdpickQuery(q adpickQuery) bool {
	return validAdpickCategory(q.Vertical, q.Category) || (q.Vertical == "diagnostic" && q.Category == "retail_control" && (q.Keyword == "노트북" || q.Keyword == "무선이어폰"))
}

func diagnosticAdpickQueries() []adpickQuery {
	return []adpickQuery{
		{"travel", "stays", "트립닷컴 호텔"}, {"travel", "stays", "야놀자 호텔"},
		{"travel", "stays", "마이리얼트립 호텔"}, {"travel", "stays", "KKday 호텔"},
		{"travel", "stays", "클룩 호텔"}, {"travel", "stays", "트래블로카 호텔"},
		{"travel", "stays", "호텔스닷컴 호텔"}, {"travel", "flights", "항공권"},
		{"travel", "experiences", "제주 투어"}, {"travel", "packages", "패키지 여행"},
		{"services", "design", "크몽 로고 디자인"}, {"services", "development", "크몽 홈페이지 제작"},
		{"services", "marketing", "크몽 마케팅"}, {"services", "writing", "크몽 번역"},
		{"diagnostic", "retail_control", "노트북"}, {"diagnostic", "retail_control", "무선이어폰"},
	}
}

type adpickDiagnosticSample struct {
	Code   string            `json:"cp_code"`
	Name   string            `json:"cp_name"`
	Title  string            `json:"title"`
	Fields map[string]string `json:"field_types"`
}

var adpickDiagnosticFieldPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,47}$`)

func (o *adpickOffer) UnmarshalJSON(data []byte) error {
	type plain adpickOffer
	var p plain
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	*o = adpickOffer(p)
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	o.FieldTypes = map[string]string{}
	// Bounded field names and JSON types show API shape drift without payloads.
	names := []string{}
	for name := range fields {
		if adpickDiagnosticFieldPattern.MatchString(name) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if len(names) > 24 {
		names = names[:24]
	}
	names = append(names, "cp_code", "cp_name", "title", "price", "photo", "commissionlink")
	for _, name := range names {
		b, exists := fields[name]
		kind := "missing"
		if exists {
			kind = "other"
			var v any
			if json.Unmarshal(b, &v) == nil {
				switch v.(type) {
				case string:
					kind = "string"
				case float64:
					kind = "number"
				case nil:
					kind = "null"
				case bool:
					kind = "boolean"
				case map[string]any:
					kind = "object"
				case []any:
					kind = "array"
				}
			}
		}
		o.FieldTypes[name] = kind
	}
	return nil
}

func diagnosticAdpickText(raw, key string, limit int) string {
	if key != "" {
		raw = strings.ReplaceAll(raw, key, "[redacted]")
	}
	value := adpickText(raw, limit)
	lower := strings.ToLower(value)
	if strings.Contains(lower, "http:") || strings.Contains(lower, "https:") || strings.Contains(lower, "api_key") || strings.Contains(lower, "apikey") {
		return "[redacted]"
	}
	return value
}

func (s *adpickQueryCoverage) observe(o adpickOffer, key string) {
	code := o.Code
	if code == "" {
		code = "<missing>"
	} else if !adpickCodePattern.MatchString(code) || code == key {
		code = "<invalid>"
	}
	s.MerchantCodes[code]++
	if len(s.Samples) < 2 {
		fields := map[string]string{}
		for name, kind := range o.FieldTypes {
			if name != key {
				fields[name] = kind
			}
		}
		s.Samples = append(s.Samples, adpickDiagnosticSample{code, diagnosticAdpickText(o.Name, key, 80), diagnosticAdpickText(o.Title, key, 160), fields})
	}
}

func readPreviousAdpickCatalog(ctx context.Context, pub *ClickHouseRawPublisher) ([]adpickCatalogRecord, error) {
	columns := strings.Replace(adpickRecordColumns, "record_type", "row_type AS record_type", 1)
	query, err := pub.adpickGuardedRead("SELECT " + columns + " FROM " + adpickLatestView + " ORDER BY item_key LIMIT 4809 SETTINGS max_threads=1,max_execution_time=15 FORMAT JSONEachRow")
	if err != nil {
		return nil, err
	}
	body, err := pub.queryBody(ctx, query, adpickCatalogLimit)
	if err != nil {
		return nil, fmt.Errorf("Adpick previous catalog unavailable; publication retained")
	}
	d := json.NewDecoder(bytes.NewReader(body))
	rows := []adpickCatalogRecord{}
	for {
		var row adpickCatalogRecord
		err = d.Decode(&row)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("Adpick previous catalog invalid")
		}
		rows = append(rows, row)
	}
	if len(rows) > 4808 {
		return nil, fmt.Errorf("Adpick previous catalog exceeds bound")
	}
	return rows, nil
}

// Search is an observation sample, not a complete merchant inventory. Retain
// older verified offers when a keyword is empty or collection ends early.
func mergeAdpickHarvest(fresh, previous []adpickCatalogRecord) ([]adpickCatalogRecord, int, int, error) {
	if len(fresh) == 0 {
		return nil, 0, 0, fmt.Errorf("Adpick verified discovery required")
	}
	merchants := map[string]adpickCatalogRecord{}
	seen := map[string]bool{}
	merged := append([]adpickCatalogRecord(nil), fresh...)
	for _, row := range fresh {
		seen[row.ItemKey] = true
		if row.RecordType == "merchant" {
			merchants[row.MerchantCode] = row
		}
	}
	retained := []adpickCatalogRecord{}
	for _, row := range previous {
		if row.RecordType != "offer" || seen[row.ItemKey] {
			continue
		}
		merchant, ok := merchants[row.MerchantCode]
		if !ok || merchant.MerchantKey != row.MerchantKey || merchant.Vertical != row.Vertical {
			continue
		}
		stamp, err := time.Parse("2006-01-02 15:04:05.999999999", row.CollectedAt)
		key := fmt.Sprintf("%x", sha256.Sum256([]byte(row.MerchantCode+"\x1f"+row.AffiliateURL)))
		if err != nil || row.Source != "adpick_biz" || !validAdpickCategory(row.Vertical, row.CategorySlug) || row.ItemKey != key || row.Title == "" || safeAdpickURL(row.AffiliateURL, true) != row.AffiliateURL || (row.ImageURL != "" && safeAdpickURL(row.ImageURL, false) != row.ImageURL) {
			return nil, 0, 0, fmt.Errorf("Adpick previous offer validation failed")
		}
		row.CollectedAt = stamp.Format("2006-01-02 15:04:05.000")
		row.CollectRunUUID = merchant.CollectRunUUID
		row.Version = merchant.Version
		seen[row.ItemKey] = true
		retained = append(retained, row)
	}
	sort.Slice(retained, func(i, j int) bool {
		if retained[i].CollectedAt == retained[j].CollectedAt {
			return retained[i].ItemKey < retained[j].ItemKey
		}
		return retained[i].CollectedAt > retained[j].CollectedAt
	})
	room := 4808 - len(merged)
	if room < 0 {
		return nil, 0, 0, fmt.Errorf("Adpick harvest exceeds bound")
	}
	evicted := 0
	if len(retained) > room {
		evicted = len(retained) - room
		retained = retained[:room]
	}
	merged = append(merged, retained...)
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Vertical+merged[i].ItemKey < merged[j].Vertical+merged[j].ItemKey
	})
	return merged, len(retained), evicted, nil
}
