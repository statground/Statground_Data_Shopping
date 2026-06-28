package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const GmarketRawEventType = "shopping.gmarket.raw.v1"

func PublishGmarketRowsFromEnv(rows []Row) error {
	pub, err := NewKafkaPublisherFromEnv()
	if err != nil {
		return err
	}
	ctx := context.Background()
	if pub.Cfg.PreflightRequired {
		if err := pub.Validate(ctx); err != nil {
			return err
		}
	}

	runUUID := envString("GMARKET_RUN_UUID", "")
	if runUUID == "" {
		runUUID = NewUUIDv7()
	}
	now := NowKST()
	events := make([]KafkaEvent, 0, len(rows))
	for _, row := range rows {
		productCode := CleanText(row["상품코드"])
		if productCode == "" {
			continue
		}
		payload := BuildGmarketPayload(row, runUUID, now)
		ev, err := pub.NewEvent(GmarketRawEventType, "", bestSourceURL(row), FormatCHDateTime64Millis(now), payload)
		if err != nil {
			return err
		}
		events = append(events, ev)
	}
	if len(events) == 0 {
		return fmt.Errorf("no publishable Gmarket rows with 상품코드")
	}

	publishCtx, cancel := context.WithTimeout(ctx, pub.Cfg.PublishTimeout)
	defer cancel()
	if err := pub.Publish(publishCtx, events); err != nil {
		return err
	}
	fmt.Printf("[kafka] published gmarket raw events=%d topic=%s run_uuid=%s\n", len(events), pub.Cfg.Topic, runUUID)
	return nil
}

func BuildGmarketPayload(row Row, runUUID string, collectedAt time.Time) map[string]any {
	collectedAtText := FormatCHDateTime64Millis(collectedAt)
	version := uint64(collectedAt.UnixMilli())
	productCode := CleanText(row["상품코드"])

	return map[string]any{
		"uuid":                    NewUUIDv7(),
		"provider":                "gmarket",
		"product_code":            productCode,
		"version":                 version,
		"created_at":              collectedAtText,
		"created_log":             "github_actions_gmarket_collect",
		"updated_at":              collectedAtText,
		"updated_log":             "github_actions_gmarket_collect",
		"collected_at":            collectedAtText,
		"collect_run_uuid":        runUUID,
		"collect_mode":            CollectMode,
		"list_source_mode":        ListSourceMode,
		"detail_source_mode":      DetailSourceMode,
		"source_category":         CleanText(row["수집카테고리"]),
		"group_code":              CleanText(row["groupCode"]),
		"search_keyword":          CleanText(row["검색어"]),
		"search_page":             parseUInt(row["페이지"]),
		"source_url":              CleanText(row["수집URL"]),
		"product_name":            FirstNonEmpty(row, []string{"국내_상품명", "글로벌_상품명", "상품명", "상품명_목록"}),
		"domestic_url":            FirstNonEmpty(row, []string{"국내_상세URL", "상품URL_국내"}),
		"global_url":              FirstNonEmpty(row, []string{"글로벌_상세URL", "상품URL_글로벌"}),
		"raw_url":                 CleanText(row["상품URL_raw"]),
		"image_url":               CleanText(row["목록_이미지URL"]),
		"list_price_krw":          parseNullableUInt(row["목록_판매가_KRW"]),
		"list_original_price_krw": parseNullableUInt(row["목록_정가_KRW"]),
		"domestic_price_krw":      parseNullableUInt(row["국내_가격_KRW"]),
		"global_price_krw":        parseNullableUInt(row["글로벌_가격_KRW"]),
		"global_price_usd":        parseNullableFloat(row["글로벌_가격_USD"]),
		"brand":                   FirstNonEmpty(row, []string{"국내_브랜드", "글로벌_브랜드"}),
		"seller":                  FirstNonEmpty(row, []string{"국내_판매자", "글로벌_판매자"}),
		"category_path":           FirstNonEmpty(row, []string{"국내_카테고리", "글로벌_카테고리", "수집카테고리"}),
		"review_count":            parseNullableUInt(FirstNonEmpty(row, []string{"국내_리뷰수", "글로벌_리뷰수"})),
		"order_count":             parseNullableUInt(FirstNonEmpty(row, []string{"국내_주문수", "글로벌_주문수"})),
		"description_text":        FirstNonEmpty(row, []string{"국내_상세설명_텍스트", "글로벌_상세설명_텍스트"}),
		"description_image_urls":  mergedPipeLists(row["국내_상세설명_이미지URL목록"], row["글로벌_상세설명_이미지URL목록"]),
		"all_image_urls":          collectImageURLs(row),
		"korean_collect_success":  strings.EqualFold(row["국내_수집성공"], "true"),
		"global_collect_success":  strings.EqualFold(row["글로벌_수집성공"], "true"),
		"raw_row":                 map[string]string(row),
	}
}

func bestSourceURL(row Row) string {
	return FirstNonEmpty(row, []string{"상품URL_국내", "상품URL_글로벌", "수집URL", "상품URL_raw"})
}

func parseNullableUInt(raw string) any {
	raw = strings.ReplaceAll(CleanText(raw), ",", "")
	if raw == "" {
		return nil
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return nil
	}
	return n
}

func parseUInt(raw string) uint64 {
	raw = strings.ReplaceAll(CleanText(raw), ",", "")
	if raw == "" {
		return 0
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func parseNullableFloat(raw string) any {
	raw = strings.ReplaceAll(CleanText(raw), ",", "")
	if raw == "" {
		return nil
	}
	n, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return nil
	}
	return n
}

func mergedPipeLists(values ...string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, value := range values {
		for _, part := range strings.Split(value, "|") {
			part = CleanText(part)
			if part == "" || seen[part] {
				continue
			}
			seen[part] = true
			out = append(out, part)
		}
	}
	return out
}

func collectImageURLs(row Row) []string {
	values := []string{row["목록_이미지URL"], row["국내_상세설명_이미지URL목록"], row["글로벌_상세설명_이미지URL목록"]}
	for i := 1; i <= 10; i++ {
		values = append(values, row[fmt.Sprintf("국내_상세이미지_URL_%d", i)])
		values = append(values, row[fmt.Sprintf("글로벌_상세이미지_URL_%d", i)])
	}
	return mergedPipeLists(values...)
}
