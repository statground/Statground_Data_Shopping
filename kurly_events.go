package main

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const KurlyRawEventType = "shopping.kurly.raw.v1"

type KurlyRowPublisher struct {
	pub     *KafkaPublisher
	runUUID string
}

func NewKurlyRowPublisherFromEnv() (*KurlyRowPublisher, error) {
	pub, err := NewKafkaPublisherFromEnv()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	if pub.Cfg.PreflightRequired {
		if err := pub.Validate(ctx); err != nil {
			return nil, err
		}
	}

	runUUID := envString("KURLY_RUN_UUID", "")
	if runUUID == "" {
		runUUID = envString("GMARKET_RUN_UUID", "")
	}
	if runUUID == "" {
		runUUID = NewUUIDv7()
	}
	return &KurlyRowPublisher{pub: pub, runUUID: runUUID}, nil
}

func PublishKurlyRowsFromEnv(rows []Row) error {
	publisher, err := NewKurlyPublisherFromEnv()
	if err != nil {
		return err
	}
	return publisher.Publish(rows)
}

func (p *KurlyRowPublisher) Publish(rows []Row) error {
	if p == nil || p.pub == nil {
		return fmt.Errorf("kurly row publisher is not initialized")
	}
	now := NowKST()
	events := make([]KafkaEvent, 0, len(rows))
	for _, row := range rows {
		productCode := FirstNonEmpty(row, []string{"상품코드", "상품번호"})
		if productCode == "" {
			continue
		}
		payload := BuildKurlyPayload(row, p.runUUID, now)
		ev, err := p.pub.NewEvent(KurlyRawEventType, "", FirstNonEmpty(row, []string{"상세URL", "상품URL", "상품URL_국내", "수집URL"}), FormatCHDateTime64Millis(now), payload)
		if err != nil {
			return err
		}
		events = append(events, ev)
	}
	if len(events) == 0 {
		return fmt.Errorf("no publishable Kurly rows with 상품코드")
	}

	publishCtx, cancel := context.WithTimeout(context.Background(), p.pub.Cfg.PublishTimeout)
	defer cancel()
	if err := p.pub.Publish(publishCtx, events); err != nil {
		return err
	}
	fmt.Printf("[kafka] published kurly raw events=%d topic=%s run_uuid=%s\n", len(events), p.pub.Cfg.Topic, p.runUUID)
	return nil
}

func BuildKurlyPayload(row Row, runUUID string, collectedAt time.Time) map[string]any {
	collectedAtText := FormatCHDateTime64Millis(collectedAt)
	version := uint64(collectedAt.UnixMilli())
	productCode := FirstNonEmpty(row, []string{"상품코드", "상품번호"})
	productURL := FirstNonEmpty(row, []string{"상세URL", "상품URL", "상품URL_국내"})
	listPrice := FirstNonEmpty(row, []string{"할인가_목록", "목록_판매가_KRW", "상세_가격_KRW", "판매가_목록"})
	originalPrice := FirstNonEmpty(row, []string{"판매가_목록", "목록_정가_KRW"})
	if originalPrice == listPrice {
		originalPrice = ""
	}
	detailSuccess := strings.EqualFold(row["상세_수집성공"], "true")

	return map[string]any{
		"uuid":                       NewUUIDv7(),
		"provider":                   "kurly",
		"product_code":               productCode,
		"version":                    version,
		"created_at":                 collectedAtText,
		"created_log":                "github_actions_kurly_collect",
		"updated_at":                 collectedAtText,
		"updated_log":                "github_actions_kurly_collect",
		"collected_at":               collectedAtText,
		"collect_run_uuid":           runUUID,
		"collect_mode":               KurlyCollectMode,
		"list_source_mode":           FirstNonEmpty(row, []string{"수집방식", "list_source_mode"}),
		"detail_source_mode":         "kurly_html",
		"source_category":            FirstNonEmpty(row, []string{"수집카테고리", "수집검색어"}),
		"group_code":                 FirstNonEmpty(row, []string{"groupCode", "사이트"}),
		"search_keyword":             FirstNonEmpty(row, []string{"수집검색어", "검색어"}),
		"search_page":                parseUInt(FirstNonEmpty(row, []string{"검색페이지", "페이지"})),
		"site":                       row["사이트"],
		"source_url":                 FirstNonEmpty(row, []string{"수집URL", "상품URL"}),
		"product_url":                productURL,
		"raw_url":                    FirstNonEmpty(row, []string{"상품URL_raw", "상품URL"}),
		"image_url":                  FirstNonEmpty(row, []string{"이미지URL_목록", "목록_이미지URL"}),
		"product_name":               FirstNonEmpty(row, []string{"상세_상품명", "상품명", "상품명_목록"}),
		"brand":                      row["브랜드_목록"],
		"short_description":          row["짧은설명_목록"],
		"list_price_krw":             parseNullableUInt(listPrice),
		"list_original_price_krw":    parseNullableUInt(originalPrice),
		"discount_rate_text":         row["할인율_목록"],
		"sold_out_text":              row["품절여부_목록"],
		"delivery_type":              FirstNonEmpty(row, []string{"상세_배송", "배송유형_목록"}),
		"detail_price_krw":           parseNullableUInt(FirstNonEmpty(row, []string{"상세_가격_KRW", "목록_판매가_KRW"})),
		"seller":                     row["상세_판매자"],
		"category_path":              FirstNonEmpty(row, []string{"수집카테고리", "수집검색어"}),
		"review_count":               parseNullableUInt(row["상세_후기수"]),
		"origin":                     row["상세_원산지"],
		"unit_price":                 row["상세_단위당가격"],
		"package_type":               row["상세_포장타입"],
		"sales_unit":                 row["상세_판매단위"],
		"weight_volume":              row["상세_중량용량"],
		"allergy_info":               row["상세_알레르기정보"],
		"expiration_info":            row["상세_소비기한_유통기한"],
		"description_text":           row["상세설명_텍스트"],
		"notice_text":                row["상품고시_텍스트"],
		"description_image_urls":     mergedPipeLists(row["상세설명이미지_URL목록"]),
		"all_image_urls":             collectKurlyImageURLs(row),
		"detail_collect_success":     detailSuccess,
		"detail_collect_error":       row["상세_수집오류"],
		"detail_next_data_available": strings.EqualFold(row["상세_NEXT_DATA_존재"], "true"),
		"raw_row":                    map[string]string(row),
	}
}

func collectKurlyImageURLs(row Row) []string {
	values := []string{row["이미지URL_목록"], row["목록_이미지URL"], row["상세이미지_URL목록"], row["상세설명이미지_URL목록"]}
	for i := 1; i <= 10; i++ {
		values = append(values, row[fmt.Sprintf("상세이미지_URL_%d", i)])
		values = append(values, row[fmt.Sprintf("상세설명이미지_URL_%d", i)])
	}
	return mergedPipeLists(values...)
}
