package main

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type fakeKafkaWriter struct {
	err    error
	writes *[][]string
}

func (w *fakeKafkaWriter) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	keys := make([]string, 0, len(messages))
	for _, msg := range messages {
		keys = append(keys, string(msg.Key))
	}
	*w.writes = append(*w.writes, keys)
	return w.err
}

func (w *fakeKafkaWriter) Close() error {
	return nil
}

func TestBuildGmarketPayloadUsesProductCodeAndLatestFields(t *testing.T) {
	CollectMode = "random_best_categories"
	ListSourceMode = "gsearch_ajax"
	DetailSourceMode = "both"

	payload := BuildGmarketPayload(Row{
		"상품코드":             "1234567890",
		"상품명":              "목록 이름",
		"국내_상품명":           "국내 이름",
		"목록_판매가_KRW":       "12,300",
		"국내_가격_KRW":        "11900",
		"국내_상세설명_이미지URL목록": "https://example.com/a.jpg | https://example.com/b.jpg",
	}, "018f0000-0000-7000-8000-000000000000", NowKST())

	if payload["provider"] != "gmarket" {
		t.Fatalf("provider = %v", payload["provider"])
	}
	if payload["product_code"] != "1234567890" {
		t.Fatalf("product_code = %v", payload["product_code"])
	}
	if payload["product_name"] != "국내 이름" {
		t.Fatalf("product_name = %v", payload["product_name"])
	}
	if payload["list_price_krw"] != uint64(12300) {
		t.Fatalf("list_price_krw = %v", payload["list_price_krw"])
	}
	if got := len(payload["description_image_urls"].([]string)); got != 2 {
		t.Fatalf("description_image_urls len = %d", got)
	}
}

func TestBuildKurlyPayloadUsesKurlyFields(t *testing.T) {
	KurlyCollectMode = "search_api_keywords"

	payload := BuildKurlyPayload(Row{
		"상품코드":          "10012345",
		"상품명":           "목록 이름",
		"상세_상품명":        "상세 이름",
		"할인가_목록":        "7900",
		"판매가_목록":        "9900",
		"상세_배송":         "샛별배송",
		"상세_판매자":        "컬리",
		"상세설명이미지_URL목록": "https://img.example/a.jpg | https://img.example/b.jpg",
		"상세_수집성공":       "true",
	}, "018f0000-0000-7000-8000-000000000000", NowKST())

	if payload["provider"] != "kurly" {
		t.Fatalf("provider = %v", payload["provider"])
	}
	if payload["product_code"] != "10012345" {
		t.Fatalf("product_code = %v", payload["product_code"])
	}
	if payload["product_name"] != "상세 이름" {
		t.Fatalf("product_name = %v", payload["product_name"])
	}
	if payload["list_price_krw"] != uint64(7900) {
		t.Fatalf("list_price_krw = %v", payload["list_price_krw"])
	}
	if payload["list_original_price_krw"] != uint64(9900) {
		t.Fatalf("list_original_price_krw = %v", payload["list_original_price_krw"])
	}
	if payload["detail_collect_success"] != true {
		t.Fatalf("detail_collect_success = %v", payload["detail_collect_success"])
	}
	if got := len(payload["description_image_urls"].([]string)); got != 2 {
		t.Fatalf("description_image_urls len = %d", got)
	}
}

func TestKurlyExtractProductRowsFromSearchJSON(t *testing.T) {
	data := map[string]any{
		"data": []any{
			map[string]any{
				"no":                "10012345",
				"name":              "친환경 사과",
				"discountedPrice":   json.Number("7900"),
				"salesPrice":        json.Number("9900"),
				"thumbnailImageUrl": "//img.example/apple.jpg",
				"brandName":         "농장",
				"deliveryTypeNames": []any{"샛별배송"},
			},
			map[string]any{
				"no":   "category-1",
				"name": "카테고리",
			},
		},
	}

	rows := KurlyExtractProductRowsFromSearchJSON(data, "사과", "1", "market", "https://api.kurly.com/search")
	if len(rows) != 1 {
		t.Fatalf("row count = %d, want 1: %#v", len(rows), rows)
	}
	row := rows[0]
	if row["상품코드"] != "10012345" || row["상품명"] != "친환경 사과" {
		t.Fatalf("unexpected row identity: %#v", row)
	}
	if row["목록_판매가_KRW"] != "7900" || row["목록_정가_KRW"] != "9900" {
		t.Fatalf("unexpected price fields: %#v", row)
	}
	if row["이미지URL_목록"] != "https://img.example/apple.jpg" {
		t.Fatalf("unexpected image URL: %s", row["이미지URL_목록"])
	}
}

func TestNormalizeInsightCategoryUsesStatgroundCanonicalBuckets(t *testing.T) {
	cases := []struct {
		name          string
		provider      string
		raw           string
		categoryPath  string
		searchKeyword string
		productName   string
		want          string
	}{
		{
			name:        "gmarket food raw category",
			provider:    "gmarket",
			raw:         "가공식품",
			productName: "컵라면 12개입",
			want:        "식품",
		},
		{
			name:          "kurly fresh keyword",
			provider:      "kurly",
			searchKeyword: "양배추",
			productName:   "국산 양배추 슬라이스",
			want:          "식품",
		},
		{
			name:        "mixed gmarket hobby pet category with pet product",
			provider:    "gmarket",
			raw:         "취미/문구/펫",
			productName: "강아지 사료 2kg",
			want:        "유아/반려",
		},
		{
			name:        "mixed gmarket hobby pet category with stationery product",
			provider:    "gmarket",
			raw:         "취미/문구/펫",
			productName: "노트 필기구 세트",
			want:        "도서/취미/문구",
		},
		{
			name:        "gmarket coupon category",
			provider:    "gmarket",
			raw:         "e쿠폰",
			productName: "커피 교환권",
			want:        "여행/e쿠폰",
		},
		{
			name:        "gmarket sports health prefers sports bucket",
			provider:    "gmarket",
			raw:         "스포츠/건강",
			productName: "등산 보호대",
			want:        "스포츠/레저",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeInsightCategory(tc.provider, tc.raw, tc.categoryPath, tc.searchKeyword, tc.productName)
			if got != tc.want {
				t.Fatalf("normalizeInsightCategory() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSelectedSearchKeywordsHonorsLimitAndAllMode(t *testing.T) {
	oldKeywords := SearchKeywords
	defer func() {
		SearchKeywords = oldKeywords
	}()

	SearchKeywords = []string{"노트북", "생수", "키보드", "샴푸"}
	rand.Seed(1)

	limited := selectedSearchKeywords(2)
	if len(limited) != 2 {
		t.Fatalf("limited keyword count = %d, want 2: %#v", len(limited), limited)
	}

	all := selectedSearchKeywords(0)
	if len(all) != len(SearchKeywords) {
		t.Fatalf("all keyword count = %d, want %d: %#v", len(all), len(SearchKeywords), all)
	}
}

func TestValidateKafkaAdvertisedLeadersRejectsRetiredEndpoint(t *testing.T) {
	err := validateKafkaAdvertisedLeaders([]kafka.Partition{
		{
			Topic: "shopping.events",
			ID:    11,
			Leader: kafka.Broker{
				Host: "180.66.240.243",
				Port: 50004,
			},
		},
	}, []string{"211.178.126.139:50004"}, []string{"180.66.240.243:50004"}, "kafka broker metadata")

	if err == nil {
		t.Fatal("expected retired advertised broker error")
	}
	if !strings.Contains(err.Error(), "retired broker endpoint 180.66.240.243:50004") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestApplyShardFilterPartitionsRowsByProductCode(t *testing.T) {
	oldCount := ShardCount
	oldIndex := ShardIndex
	defer func() {
		ShardCount = oldCount
		ShardIndex = oldIndex
	}()

	rows := []Row{
		{"상품코드": "1001"},
		{"상품코드": "1002"},
		{"상품코드": "1003"},
		{"상품코드": "1004"},
		{"상품코드": "1005"},
	}

	seen := map[string]bool{}
	for shard := 0; shard < 3; shard++ {
		ShardCount = 3
		ShardIndex = shard
		for _, row := range ApplyShardFilter(rows) {
			code := row["상품코드"]
			if seen[code] {
				t.Fatalf("product code assigned to multiple shards: %s", code)
			}
			seen[code] = true
			if row["shard_count"] != "3" || row["shard_index"] != strconv.Itoa(shard) {
				t.Fatalf("missing shard metadata for %s: %+v", code, row)
			}
		}
	}

	if len(seen) != len(rows) {
		t.Fatalf("partitioned rows = %d, want %d", len(seen), len(rows))
	}
}

func TestFixedPartitionBalancerUsesRequestedPartition(t *testing.T) {
	balancer := fixedPartitionBalancer{partition: 7}
	if got := balancer.Balance(kafka.Message{}, 0, 3, 7, 9); got != 7 {
		t.Fatalf("fixed partition = %d, want 7", got)
	}
	if got := balancer.Balance(kafka.Message{}, 1, 2, 3); got != 1 {
		t.Fatalf("fallback partition = %d, want first available partition", got)
	}
}

func TestSplitIntCSVSortsAndDeduplicates(t *testing.T) {
	got := splitIntCSV("5, 1, bad, -1, 5, 3")
	want := []int{1, 3, 5}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("splitIntCSV[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestShouldUsePartitionFallbackForLeaderMetadataErrors(t *testing.T) {
	err := errors.New("[6] Not Leader For Partition: metadata are likely out of date")
	if !shouldUsePartitionFallback(err) {
		t.Fatal("expected leader metadata error to use fixed partition fallback")
	}
	if shouldUsePartitionFallback(errors.New("connection reset by peer")) {
		t.Fatal("network errors should use normal retry path")
	}
}

func TestWriteMessagesWithRetryRetriesWriterDeadline(t *testing.T) {
	pub := KafkaPublisher{Cfg: KafkaConfig{
		WriteAttempts:   2,
		WriteBackoffMin: time.Millisecond,
		WriteBackoffMax: time.Millisecond,
	}}
	messages := []kafka.Message{{Key: []byte("a"), Value: []byte("1")}}
	errs := []error{kafka.WriteErrors{context.DeadlineExceeded}, nil}
	writes := make([][]string, 0, len(errs))
	attempt := 0
	sleeps := 0

	err := pub.writeMessagesWithRetry(context.Background(), messages, func() kafkaMessageWriter {
		if attempt >= len(errs) {
			t.Fatalf("unexpected writer attempt %d", attempt+1)
		}
		err := errs[attempt]
		attempt++
		return &fakeKafkaWriter{err: err, writes: &writes}
	}, func(context.Context, time.Duration) error {
		sleeps++
		return nil
	})
	if err != nil {
		t.Fatalf("writeMessagesWithRetry returned error: %v", err)
	}
	if attempt != 2 {
		t.Fatalf("attempt count = %d, want 2", attempt)
	}
	if sleeps != 1 {
		t.Fatalf("sleep count = %d, want 1", sleeps)
	}
	if len(writes) != 2 || len(writes[0]) != 1 || writes[0][0] != "a" || len(writes[1]) != 1 || writes[1][0] != "a" {
		t.Fatalf("writes mismatch: %#v", writes)
	}
}
