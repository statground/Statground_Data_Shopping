package main

import (
	"errors"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/segmentio/kafka-go"
)

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
