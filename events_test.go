package main

import (
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
