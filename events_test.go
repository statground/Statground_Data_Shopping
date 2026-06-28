package main

import "testing"

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
