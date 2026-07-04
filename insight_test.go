package main

import "testing"

func TestTopInsightProductsInterleavesProviders(t *testing.T) {
	products := []insightProduct{}
	for i := 0; i < 20; i++ {
		products = append(products, insightProduct{
			Provider:    "kurly",
			ProductCode: "k",
			ProductName: "kurly",
			PriceKRW:    9000 + i,
			ReviewCount: 1000 - i,
			CollectedAt: "2026-07-04 20:00:00",
		})
	}
	for i := 0; i < 20; i++ {
		products = append(products, insightProduct{
			Provider:    "gmarket",
			ProductCode: "g",
			ProductName: "gmarket",
			PriceKRW:    19000 + i,
			ReviewCount: 10 - i,
			CollectedAt: "2026-07-04 19:00:00",
		})
	}

	got := topInsightProducts(products, 10)
	counts := countInsightProviders(got)
	if counts["gmarket"] != 5 || counts["kurly"] != 5 {
		t.Fatalf("provider counts = %#v, want 5/5", counts)
	}
	for i := 0; i < len(got); i += 2 {
		if got[i].Provider != "gmarket" {
			t.Fatalf("item %d provider = %q, want gmarket", i, got[i].Provider)
		}
		if i+1 < len(got) && got[i+1].Provider != "kurly" {
			t.Fatalf("item %d provider = %q, want kurly", i+1, got[i+1].Provider)
		}
	}
}

func TestBuildInsightDealCandidatesInterleavesProviders(t *testing.T) {
	products := []insightProduct{}
	for i := 0; i < 20; i++ {
		products = append(products, insightProduct{
			Provider:         "kurly",
			ProductCode:      "k",
			ProductName:      "kurly",
			SourceCategory:   "식품",
			PriceKRW:         9000 + i,
			OriginalPriceKRW: 30000,
		})
	}
	for i := 0; i < 20; i++ {
		products = append(products, insightProduct{
			Provider:         "gmarket",
			ProductCode:      "g",
			ProductName:      "gmarket",
			SourceCategory:   "식품",
			PriceKRW:         12000 + i,
			OriginalPriceKRW: 20000,
		})
	}

	got := buildInsightDealCandidates(products, 10)
	counts := countInsightDealProviders(got)
	if counts["gmarket"] != 5 || counts["kurly"] != 5 {
		t.Fatalf("provider counts = %#v, want 5/5", counts)
	}
}

func countInsightProviders(items []insightProduct) map[string]int {
	counts := map[string]int{}
	for _, item := range items {
		counts[item.Provider]++
	}
	return counts
}

func countInsightDealProviders(items []insightDealCandidate) map[string]int {
	counts := map[string]int{}
	for _, item := range items {
		counts[item.Provider]++
	}
	return counts
}
