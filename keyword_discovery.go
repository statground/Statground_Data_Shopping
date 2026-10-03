package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/statground/Statground_Data_Shopping/internal/lexicon"
)

var shoppingDiscoveryRun struct {
	sync.Once
	id string
}

func shoppingDiscoveryRunID() string {
	shoppingDiscoveryRun.Do(func() {
		shoppingDiscoveryRun.id = firstNonEmptyEnv("LEXICON_RUN_ID", "GITHUB_RUN_ID")
		if shoppingDiscoveryRun.id == "" && RandomSeed != 0 {
			shoppingDiscoveryRun.id = strconv.FormatInt(RandomSeed, 10)
		}
		if shoppingDiscoveryRun.id == "" {
			shoppingDiscoveryRun.id = lexicon.RunID()
		}
	})
	return shoppingDiscoveryRun.id
}

func shoppingKeywordBudget(scope string, count int) int {
	maximum := 120
	if scope == "gmarket" {
		maximum = 20
	}
	if count <= 0 {
		return 0
	}
	if count > maximum {
		count = maximum
	}
	// Exact halves take priority over using an odd last slot. Never expand an
	// existing provider request budget to make the ratio fit.
	return count - count%2
}

func selectShoppingKeywords(ctx context.Context, scope string, curated []string, count int, record bool) ([]lexicon.Selection, error) {
	count = shoppingKeywordBudget(scope, count)
	curated = UniqueKeepOrder(curated)
	if count > 2*len(curated) {
		count = 2 * len(curated)
	}
	if count == 0 {
		return nil, nil
	}
	pool, _, err := lexicon.FromEnv(ctx, scope)
	if err != nil {
		return nil, fmt.Errorf("%s dictionary candidates unavailable; keyword discovery skipped", scope)
	}
	selected, err := lexicon.Select(curated, pool, count, shoppingDiscoveryRunID(), scope)
	if err != nil {
		return nil, fmt.Errorf("%s exact 50/50 keyword plan unavailable; discovery skipped", scope)
	}
	selected = shoppingInterleaveSelections(selected)
	if record {
		if err := lexicon.Record(ctx, selected); err != nil {
			return nil, fmt.Errorf("%s keyword provenance not durable; discovery skipped", scope)
		}
	}
	return selected, nil
}

func shoppingInterleaveSelections(selected []lexicon.Selection) []lexicon.Selection {
	curated, random := []lexicon.Selection{}, []lexicon.Selection{}
	for _, selection := range selected {
		if selection.Origin == "curated" {
			curated = append(curated, selection)
		} else {
			random = append(random, selection)
		}
	}
	if len(curated) != len(random) {
		return selected
	}
	interleaved := make([]lexicon.Selection, 0, len(selected))
	for i := range curated {
		interleaved = append(interleaved, curated[i], random[i])
	}
	return interleaved
}

func shoppingSelectionKeywords(selected []lexicon.Selection) []string {
	keywords := make([]string, 0, len(selected))
	for _, selection := range selected {
		keywords = append(keywords, selection.Keyword)
	}
	return keywords
}

func finishShoppingKeywords(ctx context.Context, selected []lexicon.Selection, rows []Row, emptyOutcome string) {
	for _, selection := range selected {
		codes := map[string]bool{}
		for _, row := range rows {
			if FirstNonEmpty(row, []string{"검색어", "수집검색어"}) == selection.Keyword {
				if code := strings.TrimSpace(row["상품코드"]); code != "" {
					codes[code] = true
				}
			}
		}
		outcome := "completed"
		if len(codes) == 0 {
			outcome = emptyOutcome
		}
		if err := lexicon.Outcome(ctx, []lexicon.Selection{selection}, outcome, len(codes)); err != nil {
			fmt.Printf("[%s] keyword outcome not committed; selection remains reserved\n", selection.Origin)
		}
	}
}

// Plan mode never contacts a merchant. Kurly category-discovery labels are
// omitted here; a real run uses the existing category endpoint before planning.
func runShoppingKeywordPlan(ctx context.Context) error {
	plans := map[string][]lexicon.Selection{}
	for _, item := range []struct {
		scope   string
		enabled bool
		curated []string
		count   int
	}{{"gmarket", GmarketCollectEnabled, SearchKeywords, RandomKeywordCount}, {"kurly", KurlyCollectEnabled, KurlyDefaultDiverseKeywords, KurlyRandomKeywordCount}} {
		if !item.enabled {
			continue
		}
		curated := item.curated
		if item.scope == "kurly" && len(KurlySearchKeywordsOverride) > 0 {
			curated = KurlySearchKeywordsOverride
		}
		selected, err := selectShoppingKeywords(ctx, item.scope, curated, item.count, false)
		if err != nil {
			return err
		}
		plans[item.scope] = selected
	}
	return json.NewEncoder(os.Stdout).Encode(plans)
}
