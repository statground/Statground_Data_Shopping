package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/statground/Statground_Data_Shopping/internal/lexicon"
)

// Canonical copy: Statground_SQL/Clickhouse/Data_Content_Lexicon.
//
//go:embed adpick_keyword_categories.json
var adpickKeywordCategories []byte

func buildAdpickKeywordPlan(ctx context.Context, base []adpickQuery) ([]adpickQuery, []lexicon.Selection, error) {
	var categories map[string]map[string][]string
	if err := json.Unmarshal(adpickKeywordCategories, &categories); err != nil {
		return nil, nil, fmt.Errorf("invalid Adpick keyword categories")
	}
	var queries []adpickQuery
	var selections []lexicon.Selection
	snapshot := ""
	for _, vertical := range []string{"travel", "services"} {
		scope := "adpick-" + vertical
		queryByWord := map[string]adpickQuery{}
		for category, words := range categories[scope] {
			for _, word := range words {
				if _, exists := queryByWord[word]; exists || !validAdpickCategory(vertical, category) {
					return nil, nil, fmt.Errorf("invalid Adpick keyword categories")
				}
				queryByWord[word] = adpickQuery{vertical, category, word}
			}
		}
		var curated []string
		curatedQueries := map[string]adpickQuery{}
		for _, q := range base {
			if q.Vertical == vertical {
				curated = append(curated, q.Keyword)
				curatedQueries[q.Keyword] = q
			}
		}
		pool, _, err := lexicon.FromEnv(ctx, scope)
		if err != nil {
			return nil, nil, fmt.Errorf("%s committed keyword pool unavailable", scope)
		}
		eligible := make([]lexicon.Candidate, 0, len(pool))
		for _, c := range pool {
			if _, found := queryByWord[c.NormalizedWord]; found && c.Scope == scope && adpickIndependentKeyword(c.Language, c.Sources) {
				eligible = append(eligible, c)
			}
		}
		picked, err := lexicon.Select(curated, eligible, len(curated), lexicon.RunID(), scope)
		if err != nil {
			return nil, nil, fmt.Errorf("%s balanced keyword plan unavailable", scope)
		}
		for _, choice := range shoppingInterleaveSelections(picked) {
			q, ok := curatedQueries[choice.Keyword]
			if choice.Origin == "lexicon" {
				q, ok = queryByWord[choice.NormalizedWord]
				if !adpickIndependentKeyword(choice.Language, choice.Sources) || (snapshot != "" && snapshot != choice.SnapshotID) {
					return nil, nil, fmt.Errorf("Adpick keyword provenance or snapshot mismatch")
				}
				snapshot = choice.SnapshotID
				q.Keyword = choice.Keyword
			}
			if !ok || choice.Scope != scope {
				return nil, nil, fmt.Errorf("%s keyword receipt outside the permitted category", scope)
			}
			queries = append(queries, q)
			selections = append(selections, choice)
		}
	}
	return queries, selections, nil
}

func adpickIndependentKeyword(language string, sources []string) bool {
	if (language != "ko" && language != "en") || len(sources) == 0 {
		return false
	}
	for _, source := range sources {
		if !strings.HasPrefix(source, "book_") && !strings.HasPrefix(source, "inflearn_") && !strings.HasPrefix(source, "mirtype_") && !strings.HasPrefix(source, "r_") {
			return false
		}
	}
	return true
}

func recordAdpickKeywordOutcomes(ctx context.Context, selections []lexicon.Selection, queries []adpickQueryCoverage) bool {
	if len(queries) > len(selections) {
		return false
	}
	type searchOutcome struct {
		status string
		count  int
	}
	groups := map[searchOutcome][]lexicon.Selection{}
	for i, query := range queries {
		count := query.NewOffers + query.Duplicates
		outcome := searchOutcome{status: "completed", count: count}
		if count == 0 {
			outcome.status = "empty"
		}
		groups[outcome] = append(groups[outcome], selections[i])
	}
	for outcome, choices := range groups {
		if err := lexicon.Outcome(ctx, choices, outcome.status, outcome.count); err != nil {
			return false
		}
	}
	return true
}
