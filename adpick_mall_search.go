package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

const adpickMallSearchGroupSize = 5
const adpickMaximumDirectoryMalls = 1000

var adpickCoverageKeywords = []string{"베스트", "추천", "인기", "신상품", "할인"}

type adpickMallSearchResult struct {
	Code   string `json:"cp_code"`
	Status string `json:"status"`
	Offers int    `json:"offers"`
}

type adpickMallSearchReport struct {
	Schema            string                   `json:"schema"`
	DirectoryMalls    int                      `json:"directory_malls"`
	ConfiguredMalls   int                      `json:"configured_searchable_malls"`
	Requests          int                      `json:"request_attempts"`
	SearchesCompleted int                      `json:"searches_completed"`
	Complete          bool                     `json:"complete"`
	Malls             []adpickMallSearchResult `json:"malls"`
}

func parseAdpickSearchableMalls(raw string) ([]string, error) {
	var codes []string
	if len(raw) > 96<<10 || json.Unmarshal([]byte(raw), &codes) != nil || len(codes) == 0 || len(codes) > adpickMaximumDirectoryMalls {
		return nil, fmt.Errorf("verified BIZ searchable mall codes required")
	}
	seen := make(map[string]bool, len(codes))
	for _, code := range codes {
		if !adpickCodePattern.MatchString(code) || seen[code] {
			return nil, fmt.Errorf("invalid BIZ searchable mall code list")
		}
		seen[code] = true
	}
	sort.Strings(codes)
	return codes, nil
}

func searchAdpickVerifiedMalls(ctx context.Context, client *adpickClient, codes []string, runID string) (adpickMallSearchReport, error) {
	report := adpickMallSearchReport{Schema: "adpick.mall-search-coverage.v1", ConfiguredMalls: len(codes), Malls: []adpickMallSearchResult{}}
	var directory []struct {
		Code string `json:"cp_code"`
	}
	if err := client.request(ctx, "malls", url.Values{"rewardmalls": {"false"}}, &directory); err != nil {
		report.Requests = client.requests
		return report, err
	}
	report.DirectoryMalls = len(directory)
	active := make(map[string]bool, len(directory))
	if len(directory) == 0 || len(directory) > adpickMaximumDirectoryMalls {
		return report, fmt.Errorf("BIZ affiliate mall directory outside bounds")
	}
	for _, mall := range directory {
		if !adpickCodePattern.MatchString(mall.Code) || strings.Contains(mall.Code, client.key) || active[mall.Code] {
			return report, fmt.Errorf("ambiguous BIZ affiliate mall directory")
		}
		active[mall.Code] = true
	}
	eligible := make([]int, 0, len(codes))
	for _, code := range codes {
		if strings.Contains(code, client.key) {
			return report, fmt.Errorf("invalid BIZ searchable mall code list")
		}
		entry := adpickMallSearchResult{Code: code, Status: "not_in_current_directory"}
		if active[code] {
			entry.Status = "pending"
			eligible = append(eligible, len(report.Malls))
		}
		report.Malls = append(report.Malls, entry)
	}
	// Explicitly request every matched code. Provider result samples can be empty
	// or capped even for a successfully searched mall; preserve both observations.
	seed := sha256.Sum256([]byte(runID))
	for offset := 0; offset < len(eligible); offset += adpickMallSearchGroupSize {
		group := eligible[offset:min(offset+adpickMallSearchGroupSize, len(eligible))]
		groupCodes := make([]string, 0, len(group))
		counts := make(map[string]int, len(group))
		for _, index := range group {
			code := report.Malls[index].Code
			groupCodes = append(groupCodes, code)
			counts[code] = 0
		}
		keyword := adpickCoverageKeywords[(int(seed[0])+offset/adpickMallSearchGroupSize)%len(adpickCoverageKeywords)]
		var offers []struct {
			Code string `json:"cp_code"`
		}
		err := client.request(ctx, "search", url.Values{"q": {keyword}, "limit": {"20"}, "cp_code": {strings.Join(groupCodes, ",")}}, &offers)
		valid := err == nil && len(offers) <= 20
		for _, offer := range offers {
			_, requested := counts[offer.Code]
			valid = valid && requested
			counts[offer.Code]++
		}
		for _, index := range group {
			mall := &report.Malls[index]
			mall.Status = "unverified_response"
			if valid {
				mall.Status = "searched"
				mall.Offers = counts[mall.Code]
			}
		}
		if valid {
			report.SearchesCompleted += len(group)
		}
		if ctx.Err() != nil || client.requests >= adpickMaximumRequests {
			break
		}
	}
	report.Requests = client.requests
	report.Complete = report.SearchesCompleted == report.ConfiguredMalls
	if !report.Complete {
		return report, fmt.Errorf("BIZ mall search coverage incomplete")
	}
	return report, nil
}

func runAdpickMallSearchCoverage(parent context.Context) error {
	if os.Getenv("GITHUB_EVENT_NAME") != "schedule" {
		return errAdpickCollectionUnauthorized
	}
	if _, err := adpickCollectionPlanFromEnv(); err != nil {
		return err
	}
	codes, err := parseAdpickSearchableMalls(os.Getenv("ADPICK_BIZ_SEARCHABLE_MALL_CODES"))
	if err != nil {
		return err
	}
	client, err := newAdpickClient(os.Getenv("ADPICK_BIZ_API_KEY"))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 27*time.Minute)
	defer cancel()
	report, searchErr := searchAdpickVerifiedMalls(ctx, client, codes, os.Getenv("GITHUB_RUN_ID"))
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		return err
	}
	return searchErr
}
