package main

import (
	"encoding/json"
	"io"
	"os"
	"sort"
	"strings"
)

func observeAdpickMallDirectory(report *adpickCoverage, malls []adpickMall, key string) {
	observed := map[string]bool{}
	eligible := map[string]bool{}
	for _, mall := range malls {
		name := adpickText(mall.Name, 100)
		if !adpickCodePattern.MatchString(mall.Code) || name == "" || strings.Contains(name, "://") ||
			(key != "" && (strings.Contains(mall.Code, key) || strings.Contains(name, key))) {
			report.InvalidMallEntries++
			continue
		}
		if observed[mall.Code] {
			report.InvalidMallEntries++
			continue
		}
		observed[mall.Code] = true
		status := "review_required"
		if identity, ok := adpickMerchantNames[normalizedAdpickName(name)]; ok {
			status = "eligible_" + identity.Vertical
			eligible[identity.Key] = true
		}
		if strings.EqualFold(mall.Code, "COUPANG") {
			status = "excluded_coupang"
		}
		report.MallDirectory = append(report.MallDirectory, adpickMallObservation{Code: mall.Code, Name: name, Status: status})
	}
	sort.Slice(report.MallDirectory, func(i, j int) bool { return report.MallDirectory[i].Code < report.MallDirectory[j].Code })
	expected := map[string]bool{}
	for _, identity := range adpickMerchantNames {
		expected[identity.Key] = true
	}
	for merchant := range expected {
		if !eligible[merchant] {
			report.MissingEligibleMerchants = append(report.MissingEligibleMerchants, merchant)
		}
	}
	sort.Strings(report.MissingEligibleMerchants)
	previous, ok := previousAdpickMallCodes(envString("ADPICK_PREVIOUS_COVERAGE_REPORT", ""))
	report.MallBaselineAvailable = ok
	if !ok {
		return
	}
	for code := range observed {
		if !previous[code] {
			report.NewMallCodes = append(report.NewMallCodes, code)
		}
	}
	for code := range previous {
		if !observed[code] {
			report.RemovedMallCodes = append(report.RemovedMallCodes, code)
		}
	}
	sort.Strings(report.NewMallCodes)
	sort.Strings(report.RemovedMallCodes)
}

func previousAdpickMallCodes(path string) (map[string]bool, bool) {
	if path == "" {
		return nil, false
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, false
	}
	defer f.Close()
	body, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		return nil, false
	}
	var previous adpickCoverage
	if json.Unmarshal(body, &previous) != nil || !previous.DiscoveryComplete || len(previous.MallDirectory) == 0 || len(previous.MallDirectory) > 1000 {
		return nil, false
	}
	codes := map[string]bool{}
	for _, mall := range previous.MallDirectory {
		if !adpickCodePattern.MatchString(mall.Code) || codes[mall.Code] {
			return nil, false
		}
		codes[mall.Code] = true
	}
	return codes, true
}
