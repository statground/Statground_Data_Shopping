package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestAdpickMallMonitorDetectsChangesWithoutAuthorizingMalls(t *testing.T) {
	previous := newAdpickCoverage(0)
	previous.DiscoveryComplete = true
	previous.MallDirectory = []adpickMallObservation{{Code: "OLD", Name: "Previous mall"}, {Code: "TRIP", Name: "트립닷컴"}}
	body, _ := json.Marshal(previous)
	path := filepath.Join(t.TempDir(), "previous.json")
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ADPICK_PREVIOUS_COVERAGE_REPORT", path)
	report := newAdpickCoverage(0)
	observeAdpickMallDirectory(report, []adpickMall{
		{Code: "TRIP", Name: "트립닷컴", Link: "https://bitl.bz/not-output"},
		{Code: "RETAIL", Name: "New shopping mall"},
		{Code: "COUPANG", Name: "쿠팡"},
		{Code: "PRIVATE", Name: "fixture-secret"},
	}, "fixture-secret")
	if !report.MallBaselineAvailable || !reflect.DeepEqual(report.NewMallCodes, []string{"COUPANG", "RETAIL"}) ||
		!reflect.DeepEqual(report.RemovedMallCodes, []string{"OLD"}) || report.InvalidMallEntries != 1 {
		t.Fatalf("unexpected directory delta: %+v", report)
	}
	if report.MallDirectory[0].Status != "excluded_coupang" || report.MallDirectory[1].Status != "review_required" ||
		report.MallDirectory[2].Status != "eligible_travel" {
		t.Fatalf("invalid statuses: %+v", report.MallDirectory)
	}
	encoded, _ := json.Marshal(report)
	if strings.Contains(string(encoded), "not-output") || strings.Contains(string(encoded), "fixture-secret") {
		t.Fatal("directory monitor exposed provider links or key")
	}
	if _, allowed := adpickMerchantNames[normalizedAdpickName("New shopping mall")]; allowed {
		t.Fatal("observed mall was automatically allowlisted")
	}
}

func TestAdpickMallMonitorKeepsUnknownBaselineDistinct(t *testing.T) {
	t.Setenv("ADPICK_PREVIOUS_COVERAGE_REPORT", filepath.Join(t.TempDir(), "missing.json"))
	report := newAdpickCoverage(0)
	observeAdpickMallDirectory(report, []adpickMall{{Code: "TRIP", Name: "트립닷컴"}}, "")
	if report.MallBaselineAvailable || len(report.NewMallCodes) != 0 || len(report.RemovedMallCodes) != 0 {
		t.Fatal("missing baseline became an empty verified directory")
	}
	for _, key := range report.MissingEligibleMerchants {
		if key == "trip_com" {
			t.Fatal("present merchant reported missing")
		}
	}
}
