package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestAdpickMallCoverageSearchesEveryMatchedCodeWithinProviderCaps(t *testing.T) {
	codes, err := parseAdpickSearchableMalls(`["F","E","D","C","B","A","REMOVED"]`)
	if err != nil {
		t.Fatal(err)
	}
	groups := make(chan string, 2)
	client := adpickFixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/malls"):
			if r.URL.Query().Get("rewardmalls") != "false" {
				t.Error("full current directory required")
			}
			fmt.Fprint(w, `{"success":true,"data":[{"cp_code":"A"},{"cp_code":"B"},{"cp_code":"C"},{"cp_code":"D"},{"cp_code":"E"},{"cp_code":"F"},{"cp_code":"UNAPPROVED"}]}`)
		case strings.HasSuffix(r.URL.Path, "/search"):
			values := r.URL.Query()
			if values.Get("limit") != "20" || values.Get("q") == "" {
				t.Error("bounded keyword search required")
			}
			groups <- values.Get("cp_code")
			if values.Get("cp_code") == "F" {
				fmt.Fprint(w, `{"success":true,"data":[]}`)
				return
			}
			fmt.Fprint(w, `{"success":true,"data":[{"cp_code":"A","title":"NON_RETAINED_PRODUCT","commissionlink":"https://deg.kr/non-retained"}]}`)
		default:
			t.Errorf("unexpected request path %s", r.URL.Path)
		}
	})
	report, err := searchAdpickVerifiedMalls(context.Background(), client, codes, "test-run")
	if err == nil || report.Complete || report.SearchesCompleted != 6 || report.DirectoryMalls != 7 || report.Requests != 3 {
		t.Fatalf("coverage %+v error=%v", report, err)
	}
	grouped := []string{<-groups, <-groups}
	if !reflect.DeepEqual(grouped, []string{"A,B,C,D,E", "F"}) || report.Malls[0].Offers != 1 || report.Malls[5].Status != "searched" || report.Malls[6].Status != "not_in_current_directory" {
		t.Fatalf("scope or empty-result classification changed: %+v", report)
	}
	data, err := json.Marshal(report)
	if err != nil || strings.Contains(string(data), "NON_RETAINED_PRODUCT") || strings.Contains(string(data), "https://") {
		t.Fatal("product material escaped into the receipt")
	}
}

func TestAdpickMallCoverageRejectsUnrequestedSearchResults(t *testing.T) {
	client := adpickFixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		code := "APPROVED"
		if strings.HasSuffix(r.URL.Path, "/search") {
			code = "OUTSIDE"
		}
		fmt.Fprintf(w, `{"success":true,"data":[{"cp_code":%q}]}`, code)
	})
	report, err := searchAdpickVerifiedMalls(context.Background(), client, []string{"APPROVED"}, "run")
	if err == nil || report.Complete || report.Malls[0].Status != "unverified_response" {
		t.Fatalf("unexpected provider scope accepted: %+v error=%v", report, err)
	}
}

func TestAdpickMallCoverageRequiresExplicitVerifiedCodesAndSchedule(t *testing.T) {
	for _, raw := range []string{"", `[]`, `["A","A"]`, `["../../api"]`, `["A,B"]`} {
		if _, err := parseAdpickSearchableMalls(raw); err == nil {
			t.Fatal("unverified mall configuration accepted")
		}
	}
	setAdpickEventForTest(t, "workflow_dispatch", `{"inputs":{"confirm_collection":true}}`)
	if err := runAdpickMallSearchCoverage(context.Background()); err != errAdpickCollectionUnauthorized {
		t.Fatalf("coverage escaped scheduled authority: %v", err)
	}
}
