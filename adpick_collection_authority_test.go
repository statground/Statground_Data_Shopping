package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestAdpickFreshCollectionAuthorization(t *testing.T) {
	for _, test := range []struct {
		name, actions, event, ref, payload string
		allowed, scheduled                 bool
	}{
		{"daily", "true", "schedule", adpickCollectorWorkflow + "main", `{"schedule":"25 2 * * *"}`, true, true},
		{"human_string", "true", "workflow_dispatch", adpickCollectorWorkflow + "main", `{"inputs":{"confirm_collection":"true"}}`, true, false},
		{"human_bool", "true", "workflow_dispatch", adpickCollectorWorkflow + "review", `{"inputs":{"confirm_collection":true}}`, true, false},
		{"local", "", "schedule", adpickCollectorWorkflow + "main", `{"schedule":"25 2 * * *"}`, false, false},
		{"push", "true", "push", adpickCollectorWorkflow + "main", `{"inputs":{"confirm_collection":true}}`, false, false},
		{"workflow_run", "true", "workflow_run", adpickCollectorWorkflow + "main", `{"inputs":{"confirm_collection":true}}`, false, false},
		{"repository_dispatch", "true", "repository_dispatch", adpickCollectorWorkflow + "main", `{"inputs":{"confirm_collection":true}}`, false, false},
		{"other_schedule", "true", "schedule", adpickCollectorWorkflow + "main", `{"schedule":"10 */6 * * *"}`, false, false},
		{"scheduled_branch", "true", "schedule", adpickCollectorWorkflow + "other", `{"schedule":"25 2 * * *"}`, false, false},
		{"other_workflow", "true", "schedule", "statground/Statground_Data_Shopping/.github/workflows/gmarket-crawl.yml@refs/heads/main", `{"schedule":"25 2 * * *"}`, false, false},
		{"unconfirmed", "true", "workflow_dispatch", adpickCollectorWorkflow + "main", `{"inputs":{"confirm_collection":"false"}}`, false, false},
		{"missing_confirmation", "true", "workflow_dispatch", adpickCollectorWorkflow + "main", `{}`, false, false},
		{"malformed", "true", "schedule", adpickCollectorWorkflow + "main", `{`, false, false},
		{"oversize", "true", "schedule", adpickCollectorWorkflow + "main", strings.Repeat(" ", adpickWorkflowEventLimit+1), false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheduled, err := authorizeAdpickCollection(test.actions, test.event, test.ref, []byte(test.payload))
			if test.allowed {
				if err != nil || scheduled != test.scheduled {
					t.Fatalf("authorization scheduled=%v error=%v", scheduled, err)
				}
			} else if !errors.Is(err, errAdpickCollectionUnauthorized) {
				t.Fatalf("unexpected authorization error=%v", err)
			}
		})
	}
}

func setAdpickEventForTest(t *testing.T, name, payload string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "event.json")
	if err := os.WriteFile(path, []byte(payload), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GITHUB_ACTIONS", "true")
	t.Setenv("GITHUB_EVENT_NAME", name)
	t.Setenv("GITHUB_WORKFLOW_REF", adpickCollectorWorkflow+"main")
	t.Setenv("GITHUB_EVENT_PATH", path)
}

func TestAdpickDailyCollectionCannotExpandProfileOrQueries(t *testing.T) {
	setAdpickEventForTest(t, "schedule", `{"schedule":"25 2 * * *"}`)
	t.Setenv("ADPICK_QUERY_PROFILE", "diagnostic")
	t.Setenv("ADPICK_SEARCH_QUERIES_JSON", `[{"vertical":"diagnostic","category":"retail_control","keyword":"노트북"}]`)
	t.Setenv("ADPICK_SEARCH_LIMIT", "1")
	plan, err := adpickCollectionPlanFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plan.queries, defaultAdpickQueries) || plan.searchLimit != 20 || plan.directoryOnly {
		t.Fatalf("daily plan changed its standard travel/services scope: %+v", plan)
	}
	for _, query := range plan.queries {
		if !validAdpickCategory(query.Vertical, query.Category) {
			t.Fatalf("daily plan contains another vertical: %+v", query)
		}
	}
	plan.queries[0].Keyword = "changed"
	if defaultAdpickQueries[0].Keyword == "changed" {
		t.Fatal("daily plan shares a mutable default query slice")
	}
}

func TestAdpickHumanCollectionRequiresConfirmationAndPreservesProfile(t *testing.T) {
	setAdpickEventForTest(t, "workflow_dispatch", `{"inputs":{"confirm_collection":"true"}}`)
	t.Setenv("ADPICK_QUERY_PROFILE", "directory")
	t.Setenv("ADPICK_SEARCH_QUERIES_JSON", "")
	t.Setenv("ADPICK_SEARCH_LIMIT", "20")
	plan, err := adpickCollectionPlanFromEnv()
	if err != nil || !plan.directoryOnly || len(plan.queries) != 0 {
		t.Fatalf("confirmed manual directory plan=%+v error=%v", plan, err)
	}
}

func TestAdpickFreshUnauthorizedBeforeCredentialsOrDatabase(t *testing.T) {
	t.Setenv("ADPICK_REPLAY_HARVEST_FILE", "")
	t.Setenv("ADPICK_COLLECT_ONLY", "true")
	t.Setenv("ADPICK_BIZ_API_KEY", "")
	t.Setenv("GITHUB_ACTIONS", "")
	t.Setenv("GITHUB_EVENT_NAME", "")
	if err := RunAdpickCatalogFromEnv(context.Background()); !errors.Is(err, errAdpickCollectionUnauthorized) {
		t.Fatalf("local flag-only fresh collection error=%v", err)
	}
	t.Setenv("GITHUB_ACTIONS", "true")
	t.Setenv("GITHUB_EVENT_NAME", "push")
	if err := RunAdpickCatalogFromEnv(context.Background()); !errors.Is(err, errAdpickCollectionUnauthorized) {
		t.Fatalf("push fresh collection error=%v", err)
	}
}

func TestAdpickFreshAuthorityLeavesDatabaseReplayBranchIntact(t *testing.T) {
	t.Setenv("GITHUB_ACTIONS", "")
	t.Setenv("GITHUB_EVENT_NAME", "")
	t.Setenv("ADPICK_REPLAY_HARVEST_FILE", filepath.Join(t.TempDir(), "missing-harvest.json"))
	err := RunAdpickCatalogFromEnv(context.Background())
	if err == nil || errors.Is(err, errAdpickCollectionUnauthorized) || !strings.Contains(err.Error(), "replay harvest") {
		t.Fatalf("replay was intercepted by fresh API authorization: %v", err)
	}
}

func TestAdpickCollectorWorkflowHasNoPushCollection(t *testing.T) {
	body, err := os.ReadFile(".github/workflows/adpick-catalog.yml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	if strings.Contains(text, "\n  push:") || strings.Contains(text, "\n  repository_dispatch:") || strings.Contains(text, "\n  workflow_run:") {
		t.Fatal("collector has an automatic event outside its daily schedule")
	}
	for _, required := range []string{"cron: '25 2 * * *'", "workflow_dispatch:", "confirm_collection:", "default: false", "github.event.schedule == '25 2 * * *'", "github.event.inputs.confirm_collection == 'true'", "github.event_name == 'workflow_dispatch' && vars.ADPICK_SEARCH_QUERIES_JSON"} {
		if !strings.Contains(text, required) {
			t.Fatalf("collector workflow lacks authorization boundary %s", required)
		}
	}
}
