package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const adpickDailySchedule = "25 2 * * *"
const adpickCollectorWorkflow = "statground/Statground_Data_Shopping/.github/workflows/adpick-catalog.yml@refs/heads/"
const adpickWorkflowEventLimit = 256 << 10

var errAdpickCollectionUnauthorized = errors.New("Adpick fresh collection requires the daily travel/services schedule or confirmed human workflow dispatch")

type adpickCollectionPlan struct {
	queries       []adpickQuery
	directoryOnly bool
	searchLimit   int
}

func authorizeAdpickCollection(actions, eventName, workflowRef string, payload []byte) (scheduled bool, err error) {
	if actions != "true" || !strings.HasPrefix(workflowRef, adpickCollectorWorkflow) || len(workflowRef) <= len(adpickCollectorWorkflow) ||
		(eventName != "schedule" && eventName != "workflow_dispatch") || len(payload) == 0 || len(payload) > adpickWorkflowEventLimit {
		return false, errAdpickCollectionUnauthorized
	}
	var event struct {
		Schedule string `json:"schedule"`
		Inputs   struct {
			ConfirmCollection json.RawMessage `json:"confirm_collection"`
		} `json:"inputs"`
	}
	if json.Unmarshal(payload, &event) != nil {
		return false, errAdpickCollectionUnauthorized
	}
	if eventName == "schedule" {
		if event.Schedule != adpickDailySchedule || workflowRef != adpickCollectorWorkflow+"main" {
			return false, errAdpickCollectionUnauthorized
		}
		return true, nil
	}
	confirmation := bytes.TrimSpace(event.Inputs.ConfirmCollection)
	if !bytes.Equal(confirmation, []byte("true")) && !bytes.Equal(confirmation, []byte(`"true"`)) {
		return false, errAdpickCollectionUnauthorized
	}
	return false, nil
}

func adpickCollectionPlanFromEnv() (adpickCollectionPlan, error) {
	actions, eventName, workflowRef := os.Getenv("GITHUB_ACTIONS"), os.Getenv("GITHUB_EVENT_NAME"), os.Getenv("GITHUB_WORKFLOW_REF")
	// Reject ordinary local, push and background runs before reading event files,
	// looking up API credentials or constructing database clients.
	if actions != "true" || (eventName != "schedule" && eventName != "workflow_dispatch") || !strings.HasPrefix(workflowRef, adpickCollectorWorkflow) {
		return adpickCollectionPlan{}, errAdpickCollectionUnauthorized
	}
	eventPath := os.Getenv("GITHUB_EVENT_PATH")
	info, err := os.Lstat(eventPath)
	if err != nil || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > adpickWorkflowEventLimit {
		return adpickCollectionPlan{}, errAdpickCollectionUnauthorized
	}
	file, err := os.Open(eventPath)
	if err != nil {
		return adpickCollectionPlan{}, errAdpickCollectionUnauthorized
	}
	defer file.Close()
	info, err = file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > adpickWorkflowEventLimit {
		return adpickCollectionPlan{}, errAdpickCollectionUnauthorized
	}
	payload, err := io.ReadAll(io.LimitReader(file, adpickWorkflowEventLimit+1))
	if err != nil {
		return adpickCollectionPlan{}, errAdpickCollectionUnauthorized
	}
	scheduled, err := authorizeAdpickCollection(actions, eventName, workflowRef, payload)
	if err != nil {
		return adpickCollectionPlan{}, err
	}
	if scheduled {
		// The daily exemption always keeps its existing standard travel/services
		// queries and limit, regardless of profile or custom-query environment.
		return adpickCollectionPlan{queries: append([]adpickQuery(nil), defaultAdpickQueries...), searchLimit: 20}, nil
	}
	queries, err := adpickQueriesFromEnv()
	if err != nil {
		return adpickCollectionPlan{}, err
	}
	limit := boundedRawInt(envString("ADPICK_SEARCH_LIMIT", "20"), 0, 1, 20)
	if limit == 0 {
		return adpickCollectionPlan{}, fmt.Errorf("ADPICK_SEARCH_LIMIT must be between 1 and 20")
	}
	return adpickCollectionPlan{queries: queries, directoryOnly: envString("ADPICK_QUERY_PROFILE", "standard") == "directory", searchLimit: limit}, nil
}
