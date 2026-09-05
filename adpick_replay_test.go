package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func replayTestHarvest() (*adpickReplayHarvest, time.Time) {
	now := NowKST().Truncate(time.Millisecond)
	observed := now.Add(-time.Hour)
	merchant := adpickCatalogRecord{Source: "adpick_biz", Vertical: "travel", RecordType: "merchant", MerchantCode: "TRIP", MerchantKey: "trip_com", MerchantName: "트립닷컴", Title: "트립닷컴", AffiliateURL: "https://bitl.bz/trip", CollectedAt: FormatCHDateTime64Millis(observed), CollectRunUUID: adpickTestRun, Version: uint64(observed.UnixMilli())}
	merchant.ItemKey = fmt.Sprintf("%x", sha256.Sum256([]byte("merchant\x1fTRIP")))
	offer := merchant
	offer.RecordType, offer.CategorySlug, offer.Title, offer.AffiliateURL = "offer", "stays", "Observed hotel", "https://bitl.bz/hotel"
	offer.ItemKey = fmt.Sprintf("%x", sha256.Sum256([]byte("TRIP\x1f"+offer.AffiliateURL)))
	offer.SearchKeyword, offer.PriceText, offer.PriceKRW = "제주 호텔", "19,900원", adpickPrice("19,900원")
	return &adpickReplayHarvest{Schema: "adpick.harvest.v1", RunUUID: adpickTestRun, Completed: 1, Records: []adpickCatalogRecord{merchant, offer}}, now
}

func writeReplayTestFile(t *testing.T, harvest *adpickReplayHarvest) string {
	t.Helper()
	records, err := json.Marshal(harvest.Records)
	if err != nil {
		t.Fatal(err)
	}
	harvest.RecordsSHA256 = fmt.Sprintf("%x", sha256.Sum256(records))
	body, err := json.Marshal(harvest)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "harvest.json")
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestAdpickReplayLoadsCheckpointWithoutAPIKeyAndKeepsObservations(t *testing.T) {
	t.Setenv("ADPICK_BIZ_API_KEY", "")
	harvest, now := replayTestHarvest()
	loaded, err := loadAdpickReplayHarvest(writeReplayTestFile(t, harvest), now)
	if err != nil || !reflect.DeepEqual(loaded, harvest) {
		t.Fatalf("load changed verified observations: %v", err)
	}
	// The producer's real atomic writer and replay decoder share one format.
	path := filepath.Join(t.TempDir(), "producer.json")
	t.Setenv("ADPICK_HARVEST_FILE", path)
	report := newAdpickCoverage(2)
	report.RunUUID, report.QueriesCompleted = harvest.RunUUID, harvest.Completed
	if err := writeAdpickHarvest(harvest.Records, report, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := loadAdpickReplayHarvest(path, now); err != nil {
		t.Fatalf("producer checkpoint cannot be replayed: %v", err)
	}
	harvest.Records[1].MerchantName = "Trip.com"
	if _, err := loadAdpickReplayHarvest(writeReplayTestFile(t, harvest), now); err != nil {
		t.Fatalf("observed alias of the same verified merchant rejected: %v", err)
	}
}

func TestAdpickReplayRejectsAmbiguousCorruptAndUnsafeEnvelopes(t *testing.T) {
	for _, name := range []string{"checksum", "schema", "unknown field", "duplicate key", "trailing JSON", "invalid UTF8", "symlink", "oversized"} {
		t.Run(name, func(t *testing.T) {
			harvest, now := replayTestHarvest()
			path := writeReplayTestFile(t, harvest)
			body, _ := os.ReadFile(path)
			switch name {
			case "checksum":
				body = []byte(strings.Replace(string(body), "Observed hotel", "Altered hotel", 1))
			case "schema":
				body = []byte(strings.Replace(string(body), "adpick.harvest.v1", "adpick.harvest.v2", 1))
			case "unknown field":
				body = append([]byte(`{"unapproved":true,`), body[1:]...)
			case "duplicate key":
				body = append([]byte(`{"run_uuid":"`+adpickTestRun+`",`), body[1:]...)
			case "trailing JSON":
				body = append(body, []byte(` {}`)...)
			case "invalid UTF8":
				body = append(body, 0xff)
			case "symlink":
				link := filepath.Join(t.TempDir(), "link.json")
				if err := os.Symlink(path, link); err != nil {
					t.Fatal(err)
				}
				path = link
			case "oversized":
				if err := os.Truncate(path, adpickCatalogLimit+1); err != nil {
					t.Fatal(err)
				}
			}
			if name != "symlink" && name != "oversized" {
				if err := os.WriteFile(path, body, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := loadAdpickReplayHarvest(path, now); err == nil {
				t.Fatal("unsafe envelope accepted")
			}
		})
	}
}

func TestAdpickReplayValidDigestDoesNotAuthorizeInvalidRecords(t *testing.T) {
	for _, name := range []string{"excluded mall", "wrong CP", "wrong merchant key", "wrong category", "wrong item hash", "duplicate item", "missing merchant", "unsafe affiliate", "unsafe image", "price mismatch", "future observation", "mixed run", "mixed generation", "invalid time", "credential", "query count"} {
		t.Run(name, func(t *testing.T) {
			harvest, now := replayTestHarvest()
			switch name {
			case "excluded mall":
				harvest.Records[0].MerchantName = "쿠팡"
			case "wrong CP":
				harvest.Records[1].MerchantCode = "OTHER"
			case "wrong merchant key":
				harvest.Records[1].MerchantKey = "kmong"
			case "wrong category":
				harvest.Records[1].CategorySlug = "design"
			case "wrong item hash":
				harvest.Records[1].ItemKey = strings.Repeat("a", 64)
			case "duplicate item":
				harvest.Records = append(harvest.Records, harvest.Records[1])
			case "missing merchant":
				harvest.Records = harvest.Records[1:]
			case "unsafe affiliate":
				harvest.Records[1].AffiliateURL = "https://credential@bitl.bz/hotel"
			case "unsafe image":
				harvest.Records[1].ImageURL = "https://127.0.0.1/private"
			case "price mismatch":
				harvest.Records[1].PriceKRW = adpickPrice("29900")
			case "future observation":
				harvest.Records[1].CollectedAt = FormatCHDateTime64Millis(now.Add(time.Hour))
			case "mixed run":
				harvest.Records[1].CollectRunUUID = NewUUIDv7()
			case "mixed generation":
				harvest.Records[1].Version++
			case "invalid time":
				harvest.Records[1].CollectedAt = "2026-99-99 01:00:00.000"
			case "credential":
				t.Setenv("ADPICK_BIZ_API_KEY", "fixture-private-value")
				harvest.Records[1].Title = "fixture-private-value"
			case "query count":
				harvest.Completed = 241
			}
			if _, err := loadAdpickReplayHarvest(writeReplayTestFile(t, harvest), now); err == nil || strings.Contains(err.Error(), "fixture-private-value") {
				t.Fatalf("invalid records accepted or unsafe error: %v", err)
			}
		})
	}
}

func TestAdpickReplayPreservesNewerPublishedPriceAndCollectionStatus(t *testing.T) {
	harvest, now := replayTestHarvest()
	previous := append([]adpickCatalogRecord(nil), harvest.Records...)
	for i := range previous {
		previous[i].CollectedAt = FormatCHDateTime64Millis(now.Add(-time.Minute))
		previous[i].Version = uint64(now.Add(-time.Minute).UnixMilli())
	}
	previous[1].PriceText, previous[1].PriceKRW = "29,900원", adpickPrice("29,900원")
	runUUID := NewUUIDv7()
	var checkpoints int
	var stages []string
	ops := adpickReplayOperations{
		checkpoint: func(rows []adpickCatalogRecord, report *adpickCoverage) error {
			checkpoints++
			stages = append(stages, "checkpoint")
			if report.CollectionComplete || report.PublicationComplete || report.QueriesCompleted != 1 || report.RunUUID != runUUID {
				t.Fatal("replay fabricated successful source collection")
			}
			return nil
		},
		gate:      func(context.Context) error { stages = append(stages, "gate"); return nil },
		preflight: func(context.Context) error { stages = append(stages, "preflight"); return nil },
		previous: func(context.Context) ([]adpickCatalogRecord, error) {
			stages = append(stages, "previous")
			return previous, nil
		},
		publish: func(ctx context.Context, rows []adpickCatalogRecord) error {
			stages = append(stages, "publish")
			if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 10*time.Minute {
				t.Fatal("unbounded replay publication")
			}
			if len(rows) != 2 {
				t.Fatal("replay dropped known observations")
			}
			for _, row := range rows {
				if row.CollectRunUUID != runUUID || row.CollectedAt != previous[0].CollectedAt || row.Version <= previous[0].Version {
					t.Fatal("publication identity or observation date corrupted")
				}
				if row.RecordType == "offer" && (row.PriceText != "29,900원" || *row.PriceKRW != 29900) {
					t.Fatal("older artifact replaced a newer price")
				}
			}
			return nil
		},
	}
	if err := replayAdpickHarvest(context.Background(), harvest, runUUID, now, ops); err != nil {
		t.Fatal(err)
	}
	if checkpoints != 2 || strings.Join(stages, ",") != "checkpoint,gate,preflight,previous,checkpoint,publish" {
		t.Fatalf("unsafe replay order: %v", stages)
	}
	if harvest.Records[1].PriceText != "19,900원" || harvest.Records[0].CollectRunUUID != adpickTestRun {
		t.Fatal("replay mutated source artifact")
	}
}

func TestAdpickReplayFailureRetainsCheckpointAndDoesNotPublish(t *testing.T) {
	for _, failure := range []string{"checkpoint", "gate", "preflight", "previous", "publish"} {
		t.Run(failure, func(t *testing.T) {
			harvest, now := replayTestHarvest()
			stages := []string{}
			step := func(name string) error {
				stages = append(stages, name)
				if name == failure {
					return errors.New("fixture failure")
				}
				return nil
			}
			err := replayAdpickHarvest(context.Background(), harvest, NewUUIDv7(), now, adpickReplayOperations{
				checkpoint: func([]adpickCatalogRecord, *adpickCoverage) error { return step("checkpoint") },
				gate:       func(context.Context) error { return step("gate") },
				preflight:  func(context.Context) error { return step("preflight") },
				previous:   func(context.Context) ([]adpickCatalogRecord, error) { return nil, step("previous") },
				publish:    func(context.Context, []adpickCatalogRecord) error { return step("publish") },
			})
			if err == nil || stages[len(stages)-1] != failure || (failure != "publish" && strings.Contains(strings.Join(stages, ","), "publish")) {
				t.Fatalf("failure hidden or publication continued: %v %v", stages, err)
			}
		})
	}
}

func TestAdpickReplayRejectsInvalidFileBeforeWriterConfiguration(t *testing.T) {
	t.Setenv("ADPICK_REPLAY_HARVEST_FILE", filepath.Join(t.TempDir(), "absent.json"))
	t.Setenv("ADPICK_BIZ_API_KEY", "")
	t.Setenv("CLICKHOUSE_HOST", "")
	if err := RunAdpickReplayFromEnv(context.Background()); err == nil || !strings.Contains(err.Error(), "harvest file rejected") {
		t.Fatalf("database configuration accessed before file validation: %v", err)
	}
}
