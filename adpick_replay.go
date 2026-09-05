package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"
)

type adpickReplayHarvest struct {
	Schema        string                `json:"schema"`
	RunUUID       string                `json:"run_uuid"`
	Completed     int                   `json:"queries_completed"`
	RecordsSHA256 string                `json:"records_sha256"`
	Records       []adpickCatalogRecord `json:"records"`
}

// No API credential is needed: the source is a verified, bounded workflow
// artifact. Parsing and all semantic checks finish before any database access.
func loadAdpickReplayHarvest(path string, now time.Time) (*adpickReplayHarvest, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > adpickCatalogLimit {
		return nil, fmt.Errorf("Adpick replay harvest file rejected")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Adpick replay harvest file unavailable")
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, fmt.Errorf("Adpick replay harvest file changed")
	}
	body, err := io.ReadAll(io.LimitReader(f, adpickCatalogLimit+1))
	if err != nil || len(body) > adpickCatalogLimit || !utf8.Valid(body) || !adpickReplayUniqueJSON(body) {
		return nil, fmt.Errorf("Adpick replay harvest JSON rejected")
	}
	var harvest adpickReplayHarvest
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(&harvest) != nil {
		return nil, fmt.Errorf("Adpick replay harvest schema rejected")
	}
	encoded, err := json.Marshal(harvest.Records)
	if err != nil || harvest.Schema != "adpick.harvest.v1" || !rawOutboxUUIDPattern.MatchString(harvest.RunUUID) || harvest.Completed < 0 || harvest.Completed > adpickMaximumQueries || len(harvest.Records) < 1 || len(harvest.Records) > 4808 || harvest.RecordsSHA256 != fmt.Sprintf("%x", sha256.Sum256(encoded)) {
		return nil, fmt.Errorf("Adpick replay harvest integrity rejected")
	}
	if err := validateAdpickReplayRecords(harvest.Records, harvest.RunUUID, now); err != nil {
		return nil, err
	}
	return &harvest, nil
}

func adpickReplayTexts(row adpickCatalogRecord) []string {
	return []string{row.Source, row.Vertical, row.RecordType, row.MerchantCode, row.MerchantKey, row.MerchantName, row.ItemKey, row.CategorySlug, row.Title, row.Description, row.ImageURL, row.AffiliateURL, row.PriceText, row.SearchKeyword, row.CollectedAt, row.CollectRunUUID}
}

// Reject duplicate keys and trailing JSON rather than accepting a decoder's
// last-value-wins interpretation of an ambiguous integrity envelope.
func adpickReplayUniqueJSON(body []byte) bool {
	d := json.NewDecoder(bytes.NewReader(body))
	var read func(int) bool
	read = func(depth int) bool {
		if depth > 5 {
			return false
		}
		token, err := d.Token()
		if err != nil {
			return false
		}
		delim, container := token.(json.Delim)
		if !container {
			return true
		}
		if delim == '{' {
			seen := map[string]bool{}
			for d.More() {
				key, err := d.Token()
				name, ok := key.(string)
				if err != nil || !ok || seen[name] || !read(depth+1) {
					return false
				}
				seen[name] = true
			}
			end, err := d.Token()
			return err == nil && end == json.Delim('}')
		}
		if delim == '[' {
			for d.More() {
				if !read(depth + 1) {
					return false
				}
			}
			end, err := d.Token()
			return err == nil && end == json.Delim(']')
		}
		return false
	}
	if !read(0) {
		return false
	}
	_, err := d.Token()
	return err == io.EOF
}

func validateAdpickReplayRecords(records []adpickCatalogRecord, runUUID string, now time.Time) error {
	if len(records) < 1 || len(records) > 4808 {
		return fmt.Errorf("Adpick replay record count rejected")
	}
	merchants := map[string]adpickCatalogRecord{}
	merchantKeys, itemKeys := map[string]bool{}, map[string]bool{}
	version := records[0].Version
	secrets := []string{}
	for _, name := range []string{"ADPICK_BIZ_API_KEY", "ADPICK_API", "CLICKHOUSE_PASSWORD", "CH_PASSWORD"} {
		if value := os.Getenv(name); value != "" {
			secrets = append(secrets, value)
		}
	}
	for i, row := range records {
		// Compare decoded text, so JSON escaping cannot conceal credentials.
		for _, value := range adpickReplayTexts(row) {
			for _, secret := range secrets {
				if strings.Contains(value, secret) {
					return fmt.Errorf("Adpick replay harvest contains credential material")
				}
			}
		}
		stamp, stampErr := time.ParseInLocation("2006-01-02 15:04:05.000", row.CollectedAt, now.Location())
		identity, known := adpickMerchantNames[normalizedAdpickName(row.MerchantName)]
		if row.Source != "adpick_biz" || !known || identity.Key != row.MerchantKey || identity.Vertical != row.Vertical || !adpickCodePattern.MatchString(row.MerchantCode) || strings.EqualFold(row.MerchantCode, "COUPANG") || itemKeys[row.ItemKey] || row.CollectRunUUID != runUUID || row.Version != version || version == 0 || version > uint64(now.Add(5*time.Minute).UnixMilli()) || stampErr != nil || stamp.Year() < 2000 || stamp.After(now.Add(5*time.Minute)) || uint64(stamp.UnixMilli()) > version {
			return fmt.Errorf("Adpick replay record identity rejected at index %d", i)
		}
		if row.Title == "" || adpickText(row.Title, 300) != row.Title || adpickText(row.MerchantName, 100) != row.MerchantName || adpickText(row.Description, 500) != row.Description || adpickText(row.PriceText, 100) != row.PriceText || (row.ImageURL != "" && safeAdpickURL(row.ImageURL, false) != row.ImageURL) || (row.AffiliateURL != "" && safeAdpickURL(row.AffiliateURL, true) != row.AffiliateURL) || !reflect.DeepEqual(adpickPrice(row.PriceText), row.PriceKRW) {
			return fmt.Errorf("Adpick replay public fields rejected at index %d", i)
		}
		itemKeys[row.ItemKey] = true
		switch row.RecordType {
		case "merchant":
			if _, exists := merchants[row.MerchantCode]; exists || merchantKeys[row.MerchantKey] || row.ItemKey != fmt.Sprintf("%x", sha256.Sum256([]byte("merchant\x1f"+row.MerchantCode))) || row.Title != row.MerchantName || row.CategorySlug != "" || row.SearchKeyword != "" || row.PriceText != "" || row.PriceKRW != nil {
				return fmt.Errorf("Adpick replay merchant rejected at index %d", i)
			}
			merchants[row.MerchantCode] = row
			merchantKeys[row.MerchantKey] = true
		case "offer":
			if !validAdpickCategory(row.Vertical, row.CategorySlug) || row.AffiliateURL == "" || row.ItemKey != fmt.Sprintf("%x", sha256.Sum256([]byte(row.MerchantCode+"\x1f"+row.AffiliateURL))) || row.Description != "" || strings.TrimSpace(row.SearchKeyword) != row.SearchKeyword || row.SearchKeyword == "" || len([]rune(row.SearchKeyword)) > 100 || strings.ContainsAny(row.SearchKeyword, "\r\n\t") {
				return fmt.Errorf("Adpick replay offer rejected at index %d", i)
			}
		default:
			return fmt.Errorf("Adpick replay record type rejected at index %d", i)
		}
	}
	if len(merchants) == 0 || len(merchants) > 8 {
		return fmt.Errorf("Adpick replay merchant count rejected")
	}
	for _, row := range records {
		merchant, exists := merchants[row.MerchantCode]
		if !exists || row.MerchantKey != merchant.MerchantKey || row.Vertical != merchant.Vertical {
			return fmt.Errorf("Adpick replay offer has no matching verified merchant")
		}
	}
	return nil
}

type adpickReplayOperations struct {
	checkpoint func([]adpickCatalogRecord, *adpickCoverage) error
	gate       func(context.Context) error
	preflight  func(context.Context) error
	previous   func(context.Context) ([]adpickCatalogRecord, error)
	publish    func(context.Context, []adpickCatalogRecord) error
}

func replayAdpickHarvest(ctx context.Context, harvest *adpickReplayHarvest, runUUID string, now time.Time, ops adpickReplayOperations) error {
	if harvest == nil || !rawOutboxUUIDPattern.MatchString(runUUID) || validateAdpickReplayRecords(harvest.Records, harvest.RunUUID, now) != nil {
		return fmt.Errorf("Adpick replay validated harvest required")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	fresh := append([]adpickCatalogRecord(nil), harvest.Records...)
	version := uint64(now.UnixMilli())
	for _, row := range fresh {
		if row.Version >= version {
			version = row.Version + 1
		}
	}
	for i := range fresh {
		fresh[i].CollectRunUUID, fresh[i].Version = runUUID, version
	}
	report := newAdpickCoverage(harvest.Completed)
	report.RunUUID, report.QueriesCompleted, report.DiscoveryComplete = runUUID, harvest.Completed, true
	// A replay proves publication only. It cannot turn an incomplete collection
	// into a complete one or change the original failed workflow's outcome.
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ops.checkpoint(fresh, report); err != nil {
		return err
	}
	if err := ops.gate(ctx); err != nil {
		return err
	}
	if err := ops.preflight(ctx); err != nil {
		return err
	}
	previous, err := ops.previous(ctx)
	if err != nil {
		return err
	}
	// Never roll a newer published observation back to an older artifact.
	latest := map[string]adpickCatalogRecord{}
	for _, row := range previous {
		if row.Version == 0 || row.Version > uint64(now.Add(5*time.Minute).UnixMilli()) {
			return fmt.Errorf("Adpick replay previous generation rejected")
		}
		stamp, err := time.Parse("2006-01-02 15:04:05.999999999", row.CollectedAt)
		if err != nil {
			return fmt.Errorf("Adpick replay previous observation timestamp rejected")
		}
		row.CollectedAt = stamp.Format("2006-01-02 15:04:05.000")
		if row.Version >= version {
			version = row.Version + 1
		}
		latest[row.ItemKey] = row
	}
	selected := make([]adpickCatalogRecord, 0, len(fresh))
	for _, row := range fresh {
		row.Version = version
		if old, exists := latest[row.ItemKey]; exists && old.CollectedAt >= row.CollectedAt {
			if old.MerchantCode != row.MerchantCode || old.MerchantKey != row.MerchantKey || old.Vertical != row.Vertical || old.RecordType != row.RecordType {
				return fmt.Errorf("Adpick replay existing identity mismatch")
			}
			if row.RecordType == "offer" {
				continue // mergeAdpickHarvest revalidates and retains this observation.
			}
			old.CollectRunUUID, old.Version = runUUID, version
			row = old
		}
		selected = append(selected, row)
	}
	merged, retained, evicted, err := mergeAdpickHarvest(selected, previous)
	if err != nil {
		return err
	}
	if err := validateAdpickReplayRecords(merged, runUUID, now); err != nil {
		return err
	}
	report.RetainedOffers, report.EvictedOffers = retained, evicted
	if err := ops.checkpoint(merged, report); err != nil {
		return err
	}
	if err := ops.publish(ctx, merged); err != nil {
		return err
	}
	fmt.Printf("[adpick] verified harvest replay published records=%d source_queries_completed=%d; original collection completeness unchanged\n", len(merged), harvest.Completed)
	return nil
}

func RunAdpickReplayFromEnv(parent context.Context) error {
	harvest, err := loadAdpickReplayHarvest(envString("ADPICK_REPLAY_HARVEST_FILE", ""), NowKST())
	if err != nil {
		return err
	}
	if !ShouldWriteClickHouse() || envString("ADPICK_HARVEST_FILE", "") == "" {
		return fmt.Errorf("Adpick replay requires ClickHouse mode and a durable checkpoint destination")
	}
	pub, err := NewClickHouseRawPublisherFromEnv()
	if err != nil {
		return err
	}
	return replayAdpickHarvest(parent, harvest, NewUUIDv7(), NowKST(), adpickReplayOperations{
		checkpoint: func(rows []adpickCatalogRecord, report *adpickCoverage) error {
			return writeAdpickHarvest(rows, report, "")
		},
		gate:      func(ctx context.Context) error { return runAdpickPublicationGate(ctx, pub) },
		preflight: func(ctx context.Context) error { return preflightAdpickCatalog(ctx, pub) },
		previous:  func(ctx context.Context) ([]adpickCatalogRecord, error) { return readPreviousAdpickCatalog(ctx, pub) },
		publish: func(ctx context.Context, rows []adpickCatalogRecord) error {
			return publishAdpickCatalog(ctx, pub, rows)
		},
	})
}
