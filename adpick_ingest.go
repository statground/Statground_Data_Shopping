package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"
)

const (
	adpickRawTable       = "Data_Shopping_Raw.adpick_vertical_catalog_raw_endpoint_history"
	adpickSnapshotTable  = "Data_Shopping_Service.adpick_vertical_catalog_snapshot"
	adpickPublishedTable = "Data_Shopping_Service.adpick_vertical_catalog_published_batch"
	adpickLatestView     = "Data_Shopping_Service.adpick_vertical_catalog_latest"
	adpickRecordColumns  = "source, vertical, record_type, merchant_code, merchant_key, merchant_name, item_key, category_slug, title, description, image_url, affiliate_url, price_text, price_krw, search_keyword, toString(collected_at) AS collected_at, toString(collect_run_uuid) AS collect_run_uuid, version"
	adpickInputColumns   = "source, vertical, record_type, merchant_code, merchant_key, merchant_name, item_key, category_slug, title, description, image_url, affiliate_url, price_text, price_krw, search_keyword, collected_at, collect_run_uuid, version"
	adpickInputSchema    = "source String, vertical String, record_type String, merchant_code String, merchant_key String, merchant_name String, item_key String, category_slug String, title String, description String, image_url String, affiliate_url String, price_text String, price_krw Nullable(UInt64), search_keyword String, collected_at DateTime64(3, 'Asia/Seoul'), collect_run_uuid UUID, version UInt64"
)

func adpickWriteTarget(table string) bool {
	return table == adpickRawTable || table == adpickSnapshotTable
}

func adpickSQLString(value string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(value, "\\", "\\\\"), "'", "\\'") + "'"
}

func adpickEndpointGuard(hostname string) (string, error) {
	if !validDirectEndpointHostname(hostname) {
		return "", fmt.Errorf("invalid Adpick endpoint identity: %w", errDirectEndpointHostnameMismatch)
	}
	// This scalar runs on the INSERT/SELECT initiator, not a separately routed
	// HTTP preflight or a source shard. A gateway switch must fail the statement.
	return "(SELECT throwIf(hostName() != " + adpickSQLString(hostname) + ", 'adpick_catalog_endpoint_mismatch')) = 0", nil
}

func (p *ClickHouseRawPublisher) catalogRawInsertBody(table string, batch canonicalRawBatch) ([]byte, error) {
	if !adpickWriteTarget(table) {
		return rawInsertBody(table, batch, true)
	}
	columns, schema := adpickInputColumns, adpickInputSchema
	if table == adpickRawTable {
		columns += ", event_uuid, uuid, producer_source, payload"
		schema += ", event_uuid UUID, uuid UUID, producer_source String, payload String"
	} else {
		columns += ", event_uuid, snapshot_uuid, generated_at"
		schema += ", event_uuid UUID, snapshot_uuid UUID, generated_at DateTime64(3, 'Asia/Seoul')"
	}
	return p.adpickInputInsertBody(table, columns, schema, batch, maxRawOutboxPayloadBytes)
}

func (p *ClickHouseRawPublisher) adpickInputInsertBody(table, columns, schema string, batch canonicalRawBatch, maximum int) ([]byte, error) {
	if _, err := rawInsertBodyWithLimit(table, batch, false, maximum); err != nil {
		return nil, err
	}
	guard, err := adpickEndpointGuard(p.cfg.DirectEndpointHostname)
	if err != nil {
		return nil, err
	}
	query := "INSERT INTO " + table + " (" + columns + ") SELECT " + columns + " FROM input(" + adpickSQLString(schema) + ") WHERE " + guard +
		" SETTINGS insert_deduplicate = 1, insert_deduplication_token = '" + batch.token + "', max_threads = 1 FORMAT JSONEachRow\n"
	return append([]byte(query), batch.payload...), nil
}

func (p *ClickHouseRawPublisher) adpickGuardedRead(query string) (string, error) {
	guard, err := adpickEndpointGuard(p.cfg.DirectEndpointHostname)
	if err != nil {
		return "", err
	}
	// Leave FORMAT at the outermost level; SETTINGS are valid inside the SELECT.
	pos := strings.LastIndex(query, " FORMAT ")
	if pos < 0 {
		pos = strings.LastIndex(query, "\nFORMAT ")
	}
	format := ""
	if pos >= 0 {
		format, query = " "+strings.TrimSpace(query[pos:]), query[:pos]
	}
	return "SELECT * FROM (" + query + ") WHERE " + guard + format, nil
}

func RunAdpickCatalogFromEnv(parent context.Context) (resultErr error) {
	if envString("ADPICK_REPLAY_HARVEST_FILE", "") != "" {
		return RunAdpickReplayFromEnv(parent)
	}
	// Reserve publication time even when a bounded search harvest times out.
	ctx, cancel := context.WithTimeout(parent, 35*time.Minute)
	defer cancel()
	if !ShouldWriteClickHouse() {
		return fmt.Errorf("Adpick collection requires INGEST_MODE=clickhouse")
	}
	client, err := newAdpickClient(envString("ADPICK_BIZ_API_KEY", ""))
	if err != nil {
		return err
	}
	queries, err := adpickQueriesFromEnv()
	if err != nil {
		return err
	}
	report := newAdpickCoverage(len(queries))
	client.coverage = report
	client.progress = func() error { return writeAdpickCoverage(report, client) }
	defer func() {
		if resultErr != nil && report.Failure == "" {
			report.Failure = "collection_or_publication_failed"
		}
		if err := writeAdpickCoverage(report, client); err != nil && resultErr == nil {
			resultErr = err
		}
	}()
	limit := boundedRawInt(envString("ADPICK_SEARCH_LIMIT", "20"), 0, 1, 20)
	if limit == 0 {
		return fmt.Errorf("ADPICK_SEARCH_LIMIT must be between 1 and 20")
	}
	pub, err := NewClickHouseRawPublisherFromEnv()
	if err != nil {
		return err
	}
	if err := preflightAdpickCatalog(ctx, pub); err != nil {
		return err
	}
	runUUID := envString("ADPICK_RUN_UUID", NewUUIDv7())
	report.RunUUID = runUUID
	records, searchErr := collectAdpickCatalog(ctx, client, queries, limit, runUUID, NowKST())
	cancel()
	if searchErr != nil {
		report.Failure = "search_incomplete"
	}
	return finishAdpickHarvest(parent, report, records, searchErr, func(publishCtx context.Context, harvested []adpickCatalogRecord) error {
		if err := runAdpickPublicationGate(publishCtx, pub); err != nil {
			return err
		}
		if err := preflightAdpickCatalog(publishCtx, pub); err != nil {
			return err
		}
		previous, err := readPreviousAdpickCatalog(publishCtx, pub)
		if err != nil {
			return err
		}
		merged, retained, evicted, err := mergeAdpickHarvest(harvested, previous)
		if err != nil {
			return err
		}
		report.RetainedOffers, report.EvictedOffers = retained, evicted
		if err := writeAdpickHarvest(merged, report, client.key); err != nil {
			return err
		}
		return publishAdpickCatalog(publishCtx, pub, merged)
	})
}

func adpickPublicationGateEnv(pub *ClickHouseRawPublisher) []string {
	overrides := map[string]string{
		"CLICKHOUSE_HOST": pub.cfg.URL, "CLICKHOUSE_PORT": "", "CLICKHOUSE_PROTOCOL": "", "CLICKHOUSE_HTTP_URL_PATH": "",
		"CLICKHOUSE_USER": pub.cfg.User, "CLICKHOUSE_PASSWORD": pub.cfg.Password,
		"CH_HOST": pub.cfg.URL, "CH_PORT": "", "CH_PROTOCOL": "", "CH_HTTP_URL_PATH": "",
		"CH_USER": pub.cfg.User, "CH_PASSWORD": pub.cfg.Password,
		"CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME": pub.cfg.DirectEndpointHostname,
		"CLICKHOUSE_PRESSURE_GATE_TARGETS":    "local:" + adpickRawTable + ",local:" + adpickSnapshotTable + ",local:" + adpickPublishedTable + ",local:" + pub.cfg.OutboxTable,
	}
	env := []string{}
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if _, override := overrides[key]; !override && key != "ADPICK_BIZ_API_KEY" {
			env = append(env, entry)
		}
	}
	for key, value := range overrides {
		env = append(env, key+"="+value)
	}
	return env
}

func runAdpickPublicationGate(ctx context.Context, pub *ClickHouseRawPublisher) error {
	return waitAdpickPublicationGate(ctx, func(ctx context.Context) ([]byte, error) {
		attempt, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		command := exec.CommandContext(attempt, "python3", "scripts/clickhouse_pressure_gate.py")
		command.Env = adpickPublicationGateEnv(pub)
		return command.CombinedOutput()
	}, adpickSleep)
}

func preflightAdpickCatalog(ctx context.Context, pub *ClickHouseRawPublisher) error {
	ctx, cancel := context.WithTimeout(ctx, pub.cfg.PreflightRetryBudget)
	defer cancel()
	if err := pub.ensureDirectEndpointIdentity(ctx); err != nil {
		return err
	}
	for _, table := range []string{adpickRawTable, adpickSnapshotTable, adpickPublishedTable} {
		for _, privilege := range []string{"INSERT", "SELECT"} {
			if err := pub.checkAdpickGrant(ctx, privilege, table); err != nil {
				return fmt.Errorf("Adpick catalog %s preflight failed for %s: %w", privilege, table, err)
			}
		}
		if _, err := pub.retryEndpointDeduplicationSetting(ctx, table); err != nil {
			return fmt.Errorf("Adpick catalog deduplication preflight failed for %s: %w", table, err)
		}
	}
	for _, privilege := range []string{"INSERT", "SELECT", "ALTER UPDATE"} {
		if err := pub.checkAdpickGrant(ctx, privilege, pub.cfg.OutboxTable); err != nil {
			return fmt.Errorf("Adpick catalog outbox preflight failed: %w", err)
		}
	}
	return nil
}

func (pub *ClickHouseRawPublisher) checkAdpickGrant(ctx context.Context, privilege, table string) error {
	if !validRawTableIdentifier(table) || (privilege != "INSERT" && privilege != "SELECT" && privilege != "ALTER UPDATE") {
		return fmt.Errorf("invalid Adpick grant check")
	}
	_, err := retryClickHousePreflight(ctx, pub.cfg.PreflightRetryBackoff, func(attemptCtx context.Context) error {
		// CHECK GRANT is not a SELECT: FORMAT is invalid and denied rights return
		// HTTP 200 with a zero. Only one affirmative scalar authorizes the write.
		body, err := pub.queryBody(attemptCtx, "CHECK GRANT "+privilege+" ON "+table, 16)
		if err != nil {
			return err
		}
		if strings.TrimSpace(string(body)) != "1" {
			return fmt.Errorf("Adpick required grant denied or response invalid")
		}
		return nil
	})
	return err
}

func adpickRecordMap(record adpickCatalogRecord) (clickHouseRawRow, error) {
	body, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	var result clickHouseRawRow
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

func adpickCatalogRows(records []adpickCatalogRecord, producerSource, generatedAt string, raw bool) ([]clickHouseRawRow, error) {
	rows := make([]clickHouseRawRow, 0, len(records))
	for _, record := range records {
		row, err := adpickRecordMap(record)
		if err != nil {
			return nil, err
		}
		row["event_uuid"] = NewUUIDv7()
		if raw {
			row["uuid"] = NewUUIDv7()
			row["producer_source"] = producerSource
			body, err := json.Marshal(record)
			if err != nil {
				return nil, err
			}
			row["payload"] = string(body)
		} else {
			row["snapshot_uuid"] = NewUUIDv7()
			row["generated_at"] = generatedAt
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func adpickCanonicalRecords(records []adpickCatalogRecord) ([]byte, error) {
	ordered := append([]adpickCatalogRecord(nil), records...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ItemKey < ordered[j].ItemKey })
	return json.Marshal(ordered)
}

func verifyAdpickRecords(ctx context.Context, pub *ClickHouseRawPublisher, table string, expected []adpickCatalogRecord) error {
	if (table != adpickRawTable && table != adpickSnapshotTable) || len(expected) == 0 || len(expected) > adpickMaximumQueries*20+8 {
		return fmt.Errorf("invalid Adpick verification scope")
	}
	first := expected[0]
	if !rawOutboxUUIDPattern.MatchString(first.CollectRunUUID) || (first.Vertical != "travel" && first.Vertical != "services") {
		return fmt.Errorf("invalid Adpick verification identity")
	}
	query := fmt.Sprintf("SELECT %s FROM %s WHERE collect_run_uuid = toUUID('%s') AND vertical = '%s' AND version = %d ORDER BY item_key LIMIT %d SETTINGS max_threads = 1, max_execution_time = 15 FORMAT JSONEachRow",
		adpickRecordColumns, table, first.CollectRunUUID, first.Vertical, first.Version, len(expected)+1)
	query, err := pub.adpickGuardedRead(query)
	if err != nil {
		return err
	}
	body, err := pub.queryBody(ctx, query, adpickCatalogLimit)
	if err != nil {
		return fmt.Errorf("Adpick readback failed for %s: %w", table, err)
	}
	actual := []adpickCatalogRecord{}
	decoder := json.NewDecoder(bytes.NewReader(body))
	for {
		var record adpickCatalogRecord
		if err := decoder.Decode(&record); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("Adpick readback data rejected for %s", table)
		}
		parsed, err := time.Parse("2006-01-02 15:04:05.999999999", record.CollectedAt)
		if err != nil {
			return fmt.Errorf("Adpick readback timestamp rejected")
		}
		record.CollectedAt = parsed.Format("2006-01-02 15:04:05.000")
		actual = append(actual, record)
	}
	want, err := adpickCanonicalRecords(expected)
	if err != nil {
		return err
	}
	got, err := adpickCanonicalRecords(actual)
	if err != nil {
		return err
	}
	if !bytes.Equal(want, got) {
		return fmt.Errorf("Adpick catalog readback mismatch for %s: expected=%d actual=%d; prior publication retained", table, len(expected), len(actual))
	}
	return nil
}

func adpickPublicationBody(pub *ClickHouseRawPublisher, records []adpickCatalogRecord, marker map[string]any) ([]byte, error) {
	if len(records) == 0 || len(records) > adpickMaximumQueries*20+8 {
		return nil, fmt.Errorf("invalid Adpick publication scope")
	}
	guard, err := adpickEndpointGuard(pub.cfg.DirectEndpointHostname)
	if err != nil {
		return nil, err
	}
	first := records[0]
	if !rawOutboxUUIDPattern.MatchString(first.CollectRunUUID) || (first.Vertical != "travel" && first.Vertical != "services") {
		return nil, fmt.Errorf("invalid Adpick publication identity")
	}
	identity, err := json.Marshal(marker)
	if err != nil {
		return nil, err
	}
	var payload bytes.Buffer
	encoder := json.NewEncoder(&payload)
	for _, record := range records {
		if record.CollectRunUUID != first.CollectRunUUID || record.Version != first.Version || record.Vertical != first.Vertical {
			return nil, fmt.Errorf("mixed Adpick publication identity")
		}
		if err := encoder.Encode(record); err != nil {
			return nil, err
		}
	}
	if payload.Len() > adpickCatalogLimit {
		return nil, fmt.Errorf("Adpick publication payload exceeds bounded limit")
	}
	// The expected rows remain typed JSONEachRow input, so neither titles nor
	// URLs enter SQL text. Both sides use the same typed tuple serialization.
	// Check exact contents and SHA256 on the marker's executing physical node;
	// the earlier HTTP readback alone cannot authorize a later publication.
	rowTuple := "toString(tuple(" + adpickInputColumns + "))"
	markerColumns := "refresh_uuid, vertical, version, collect_run_uuid, generated_at, source_max_collected_at, expected_merchant_count, merchant_count, offer_count, row_count, content_sha256, published_at"
	date := func(name string) string {
		return "toDateTime64(" + adpickSQLString(fmt.Sprint(marker[name])) + ", 3, 'Asia/Seoul')"
	}
	query := fmt.Sprintf(`INSERT INTO %s (%s)
SELECT toUUID(%s), %s, toUInt64(%d), toUUID(%s), %s, %s,
       toUInt16(%d), toUInt16(%d), toUInt64(%d), toUInt64(%d), %s, %s
FROM (SELECT arraySort(groupArray(%s)) AS expected_rows FROM input(%s)) AS expected
CROSS JOIN (
 SELECT count() AS actual_count, uniqExact(item_key) AS actual_unique,
        countIf(record_type = 'merchant') AS actual_merchants,
        countIf(record_type = 'offer') AS actual_offers,
        arraySort(groupArray(%s)) AS actual_rows
 FROM (SELECT %s FROM %s
       WHERE collect_run_uuid = toUUID(%s) AND vertical = %s AND version = %d
       LIMIT %d)
) AS actual
WHERE %s
  AND throwIf(actual_count != %d OR actual_unique != %d
      OR actual_merchants != %d OR actual_offers != %d
      OR SHA256(toString(actual_rows)) != SHA256(toString(expected_rows))
      OR actual_rows != expected_rows, 'adpick_catalog_publication_parity') = 0
SETTINGS insert_deduplicate = 1, insert_deduplication_token = '%x', max_threads = 1, max_execution_time = 15
FORMAT JSONEachRow
`, adpickPublishedTable, markerColumns,
		adpickSQLString(fmt.Sprint(marker["refresh_uuid"])), adpickSQLString(first.Vertical), first.Version, adpickSQLString(first.CollectRunUUID), date("generated_at"), date("source_max_collected_at"),
		marker["expected_merchant_count"], marker["merchant_count"], marker["offer_count"], len(records), adpickSQLString(fmt.Sprint(marker["content_sha256"])), date("published_at"),
		rowTuple, adpickSQLString(adpickInputSchema), rowTuple, adpickInputColumns, adpickSnapshotTable,
		adpickSQLString(first.CollectRunUUID), adpickSQLString(first.Vertical), first.Version, len(records)+1,
		guard, len(records), len(records), marker["merchant_count"], marker["offer_count"], sha256.Sum256(identity))
	return append([]byte(query), payload.Bytes()...), nil
}

func publishAdpickCatalog(ctx context.Context, pub *ClickHouseRawPublisher, records []adpickCatalogRecord) error {
	if len(records) == 0 || len(records) > adpickMaximumQueries*20+8 {
		return fmt.Errorf("Adpick catalog publication exceeds bounded limit")
	}
	generatedAt := FormatCHDateTime64Millis(NowKST())
	rawRows, err := adpickCatalogRows(records, pub.cfg.ProducerSource, generatedAt, true)
	if err != nil {
		return err
	}
	if err := pub.insertJSONEachRow(ctx, adpickRawTable, rawRows); err != nil {
		return err
	}
	for _, vertical := range []string{"travel", "services"} {
		selected := []adpickCatalogRecord{}
		merchants, offers := 0, 0
		for _, record := range records {
			if record.Vertical != vertical {
				continue
			}
			selected = append(selected, record)
			if record.RecordType == "merchant" {
				merchants++
			} else {
				offers++
			}
		}
		if len(selected) == 0 {
			fmt.Printf("[adpick] no eligible merchants vertical=%s; prior catalog retained\n", vertical)
			continue
		}
		if merchants == 0 {
			return fmt.Errorf("Adpick publication missing verified merchants")
		}
		if err := verifyAdpickRecords(ctx, pub, adpickRawTable, selected); err != nil {
			return err
		}
		snapshotRows, err := adpickCatalogRows(selected, pub.cfg.ProducerSource, generatedAt, false)
		if err != nil {
			return err
		}
		if err := pub.insertJSONEachRow(ctx, adpickSnapshotTable, snapshotRows); err != nil {
			return err
		}
		if err := verifyAdpickRecords(ctx, pub, adpickSnapshotTable, selected); err != nil {
			return err
		}
		canonical, err := adpickCanonicalRecords(selected)
		if err != nil {
			return err
		}
		first := selected[0]
		sourceMaxCollectedAt := first.CollectedAt
		for _, record := range selected {
			if record.CollectedAt > sourceMaxCollectedAt {
				sourceMaxCollectedAt = record.CollectedAt
			}
		}
		marker := map[string]any{"refresh_uuid": NewUUIDv7(), "vertical": vertical, "version": first.Version, "collect_run_uuid": first.CollectRunUUID,
			"generated_at": generatedAt, "source_max_collected_at": sourceMaxCollectedAt, "published_at": FormatCHDateTime64Millis(NowKST()),
			"expected_merchant_count": merchants, "merchant_count": merchants, "offer_count": offers, "row_count": len(selected),
			"content_sha256": fmt.Sprintf("%x", sha256.Sum256(canonical))}
		body, err := adpickPublicationBody(pub, selected, marker)
		if err != nil {
			return err
		}
		if err := pub.postBodySingleAttempt(ctx, body); err != nil {
			return fmt.Errorf("Adpick publication marker failed: %w", err)
		}
		proofQuery := fmt.Sprintf("SELECT count() FROM %s WHERE refresh_uuid = toUUID('%s') FORMAT TabSeparated", adpickPublishedTable, marker["refresh_uuid"])
		proofQuery, err = pub.adpickGuardedRead(proofQuery)
		if err != nil {
			return err
		}
		proof, err := pub.queryBody(ctx, proofQuery, 64)
		if err != nil || strings.TrimSpace(string(proof)) != "1" {
			return fmt.Errorf("Adpick publication marker readback failed")
		}
		fmt.Printf("[adpick] catalog published vertical=%s merchants=%d offers=%d run_uuid=%s\n", vertical, merchants, offers, first.CollectRunUUID)
	}
	return nil
}
