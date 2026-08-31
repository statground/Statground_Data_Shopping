package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	maxRawOutboxRows            = 1000
	maxRawOutboxReplayRows      = 100
	maxRawOutboxPayloadBytes    = 16 * 1024 * 1024
	maxRawOutboxHeaderBytes     = 2 * 1024 * 1024
	maxRawOutboxResponseBytes   = 2*maxRawOutboxPayloadBytes + 1024*1024
	maxClickHouseErrorBytes     = 64 * 1024
	rawOutboxPersistenceTimeout = 30 * time.Second
)

var (
	rawOutboxUUIDPattern     = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	rawLowerSHA256Pattern    = regexp.MustCompile(`^[0-9a-f]{64}$`)
	rawClickHouseCodePattern = regexp.MustCompile(`(?i)\bcode(?:\s*[:=])?\s*([0-9]+)\b`)
	rawQuotedTablePattern    = regexp.MustCompile("^`[A-Za-z0-9_]+`\\.`[A-Za-z0-9_]+`$")
)

type canonicalRawBatch struct {
	payload    []byte
	rowsJSON   string
	rowCount   int
	token      string
	eventUUIDs []string
}

type clickHouseHTTPStatusError struct {
	status int
	code   int
}

type rawDeliveryUnknownError struct {
	err error
}

func (e *rawDeliveryUnknownError) Error() string { return e.err.Error() }
func (e *rawDeliveryUnknownError) Unwrap() error { return e.err }

func (e *clickHouseHTTPStatusError) Error() string {
	if e.code != 0 {
		return fmt.Sprintf("clickhouse status=%d code=%d", e.status, e.code)
	}
	return fmt.Sprintf("clickhouse status=%d", e.status)
}

type rawOutboxHeader struct {
	OutboxUUID         string `json:"outbox_uuid"`
	TargetTable        string `json:"target_table"`
	RowCount           uint32 `json:"row_count"`
	DeduplicationToken string `json:"deduplication_token"`
}

type rawOutboxPayloadRecord struct {
	RowsJSON string `json:"rows_json"`
}

func boundedRawInt(raw string, fallback, minimum, maximum int) int {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value < minimum || value > maximum {
		return fallback
	}
	return value
}

func canonicalRawBatches(table string, rows []clickHouseRawRow, maxRows int) ([]canonicalRawBatch, error) {
	return canonicalRawBatchesWithLimit(table, rows, maxRows, maxRawOutboxPayloadBytes)
}

func canonicalRawBatchesWithLimit(table string, rows []clickHouseRawRow, maxRows, maxPayloadBytes int) ([]canonicalRawBatch, error) {
	if !validRawTableIdentifier(table) || len(rows) == 0 {
		return nil, fmt.Errorf("invalid target or empty batch")
	}
	if maxPayloadBytes < 2 || maxPayloadBytes > maxRawOutboxResponseBytes {
		return nil, fmt.Errorf("invalid JSONEachRow payload bound")
	}
	if maxRows < 1 || maxRows > maxRawOutboxRows {
		maxRows = maxRawOutboxRows
	}
	batches := make([]canonicalRawBatch, 0, (len(rows)+maxRows-1)/maxRows)
	lines := make([][]byte, 0, maxRows)
	payloadBytes := 0
	flush := func() error {
		if len(lines) == 0 {
			return nil
		}
		var payload bytes.Buffer
		var array bytes.Buffer
		array.WriteByte('[')
		for index, line := range lines {
			if index > 0 {
				array.WriteByte(',')
			}
			payload.Write(line)
			payload.WriteByte('\n')
			array.Write(line)
		}
		array.WriteByte(']')
		payloadCopy := append([]byte(nil), payload.Bytes()...)
		token := fmt.Sprintf("%x", sha256.Sum256(append([]byte(table+"\x1f"), payloadCopy...)))
		batches = append(batches, canonicalRawBatch{
			payload: payloadCopy, rowsJSON: array.String(), rowCount: len(lines), token: token,
		})
		lines = lines[:0]
		payloadBytes = 0
		return nil
	}
	for _, row := range rows {
		line, err := json.Marshal(row)
		if err != nil {
			return nil, err
		}
		if len(line)+2 > maxPayloadBytes {
			return nil, fmt.Errorf("one JSONEachRow row exceeds the bounded payload")
		}
		if len(lines) > 0 && (len(lines) >= maxRows || payloadBytes+len(line)+2 > maxPayloadBytes) {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		lines = append(lines, append([]byte(nil), line...))
		payloadBytes += len(line) + 1
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return batches, nil
}

func decodeRawOutboxPayload(table, rowsJSON, expectedToken string, expectedRows int) (canonicalRawBatch, error) {
	if !validRawTableIdentifier(table) || len(rowsJSON) == 0 || len(rowsJSON) > maxRawOutboxPayloadBytes ||
		expectedRows < 1 || expectedRows > maxRawOutboxRows || !rawLowerSHA256Pattern.MatchString(expectedToken) {
		return canonicalRawBatch{}, fmt.Errorf("outbox payload is outside the bounded contract")
	}
	var rows []json.RawMessage
	if err := json.Unmarshal([]byte(rowsJSON), &rows); err != nil {
		return canonicalRawBatch{}, err
	}
	if len(rows) != expectedRows {
		return canonicalRawBatch{}, fmt.Errorf("outbox row count mismatch")
	}
	eventUUIDs, err := rawEventUUIDs(rows)
	if err != nil {
		return canonicalRawBatch{}, err
	}
	var payload bytes.Buffer
	for _, row := range rows {
		if len(row) == 0 || row[0] != '{' || !json.Valid(row) {
			return canonicalRawBatch{}, fmt.Errorf("outbox row is not a valid JSON object")
		}
		payload.Write(row)
		payload.WriteByte('\n')
	}
	if payload.Len() > maxRawOutboxPayloadBytes {
		return canonicalRawBatch{}, fmt.Errorf("outbox JSONEachRow payload exceeds 16 MiB")
	}
	payloadCopy := append([]byte(nil), payload.Bytes()...)
	token := fmt.Sprintf("%x", sha256.Sum256(append([]byte(table+"\x1f"), payloadCopy...)))
	if token != expectedToken {
		return canonicalRawBatch{}, fmt.Errorf("outbox payload token mismatch")
	}
	return canonicalRawBatch{payload: payloadCopy, rowsJSON: rowsJSON, rowCount: expectedRows, token: token, eventUUIDs: eventUUIDs}, nil
}

func rawEventUUIDs(rows []json.RawMessage) ([]string, error) {
	seen := make(map[string]struct{}, len(rows))
	eventUUIDs := make([]string, 0, len(rows))
	for _, raw := range rows {
		var identity struct {
			EventUUID string `json:"event_uuid"`
		}
		if err := json.Unmarshal(raw, &identity); err != nil {
			return nil, err
		}
		identity.EventUUID = strings.TrimSpace(identity.EventUUID)
		if !rawOutboxUUIDPattern.MatchString(identity.EventUUID) {
			return nil, fmt.Errorf("raw outbox row has no valid event_uuid")
		}
		if _, duplicate := seen[identity.EventUUID]; duplicate {
			return nil, fmt.Errorf("raw outbox batch contains duplicate event_uuid")
		}
		seen[identity.EventUUID] = struct{}{}
		eventUUIDs = append(eventUUIDs, identity.EventUUID)
	}
	return eventUUIDs, nil
}

func rawEventUUIDsFromPayload(payload []byte, expectedRows int) ([]string, error) {
	if len(payload) == 0 || payload[len(payload)-1] != '\n' {
		return nil, fmt.Errorf("JSONEachRow payload is not newline terminated")
	}
	lines := bytes.Split(payload[:len(payload)-1], []byte{'\n'})
	if len(lines) != expectedRows {
		return nil, fmt.Errorf("JSONEachRow payload row count mismatch")
	}
	rows := make([]json.RawMessage, 0, len(lines))
	for _, line := range lines {
		rows = append(rows, json.RawMessage(line))
	}
	return rawEventUUIDs(rows)
}

func rawInsertBody(table string, batch canonicalRawBatch, distributedSync bool) ([]byte, error) {
	return rawInsertBodyWithLimit(table, batch, distributedSync, maxRawOutboxPayloadBytes)
}

func rawInsertBodyWithLimit(table string, batch canonicalRawBatch, distributedSync bool, maxPayloadBytes int) ([]byte, error) {
	if !validRawTableIdentifier(table) || batch.rowCount < 1 || batch.rowCount > maxRawOutboxRows ||
		len(batch.payload) == 0 || len(batch.payload) > maxPayloadBytes || !rawLowerSHA256Pattern.MatchString(batch.token) {
		return nil, fmt.Errorf("invalid bounded ClickHouse insert batch")
	}
	settings := "insert_deduplicate = 1, insert_deduplication_token = '" + batch.token + "'"
	if distributedSync {
		settings += ", insert_distributed_sync = 1"
	}
	query := "INSERT INTO " + table + " SETTINGS " + settings + " FORMAT JSONEachRow\n"
	body := make([]byte, 0, len(query)+len(batch.payload))
	body = append(body, query...)
	body = append(body, batch.payload...)
	return body, nil
}

func validRawTableIdentifier(table string) bool {
	return safeInsightIdentifierPath(table) != "" || rawQuotedTablePattern.MatchString(strings.TrimSpace(table))
}

func (p *ClickHouseRawPublisher) insertRawBatchWithOutbox(ctx context.Context, table string, batch canonicalRawBatch) error {
	if err := p.ensureDirectEndpointIdentity(ctx); err != nil {
		return err
	}
	eventUUIDs, err := rawEventUUIDsFromPayload(batch.payload, batch.rowCount)
	if err != nil {
		return fmt.Errorf("raw batch identity rejected: %w", err)
	}
	batch.eventUUIDs = eventUUIDs
	body, err := rawInsertBody(table, batch, true)
	if err != nil {
		return err
	}
	targetErr := p.postBodySingleAttempt(ctx, body)
	if targetErr == nil {
		return nil
	}
	if !isRetryableRawInsertError(targetErr) && !isAmbiguousRawInsertError(targetErr) {
		return targetErr
	}
	outboxCtx, cancel := context.WithTimeout(context.Background(), rawOutboxPersistenceTimeout)
	defer cancel()
	if err := p.enqueueRawOutbox(outboxCtx, table, batch, targetErr); err != nil {
		return fmt.Errorf("target transient and raw outbox persistence failed: target=%w outbox=%w", targetErr, err)
	}
	fmt.Printf("[clickhouse] preserved raw batch in endpoint-local outbox table=%s target=%s rows=%d token=%s\n", p.cfg.OutboxTable, table, batch.rowCount, batch.token)
	return nil
}

func (p *ClickHouseRawPublisher) enqueueRawOutbox(ctx context.Context, table string, batch canonicalRawBatch, sourceErr error) error {
	row := clickHouseRawRow{
		"outbox_uuid":         NewUUIDv7(),
		"created_at":          FormatCHDateTime64Millis(NowKST()),
		"target_table":        table,
		"rows_json":           batch.rowsJSON,
		"row_count":           batch.rowCount,
		"deduplication_token": batch.token,
		"source_error":        safeRawInsertErrorReason(sourceErr),
	}
	batches, err := canonicalRawBatchesWithLimit(p.cfg.OutboxTable, []clickHouseRawRow{row}, 1, maxRawOutboxResponseBytes)
	if err != nil || len(batches) != 1 {
		if err == nil {
			err = fmt.Errorf("outbox row encoding did not produce one batch")
		}
		return err
	}
	body, err := rawInsertBodyWithLimit(p.cfg.OutboxTable, batches[0], false, maxRawOutboxResponseBytes)
	if err != nil {
		return err
	}
	// The caller supplies an independent bounded persistence context. Do not
	// reapply the target attempt timeout here: it may already have expired.
	return p.postBodyOnce(ctx, body)
}

func (p *ClickHouseRawPublisher) postBodySingleAttempt(ctx context.Context, body []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	attemptCtx, cancel := context.WithTimeout(ctx, p.cfg.AttemptTimeout)
	defer cancel()
	return p.postBodyOnce(attemptCtx, body)
}

func (p *ClickHouseRawPublisher) postBodyOnce(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.SetBasicAuth(p.cfg.User, p.cfg.Password)
	requestClient := *p.client
	requestClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	resp, err := requestClient.Do(req)
	if err != nil {
		return &rawDeliveryUnknownError{err: err}
	}
	defer resp.Body.Close()
	responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxClickHouseErrorBytes+1))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if readErr != nil {
			return &rawDeliveryUnknownError{err: readErr}
		}
		if len(responseBody) > maxClickHouseErrorBytes {
			return &rawDeliveryUnknownError{err: io.ErrUnexpectedEOF}
		}
		return nil
	}
	if readErr != nil {
		return &rawDeliveryUnknownError{err: readErr}
	}
	return &clickHouseHTTPStatusError{status: resp.StatusCode, code: rawClickHouseErrorCode(string(responseBody))}
}

func (p *ClickHouseRawPublisher) replayRawOutbox(ctx context.Context) error {
	if err := p.ensureDirectEndpointIdentity(ctx); err != nil {
		return fmt.Errorf("shopping raw outbox endpoint identity rejected: %w", err)
	}
	limit := p.cfg.OutboxReplayLimit
	if limit < 1 || limit > maxRawOutboxReplayRows {
		limit = 25
	}
	headers, err := p.pendingRawOutboxHeaders(ctx, limit+1)
	if err != nil {
		return fmt.Errorf("shopping raw outbox replay query failed: %w", err)
	}
	if len(headers) == 0 {
		fmt.Println("[clickhouse] shopping raw outbox has no pending batches")
		return nil
	}
	overLimit := len(headers) > limit
	if overLimit {
		headers = headers[:limit]
	}
	health := make(map[string]bool)
	replayed := make([]string, 0, len(headers))
	markThenReturn := func(replayErr error) error {
		if len(replayed) > 0 {
			if markErr := p.markRawOutboxReplayed(replayed); markErr != nil {
				return fmt.Errorf("shopping raw outbox acknowledgement failed: %w", markErr)
			}
		}
		return replayErr
	}
	for _, header := range headers {
		outboxUUID := strings.TrimSpace(header.OutboxUUID)
		target := strings.TrimSpace(header.TargetTable)
		token := strings.TrimSpace(header.DeduplicationToken)
		if !rawOutboxUUIDPattern.MatchString(outboxUUID) || !rawLowerSHA256Pattern.MatchString(token) || !p.allowedRawReplayTarget(target) {
			return markThenReturn(fmt.Errorf("shopping raw outbox contains a non-allowlisted record"))
		}
		if !health[target] {
			if err := p.checkRawReplayTarget(ctx, target); err != nil {
				return markThenReturn(fmt.Errorf("shopping raw outbox target gate failed: %w", err))
			}
			health[target] = true
		}
		rowsJSON, err := p.fetchRawOutboxPayload(ctx, outboxUUID)
		if err != nil {
			return markThenReturn(fmt.Errorf("shopping raw outbox payload query failed: %w", err))
		}
		batch, err := decodeRawOutboxPayload(target, rowsJSON, token, int(header.RowCount))
		if err != nil {
			return markThenReturn(fmt.Errorf("shopping raw outbox payload rejected: %w", err))
		}
		alreadyAccepted, err := p.rawTargetBatchState(ctx, target, batch.eventUUIDs)
		if err != nil {
			return markThenReturn(fmt.Errorf("shopping raw outbox target reconciliation failed: %w", err))
		}
		if !alreadyAccepted {
			body, err := rawInsertBody(target, batch, true)
			if err != nil {
				return markThenReturn(err)
			}
			if err := p.postBodySingleAttempt(ctx, body); err != nil {
				return markThenReturn(fmt.Errorf("shopping raw outbox target replay failed: %w", err))
			}
		}
		replayed = append(replayed, outboxUUID)
	}
	if err := markThenReturn(nil); err != nil {
		return err
	}
	if overLimit {
		return fmt.Errorf("shopping raw outbox backlog exceeds bounded replay limit=%d; rerun manual replay", limit)
	}
	fmt.Printf("[clickhouse] shopping raw outbox replayed batches=%d\n", len(replayed))
	return nil
}

func isAmbiguousRawInsertError(err error) bool {
	if err == nil {
		return false
	}
	var deliveryErr *rawDeliveryUnknownError
	if errors.As(err, &deliveryErr) {
		return true
	}
	var statusErr *clickHouseHTTPStatusError
	if !errors.As(err, &statusErr) {
		return false
	}
	switch statusErr.code {
	case 319, 394, 677, 734, 735:
		return true
	default:
		return false
	}
}

func (p *ClickHouseRawPublisher) pendingRawOutboxHeaders(ctx context.Context, limit int) ([]rawOutboxHeader, error) {
	sql := fmt.Sprintf(`SELECT
    toString(outbox_uuid) AS outbox_uuid,
    target_table,
    row_count,
    deduplication_token
FROM %s
WHERE replayed_at IS NULL
ORDER BY created_at ASC, outbox_uuid ASC
LIMIT %d
SETTINGS max_threads = 1, max_execution_time = 10
FORMAT JSONEachRow`, p.cfg.OutboxTable, limit)
	body, err := p.queryBody(ctx, sql, maxRawOutboxHeaderBytes)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	headers := make([]rawOutboxHeader, 0, limit)
	for {
		var header rawOutboxHeader
		if err := decoder.Decode(&header); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}
		headers = append(headers, header)
	}
	return headers, nil
}

func (p *ClickHouseRawPublisher) fetchRawOutboxPayload(ctx context.Context, outboxUUID string) (string, error) {
	if !rawOutboxUUIDPattern.MatchString(outboxUUID) {
		return "", fmt.Errorf("invalid outbox UUID")
	}
	sql := fmt.Sprintf(`SELECT rows_json
FROM %s
WHERE outbox_uuid = toUUID('%s') AND replayed_at IS NULL
ORDER BY created_at ASC
LIMIT 1
SETTINGS max_threads = 1, max_execution_time = 10
FORMAT JSONEachRow`, p.cfg.OutboxTable, outboxUUID)
	body, err := p.queryBody(ctx, sql, maxRawOutboxResponseBytes)
	if err != nil {
		return "", err
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	var record rawOutboxPayloadRecord
	if err := decoder.Decode(&record); err != nil {
		return "", err
	}
	var trailing rawOutboxPayloadRecord
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("outbox payload query returned multiple records")
	}
	return record.RowsJSON, nil
}

func (p *ClickHouseRawPublisher) queryBody(ctx context.Context, sql string, maximum int64) ([]byte, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, p.cfg.AttemptTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, p.cfg.URL, strings.NewReader(sql))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(p.cfg.User, p.cfg.Password)
	requestClient := *p.client
	requestClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	resp, err := requestClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	readLimit := maximum + 1
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		readLimit = maxClickHouseErrorBytes + 1
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, readLimit))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &clickHouseHTTPStatusError{status: resp.StatusCode, code: rawClickHouseErrorCode(string(body))}
	}
	if readErr != nil {
		return nil, readErr
	}
	if int64(len(body)) > maximum {
		return nil, fmt.Errorf("clickhouse response exceeds bounded limit")
	}
	return body, nil
}

func (p *ClickHouseRawPublisher) checkRawReplayTarget(ctx context.Context, target string) error {
	if !p.allowedRawReplayTarget(target) {
		return fmt.Errorf("target is not allowlisted")
	}
	attemptCtx, cancel := context.WithTimeout(ctx, p.cfg.AttemptTimeout)
	defer cancel()
	return p.exec(attemptCtx, "SELECT 1 FROM "+target+" LIMIT 0 SETTINGS max_threads = 1, max_execution_time = 5")
}

func (p *ClickHouseRawPublisher) rawTargetBatchState(ctx context.Context, target string, eventUUIDs []string) (bool, error) {
	if !p.allowedRawReplayTarget(target) || len(eventUUIDs) < 1 || len(eventUUIDs) > maxRawOutboxRows {
		return false, fmt.Errorf("invalid raw replay identity batch")
	}
	values := make([]string, 0, len(eventUUIDs))
	seen := make(map[string]struct{}, len(eventUUIDs))
	for _, eventUUID := range eventUUIDs {
		if !rawOutboxUUIDPattern.MatchString(eventUUID) {
			return false, fmt.Errorf("invalid event UUID")
		}
		if _, duplicate := seen[eventUUID]; duplicate {
			return false, fmt.Errorf("duplicate event UUID")
		}
		seen[eventUUID] = struct{}{}
		values = append(values, "toUUID('"+eventUUID+"')")
	}
	sql := fmt.Sprintf(`SELECT
    count() AS matched,
    uniqExact(event_uuid) AS unique_events
FROM %s
WHERE event_uuid IN (%s)
SETTINGS max_threads = 1, max_execution_time = 10
FORMAT JSONEachRow`, target, strings.Join(values, ", "))
	body, err := p.queryBody(ctx, sql, maxRawOutboxHeaderBytes)
	if err != nil {
		return false, err
	}
	var state struct {
		Matched      uint64 `json:"matched"`
		UniqueEvents uint64 `json:"unique_events"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&state); err != nil {
		return false, err
	}
	want := uint64(len(eventUUIDs))
	if state.Matched == 0 && state.UniqueEvents == 0 {
		return false, nil
	}
	if state.Matched == want && state.UniqueEvents == want {
		return true, nil
	}
	return false, fmt.Errorf("partial or duplicate event_uuid state matched=%d unique=%d expected=%d", state.Matched, state.UniqueEvents, want)
}

func (p *ClickHouseRawPublisher) allowedRawReplayTarget(target string) bool {
	return target == p.cfg.GmarketTable || target == p.cfg.KurlyTable
}

func (p *ClickHouseRawPublisher) markRawOutboxReplayed(outboxUUIDs []string) error {
	if len(outboxUUIDs) == 0 {
		return nil
	}
	if len(outboxUUIDs) > maxRawOutboxReplayRows {
		return fmt.Errorf("raw outbox acknowledgement exceeds bounded limit")
	}
	values := make([]string, 0, len(outboxUUIDs))
	for _, value := range outboxUUIDs {
		if !rawOutboxUUIDPattern.MatchString(value) {
			return fmt.Errorf("invalid outbox UUID")
		}
		values = append(values, "toUUID('"+value+"')")
	}
	// Recovery is manual-only and bounded. One synchronous mutation acknowledges
	// the whole run; healthy first inserts never execute an outbox mutation.
	sql := fmt.Sprintf(`ALTER TABLE %s
UPDATE replayed_at = now64(3, 'Asia/Seoul')
WHERE outbox_uuid IN (%s)
SETTINGS mutations_sync = 1`, p.cfg.OutboxTable, strings.Join(values, ", "))
	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.AttemptTimeout)
	defer cancel()
	return p.exec(ctx, sql)
}

func isRetryableRawInsertError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var networkError net.Error
	if errors.As(err, &networkError) && (networkError.Timeout() || networkError.Temporary()) {
		return true
	}
	message := strings.ToLower(err.Error())
	if code := rawClickHouseErrorCode(message); code != 0 {
		switch code {
		case 6, 27, 47, 60, 62, 81, 497, 516:
			return false
		case 159, 164, 202, 203, 209, 210, 225, 241, 242, 243, 244, 252, 254, 255,
			265, 279, 285, 286, 289, 297, 319, 364, 369, 384, 394, 410, 415, 416,
			425, 439, 473, 519, 574, 667, 677, 692, 700, 722, 733, 734, 735, 738,
			745, 749, 762, 999:
			return true
		}
	}
	for _, marker := range []string{
		"timeout", "deadline", "connection reset", "connection refused", "connection aborted",
		"broken pipe", "unexpected eof", "not initialized", "keeper", "coordination",
		"readonly", "read-only", "temporarily unavailable", "status=408", "status=429",
		"status=500", "status=502", "status=503", "status=504",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func safeRawInsertErrorReason(err error) string {
	if err == nil {
		return "request_failed"
	}
	message := strings.ToLower(err.Error())
	if code := rawClickHouseErrorCode(message); code != 0 {
		switch code {
		case 497, 516:
			return "auth_or_permission"
		case 6, 27, 47, 60, 62, 81:
			return "query_rejected"
		case 202, 241, 242, 252:
			return "query_admission"
		}
	}
	for _, marker := range []string{"timeout", "deadline"} {
		if strings.Contains(message, marker) {
			return "transport_timeout"
		}
	}
	for _, marker := range []string{"connection reset", "connection refused", "connection aborted", "broken pipe", "unexpected eof"} {
		if strings.Contains(message, marker) {
			return "transport_interrupted"
		}
	}
	var statusErr *clickHouseHTTPStatusError
	if errors.As(err, &statusErr) {
		switch statusErr.status {
		case 408:
			return "read_timeout"
		case 429:
			return "query_admission"
		case 500, 502, 503, 504:
			return "server_unavailable"
		}
	}
	return "request_failed"
}

func rawClickHouseErrorCode(message string) int {
	matches := rawClickHouseCodePattern.FindStringSubmatch(message)
	if len(matches) != 2 {
		return 0
	}
	code, _ := strconv.Atoi(matches[1])
	return code
}
