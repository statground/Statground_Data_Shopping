package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

type countedReadCloser struct {
	reader io.Reader
	read   int
}

func (r *countedReadCloser) Read(payload []byte) (int, error) {
	count, err := r.reader.Read(payload)
	r.read += count
	return count, err
}

func (r *countedReadCloser) Close() error { return nil }

type rawRoundTripFunc func(*http.Request) (*http.Response, error)

func (f rawRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func rawPublisherForTest(server *httptest.Server) *ClickHouseRawPublisher {
	return &ClickHouseRawPublisher{
		cfg: ClickHouseRawConfig{
			URL:                    server.URL,
			User:                   "test",
			Password:               "secret",
			DirectEndpointHostname: "clickhouse-test",
			GmarketTable:           defaultGmarketRawInsertTable,
			KurlyTable:             defaultKurlyRawInsertTable,
			OutboxTable:            defaultShoppingRawOutboxTable,
			OutboxReplayLimit:      25,
			InsertChunkSize:        100,
			AttemptTimeout:         500 * time.Millisecond,
			RequestTimeout:         5 * time.Second,
			PreflightRetryBudget:   time.Second,
		},
		client:                   server.Client(),
		endpointIdentityVerified: true,
	}
}

func requestBody(t *testing.T, request *http.Request) string {
	t.Helper()
	body, err := io.ReadAll(request.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func bodyPayload(t *testing.T, body string) []byte {
	t.Helper()
	parts := strings.SplitN(body, "FORMAT JSONEachRow\n", 2)
	if len(parts) != 2 {
		t.Fatalf("body is missing JSONEachRow payload: %s", body)
	}
	return []byte(parts[1])
}

func TestCanonicalRawBatchUsesSameBytesAndTokenForReplay(t *testing.T) {
	rows := []clickHouseRawRow{{
		"product_code": "A-1",
		"text":         "<tag>&value",
		"version":      uint64(7),
		"event_uuid":   "01900000-0000-7000-8000-000000000001",
	}}
	batches, err := canonicalRawBatches(defaultGmarketRawInsertTable, rows, 100)
	if err != nil || len(batches) != 1 {
		t.Fatalf("batches=%d error=%v", len(batches), err)
	}
	batch := batches[0]
	if !strings.Contains(batch.rowsJSON, `\u003ctag\u003e\u0026value`) {
		t.Fatalf("canonical rows_json changed pre-existing Go JSON escaping: %s", batch.rowsJSON)
	}
	replayed, err := decodeRawOutboxPayload(defaultGmarketRawInsertTable, batch.rowsJSON, batch.token, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(batch.payload, replayed.payload) || batch.token != replayed.token {
		t.Fatalf("first and replay payloads differ\nfirst=%s\nreplay=%s", batch.payload, replayed.payload)
	}
}

func TestCanonicalRawBatchAcceptsConfiguredQuotedIdentifier(t *testing.T) {
	table := safeInsightIdentifierPath(defaultGmarketRawInsertTable)
	batches, err := canonicalRawBatches(table, []clickHouseRawRow{{
		"product_code": "A-quoted", "event_uuid": "01900000-0000-7000-8000-000000000031",
	}}, 100)
	if err != nil || len(batches) != 1 {
		t.Fatalf("quoted configured target batches=%d error=%v", len(batches), err)
	}
	if _, err := decodeRawOutboxPayload(table, batches[0].rowsJSON, batches[0].token, 1); err != nil {
		t.Fatalf("quoted configured target replay rejected: %v", err)
	}
	body, err := rawInsertBody(table, batches[0], true)
	if err != nil || !strings.HasPrefix(string(body), "INSERT INTO `Data_Shopping_Raw`.`gmarket_product_raw_endpoint_history`") {
		t.Fatalf("quoted insert body=%q error=%v", body, err)
	}
}

func TestCanonicalRawBatchesBoundRowsAndPayload(t *testing.T) {
	rows := []clickHouseRawRow{{"id": 1}, {"id": 2}, {"id": 3}}
	batches, err := canonicalRawBatches(defaultGmarketRawInsertTable, rows, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) != 2 || batches[0].rowCount != 2 || batches[1].rowCount != 1 {
		t.Fatalf("bounded batches=%#v", batches)
	}
	tooLarge := []clickHouseRawRow{{"value": strings.Repeat("x", maxRawOutboxPayloadBytes)}}
	if _, err := canonicalRawBatches(defaultGmarketRawInsertTable, tooLarge, 1); err == nil {
		t.Fatal("oversized JSONEachRow row was accepted")
	}
}

func TestDirectEndpointIdentityPrecedesTargetAndIsCached(t *testing.T) {
	requests := make([]string, 0, 3)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body := requestBody(t, request)
		requests = append(requests, body)
		if body == "SELECT hostName() FORMAT TabSeparatedRaw" {
			_, _ = writer.Write([]byte("clickhouse-s1-r1\n"))
			return
		}
		if strings.HasPrefix(body, "INSERT INTO "+defaultGmarketRawInsertTable+" ") {
			writer.WriteHeader(http.StatusOK)
			return
		}
		t.Errorf("unexpected request: %s", body)
		writer.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()
	pub := rawPublisherForTest(server)
	pub.cfg.DirectEndpointHostname = "clickhouse-s1-r1"
	pub.endpointIdentityVerified = false
	for _, eventUUID := range []string{
		"01900000-0000-7000-8000-000000000041",
		"01900000-0000-7000-8000-000000000042",
	} {
		batches, err := canonicalRawBatches(defaultGmarketRawInsertTable, []clickHouseRawRow{{
			"product_code": eventUUID, "event_uuid": eventUUID,
		}}, 100)
		if err != nil {
			t.Fatal(err)
		}
		if err := pub.insertRawBatchWithOutbox(context.Background(), defaultGmarketRawInsertTable, batches[0]); err != nil {
			t.Fatal(err)
		}
	}
	if len(requests) != 3 || requests[0] != "SELECT hostName() FORMAT TabSeparatedRaw" ||
		!strings.HasPrefix(requests[1], "INSERT INTO "+defaultGmarketRawInsertTable+" ") ||
		!strings.HasPrefix(requests[2], "INSERT INTO "+defaultGmarketRawInsertTable+" ") {
		t.Fatalf("request order=%q", requests)
	}
}

func TestDirectEndpointMismatchBlocksTargetAndOutbox(t *testing.T) {
	requests := make([]string, 0, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests = append(requests, requestBody(t, request))
		_, _ = writer.Write([]byte("clickhouse-s1-r2\n"))
	}))
	defer server.Close()
	pub := rawPublisherForTest(server)
	pub.cfg.DirectEndpointHostname = "clickhouse-s1-r1"
	pub.endpointIdentityVerified = false
	batches, err := canonicalRawBatches(defaultGmarketRawInsertTable, []clickHouseRawRow{{
		"product_code": "A-mismatch", "event_uuid": "01900000-0000-7000-8000-000000000043",
	}}, 100)
	if err != nil {
		t.Fatal(err)
	}
	err = pub.insertRawBatchWithOutbox(context.Background(), defaultGmarketRawInsertTable, batches[0])
	if !errors.Is(err, errDirectEndpointHostnameMismatch) || shouldRetryStreamingPublish(err) {
		t.Fatalf("endpoint mismatch error=%v retry=%t", err, shouldRetryStreamingPublish(err))
	}
	if len(requests) != 1 || requests[0] != "SELECT hostName() FORMAT TabSeparatedRaw" {
		t.Fatalf("mismatch requests=%q, want identity query only", requests)
	}
}

func TestDirectEndpointIdentityRequiresExactResponse(t *testing.T) {
	for _, response := range []string{
		"CLICKHOUSE-S1-R1\n",
		"clickhouse-s1-r1 \n",
		"clickhouse-s1-r1\nextra\n",
		"clickhouse-s1-r1",
	} {
		t.Run(fmt.Sprintf("response_%x", []byte(response)), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if body := requestBody(t, request); body != "SELECT hostName() FORMAT TabSeparatedRaw" {
					t.Fatalf("identity query=%q", body)
				}
				_, _ = writer.Write([]byte(response))
			}))
			defer server.Close()
			pub := rawPublisherForTest(server)
			pub.cfg.DirectEndpointHostname = "clickhouse-s1-r1"
			pub.endpointIdentityVerified = false
			if err := pub.ensureDirectEndpointIdentity(context.Background()); !errors.Is(err, errDirectEndpointHostnameMismatch) {
				t.Fatalf("response=%q error=%v", response, err)
			}
		})
	}
}

func TestRawOutboxReplayRejectsEndpointMismatchBeforeOutboxRead(t *testing.T) {
	requests := make([]string, 0, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests = append(requests, requestBody(t, request))
		_, _ = writer.Write([]byte("clickhouse-s1-r2\n"))
	}))
	defer server.Close()
	pub := rawPublisherForTest(server)
	pub.cfg.DirectEndpointHostname = "clickhouse-s1-r1"
	pub.endpointIdentityVerified = false
	err := pub.replayRawOutbox(context.Background())
	if !errors.Is(err, errDirectEndpointHostnameMismatch) {
		t.Fatalf("replay mismatch error=%v", err)
	}
	if len(requests) != 1 || requests[0] != "SELECT hostName() FORMAT TabSeparatedRaw" {
		t.Fatalf("replay mismatch requests=%q, want identity query only", requests)
	}
}

func TestDirectEndpointIdentityQueryDoesNotFollowHTTPRedirect(t *testing.T) {
	requests := 0
	redirected := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		_ = requestBody(t, request)
		if request.URL.Path == "/redirected" {
			redirected++
			_, _ = writer.Write([]byte("clickhouse-s1-r1\n"))
			return
		}
		http.Redirect(writer, request, "/redirected", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	pub := rawPublisherForTest(server)
	pub.cfg.DirectEndpointHostname = "clickhouse-s1-r1"
	pub.endpointIdentityVerified = false
	err := pub.ensureDirectEndpointIdentity(context.Background())
	var statusErr *clickHouseHTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.status != http.StatusTemporaryRedirect {
		t.Fatalf("identity redirect error=%T %v", err, err)
	}
	if requests != 1 || redirected != 0 {
		t.Fatalf("requests=%d redirected=%d, want no redirect follow", requests, redirected)
	}
}

func TestTransientRawInsertPreservesExactBatchInEndpointLocalOutbox(t *testing.T) {
	var mu sync.Mutex
	var targetBodies []string
	var outboxBody string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body := requestBody(t, request)
		mu.Lock()
		defer mu.Unlock()
		switch {
		case strings.HasPrefix(body, "INSERT INTO "+defaultGmarketRawInsertTable+" "):
			targetBodies = append(targetBodies, body)
			writer.WriteHeader(http.StatusServiceUnavailable)
			_, _ = writer.Write([]byte("Code: 252. DB::Exception: too many parts"))
		case strings.HasPrefix(body, "INSERT INTO "+defaultShoppingRawOutboxTable+" "):
			outboxBody = body
			writer.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request: %s", body)
			writer.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	pub := rawPublisherForTest(server)
	batches, err := canonicalRawBatches(defaultGmarketRawInsertTable, []clickHouseRawRow{{"product_code": "A-1", "text": "<&", "event_uuid": "01900000-0000-7000-8000-000000000002"}}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := pub.insertRawBatchWithOutbox(context.Background(), defaultGmarketRawInsertTable, batches[0]); err != nil {
		t.Fatal(err)
	}
	if len(targetBodies) != 1 {
		t.Fatalf("target attempts=%d, want one non-retried target request", len(targetBodies))
	}
	for _, body := range targetBodies {
		if !strings.Contains(body, "insert_distributed_sync = 1") || !strings.Contains(body, batches[0].token) {
			t.Fatalf("target insert is missing synchronous token settings: %s", body)
		}
	}
	if outboxBody == "" || strings.Contains(outboxBody, "insert_distributed_sync = 1") {
		t.Fatalf("endpoint-local outbox insert contract is wrong: %s", outboxBody)
	}
	var record struct {
		TargetTable        string `json:"target_table"`
		RowsJSON           string `json:"rows_json"`
		RowCount           int    `json:"row_count"`
		DeduplicationToken string `json:"deduplication_token"`
		SourceError        string `json:"source_error"`
	}
	if err := json.Unmarshal(bodyPayload(t, outboxBody), &record); err != nil {
		t.Fatal(err)
	}
	if record.TargetTable != defaultGmarketRawInsertTable || record.DeduplicationToken != batches[0].token || record.SourceError != "query_admission" {
		t.Fatalf("outbox record=%+v", record)
	}
	replay, err := decodeRawOutboxPayload(record.TargetTable, record.RowsJSON, record.DeduplicationToken, record.RowCount)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bodyPayload(t, targetBodies[0]), replay.payload) {
		t.Fatalf("outbox changed target bytes\ntarget=%s\noutbox=%s", bodyPayload(t, targetBodies[0]), replay.payload)
	}
}

func TestUnknownInsertStatusIsNotRetriedAndIsPreservedInOutbox(t *testing.T) {
	targetAttempts := 0
	outboxAttempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body := requestBody(t, request)
		switch {
		case strings.HasPrefix(body, "INSERT INTO "+defaultGmarketRawInsertTable+" "):
			targetAttempts++
			writer.WriteHeader(http.StatusInternalServerError)
			_, _ = writer.Write([]byte("Code: 319. DB::Exception: UNKNOWN_STATUS_OF_INSERT"))
		case strings.HasPrefix(body, "INSERT INTO "+defaultShoppingRawOutboxTable+" "):
			outboxAttempts++
			writer.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request: %s", body)
			writer.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	pub := rawPublisherForTest(server)
	batches, err := canonicalRawBatches(defaultGmarketRawInsertTable, []clickHouseRawRow{{
		"product_code": "A-319", "event_uuid": "01900000-0000-7000-8000-000000000032",
	}}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := pub.insertRawBatchWithOutbox(context.Background(), defaultGmarketRawInsertTable, batches[0]); err != nil {
		t.Fatal(err)
	}
	if targetAttempts != 1 || outboxAttempts != 1 {
		t.Fatalf("target attempts=%d outbox attempts=%d, want 1/1", targetAttempts, outboxAttempts)
	}
}

func TestHealthyRawInsertDoesNotWriteOutboxOrMutation(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		body := requestBody(t, request)
		if strings.Contains(body, "outbox") || strings.HasPrefix(body, "ALTER TABLE") {
			t.Errorf("healthy insert used recovery path: %s", body)
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	pub := rawPublisherForTest(server)
	batches, _ := canonicalRawBatches(defaultKurlyRawInsertTable, []clickHouseRawRow{{"product_code": "K-1", "event_uuid": "01900000-0000-7000-8000-000000000003"}}, 100)
	if err := pub.insertRawBatchWithOutbox(context.Background(), defaultKurlyRawInsertTable, batches[0]); err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("healthy request count=%d, want 1", requests)
	}
}

func TestNonTransientRawInsertNeverEntersOutbox(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		_ = requestBody(t, request)
		writer.WriteHeader(http.StatusInternalServerError)
		_, _ = writer.Write([]byte("Code: 60. DB::Exception: Unknown table"))
	}))
	defer server.Close()
	pub := rawPublisherForTest(server)
	batches, _ := canonicalRawBatches(defaultGmarketRawInsertTable, []clickHouseRawRow{{"product_code": "A-1", "event_uuid": "01900000-0000-7000-8000-000000000004"}}, 100)
	if err := pub.insertRawBatchWithOutbox(context.Background(), defaultGmarketRawInsertTable, batches[0]); err == nil {
		t.Fatal("non-transient target error was accepted")
	}
	if requests != 1 {
		t.Fatalf("non-transient requests=%d, want one target attempt and no outbox", requests)
	}
}

func TestRawInsertTransientClassifierIsClosedForContractsAndOpenForAmbiguousTransport(t *testing.T) {
	for _, err := range []error{
		context.DeadlineExceeded,
		io.ErrUnexpectedEOF,
		fmt.Errorf("dial tcp 127.0.0.1:8123: connect: connection refused"),
		&clickHouseHTTPStatusError{status: http.StatusServiceUnavailable, code: 252},
		&clickHouseHTTPStatusError{status: http.StatusBadRequest, code: 745},
	} {
		if !isRetryableRawInsertError(err) {
			t.Errorf("transient error was not retryable: %v", err)
		}
	}
	for _, err := range []error{
		context.Canceled,
		&clickHouseHTTPStatusError{status: http.StatusInternalServerError, code: 60},
		&clickHouseHTTPStatusError{status: http.StatusUnauthorized, code: 516},
	} {
		if isRetryableRawInsertError(err) {
			t.Errorf("contract error entered outbox path: %v", err)
		}
	}
	if !isAmbiguousRawInsertError(&rawDeliveryUnknownError{err: context.Canceled}) ||
		!isAmbiguousRawInsertError(&clickHouseHTTPStatusError{status: http.StatusInternalServerError, code: 319}) ||
		!isAmbiguousRawInsertError(&clickHouseHTTPStatusError{status: http.StatusInternalServerError, code: 735}) {
		t.Fatal("post-start cancellation or UNKNOWN_STATUS_OF_INSERT was not classified ambiguous")
	}
}

func TestRawOutboxPersistenceUsesCallerIndependentBudget(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		_ = requestBody(t, request)
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	pub := rawPublisherForTest(server)
	pub.cfg.AttemptTimeout = time.Nanosecond
	batches, err := canonicalRawBatches(defaultGmarketRawInsertTable, []clickHouseRawRow{{
		"product_code": "A-1",
		"event_uuid":   "01900000-0000-7000-8000-000000000099",
	}}, 100)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := pub.enqueueRawOutbox(ctx, defaultGmarketRawInsertTable, batches[0], context.DeadlineExceeded); err != nil {
		t.Fatalf("independent outbox persistence was clipped by target attempt budget: %v", err)
	}
	if requests != 1 {
		t.Fatalf("outbox requests=%d, want one", requests)
	}
}

func TestRawOutboxFailurePreservesOriginalAmbiguousTargetError(t *testing.T) {
	requests := 0
	pub := &ClickHouseRawPublisher{
		cfg: ClickHouseRawConfig{
			URL:                    "http://clickhouse.invalid/",
			User:                   "test",
			Password:               "secret",
			DirectEndpointHostname: "clickhouse-test",
			GmarketTable:           defaultGmarketRawInsertTable,
			KurlyTable:             defaultKurlyRawInsertTable,
			OutboxTable:            defaultShoppingRawOutboxTable,
			AttemptTimeout:         time.Second,
		},
		endpointIdentityVerified: true,
		client: &http.Client{Transport: rawRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			requests++
			if requests == 1 {
				return nil, context.DeadlineExceeded
			}
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       io.NopCloser(strings.NewReader("Code: 60. DB::Exception: Unknown table")),
				Header:     make(http.Header),
			}, nil
		})},
	}
	batches, err := canonicalRawBatches(defaultGmarketRawInsertTable, []clickHouseRawRow{{
		"product_code": "A-1",
		"event_uuid":   "01900000-0000-7000-8000-000000000098",
	}}, 100)
	if err != nil {
		t.Fatal(err)
	}
	err = pub.insertRawBatchWithOutbox(context.Background(), defaultGmarketRawInsertTable, batches[0])
	if err == nil || !isAmbiguousRawInsertError(err) {
		t.Fatalf("error=%v, want original target ambiguity preserved", err)
	}
	if requests != 2 {
		t.Fatalf("requests=%d, want one target and one outbox attempt", requests)
	}
}

func TestRawTargetInsertDoesNotFollowHTTPRedirect(t *testing.T) {
	requests := 0
	redirected := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		if request.URL.Path == "/redirected" {
			redirected++
			writer.WriteHeader(http.StatusOK)
			return
		}
		http.Redirect(writer, request, "/redirected", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	pub := rawPublisherForTest(server)
	batches, err := canonicalRawBatches(defaultGmarketRawInsertTable, []clickHouseRawRow{{
		"product_code": "A-1",
		"event_uuid":   "01900000-0000-7000-8000-000000000097",
	}}, 100)
	if err != nil {
		t.Fatal(err)
	}
	err = pub.insertRawBatchWithOutbox(context.Background(), defaultGmarketRawInsertTable, batches[0])
	var statusErr *clickHouseHTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.status != http.StatusTemporaryRedirect {
		t.Fatalf("error=%T %v, want redirect response without replay", err, err)
	}
	if requests != 1 || redirected != 0 {
		t.Fatalf("requests=%d redirected=%d, want exactly one target exchange", requests, redirected)
	}
}

func TestRawOutboxQueryBoundsAndSanitizesHTTPErrorBody(t *testing.T) {
	payload := []byte("Code: 319. " + strings.Repeat("secret-row-value", 10000))
	body := &countedReadCloser{reader: bytes.NewReader(payload)}
	pub := &ClickHouseRawPublisher{
		cfg: ClickHouseRawConfig{
			URL:            "http://clickhouse.invalid/",
			User:           "test",
			Password:       "secret",
			AttemptTimeout: time.Second,
		},
		client: &http.Client{Transport: rawRoundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       body,
				Header:     make(http.Header),
			}, nil
		})},
	}
	_, err := pub.queryBody(context.Background(), "SELECT 1", maxRawOutboxResponseBytes)
	var statusErr *clickHouseHTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.code != 319 || strings.Contains(err.Error(), "secret-row-value") {
		t.Fatalf("sanitized bounded query error=%T %v", err, err)
	}
	if body.read != maxClickHouseErrorBytes+1 {
		t.Fatalf("error body bytes read=%d, want bounded %d", body.read, maxClickHouseErrorBytes+1)
	}
}

func TestPendingRawOutboxHeadersUsesOneValidWhereClause(t *testing.T) {
	var query string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		query = requestBody(t, request)
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	pub := rawPublisherForTest(server)
	headers, err := pub.pendingRawOutboxHeaders(context.Background(), 26)
	if err != nil {
		t.Fatal(err)
	}
	if len(headers) != 0 {
		t.Fatalf("empty pending query returned %d headers", len(headers))
	}
	if strings.Count(query, "WHERE replayed_at IS NULL") != 1 || strings.Count(query, "\nWHERE ") != 1 {
		t.Fatalf("pending outbox query must contain exactly one WHERE clause: %s", query)
	}
	for _, fragment := range []string{
		"FROM " + defaultShoppingRawOutboxTable,
		"ORDER BY created_at ASC, outbox_uuid ASC",
		"LIMIT 26",
		"SETTINGS max_threads = 1, max_execution_time = 10",
		"FORMAT JSONEachRow",
	} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("pending outbox query is missing %q: %s", fragment, query)
		}
	}
}

func TestManualRawOutboxReplayUsesSameBytesAndOneAcknowledgementMutation(t *testing.T) {
	batches, err := canonicalRawBatches(defaultGmarketRawInsertTable, []clickHouseRawRow{{"product_code": "A-1", "text": "<&", "event_uuid": "01900000-0000-7000-8000-000000000005"}}, 100)
	if err != nil {
		t.Fatal(err)
	}
	batch := batches[0]
	outboxUUID := "01900000-0000-7000-8000-000000000011"
	var replayBody, mutationBody string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body := requestBody(t, request)
		switch {
		case strings.Contains(body, "toString(outbox_uuid) AS outbox_uuid"):
			_ = json.NewEncoder(writer).Encode(rawOutboxHeader{OutboxUUID: outboxUUID, TargetTable: defaultGmarketRawInsertTable, RowCount: 1, DeduplicationToken: batch.token})
		case strings.HasPrefix(body, "SELECT 1 FROM "+defaultGmarketRawInsertTable):
			writer.WriteHeader(http.StatusOK)
		case strings.HasPrefix(body, "SELECT rows_json"):
			_ = json.NewEncoder(writer).Encode(rawOutboxPayloadRecord{RowsJSON: batch.rowsJSON})
		case strings.Contains(body, "uniqExact(event_uuid) AS unique_events"):
			_, _ = writer.Write([]byte("{\"matched\":0,\"unique_events\":0}\n"))
		case strings.HasPrefix(body, "INSERT INTO "+defaultGmarketRawInsertTable+" "):
			replayBody = body
			writer.WriteHeader(http.StatusOK)
		case strings.HasPrefix(body, "ALTER TABLE "+defaultShoppingRawOutboxTable):
			mutationBody = body
			writer.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected replay request: %s", body)
			writer.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	pub := rawPublisherForTest(server)
	if err := pub.replayRawOutbox(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bodyPayload(t, replayBody), batch.payload) || !strings.Contains(replayBody, batch.token) || !strings.Contains(replayBody, "insert_distributed_sync = 1") {
		t.Fatalf("replay did not preserve first bytes/token: %s", replayBody)
	}
	for _, required := range []string{outboxUUID, "mutations_sync = 1", "UPDATE replayed_at"} {
		if !strings.Contains(mutationBody, required) {
			t.Fatalf("one acknowledgement mutation is missing %q: %s", required, mutationBody)
		}
	}
}

func TestManualRawOutboxReplayReconcilesAlreadyAcceptedBatchWithoutDuplicateInsert(t *testing.T) {
	batches, err := canonicalRawBatches(defaultGmarketRawInsertTable, []clickHouseRawRow{{
		"product_code": "A-2", "event_uuid": "01900000-0000-7000-8000-000000000012",
	}}, 100)
	if err != nil {
		t.Fatal(err)
	}
	batch := batches[0]
	outboxUUID := "01900000-0000-7000-8000-000000000013"
	targetInserts := 0
	mutations := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body := requestBody(t, request)
		switch {
		case strings.Contains(body, "toString(outbox_uuid) AS outbox_uuid"):
			_ = json.NewEncoder(writer).Encode(rawOutboxHeader{OutboxUUID: outboxUUID, TargetTable: defaultGmarketRawInsertTable, RowCount: 1, DeduplicationToken: batch.token})
		case strings.HasPrefix(body, "SELECT 1 FROM "+defaultGmarketRawInsertTable):
			writer.WriteHeader(http.StatusOK)
		case strings.HasPrefix(body, "SELECT rows_json"):
			_ = json.NewEncoder(writer).Encode(rawOutboxPayloadRecord{RowsJSON: batch.rowsJSON})
		case strings.Contains(body, "uniqExact(event_uuid) AS unique_events"):
			_, _ = writer.Write([]byte("{\"matched\":1,\"unique_events\":1}\n"))
		case strings.HasPrefix(body, "INSERT INTO "+defaultGmarketRawInsertTable+" "):
			targetInserts++
			writer.WriteHeader(http.StatusOK)
		case strings.HasPrefix(body, "ALTER TABLE "+defaultShoppingRawOutboxTable):
			mutations++
			writer.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected replay request: %s", body)
			writer.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	pub := rawPublisherForTest(server)
	if err := pub.replayRawOutbox(context.Background()); err != nil {
		t.Fatal(err)
	}
	if targetInserts != 0 || mutations != 1 {
		t.Fatalf("ambiguous-success reconcile inserted=%d mutations=%d, want 0/1", targetInserts, mutations)
	}
}

func TestRawOutboxReplayDrainsBoundedBatchThenFailsClosed(t *testing.T) {
	batches, _ := canonicalRawBatches(defaultKurlyRawInsertTable, []clickHouseRawRow{{"product_code": "K-1", "event_uuid": "01900000-0000-7000-8000-000000000006"}}, 100)
	batch := batches[0]
	headers := []rawOutboxHeader{
		{OutboxUUID: "01900000-0000-7000-8000-000000000021", TargetTable: defaultKurlyRawInsertTable, RowCount: 1, DeduplicationToken: batch.token},
		{OutboxUUID: "01900000-0000-7000-8000-000000000022", TargetTable: defaultKurlyRawInsertTable, RowCount: 1, DeduplicationToken: batch.token},
	}
	mutations := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body := requestBody(t, request)
		switch {
		case strings.Contains(body, "toString(outbox_uuid) AS outbox_uuid"):
			for _, header := range headers {
				_ = json.NewEncoder(writer).Encode(header)
			}
		case strings.HasPrefix(body, "SELECT 1 FROM "+defaultKurlyRawInsertTable):
			writer.WriteHeader(http.StatusOK)
		case strings.HasPrefix(body, "SELECT rows_json"):
			_ = json.NewEncoder(writer).Encode(rawOutboxPayloadRecord{RowsJSON: batch.rowsJSON})
		case strings.Contains(body, "uniqExact(event_uuid) AS unique_events"):
			_, _ = writer.Write([]byte("{\"matched\":0,\"unique_events\":0}\n"))
		case strings.HasPrefix(body, "INSERT INTO "+defaultKurlyRawInsertTable+" "):
			writer.WriteHeader(http.StatusOK)
		case strings.HasPrefix(body, "ALTER TABLE "+defaultShoppingRawOutboxTable):
			mutations++
			writer.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected replay request: %s", body)
			writer.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	pub := rawPublisherForTest(server)
	pub.cfg.OutboxReplayLimit = 1
	err := pub.replayRawOutbox(context.Background())
	if err == nil || !strings.Contains(err.Error(), "backlog exceeds bounded replay limit=1") {
		t.Fatalf("bounded replay error=%v", err)
	}
	if mutations != 1 {
		t.Fatalf("ack mutations=%d, want one", mutations)
	}
}

func TestShoppingWorkflowKeepsRawOutboxReplayManualAndBounded(t *testing.T) {
	source, err := os.ReadFile(".github/workflows/gmarket-crawl.yml")
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(source)
	if !strings.Contains(workflow, "group: statground-shopping-clickhouse-writer-replayer") ||
		!strings.Contains(workflow, "cancel-in-progress: false") ||
		strings.Contains(workflow, "cancel-in-progress: true") {
		t.Fatal("Shopping writers and replayers are not serialized without cancellation")
	}
	for _, required := range []string{
		"- raw_outbox_replay",
		"raw_outbox_replay_limit:",
		"github.event_name == 'workflow_dispatch' && github.event.inputs.run_mode == 'raw_outbox_replay'",
		`SHOPPING_RAW_OUTBOX_REPLAY_ONLY: "true"`,
		`SHOPPING_RAW_OUTBOX_REPLAY_ENABLED: "true"`,
		"SHOPPING_RAW_OUTBOX_REPLAY_LIMIT: ${{ github.event.inputs.raw_outbox_replay_limit || '25' }}",
		"Replay bounded Shopping raw outbox",
	} {
		if !strings.Contains(workflow, required) {
			t.Fatalf("workflow is missing %q", required)
		}
	}
	jobStart := strings.Index(workflow, "  raw_outbox_replay:\n")
	crawlStart := strings.Index(workflow, "  crawl:\n")
	if jobStart < 0 || crawlStart <= jobStart {
		t.Fatal("dedicated replay job is missing")
	}
	replayJob := workflow[jobStart:crawlStart]
	if strings.Index(replayJob, "Gate ClickHouse writes on storage pressure") > strings.Index(replayJob, "Replay bounded Shopping raw outbox") {
		t.Fatal("manual replay runs before the pressure gate")
	}
	if strings.Contains(replayJob, "schedule") {
		t.Fatal("raw outbox replay is not manual-only")
	}
	if got := strings.Count(replayJob, "SHOPPING_RAW_OUTBOX_REPLAY_ENABLED:"); got != 1 {
		t.Fatalf("manual replay enable count=%d, want 1", got)
	}
}

func Example_safeRawInsertErrorReason() {
	fmt.Println(safeRawInsertErrorReason(&clickHouseHTTPStatusError{status: 503, code: 252}))
	// Output: query_admission
}
