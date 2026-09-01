package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

type recordingRowPublisher struct {
	batches [][]Row
	fail    bool
}

type fixedErrorRowPublisher struct {
	calls int
	err   error
}

func (p *fixedErrorRowPublisher) Publish([]Row) error {
	p.calls++
	return p.err
}

func (p *recordingRowPublisher) Publish(rows []Row) error {
	if p.fail {
		return errors.New("temporary publish failure")
	}
	p.batches = append(p.batches, append([]Row(nil), rows...))
	return nil
}

func TestBufferedRowPublisherFlushesConfiguredBatch(t *testing.T) {
	recorder := &recordingRowPublisher{}
	batcher := NewBufferedRowPublisher(recorder, 2)
	if err := batcher.Add(Row{"상품코드": "1"}); err != nil {
		t.Fatal(err)
	}
	if len(recorder.batches) != 0 {
		t.Fatalf("unexpected early publish: %d batches", len(recorder.batches))
	}
	if err := batcher.Add(Row{"상품코드": "2"}); err != nil {
		t.Fatal(err)
	}
	if len(recorder.batches) != 1 || len(recorder.batches[0]) != 2 {
		t.Fatalf("published batches = %#v", recorder.batches)
	}
}

func TestBufferedRowPublisherRetainsRowsAfterFailure(t *testing.T) {
	recorder := &recordingRowPublisher{fail: true}
	batcher := NewBufferedRowPublisher(recorder, 2)
	_ = batcher.Add(Row{"상품코드": "1"})
	_ = batcher.Add(Row{"상품코드": "2"})
	_ = batcher.Add(Row{"상품코드": "3"})
	if err := batcher.Flush(); err == nil {
		t.Fatal("expected flush failure")
	}
	if got := len(batcher.PendingRows()); got != 3 {
		t.Fatalf("pending rows = %d, want 3", got)
	}
	recorder.fail = false
	if err := batcher.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := len(batcher.PendingRows()); got != 0 {
		t.Fatalf("pending rows after recovery = %d", got)
	}
}

func TestBufferedRowPublisherDoesNotRetryAmbiguousFailureOnFlush(t *testing.T) {
	publisher := &fixedErrorRowPublisher{err: &rawDeliveryUnknownError{err: context.DeadlineExceeded}}
	batcher := NewBufferedRowPublisher(publisher, 2)
	_ = batcher.Add(Row{"상품코드": "1"})
	_ = batcher.Add(Row{"상품코드": "2"})
	if err := batcher.Flush(); !isAmbiguousRawInsertError(err) {
		t.Fatalf("flush error=%v, want preserved ambiguous failure", err)
	}
	if publisher.calls != 1 {
		t.Fatalf("publisher calls=%d, want no immediate retry after ambiguous delivery", publisher.calls)
	}
	if shouldRetryStreamingPublish(publisher.err) {
		t.Fatal("ambiguous streaming publish entered caller retry path")
	}
}

func TestClickHousePreflightRetryConfigDefaultsAndOverrides(t *testing.T) {
	for name, value := range map[string]string{
		"CLICKHOUSE_HOST":                            "clickhouse.example.invalid",
		"CLICKHOUSE_PORT":                            "8123",
		"CLICKHOUSE_USER":                            "test",
		"CLICKHOUSE_PASSWORD":                        "secret",
		"CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME":        "clickhouse-s1-r1",
		"CLICKHOUSE_PREFLIGHT_RETRY_BUDGET_SECONDS":  "",
		"CLICKHOUSE_PREFLIGHT_RETRY_BACKOFF_SECONDS": "",
	} {
		t.Setenv(name, value)
	}
	pub, err := NewClickHouseRawPublisherFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if pub.cfg.PreflightRetryBudget != 90*time.Second || pub.cfg.PreflightRetryBackoff != 5*time.Second {
		t.Fatalf("preflight config=%s/%s, want 90s/5s", pub.cfg.PreflightRetryBudget, pub.cfg.PreflightRetryBackoff)
	}
	if pub.cfg.DirectEndpointHostname != "clickhouse-s1-r1" {
		t.Fatalf("direct endpoint hostname=%q", pub.cfg.DirectEndpointHostname)
	}
	if pub.cfg.OutboxTable != safeInsightIdentifierPath(defaultShoppingRawOutboxTable) || pub.cfg.OutboxReplayEnabled || pub.cfg.OutboxReplayLimit != 25 || pub.cfg.InsertChunkSize != 100 {
		t.Fatalf("outbox defaults=%q enabled=%t limit=%d chunk=%d", pub.cfg.OutboxTable, pub.cfg.OutboxReplayEnabled, pub.cfg.OutboxReplayLimit, pub.cfg.InsertChunkSize)
	}
	t.Setenv("CLICKHOUSE_PREFLIGHT_RETRY_BUDGET_SECONDS", "120")
	t.Setenv("CLICKHOUSE_PREFLIGHT_RETRY_BACKOFF_SECONDS", "3")
	pub, err = NewClickHouseRawPublisherFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if pub.cfg.PreflightRetryBudget != 120*time.Second || pub.cfg.PreflightRetryBackoff != 3*time.Second {
		t.Fatalf("overridden preflight config=%s/%s, want 120s/3s", pub.cfg.PreflightRetryBudget, pub.cfg.PreflightRetryBackoff)
	}
	t.Setenv("SHOPPING_RAW_OUTBOX_REPLAY_ENABLED", "true")
	t.Setenv("SHOPPING_RAW_OUTBOX_REPLAY_LIMIT", "101")
	t.Setenv("CLICKHOUSE_DIRECT_INSERT_CHUNK_SIZE", "1001")
	pub, err = NewClickHouseRawPublisherFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if !pub.cfg.OutboxReplayEnabled || pub.cfg.OutboxReplayLimit != 25 || pub.cfg.InsertChunkSize != 100 {
		t.Fatalf("bounded overrides enabled=%t replay=%d chunk=%d", pub.cfg.OutboxReplayEnabled, pub.cfg.OutboxReplayLimit, pub.cfg.InsertChunkSize)
	}
}

func TestClickHouseDirectEndpointHostnameConfigIsRequiredAndSafe(t *testing.T) {
	for name, value := range map[string]string{
		"CLICKHOUSE_HOST":     "clickhouse.example.invalid",
		"CLICKHOUSE_PORT":     "8123",
		"CLICKHOUSE_USER":     "test",
		"CLICKHOUSE_PASSWORD": "secret",
	} {
		t.Setenv(name, value)
	}
	t.Setenv("CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME", "")
	if _, err := NewClickHouseRawPublisherFromEnv(); err == nil || !strings.Contains(err.Error(), "missing ClickHouse env") {
		t.Fatalf("missing endpoint hostname error=%v", err)
	}

	for _, invalid := range []string{
		" clickhouse-s1-r1",
		"clickhouse-s1-r1 ",
		"clickhouse/s1/r1",
		"clickhouse's1r1",
		"-clickhouse-s1-r1",
		"clickhouse-s1-r1-",
		"clickhouse-cluster-gateway",
		"Clickhouse_Cluster_Gateway",
		"s1-GATEWAY-r1",
		strings.Repeat("a", maximumDirectEndpointHostnameBytes+1),
	} {
		t.Setenv("CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME", invalid)
		if _, err := NewClickHouseRawPublisherFromEnv(); err == nil || !strings.Contains(err.Error(), "invalid CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME") {
			t.Fatalf("unsafe endpoint hostname %q error=%v", invalid, err)
		}
	}

	for _, valid := range []string{"clickhouse-s1-r1", "Clickhouse_S1_R1", "10da13a53a1e", "s1-r1.db.internal"} {
		t.Setenv("CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME", valid)
		pub, err := NewClickHouseRawPublisherFromEnv()
		if err != nil {
			t.Fatalf("safe endpoint hostname %q error=%v", valid, err)
		}
		if pub.cfg.DirectEndpointHostname != valid {
			t.Fatalf("safe endpoint hostname config=%q, want %q", pub.cfg.DirectEndpointHostname, valid)
		}
	}
}

func TestClickHousePreflightRejectsEndpointMismatchBeforeOtherQueries(t *testing.T) {
	requests := make([]string, 0, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests = append(requests, requestBody(t, request))
		_, _ = writer.Write([]byte("clickhouse-s1-r2\n"))
	}))
	defer server.Close()
	endpoint, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{
		"CLICKHOUSE_HOST":                            endpoint.Hostname(),
		"CLICKHOUSE_PORT":                            endpoint.Port(),
		"CLICKHOUSE_PROTOCOL":                        endpoint.Scheme,
		"CLICKHOUSE_USER":                            "test",
		"CLICKHOUSE_PASSWORD":                        "secret",
		"CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME":        "clickhouse-s1-r1",
		"CLICKHOUSE_PREFLIGHT_RETRY_BUDGET_SECONDS":  "1",
		"CLICKHOUSE_PREFLIGHT_RETRY_BACKOFF_SECONDS": "1",
	} {
		t.Setenv(name, value)
	}
	err = PreflightClickHouseDirectFromEnv(context.Background())
	if !errors.Is(err, errDirectEndpointHostnameMismatch) {
		t.Fatalf("preflight mismatch error=%v", err)
	}
	if len(requests) != 1 || requests[0] != "SELECT hostName() FORMAT TabSeparatedRaw" {
		t.Fatalf("preflight requests=%q, want identity query only", requests)
	}
}

func TestRetryClickHousePreflightRecoversAndIsBounded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	calls := 0
	attempts, err := retryClickHousePreflight(ctx, time.Millisecond, func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("connection refused")
		}
		return nil
	})
	if err != nil || attempts != 3 {
		t.Fatalf("attempts=%d error=%v, want recovery on third attempt", attempts, err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	attempts, err = retryClickHousePreflight(ctx, 5*time.Millisecond, func(context.Context) error {
		return errors.New("connection refused")
	})
	if err == nil || attempts < 2 || time.Since(started) > 250*time.Millisecond {
		t.Fatalf("attempts=%d elapsed=%s error=%v, want bounded transient retries", attempts, time.Since(started), err)
	}
}

func TestRetryClickHousePreflightFailsContractImmediately(t *testing.T) {
	for _, testErr := range []error{
		errors.New("clickhouse status=401 body=authentication failed"),
		errors.New("clickhouse status=500 body=DB::Exception: Unknown table"),
		&clickHouseHTTPStatusError{status: http.StatusInternalServerError, code: 60},
		&clickHouseHTTPStatusError{status: http.StatusUnauthorized, code: 516},
	} {
		calls := 0
		attempts, err := retryClickHousePreflight(context.Background(), time.Millisecond, func(context.Context) error {
			calls++
			return testErr
		})
		if err == nil || attempts != 1 || calls != 1 {
			t.Fatalf("testErr=%v attempts=%d calls=%d error=%v, want immediate failure", testErr, attempts, calls, err)
		}
	}
}

func TestRetryClickHousePreflightRetriesTypedTransientStatus(t *testing.T) {
	calls := 0
	attempts, err := retryClickHousePreflight(context.Background(), time.Millisecond, func(context.Context) error {
		calls++
		if calls == 1 {
			return &clickHouseHTTPStatusError{status: http.StatusInternalServerError, code: 319}
		}
		return nil
	})
	if err != nil || attempts != 2 || calls != 2 {
		t.Fatalf("attempts=%d calls=%d error=%v, want one typed transient retry", attempts, calls, err)
	}
}

func TestRetryClickHousePreflightHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	attempts, err := retryClickHousePreflight(ctx, time.Second, func(context.Context) error { return nil })
	if !errors.Is(err, context.Canceled) || attempts != 0 {
		t.Fatalf("attempts=%d error=%v, want cancellation before attempt", attempts, err)
	}
}

func TestEndpointHistoryDeduplicationPreflightValidatesRuntimeDDL(t *testing.T) {
	tests := []struct {
		name    string
		ddl     string
		wantErr string
	}{
		{
			name: "ready",
			ddl:  "CREATE TABLE Data_Shopping_Raw.gmarket_product_raw_endpoint_history (...) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192, non_replicated_deduplication_window = 1000",
		},
		{
			name:    "missing setting",
			ddl:     "CREATE TABLE Data_Shopping_Raw.gmarket_product_raw_endpoint_history (...) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192",
			wantErr: "missing non_replicated_deduplication_window",
		},
		{
			name:    "window too low",
			ddl:     "CREATE TABLE Data_Shopping_Raw.gmarket_product_raw_endpoint_history (...) ENGINE = MergeTree ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 999",
			wantErr: "below 1000",
		},
		{
			name:    "replicated engine",
			ddl:     "CREATE TABLE Data_Shopping_Raw.gmarket_product_raw_endpoint_history (...) ENGINE = ReplicatedMergeTree('/path', 'replica') ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 1000",
			wantErr: "non-replicated MergeTree",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				requests++
				body := requestBody(t, request)
				if body != "SHOW CREATE TABLE "+defaultGmarketRawInsertTable+" FORMAT TabSeparatedRaw" {
					t.Fatalf("unexpected runtime DDL query: %s", body)
				}
				writer.WriteHeader(http.StatusOK)
				_, _ = writer.Write([]byte(test.ddl))
			}))
			defer server.Close()
			pub := rawPublisherForTest(server)
			pub.cfg.PreflightRetryBackoff = time.Millisecond
			attempts, err := pub.retryEndpointDeduplicationSetting(context.Background(), defaultGmarketRawInsertTable)
			if test.wantErr == "" {
				if err != nil || attempts != 1 || requests != 1 {
					t.Fatalf("attempts=%d requests=%d error=%v", attempts, requests, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) || attempts != 1 || requests != 1 {
				t.Fatalf("attempts=%d requests=%d error=%v, want %q", attempts, requests, err, test.wantErr)
			}
		})
	}
}

func TestClickHouseExecBoundsAndSanitizesErrorBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_ = requestBody(t, request)
		writer.WriteHeader(http.StatusInternalServerError)
		_, _ = writer.Write([]byte("Code: 319. " + strings.Repeat("secret-row-value", 10000)))
	}))
	defer server.Close()
	pub := rawPublisherForTest(server)
	err := pub.exec(context.Background(), "INSERT INTO db.target SELECT 1")
	var statusErr *clickHouseHTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.code != 319 || strings.Contains(err.Error(), "secret-row-value") {
		t.Fatalf("sanitized bounded error=%T %v", err, err)
	}
}

func TestClickHouseExecDoesNotFollowHTTPRedirect(t *testing.T) {
	requests := 0
	redirected := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		_ = requestBody(t, request)
		if request.URL.Path == "/redirected" {
			redirected++
			writer.WriteHeader(http.StatusOK)
			return
		}
		http.Redirect(writer, request, "/redirected", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	pub := rawPublisherForTest(server)
	err := pub.exec(context.Background(), "SELECT 1")
	var statusErr *clickHouseHTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.status != http.StatusTemporaryRedirect {
		t.Fatalf("redirect error=%T %v", err, err)
	}
	if requests != 1 || redirected != 0 {
		t.Fatalf("requests=%d redirected=%d, want no redirect follow", requests, redirected)
	}
}

func TestShoppingWorkflowPinsBoundedPreflightRetry(t *testing.T) {
	source, err := os.ReadFile(".github/workflows/gmarket-crawl.yml")
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(source)
	if got := strings.Count(workflow, `CLICKHOUSE_PREFLIGHT_RETRY_BUDGET_SECONDS: "90"`); got != 3 {
		t.Fatalf("preflight retry budget count=%d, want crawl, detail, and manual replay jobs", got)
	}
	if got := strings.Count(workflow, `CLICKHOUSE_PREFLIGHT_RETRY_BACKOFF_SECONDS: "5"`); got != 3 {
		t.Fatalf("preflight retry backoff count=%d, want crawl, detail, and manual replay jobs", got)
	}
	if got := strings.Count(workflow, `CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME: ${{ vars.CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME || secrets.CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME }}`); got != 1 {
		t.Fatalf("workflow-level direct endpoint hostname binding count=%d, want 1", got)
	}
	if got := strings.Count(workflow, `test -n "$CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME"`); got != 3 {
		t.Fatalf("direct endpoint hostname required check count=%d, want raw writer/replayer jobs", got)
	}
	if got := strings.Count(workflow, `[[ "$CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME" =~ ^[A-Za-z0-9]([A-Za-z0-9._-]{0,251}[A-Za-z0-9])?$ ]]`); got != 3 {
		t.Fatalf("direct endpoint hostname safety check count=%d, want raw writer/replayer jobs", got)
	}
	if got := strings.Count(workflow, `[[ "${CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME,,}" != *gateway* ]]`); got != 3 {
		t.Fatalf("direct endpoint gateway rejection count=%d, want raw writer/replayer jobs", got)
	}
}
