package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

type recordingRowPublisher struct {
	batches [][]Row
	fail    bool
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

func TestClickHousePreflightRetryConfigDefaultsAndOverrides(t *testing.T) {
	for name, value := range map[string]string{
		"CLICKHOUSE_HOST":                            "clickhouse.example.invalid",
		"CLICKHOUSE_PORT":                            "8123",
		"CLICKHOUSE_USER":                            "test",
		"CLICKHOUSE_PASSWORD":                        "secret",
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
	t.Setenv("CLICKHOUSE_PREFLIGHT_RETRY_BUDGET_SECONDS", "120")
	t.Setenv("CLICKHOUSE_PREFLIGHT_RETRY_BACKOFF_SECONDS", "3")
	pub, err = NewClickHouseRawPublisherFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if pub.cfg.PreflightRetryBudget != 120*time.Second || pub.cfg.PreflightRetryBackoff != 3*time.Second {
		t.Fatalf("overridden preflight config=%s/%s, want 120s/3s", pub.cfg.PreflightRetryBudget, pub.cfg.PreflightRetryBackoff)
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
	for _, message := range []string{
		"clickhouse status=401 body=authentication failed",
		"clickhouse status=500 body=DB::Exception: Unknown table",
	} {
		calls := 0
		attempts, err := retryClickHousePreflight(context.Background(), time.Millisecond, func(context.Context) error {
			calls++
			return errors.New(message)
		})
		if err == nil || attempts != 1 || calls != 1 {
			t.Fatalf("message=%q attempts=%d calls=%d error=%v, want immediate failure", message, attempts, calls, err)
		}
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

func TestShoppingWorkflowPinsBoundedPreflightRetry(t *testing.T) {
	source, err := os.ReadFile(".github/workflows/gmarket-crawl.yml")
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(source)
	if got := strings.Count(workflow, `CLICKHOUSE_PREFLIGHT_RETRY_BUDGET_SECONDS: "90"`); got != 2 {
		t.Fatalf("preflight retry budget count=%d, want crawl and detail jobs", got)
	}
	if got := strings.Count(workflow, `CLICKHOUSE_PREFLIGHT_RETRY_BACKOFF_SECONDS: "5"`); got != 2 {
		t.Fatalf("preflight retry backoff count=%d, want crawl and detail jobs", got)
	}
}
