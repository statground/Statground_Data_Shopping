package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultGmarketRawInsertTable  = "Data_Shopping_Raw.gmarket_product_raw_endpoint_history"
	defaultKurlyRawInsertTable    = "Data_Shopping_Raw.kurly_product_raw_endpoint_history"
	defaultShoppingRawOutboxTable = "Data_Shopping_Log.shopping_raw_direct_insert_outbox"
)

const (
	minimumEndpointDeduplicationWindow       = 1000
	maximumDirectEndpointHostnameBytes       = 253
	maximumDirectEndpointHostnameResultBytes = maximumDirectEndpointHostnameBytes + 1
)

var (
	endpointDeduplicationWindowPattern = regexp.MustCompile(`(?i)non_replicated_deduplication_window\s*=\s*([0-9]+)`)
	directEndpointHostnamePattern      = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._-]{0,251}[A-Za-z0-9])?$`)
	errDirectEndpointHostnameMismatch  = errors.New("clickhouse direct endpoint hostname mismatch")
)

type RowPublisher interface {
	Publish([]Row) error
}

type BufferedRowPublisher struct {
	publisher RowPublisher
	batchSize int
	pending   []Row
	lastErr   error
}

func NewBufferedRowPublisher(publisher RowPublisher, batchSize int) *BufferedRowPublisher {
	if batchSize <= 0 {
		batchSize = 50
	}
	return &BufferedRowPublisher{publisher: publisher, batchSize: batchSize, pending: make([]Row, 0, batchSize)}
}

func (b *BufferedRowPublisher) Add(row Row) error {
	b.pending = append(b.pending, row)
	if b.lastErr != nil || len(b.pending) < b.batchSize {
		return nil
	}
	if err := b.flush(); err != nil {
		b.lastErr = err
	}
	return nil
}

func (b *BufferedRowPublisher) Flush() error {
	if len(b.pending) == 0 {
		return b.lastErr
	}
	if b.lastErr != nil && !shouldRetryStreamingPublish(b.lastErr) {
		return b.lastErr
	}
	if err := b.flush(); err != nil {
		b.lastErr = err
		return err
	}
	b.lastErr = nil
	return nil
}

func shouldRetryStreamingPublish(err error) bool {
	return err != nil && !isAmbiguousRawInsertError(err) && !errors.Is(err, errDirectEndpointHostnameMismatch)
}

func (b *BufferedRowPublisher) PendingRows() []Row {
	return append([]Row(nil), b.pending...)
}

func (b *BufferedRowPublisher) flush() error {
	if b.publisher == nil {
		return fmt.Errorf("buffered row publisher is not initialized")
	}
	if len(b.pending) == 0 {
		return nil
	}
	batch := append([]Row(nil), b.pending...)
	if err := b.publisher.Publish(batch); err != nil {
		return err
	}
	b.pending = b.pending[:0]
	return nil
}

type ClickHouseRawConfig struct {
	URL                    string
	User                   string
	Password               string
	DirectEndpointHostname string
	GmarketTable           string
	KurlyTable             string
	OutboxTable            string
	OutboxReplayEnabled    bool
	OutboxReplayLimit      int
	InsertChunkSize        int
	RequestTimeout         time.Duration
	AttemptTimeout         time.Duration
	PreflightRetryBudget   time.Duration
	PreflightRetryBackoff  time.Duration
	ProducerSource         string
	LineageTopic           string
	LineagePartition       uint32
	LineageOffset          uint64
}

type ClickHouseRawPublisher struct {
	cfg                      ClickHouseRawConfig
	client                   *http.Client
	endpointIdentityMu       sync.Mutex
	endpointIdentityVerified bool
}

type clickHouseRawRow map[string]any

func ShouldWriteClickHouse() bool {
	mode := strings.ToLower(strings.TrimSpace(IngestMode))
	switch mode {
	case "clickhouse", "clickhouse_direct", "direct_clickhouse", "direct-clickhouse", "db", "database":
		return true
	default:
		return false
	}
}

func ShouldPublishRows() bool {
	return ShouldPublishKafka() || ShouldWriteClickHouse()
}

func shortIngestError(err error) string {
	return shortKafkaError(err)
}

func NewGmarketPublisherFromEnv() (RowPublisher, error) {
	if ShouldWriteClickHouse() {
		pub, err := NewClickHouseRawPublisherFromEnv()
		if err != nil {
			return nil, err
		}
		runUUID := envString("GMARKET_RUN_UUID", "")
		if runUUID == "" {
			runUUID = NewUUIDv7()
		}
		return &GmarketClickHouseRowPublisher{pub: pub, runUUID: runUUID}, nil
	}
	return NewGmarketRowPublisherFromEnv()
}

func NewKurlyPublisherFromEnv() (RowPublisher, error) {
	if ShouldWriteClickHouse() {
		pub, err := NewClickHouseRawPublisherFromEnv()
		if err != nil {
			return nil, err
		}
		runUUID := envString("KURLY_RUN_UUID", "")
		if runUUID == "" {
			runUUID = envString("GMARKET_RUN_UUID", "")
		}
		if runUUID == "" {
			runUUID = NewUUIDv7()
		}
		return &KurlyClickHouseRowPublisher{pub: pub, runUUID: runUUID}, nil
	}
	return NewKurlyRowPublisherFromEnv()
}

func NewClickHouseRawPublisherFromEnv() (*ClickHouseRawPublisher, error) {
	host := firstNonEmptyEnv("CLICKHOUSE_HOST", "CH_HOST")
	port := firstNonEmptyEnv("CLICKHOUSE_PORT", "CH_PORT")
	user := firstNonEmptyEnv("CLICKHOUSE_USER", "CH_USER")
	password := firstNonEmptyEnv("CLICKHOUSE_PASSWORD", "CH_PASSWORD")
	protocol := firstNonEmptyEnv("CLICKHOUSE_PROTOCOL", "CH_PROTOCOL")
	if protocol == "" {
		protocol = "http"
	}
	path := firstNonEmptyEnv("CLICKHOUSE_HTTP_URL_PATH", "CH_HTTP_URL_PATH")
	if path == "" {
		path = "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if host == "" || port == "" || user == "" || password == "" {
		return nil, fmt.Errorf("missing ClickHouse env: CLICKHOUSE_HOST/PORT/USER/PASSWORD")
	}
	directEndpointHostname, err := directEndpointHostnameFromEnv()
	if err != nil {
		return nil, err
	}

	gmarketTable := safeInsightIdentifierPath(envString("SHOPPING_GMARKET_RAW_INSERT_TABLE", defaultGmarketRawInsertTable))
	if gmarketTable == "" {
		return nil, fmt.Errorf("invalid SHOPPING_GMARKET_RAW_INSERT_TABLE")
	}
	kurlyTable := safeInsightIdentifierPath(envString("SHOPPING_KURLY_RAW_INSERT_TABLE", defaultKurlyRawInsertTable))
	if kurlyTable == "" {
		return nil, fmt.Errorf("invalid SHOPPING_KURLY_RAW_INSERT_TABLE")
	}
	outboxTable := safeInsightIdentifierPath(envString("SHOPPING_RAW_DIRECT_OUTBOX_TABLE", defaultShoppingRawOutboxTable))
	if outboxTable == "" {
		return nil, fmt.Errorf("invalid SHOPPING_RAW_DIRECT_OUTBOX_TABLE")
	}

	timeout := secondsDefault(envString("CLICKHOUSE_DIRECT_INSERT_TIMEOUT_SECONDS", envString("CLICKHOUSE_REQUEST_TIMEOUT_SECONDS", "120")), 120*time.Second)
	preflightBudget := secondsDefault(envString("CLICKHOUSE_PREFLIGHT_RETRY_BUDGET_SECONDS", "90"), 90*time.Second)
	if preflightBudget <= 0 || preflightBudget > 10*time.Minute {
		preflightBudget = 90 * time.Second
	}
	preflightBackoff := secondsDefault(envString("CLICKHOUSE_PREFLIGHT_RETRY_BACKOFF_SECONDS", "5"), 5*time.Second)
	if preflightBackoff <= 0 || preflightBackoff > 30*time.Second {
		preflightBackoff = 5 * time.Second
	}
	if preflightBackoff > preflightBudget {
		preflightBackoff = preflightBudget
	}
	cfg := ClickHouseRawConfig{
		URL:                    fmt.Sprintf("%s://%s:%s%s", protocol, host, port, path),
		User:                   user,
		Password:               password,
		DirectEndpointHostname: directEndpointHostname,
		GmarketTable:           gmarketTable,
		KurlyTable:             kurlyTable,
		OutboxTable:            outboxTable,
		OutboxReplayEnabled:    envBool("SHOPPING_RAW_OUTBOX_REPLAY_ENABLED", false),
		OutboxReplayLimit:      boundedRawInt(envString("SHOPPING_RAW_OUTBOX_REPLAY_LIMIT", "25"), 25, 1, maxRawOutboxReplayRows),
		InsertChunkSize:        boundedRawInt(envString("CLICKHOUSE_DIRECT_INSERT_CHUNK_SIZE", "100"), 100, 1, maxRawOutboxRows),
		RequestTimeout:         timeout,
		AttemptTimeout:         secondsDefault(envString("CLICKHOUSE_DIRECT_INSERT_ATTEMPT_TIMEOUT_SECONDS", "30"), 30*time.Second),
		PreflightRetryBudget:   preflightBudget,
		PreflightRetryBackoff:  preflightBackoff,
		ProducerSource:         envString("PRODUCER_SOURCE", "github_actions"),
		LineageTopic:           envString("CLICKHOUSE_DIRECT_LINEAGE_TOPIC", "direct_clickhouse"),
		LineagePartition:       0,
		LineageOffset:          0,
	}
	return &ClickHouseRawPublisher{
		cfg:    cfg,
		client: &http.Client{Timeout: timeout},
	}, nil
}

func directEndpointHostnameFromEnv() (string, error) {
	raw, present := os.LookupEnv("CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME")
	if !present || raw == "" {
		return "", fmt.Errorf("missing ClickHouse env: CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME")
	}
	if raw != strings.TrimSpace(raw) || !validDirectEndpointHostname(raw) {
		return "", fmt.Errorf("invalid CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME")
	}
	return raw, nil
}

func validDirectEndpointHostname(hostname string) bool {
	return len(hostname) > 0 &&
		len(hostname) <= maximumDirectEndpointHostnameBytes &&
		directEndpointHostnamePattern.MatchString(hostname) &&
		!strings.Contains(strings.ToLower(hostname), "gateway")
}

func (p *ClickHouseRawPublisher) ensureDirectEndpointIdentity(ctx context.Context) error {
	if p == nil || p.client == nil || !validDirectEndpointHostname(p.cfg.DirectEndpointHostname) {
		return fmt.Errorf("invalid clickhouse direct endpoint identity configuration: %w", errDirectEndpointHostnameMismatch)
	}
	p.endpointIdentityMu.Lock()
	defer p.endpointIdentityMu.Unlock()
	if p.endpointIdentityVerified {
		return nil
	}
	attempts, err := retryClickHousePreflight(ctx, p.cfg.PreflightRetryBackoff, func(attemptCtx context.Context) error {
		body, queryErr := p.queryBody(attemptCtx, "SELECT hostName() FORMAT TabSeparatedRaw", maximumDirectEndpointHostnameResultBytes)
		if queryErr != nil {
			return queryErr
		}
		if string(body) != p.cfg.DirectEndpointHostname+"\n" {
			return errDirectEndpointHostnameMismatch
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("clickhouse direct endpoint identity failed after %d attempts: %w", attempts, err)
	}
	p.endpointIdentityVerified = true
	return nil
}

func PreflightClickHouseDirectFromEnv(ctx context.Context) error {
	pub, err := NewClickHouseRawPublisherFromEnv()
	if err != nil {
		return err
	}
	retryCtx, cancel := context.WithTimeout(ctx, pub.cfg.PreflightRetryBudget)
	defer cancel()
	if err := pub.ensureDirectEndpointIdentity(retryCtx); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("clickhouse direct preflight endpoint identity rejected: %w", err)
	}
	attempts, connectErr := pub.retryPreflightSQL(retryCtx, "SELECT 1")
	if connectErr != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("clickhouse direct preflight failed to connect after %d attempts: %w", attempts, connectErr)
	}
	if attempts, err = pub.retryPreflightSQL(retryCtx, "CHECK GRANT INSERT ON "+pub.cfg.GmarketTable); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("clickhouse direct preflight missing Gmarket raw insert grant after %d attempts: %w", attempts, err)
	}
	if attempts, err = pub.retryPreflightSQL(retryCtx, "CHECK GRANT INSERT ON "+pub.cfg.KurlyTable); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("clickhouse direct preflight missing Kurly raw insert grant after %d attempts: %w", attempts, err)
	}
	for _, target := range []struct {
		name  string
		table string
	}{
		{name: "Gmarket raw", table: pub.cfg.GmarketTable},
		{name: "Kurly raw", table: pub.cfg.KurlyTable},
	} {
		if attempts, err = pub.retryEndpointDeduplicationSetting(retryCtx, target.table); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("clickhouse direct preflight invalid %s deduplication setting after %d attempts: %w", target.name, attempts, err)
		}
	}
	for _, target := range []struct {
		name  string
		table string
	}{
		{name: "Gmarket raw read", table: pub.cfg.GmarketTable},
		{name: "Kurly raw read", table: pub.cfg.KurlyTable},
	} {
		if attempts, err = pub.retryPreflightSQL(retryCtx, "CHECK GRANT SELECT ON "+target.table); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("clickhouse direct preflight missing %s grant after %d attempts: %w", target.name, attempts, err)
		}
	}
	for _, privilege := range []string{"SELECT", "INSERT", "ALTER UPDATE"} {
		if attempts, err = pub.retryPreflightSQL(retryCtx, "CHECK GRANT "+privilege+" ON "+pub.cfg.OutboxTable); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("clickhouse direct preflight missing raw outbox %s grant after %d attempts: %w", privilege, attempts, err)
		}
	}
	if attempts, err = pub.retryPreflightSQL(retryCtx, "SELECT 1 FROM "+pub.cfg.OutboxTable+" LIMIT 0"); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("clickhouse direct preflight raw outbox unavailable after %d attempts: %w", attempts, err)
	}
	fmt.Printf("[clickhouse] direct preflight ok gmarket_table=%s kurly_table=%s outbox_table=%s\n", pub.cfg.GmarketTable, pub.cfg.KurlyTable, pub.cfg.OutboxTable)
	if pub.cfg.OutboxReplayEnabled {
		if err := pub.replayRawOutbox(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (p *ClickHouseRawPublisher) retryEndpointDeduplicationSetting(ctx context.Context, table string) (int, error) {
	if !validRawTableIdentifier(table) {
		return 0, fmt.Errorf("invalid endpoint history table")
	}
	return retryClickHousePreflight(ctx, p.cfg.PreflightRetryBackoff, func(attemptCtx context.Context) error {
		requestCtx, cancel := context.WithTimeout(attemptCtx, p.cfg.AttemptTimeout)
		defer cancel()
		body, err := p.queryBody(requestCtx, "SHOW CREATE TABLE "+table+" FORMAT TabSeparatedRaw", maxRawOutboxHeaderBytes)
		if err != nil {
			return err
		}
		definition := string(body)
		if !regexp.MustCompile(`(?i)ENGINE\s*=\s*MergeTree(?:\(\))?`).MatchString(definition) {
			return fmt.Errorf("endpoint history must use non-replicated MergeTree")
		}
		matches := endpointDeduplicationWindowPattern.FindStringSubmatch(definition)
		if len(matches) != 2 {
			return fmt.Errorf("endpoint history is missing non_replicated_deduplication_window")
		}
		window, parseErr := strconv.Atoi(matches[1])
		if parseErr != nil || window < minimumEndpointDeduplicationWindow {
			return fmt.Errorf("endpoint history deduplication window is below %d", minimumEndpointDeduplicationWindow)
		}
		return nil
	})
}

func (p *ClickHouseRawPublisher) retryPreflightSQL(ctx context.Context, sql string) (int, error) {
	return retryClickHousePreflight(ctx, p.cfg.PreflightRetryBackoff, func(attemptCtx context.Context) error {
		requestCtx, cancel := context.WithTimeout(attemptCtx, p.cfg.AttemptTimeout)
		defer cancel()
		return p.exec(requestCtx, sql)
	})
}

func retryClickHousePreflight(ctx context.Context, backoff time.Duration, operation func(context.Context) error) (int, error) {
	if backoff <= 0 {
		backoff = 5 * time.Second
	}
	attempts := 0
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return attempts, lastErr
			}
			return attempts, err
		}
		attempts++
		lastErr = operation(ctx)
		if lastErr == nil || !isRetryableClickHousePreflightError(lastErr) {
			return attempts, lastErr
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return attempts, lastErr
		case <-timer.C:
		}
	}
}

func isRetryableClickHousePreflightError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) {
		return true
	}
	var statusErr *clickHouseHTTPStatusError
	if errors.As(err, &statusErr) {
		switch statusErr.code {
		case 6, 27, 47, 60, 62, 81, 497, 516:
			return false
		case 159, 164, 202, 203, 209, 210, 225, 241, 242, 243, 244, 252, 254, 255,
			265, 279, 285, 286, 289, 297, 319, 364, 369, 384, 394, 410, 415, 416,
			425, 439, 473, 519, 574, 667, 677, 692, 700, 722, 733, 734, 735, 738,
			745, 749, 762, 999:
			return true
		}
		switch statusErr.status {
		case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
			return false
		case http.StatusRequestTimeout, http.StatusTooManyRequests,
			http.StatusInternalServerError, http.StatusBadGateway,
			http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return true
		default:
			return false
		}
	}
	message := strings.ToLower(err.Error())
	for _, marker := range []string{
		"status=400", "status=401", "status=403", "status=404",
		"authentication", "wrong password", "access denied", "not enough privileges",
		"unknown table", "unknown database", "unknown identifier", "unknown column",
		"does not exist", "syntax error", "cannot parse", "type mismatch",
	} {
		if strings.Contains(message, marker) {
			return false
		}
	}
	for _, marker := range []string{
		"connection refused", "connection reset", "connection aborted", "broken pipe",
		"no route to host", "network is unreachable", "unexpected eof", "timeout", "deadline",
		"status=408", "status=429", "status=500", "status=502", "status=503", "status=504",
		"not initialized", "readonly", "read-only", "keeper", "coordination",
		"query was cancelled", "too many simultaneous queries", "too many pending queries",
		"memory limit exceeded", "too many parts", "temporarily unavailable",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func (p *ClickHouseRawPublisher) InsertGmarketRows(ctx context.Context, rows []Row, runUUID string) error {
	now := NowKST()
	out := make([]clickHouseRawRow, 0, len(rows))
	for _, row := range rows {
		productCode := CleanText(row["상품코드"])
		if productCode == "" {
			continue
		}
		payload := BuildGmarketPayload(row, runUUID, now)
		raw, err := p.buildRawRow(payload)
		if err != nil {
			return err
		}
		out = append(out, raw)
	}
	if len(out) == 0 {
		return fmt.Errorf("no publishable Gmarket rows with 상품코드")
	}
	if err := p.insertJSONEachRow(ctx, p.cfg.GmarketTable, out); err != nil {
		return err
	}
	fmt.Printf("[clickhouse] inserted gmarket raw rows=%d table=%s run_uuid=%s\n", len(out), p.cfg.GmarketTable, runUUID)
	return nil
}

func (p *ClickHouseRawPublisher) InsertKurlyRows(ctx context.Context, rows []Row, runUUID string) error {
	now := NowKST()
	out := make([]clickHouseRawRow, 0, len(rows))
	for _, row := range rows {
		productCode := FirstNonEmpty(row, []string{"상품코드", "상품번호"})
		if productCode == "" {
			continue
		}
		payload := BuildKurlyPayload(row, runUUID, now)
		raw, err := p.buildRawRow(payload)
		if err != nil {
			return err
		}
		out = append(out, raw)
	}
	if len(out) == 0 {
		return fmt.Errorf("no publishable Kurly rows with 상품코드")
	}
	if err := p.insertJSONEachRow(ctx, p.cfg.KurlyTable, out); err != nil {
		return err
	}
	fmt.Printf("[clickhouse] inserted kurly raw rows=%d table=%s run_uuid=%s\n", len(out), p.cfg.KurlyTable, runUUID)
	return nil
}

func (p *ClickHouseRawPublisher) buildRawRow(payload map[string]any) (clickHouseRawRow, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	row := clickHouseRawRow{}
	for key, value := range payload {
		if key == "raw_row" {
			continue
		}
		switch value := value.(type) {
		case bool:
			if value {
				row[key] = uint8(1)
			} else {
				row[key] = uint8(0)
			}
		default:
			row[key] = value
		}
	}
	row["source"] = p.cfg.ProducerSource
	row["event_uuid"] = NewUUIDv7()
	row["kafka_topic"] = p.cfg.LineageTopic
	row["kafka_partition"] = p.cfg.LineagePartition
	row["kafka_offset"] = p.cfg.LineageOffset
	row["payload"] = string(payloadBytes)
	return row, nil
}

func (p *ClickHouseRawPublisher) insertJSONEachRow(ctx context.Context, table string, rows []clickHouseRawRow) error {
	batches, err := canonicalRawBatches(table, rows, p.cfg.InsertChunkSize)
	if err != nil {
		return fmt.Errorf("clickhouse direct insert payload rejected: %w", err)
	}
	for index, batch := range batches {
		if err := p.insertRawBatchWithOutbox(ctx, table, batch); err != nil {
			return fmt.Errorf("clickhouse direct insert failed chunk=%d rows=%d total=%d: %w", index, batch.rowCount, len(rows), err)
		}
	}
	return nil
}

func (p *ClickHouseRawPublisher) exec(ctx context.Context, sql string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.URL, strings.NewReader(sql))
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
		return err
	}
	defer resp.Body.Close()
	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxClickHouseErrorBytes+1))
	if readErr != nil {
		return readErr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &clickHouseHTTPStatusError{status: resp.StatusCode, code: rawClickHouseErrorCode(string(respBody))}
	}
	if len(respBody) > maxClickHouseErrorBytes {
		return fmt.Errorf("clickhouse response exceeds bounded limit")
	}
	return nil
}

type GmarketClickHouseRowPublisher struct {
	pub     *ClickHouseRawPublisher
	runUUID string
}

func (p *GmarketClickHouseRowPublisher) Publish(rows []Row) error {
	if p == nil || p.pub == nil {
		return fmt.Errorf("gmarket clickhouse publisher is not initialized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.pub.cfg.RequestTimeout)
	defer cancel()
	return p.pub.InsertGmarketRows(ctx, rows, p.runUUID)
}

type KurlyClickHouseRowPublisher struct {
	pub     *ClickHouseRawPublisher
	runUUID string
}

func (p *KurlyClickHouseRowPublisher) Publish(rows []Row) error {
	if p == nil || p.pub == nil {
		return fmt.Errorf("kurly clickhouse publisher is not initialized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.pub.cfg.RequestTimeout)
	defer cancel()
	return p.pub.InsertKurlyRows(ctx, rows, p.runUUID)
}
