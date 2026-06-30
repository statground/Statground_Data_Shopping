package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	defaultGmarketRawInsertTable = "Data_Shopping_Raw.gmarket_product_raw_endpoint_history"
	defaultKurlyRawInsertTable   = "Data_Shopping_Raw.kurly_product_raw_endpoint_history"
)

type RowPublisher interface {
	Publish([]Row) error
}

type ClickHouseRawConfig struct {
	URL              string
	User             string
	Password         string
	GmarketTable     string
	KurlyTable       string
	InsertChunkSize  int
	RequestTimeout   time.Duration
	ProducerSource   string
	LineageTopic     string
	LineagePartition uint32
	LineageOffset    uint64
}

type ClickHouseRawPublisher struct {
	cfg    ClickHouseRawConfig
	client *http.Client
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

	gmarketTable := safeInsightIdentifierPath(envString("SHOPPING_GMARKET_RAW_INSERT_TABLE", defaultGmarketRawInsertTable))
	if gmarketTable == "" {
		return nil, fmt.Errorf("invalid SHOPPING_GMARKET_RAW_INSERT_TABLE")
	}
	kurlyTable := safeInsightIdentifierPath(envString("SHOPPING_KURLY_RAW_INSERT_TABLE", defaultKurlyRawInsertTable))
	if kurlyTable == "" {
		return nil, fmt.Errorf("invalid SHOPPING_KURLY_RAW_INSERT_TABLE")
	}

	timeout := secondsDefault(envString("CLICKHOUSE_DIRECT_INSERT_TIMEOUT_SECONDS", envString("CLICKHOUSE_REQUEST_TIMEOUT_SECONDS", "120")), 120*time.Second)
	cfg := ClickHouseRawConfig{
		URL:              fmt.Sprintf("%s://%s:%s%s", protocol, host, port, path),
		User:             user,
		Password:         password,
		GmarketTable:     gmarketTable,
		KurlyTable:       kurlyTable,
		InsertChunkSize:  positiveInt(envString("CLICKHOUSE_DIRECT_INSERT_CHUNK_SIZE", "100"), 100),
		RequestTimeout:   timeout,
		ProducerSource:   envString("PRODUCER_SOURCE", "github_actions"),
		LineageTopic:     envString("CLICKHOUSE_DIRECT_LINEAGE_TOPIC", "direct_clickhouse"),
		LineagePartition: 0,
		LineageOffset:    0,
	}
	return &ClickHouseRawPublisher{
		cfg:    cfg,
		client: &http.Client{Timeout: timeout},
	}, nil
}

func PreflightClickHouseDirectFromEnv(ctx context.Context) error {
	pub, err := NewClickHouseRawPublisherFromEnv()
	if err != nil {
		return err
	}
	if err := pub.exec(ctx, "SELECT 1"); err != nil {
		return fmt.Errorf("clickhouse direct preflight failed to connect: %w", err)
	}
	if err := pub.exec(ctx, "CHECK GRANT INSERT ON "+pub.cfg.GmarketTable); err != nil {
		return fmt.Errorf("clickhouse direct preflight missing Gmarket raw insert grant: %w", err)
	}
	if err := pub.exec(ctx, "CHECK GRANT INSERT ON "+pub.cfg.KurlyTable); err != nil {
		return fmt.Errorf("clickhouse direct preflight missing Kurly raw insert grant: %w", err)
	}
	fmt.Printf("[clickhouse] direct preflight ok gmarket_table=%s kurly_table=%s\n", pub.cfg.GmarketTable, pub.cfg.KurlyTable)
	return nil
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
	chunkSize := p.cfg.InsertChunkSize
	if chunkSize <= 0 {
		chunkSize = len(rows)
	}
	for start := 0; start < len(rows); start += chunkSize {
		end := start + chunkSize
		if end > len(rows) {
			end = len(rows)
		}
		if err := p.insertJSONEachRowChunk(ctx, table, rows[start:end]); err != nil {
			return fmt.Errorf("clickhouse direct insert failed offset=%d size=%d total=%d: %w", start, end-start, len(rows), err)
		}
	}
	return nil
}

func (p *ClickHouseRawPublisher) insertJSONEachRowChunk(ctx context.Context, table string, rows []clickHouseRawRow) error {
	var body bytes.Buffer
	body.WriteString("INSERT INTO ")
	body.WriteString(table)
	body.WriteString(" FORMAT JSONEachRow\n")
	for _, row := range rows {
		line, err := json.Marshal(row)
		if err != nil {
			return err
		}
		body.Write(line)
		body.WriteByte('\n')
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.URL, &body)
	if err != nil {
		return err
	}
	req.SetBasicAuth(p.cfg.User, p.cfg.Password)
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("clickhouse status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

func (p *ClickHouseRawPublisher) exec(ctx context.Context, sql string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.URL, strings.NewReader(sql))
	if err != nil {
		return err
	}
	req.SetBasicAuth(p.cfg.User, p.cfg.Password)
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("clickhouse status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(respBody)))
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
