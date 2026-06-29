package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

type KafkaConfig struct {
	IngestMode         string
	Brokers            []string
	Username           string
	Password           string
	Topic              string
	ClientID           string
	BatchSize          int
	BatchTimeout       time.Duration
	WriterMaxAttempts  int
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	WriteAttempts      int
	WriteBackoffMin    time.Duration
	WriteBackoffMax    time.Duration
	PublishTimeout     time.Duration
	PartitionFallback  bool
	FallbackPartitions []int
	FallbackTimeout    time.Duration
	ProducerSource     string
	ProducerIP         string
	PreflightRequired  bool
	RejectAdvertised   []string
}

type KafkaPublisher struct {
	Cfg KafkaConfig
}

type KafkaEvent struct {
	EventUUID string `json:"event_uuid"`
	Source    string `json:"source"`
	Host      string `json:"host"`
	UUIDUser  string `json:"uuid_user"`
	IP        string `json:"ip"`
	URL       string `json:"url"`
	EventType string `json:"event_type"`
	Payload   string `json:"payload"`
	CreatedAt string `json:"created_at"`
}

type kafkaMessageWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

func ShouldPublishKafka() bool {
	mode := strings.ToLower(strings.TrimSpace(IngestMode))
	return mode == "kafka" || mode == "kafka_clickhouse" || mode == "kafka-clickhouse" || mode == "events"
}

func NewKafkaPublisherFromEnv() (*KafkaPublisher, error) {
	cfg := KafkaConfig{
		IngestMode:         strings.ToLower(envString("INGEST_MODE", "excel")),
		Brokers:            splitCSV(envString("KAFKA_BROKERS", "")),
		Username:           envString("KAFKA_USERNAME", envString("KAFKA_EXTERNAL_USER", "")),
		Password:           envString("KAFKA_PASSWORD", envString("KAFKA_EXTERNAL_PASSWORD", "")),
		Topic:              envString("KAFKA_TOPIC", "shopping.events"),
		ClientID:           envString("KAFKA_CLIENT_ID", "statground-data-shopping-gmarket"),
		BatchSize:          positiveInt(envString("KAFKA_BATCH_SIZE", "100"), 100),
		BatchTimeout:       secondsDefault(envString("KAFKA_BATCH_TIMEOUT", "1.0"), time.Second),
		WriterMaxAttempts:  positiveInt(envString("KAFKA_WRITER_MAX_ATTEMPTS", "1"), 1),
		ReadTimeout:        secondsDefault(envString("KAFKA_READ_TIMEOUT_SECONDS", "8.0"), 8*time.Second),
		WriteTimeout:       secondsDefault(envString("KAFKA_WRITE_TIMEOUT_SECONDS", "8.0"), 8*time.Second),
		WriteAttempts:      positiveInt(envString("KAFKA_WRITE_ATTEMPTS", "8"), 8),
		WriteBackoffMin:    secondsDefault(envString("KAFKA_WRITE_BACKOFF_MIN", envString("KAFKA_WRITE_BACKOFF_MIN_SECONDS", "1.0")), time.Second),
		WriteBackoffMax:    secondsDefault(envString("KAFKA_WRITE_BACKOFF_MAX", envString("KAFKA_WRITE_BACKOFF_MAX_SECONDS", "20.0")), 20*time.Second),
		PublishTimeout:     secondsDefault(envString("KAFKA_RAW_PUBLISH_TIMEOUT_SECONDS", envString("KAFKA_PUBLISH_TIMEOUT_SECONDS", "120")), 120*time.Second),
		PartitionFallback:  envBool("KAFKA_PARTITION_FALLBACK_ENABLED", true),
		FallbackPartitions: splitIntCSV(envString("KAFKA_FALLBACK_PARTITIONS", "")),
		FallbackTimeout:    secondsDefault(envString("KAFKA_PARTITION_FALLBACK_TIMEOUT_SECONDS", "8.0"), 8*time.Second),
		ProducerSource:     envString("PRODUCER_SOURCE", "github_actions"),
		ProducerIP:         envString("PRODUCER_IP", "::"),
		PreflightRequired:  envBool("KAFKA_PREFLIGHT_REQUIRED", true),
		RejectAdvertised:   splitCSV(envString("KAFKA_REJECT_ADVERTISED_BROKERS", "")),
	}
	if cfg.WriteBackoffMax < cfg.WriteBackoffMin {
		cfg.WriteBackoffMax = cfg.WriteBackoffMin
	}
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("missing required env: KAFKA_BROKERS")
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return nil, fmt.Errorf("missing required env: KAFKA_TOPIC")
	}
	return &KafkaPublisher{Cfg: cfg}, nil
}

func PreflightKafkaFromEnv(ctx context.Context) error {
	pub, err := NewKafkaPublisherFromEnv()
	if err != nil {
		return err
	}
	if !pub.Cfg.PreflightRequired {
		return nil
	}
	return pub.Validate(ctx)
}

func (p *KafkaPublisher) Validate(ctx context.Context) error {
	for _, broker := range p.Cfg.Brokers {
		if isLoopbackBrokerEndpoint(broker) {
			return fmt.Errorf("KAFKA_BROKERS must be an externally reachable Kafka bootstrap address, not %q", broker)
		}
	}

	dialer := &kafka.Dialer{
		ClientID: p.Cfg.ClientID,
		Timeout:  10 * time.Second,
		DialFunc: kafkaAdvertisedBrokerDialFunc(p.Cfg.Brokers, 10*time.Second),
	}
	if strings.TrimSpace(p.Cfg.Username) != "" || strings.TrimSpace(p.Cfg.Password) != "" {
		dialer.SASLMechanism = plain.Mechanism{Username: p.Cfg.Username, Password: p.Cfg.Password}
	}

	probeCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(probeCtx, "tcp", p.Cfg.Brokers[0])
	if err != nil {
		return fmt.Errorf("kafka preflight failed to connect to bootstrap broker: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(p.Cfg.Topic)
	if err != nil {
		return fmt.Errorf("kafka preflight failed to read metadata for topic %q: %w", p.Cfg.Topic, err)
	}
	if len(partitions) == 0 {
		return fmt.Errorf("kafka preflight found zero partitions for topic %q", p.Cfg.Topic)
	}
	if err := validateKafkaAdvertisedLeaders(partitions, p.Cfg.Brokers, p.Cfg.RejectAdvertised, "kafka broker metadata"); err != nil {
		return err
	}
	fmt.Printf("[kafka] preflight ok topic=%s partitions=%d bootstrap=%s\n", p.Cfg.Topic, len(partitions), p.Cfg.Brokers[0])
	return nil
}

func (p *KafkaPublisher) NewEvent(eventType, eventUUID, sourceURL, createdAt string, payload map[string]any) (KafkaEvent, error) {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return KafkaEvent{}, err
	}
	if strings.TrimSpace(eventUUID) == "" {
		eventUUID = NewUUIDv7()
	}
	if strings.TrimSpace(createdAt) == "" {
		createdAt = FormatCHDateTime64Millis(NowKST())
	}
	return KafkaEvent{
		EventUUID: eventUUID,
		Source:    p.Cfg.ProducerSource,
		Host:      producerHost(),
		UUIDUser:  "",
		IP:        p.Cfg.ProducerIP,
		URL:       sourceURL,
		EventType: eventType,
		Payload:   string(payloadJSON),
		CreatedAt: createdAt,
	}, nil
}

func (p *KafkaPublisher) Publish(ctx context.Context, events []KafkaEvent) error {
	if len(events) == 0 {
		return nil
	}
	messages := make([]kafka.Message, 0, len(events))
	for _, ev := range events {
		body, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		messages = append(messages, kafka.Message{
			Key:   []byte(eventKey(ev)),
			Value: body,
			Time:  NowKST(),
		})
	}
	return p.writeMessagesWithRetry(ctx, messages, func() kafkaMessageWriter {
		return p.writer()
	}, sleepContext)
}

func (p *KafkaPublisher) writeMessagesWithRetry(ctx context.Context, messages []kafka.Message, newWriter func() kafkaMessageWriter, sleep func(context.Context, time.Duration) error) error {
	if len(messages) == 0 {
		return nil
	}

	pending := messages
	var lastErr error
	for attempt := 1; attempt <= p.Cfg.WriteAttempts; attempt++ {
		w := newWriter()
		err := w.WriteMessages(ctx, pending...)
		_ = w.Close()
		if err == nil {
			if attempt > 1 {
				fmt.Printf("[kafka] publish retry succeeded attempt=%d messages=%d\n", attempt, len(pending))
			}
			return nil
		}

		lastErr = err
		if ctx.Err() != nil {
			return err
		}
		failed, retryable := retryableFailedMessages(pending, err)
		if len(failed) == 0 || !retryable || attempt == p.Cfg.WriteAttempts {
			return err
		}
		if p.Cfg.PartitionFallback && shouldUsePartitionFallback(err) {
			if fallbackErr := p.writeMessagesToWritablePartition(ctx, failed); fallbackErr == nil {
				return nil
			} else {
				return fmt.Errorf("kafka publish failed after fixed-partition fallback: %s; original_error=%s", shortKafkaError(fallbackErr), shortKafkaError(err))
			}
		}
		fmt.Printf("[kafka] retrying publish attempt=%d/%d failed_messages=%d reason=%s error=%s\n", attempt+1, p.Cfg.WriteAttempts, len(failed), kafkaRetryReason(err), shortKafkaError(err))
		if err := sleep(ctx, kafkaBackoffDuration(attempt, p.Cfg.WriteBackoffMin, p.Cfg.WriteBackoffMax)); err != nil {
			return fmt.Errorf("kafka retry wait stopped: %w; last_error=%s", err, shortKafkaError(lastErr))
		}
		pending = failed
	}
	return lastErr
}

func (p *KafkaPublisher) writer() *kafka.Writer {
	return p.writerWithBalancer(&kafka.Hash{})
}

func (p *KafkaPublisher) writerWithBalancer(balancer kafka.Balancer) *kafka.Writer {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(p.Cfg.Brokers...),
		Topic:                  p.Cfg.Topic,
		Balancer:               balancer,
		MaxAttempts:            p.Cfg.WriterMaxAttempts,
		ReadTimeout:            p.Cfg.ReadTimeout,
		WriteTimeout:           p.Cfg.WriteTimeout,
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
		BatchSize:              p.Cfg.BatchSize,
		BatchTimeout:           p.Cfg.BatchTimeout,
	}
	transport := &kafka.Transport{
		ClientID: p.Cfg.ClientID,
		Dial:     kafkaAdvertisedBrokerDialFunc(p.Cfg.Brokers, 10*time.Second),
	}
	if strings.TrimSpace(p.Cfg.Username) != "" || strings.TrimSpace(p.Cfg.Password) != "" {
		transport.SASL = plain.Mechanism{Username: p.Cfg.Username, Password: p.Cfg.Password}
	}
	w.Transport = transport
	return w
}

func (p *KafkaPublisher) writeMessagesToWritablePartition(ctx context.Context, messages []kafka.Message) error {
	if len(messages) == 0 {
		return nil
	}
	partitions, err := p.fallbackPartitionIDs(ctx)
	if err != nil {
		return err
	}
	if len(partitions) == 0 {
		return fmt.Errorf("kafka partition fallback found zero partitions for topic=%s", p.Cfg.Topic)
	}

	pending := messages
	var lastErr error
	for _, partition := range partitions {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		attemptCtx, cancel := context.WithTimeout(ctx, p.Cfg.FallbackTimeout)
		w := p.writerWithBalancer(fixedPartitionBalancer{partition: partition})
		err := w.WriteMessages(attemptCtx, pending...)
		_ = w.Close()
		cancel()
		if err == nil {
			fmt.Printf("[kafka] fixed partition fallback succeeded partition=%d messages=%d\n", partition, len(pending))
			return nil
		}

		lastErr = err
		failed, retryable := retryableFailedMessages(pending, err)
		if len(failed) == 0 {
			return nil
		}
		pending = failed
		fmt.Printf("[kafka] fixed partition fallback failed partition=%d failed_messages=%d reason=%s error=%s\n", partition, len(pending), kafkaRetryReason(err), shortKafkaError(err))
		if !retryable {
			return err
		}
	}
	return fmt.Errorf("kafka fixed partition fallback exhausted partitions=%v failed_messages=%d last_error=%s", partitions, len(pending), shortKafkaError(lastErr))
}

func (p *KafkaPublisher) fallbackPartitionIDs(ctx context.Context) ([]int, error) {
	if len(p.Cfg.FallbackPartitions) > 0 {
		out := append([]int(nil), p.Cfg.FallbackPartitions...)
		sort.Ints(out)
		return uniqueInts(out), nil
	}

	dialer := &kafka.Dialer{
		ClientID: p.Cfg.ClientID,
		Timeout:  10 * time.Second,
		DialFunc: kafkaAdvertisedBrokerDialFunc(p.Cfg.Brokers, 10*time.Second),
	}
	if strings.TrimSpace(p.Cfg.Username) != "" || strings.TrimSpace(p.Cfg.Password) != "" {
		dialer.SASLMechanism = plain.Mechanism{Username: p.Cfg.Username, Password: p.Cfg.Password}
	}

	probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	conn, err := dialer.DialContext(probeCtx, "tcp", p.Cfg.Brokers[0])
	if err != nil {
		return nil, fmt.Errorf("kafka partition fallback failed to connect to bootstrap broker: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(p.Cfg.Topic)
	if err != nil {
		return nil, fmt.Errorf("kafka partition fallback failed to read metadata for topic %q: %w", p.Cfg.Topic, err)
	}
	out := make([]int, 0, len(partitions))
	for _, partition := range partitions {
		if partition.Topic == p.Cfg.Topic {
			out = append(out, partition.ID)
		}
	}
	sort.Ints(out)
	return uniqueInts(out), nil
}

type fixedPartitionBalancer struct {
	partition int
}

func (b fixedPartitionBalancer) Balance(_ kafka.Message, partitions ...int) int {
	for _, partition := range partitions {
		if partition == b.partition {
			return partition
		}
	}
	if len(partitions) > 0 {
		return partitions[0]
	}
	return b.partition
}

func eventKey(ev KafkaEvent) string {
	if strings.TrimSpace(ev.URL) != "" {
		return ev.EventType + ":" + ev.URL
	}
	return ev.EventType + ":" + ev.EventUUID
}

func producerHost() string {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		return "github-actions"
	}
	return host
}

func envString(name, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return fallback
}

func envBool(name string, fallback bool) bool {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return parsed
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func splitIntCSV(raw string) []int {
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil || n < 0 {
			continue
		}
		out = append(out, n)
	}
	sort.Ints(out)
	return uniqueInts(out)
}

func uniqueInts(values []int) []int {
	if len(values) == 0 {
		return nil
	}
	out := values[:0]
	var last int
	for i, value := range values {
		if i > 0 && value == last {
			continue
		}
		out = append(out, value)
		last = value
	}
	return out
}

func positiveInt(raw string, fallback int) int {
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func secondsDefault(raw string, fallback time.Duration) time.Duration {
	f, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || f <= 0 {
		return fallback
	}
	return time.Duration(f * float64(time.Second))
}

func retryableFailedMessages(messages []kafka.Message, err error) ([]kafka.Message, bool) {
	var writeErrs kafka.WriteErrors
	if errors.As(err, &writeErrs) {
		if len(writeErrs) != len(messages) {
			return messages, retryableKafkaWriteError(err)
		}
		failed := make([]kafka.Message, 0, writeErrs.Count())
		retryable := true
		for i, writeErr := range writeErrs {
			if writeErr == nil {
				continue
			}
			failed = append(failed, messages[i])
			if !retryableKafkaWriteError(writeErr) {
				retryable = false
			}
		}
		return failed, retryable
	}
	return messages, retryableKafkaWriteError(err)
}

func retryableKafkaWriteError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	var writeErrs kafka.WriteErrors
	if errors.As(err, &writeErrs) {
		if writeErrs.Count() == 0 {
			return false
		}
		for _, writeErr := range writeErrs {
			if writeErr != nil && !retryableKafkaWriteError(writeErr) {
				return false
			}
		}
		return true
	}
	var tempErr interface{ Temporary() bool }
	if errors.As(err, &tempErr) && tempErr.Temporary() {
		return true
	}
	var timeoutErr interface{ Timeout() bool }
	if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
		return true
	}
	return errors.Is(err, io.EOF) || isRetryableKafkaErrorText(err.Error())
}

func isRetryableKafkaErrorText(message string) bool {
	msg := strings.ToLower(message)
	return strings.Contains(msg, "not leader for partition") ||
		strings.Contains(msg, "partition has no leader") ||
		strings.Contains(msg, "has no leader") ||
		strings.Contains(msg, "leader not available") ||
		strings.Contains(msg, "metadata are likely out of date") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "failed to dial") ||
		strings.Contains(msg, "failed to open connection") ||
		strings.Contains(msg, "no route to host") ||
		strings.Contains(msg, "network is unreachable") ||
		strings.Contains(msg, "temporary failure in name resolution") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "eof")
}

func kafkaRetryReason(err error) string {
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "not leader for partition"),
		strings.Contains(msg, "partition has no leader"),
		strings.Contains(msg, "has no leader"),
		strings.Contains(msg, "metadata are likely out of date"):
		return "leader-metadata-stale"
	case strings.Contains(msg, "leader not available"):
		return "leader-not-available"
	case strings.Contains(msg, "timeout"):
		return "timeout"
	case strings.Contains(msg, "eof"),
		strings.Contains(msg, "connection reset"),
		strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "failed to dial"),
		strings.Contains(msg, "failed to open connection"),
		strings.Contains(msg, "no route to host"),
		strings.Contains(msg, "network is unreachable"),
		strings.Contains(msg, "temporary failure in name resolution"):
		return "network"
	default:
		return "temporary-kafka-error"
	}
}

func shouldUsePartitionFallback(err error) bool {
	reason := kafkaRetryReason(err)
	return reason == "leader-metadata-stale" || reason == "leader-not-available"
}

func shortKafkaError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.Join(strings.Fields(err.Error()), " ")
	if len([]rune(msg)) > 500 {
		return string([]rune(msg)[:500]) + "..."
	}
	return msg
}

func kafkaBackoffDuration(attempt int, minDelay, maxDelay time.Duration) time.Duration {
	if minDelay <= 0 {
		minDelay = time.Second
	}
	if maxDelay <= 0 {
		maxDelay = 12 * time.Second
	}
	if maxDelay < minDelay {
		maxDelay = minDelay
	}
	delay := minDelay
	for i := 1; i < attempt; i++ {
		if delay >= maxDelay/2 {
			return maxDelay
		}
		delay *= 2
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isLoopbackBrokerEndpoint(raw string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(raw))
	if err != nil {
		host = strings.TrimSpace(raw)
		if strings.Contains(host, ":") {
			host = strings.Split(host, ":")[0]
		}
	}
	return isLoopbackHost(host)
}

func validateKafkaAdvertisedLeaders(partitions []kafka.Partition, brokers []string, rejectAdvertised []string, label string) error {
	bootstrap := kafkaBootstrapEndpointSet(brokers)
	rejected := kafkaBootstrapEndpointSet(rejectAdvertised)
	nonBootstrapLeaders := 0
	topics := map[string]bool{}
	for _, partition := range partitions {
		leaderHost := strings.TrimSpace(partition.Leader.Host)
		if isLoopbackHost(leaderHost) {
			return fmt.Errorf("%s advertises loopback listener for topic=%s partition=%d", label, partition.Topic, partition.ID)
		}
		leaderEndpoint := normalizedKafkaEndpoint(leaderHost, strconv.Itoa(partition.Leader.Port))
		if rejected[leaderEndpoint] {
			return fmt.Errorf("%s advertises retired broker endpoint %s for topic=%s partition=%d; recreate Kafka_Platform with the current KAFKA_PUBLIC_HOST before running this workflow", label, leaderEndpoint, partition.Topic, partition.ID)
		}
		if len(bootstrap) > 0 && !bootstrap[leaderEndpoint] {
			nonBootstrapLeaders++
			topics[partition.Topic] = true
		}
	}
	if nonBootstrapLeaders > 0 {
		fmt.Printf("[kafka] %s has %d non-bootstrap advertised broker entries across %d topic(s); producer will dial via bootstrap rewrite\n", label, nonBootstrapLeaders, len(topics))
	}
	return nil
}

func kafkaBootstrapEndpointSet(brokers []string) map[string]bool {
	endpoints := make(map[string]bool, len(brokers))
	for _, broker := range brokers {
		host, port, ok := splitKafkaEndpoint(broker)
		if ok {
			endpoints[normalizedKafkaEndpoint(host, port)] = true
		}
	}
	return endpoints
}

func splitKafkaEndpoint(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false
	}
	host, port, err := net.SplitHostPort(raw)
	if err != nil {
		if strings.Count(raw, ":") != 1 {
			return "", "", false
		}
		parts := strings.SplitN(raw, ":", 2)
		host, port = parts[0], parts[1]
	}
	host = strings.TrimSpace(host)
	port = strings.TrimSpace(port)
	return host, port, host != "" && port != ""
}

func normalizedKafkaEndpoint(host, port string) string {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	port = strings.TrimSpace(port)
	return host + ":" + port
}

func kafkaAdvertisedBrokerDialFunc(brokers []string, timeout time.Duration) func(context.Context, string, string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	if len(brokers) != 1 {
		return dialer.DialContext
	}
	bootstrapHost, bootstrapPort, ok := splitKafkaEndpoint(brokers[0])
	if !ok {
		return dialer.DialContext
	}
	bootstrapAddress := net.JoinHostPort(strings.Trim(bootstrapHost, "[]"), bootstrapPort)
	bootstrapEndpoint := normalizedKafkaEndpoint(bootstrapHost, bootstrapPort)
	return func(ctx context.Context, network string, address string) (net.Conn, error) {
		target := address
		host, port, ok := splitKafkaEndpoint(address)
		if ok && normalizedKafkaEndpoint(host, port) != bootstrapEndpoint {
			target = bootstrapAddress
		}
		return dialer.DialContext(ctx, network, target)
	}
}

func isLoopbackHost(host string) bool {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	if host == "localhost" || host == "0.0.0.0" || host == "::" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
