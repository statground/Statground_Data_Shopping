package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func shoppingCollectionGateEnv(pub *ClickHouseRawPublisher) []string {
	targets := []string{}
	if GmarketCollectEnabled {
		targets = append(targets, "local:"+strings.ReplaceAll(pub.cfg.GmarketTable, "`", ""))
	}
	if KurlyCollectEnabled {
		targets = append(targets, "local:"+strings.ReplaceAll(pub.cfg.KurlyTable, "`", ""))
	}
	targets = append(targets, "local:"+strings.ReplaceAll(pub.cfg.OutboxTable, "`", ""))
	if (GmarketCollectEnabled && RandomKeywordCount > 0 && CollectMode != "from_excel") || (KurlyCollectEnabled && KurlyRandomKeywordCount > 0) {
		targets = append(targets, "replica:Data_Content_Lexicon.keyword_selection_log_local")
	}
	maxIOWait := "0.50"
	if raw := os.Getenv("CLICKHOUSE_PRESSURE_GATE_MAX_IOWAIT_NORMALIZED"); raw != "" {
		if limit, err := strconv.ParseFloat(raw, 64); err == nil && !math.IsNaN(limit) && !math.IsInf(limit, 0) && limit >= 0 && limit < .50 {
			maxIOWait = raw
		}
	}
	overrides := map[string]string{
		"CLICKHOUSE_HOST": pub.cfg.URL, "CLICKHOUSE_PORT": "", "CLICKHOUSE_PROTOCOL": "", "CLICKHOUSE_HTTP_URL_PATH": "",
		"CLICKHOUSE_USER": pub.cfg.User, "CLICKHOUSE_PASSWORD": pub.cfg.Password,
		"CH_HOST": pub.cfg.URL, "CH_PORT": "", "CH_PROTOCOL": "", "CH_HTTP_URL_PATH": "",
		"CH_USER": pub.cfg.User, "CH_PASSWORD": pub.cfg.Password,
		"CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME":            pub.cfg.DirectEndpointHostname,
		"CLICKHOUSE_PRESSURE_GATE_TARGETS":               strings.Join(targets, ","),
		"CLICKHOUSE_PRESSURE_GATE_MAX_IOWAIT_NORMALIZED": maxIOWait,
	}
	env := []string{}
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if _, replace := overrides[key]; !replace && key != "ADPICK_BIZ_API_KEY" {
			env = append(env, entry)
		}
	}
	for key, value := range overrides {
		env = append(env, key+"="+value)
	}
	return env
}

func runShoppingCollectionGate(ctx context.Context) error {
	pub, err := NewClickHouseRawPublisherFromEnv()
	if err != nil {
		return err
	}
	attempt, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	command := exec.CommandContext(attempt, "python3", "scripts/clickhouse_pressure_gate.py")
	command.Env = shoppingCollectionGateEnv(pub)
	output, err := command.CombinedOutput()
	for _, line := range strings.Split(string(output), "\n") {
		if len(line) <= 1500 && adpickGateDiagnosticPattern.MatchString(line) {
			fmt.Printf("[shopping] %s\n", line)
		}
	}
	if err != nil {
		return fmt.Errorf("Shopping source storage pressure gate rejected; provider collection not started")
	}
	return nil
}

func runShoppingCollectorPhase(ctx context.Context, gate, preflight func(context.Context) error, collect func(context.Context)) error {
	if gate != nil {
		if err := gate(ctx); err != nil {
			return err
		}
	}
	if preflight != nil {
		if err := preflight(ctx); err != nil {
			return err
		}
	}
	collect(ctx)
	return nil
}
