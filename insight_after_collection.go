package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

func shoppingInsightGateEnv(tables insightRefreshTables) []string {
	const key = "CLICKHOUSE_PRESSURE_GATE_TARGETS="
	env := []string{}
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, key) && !strings.HasPrefix(entry, "ADPICK_BIZ_API_KEY=") {
			env = append(env, entry)
		}
	}
	return append(env, key+"local:"+tables.snapshot+",local:"+tables.keywordSearch+",local:"+tables.publishedBatch)
}

func runShoppingInsightAfterCollection(parent context.Context) error {
	tables, err := insightRefreshTablesFromEnv()
	if err != nil {
		return err
	}
	gateCtx, cancel := context.WithTimeout(parent, 45*time.Second)
	command := exec.CommandContext(gateCtx, "python3", "scripts/clickhouse_pressure_gate.py")
	command.Env = shoppingInsightGateEnv(tables)
	output, err := command.CombinedOutput()
	cancel()
	for _, line := range strings.Split(string(output), "\n") {
		if len(line) <= 1500 && adpickGateDiagnosticPattern.MatchString(line) {
			fmt.Printf("[shopping-insight] %s\n", line)
		}
	}
	if err != nil {
		return fmt.Errorf("Shopping Price Insight storage pressure gate rejected; previous publication retained")
	}
	ctx, cancel := context.WithTimeout(parent, secondsDefault(envString("SHOPPING_ANALYSIS_REFRESH_TIMEOUT_SECONDS", "900"), 15*time.Minute))
	defer cancel()
	return RunShoppingInsightRefreshFromEnv(ctx)
}
