package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// The outbox is endpoint-local. Verify the endpoint before asking whether the
// source is drained; an empty queue on a gateway or another replica is no proof.
// Pending payloads remain untouched for the existing explicit manual replay.
func (c *insightCHClient) verifyInsightRawIngestion(ctx context.Context) error {
	expectedHost, err := directEndpointHostnameFromEnv()
	if err != nil {
		return err
	}
	body, err := c.post(ctx, "SELECT hostName() FORMAT TabSeparatedRaw")
	if err != nil || string(body) != expectedHost+"\n" {
		return fmt.Errorf("raw ingestion endpoint identity unverified")
	}
	query, err := shoppingInsightPendingRawSQL()
	if err != nil {
		return err
	}
	body, err = c.post(ctx, query)
	if err != nil {
		return fmt.Errorf("raw outbox read unavailable")
	}
	if len(body) > 1024 {
		return fmt.Errorf("raw outbox read exceeded its bound")
	}
	var state struct {
		Pending *uint64 `json:"pending"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&state); err != nil || state.Pending == nil {
		return fmt.Errorf("raw outbox readback invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("raw outbox readback returned multiple records")
	}
	if *state.Pending != 0 {
		return fmt.Errorf("raw batches pending=%d; manual replay required; previous publication retained", *state.Pending)
	}
	return nil
}

func shoppingInsightPendingRawSQL() (string, error) {
	outbox := safeInsightIdentifierPath(envString("SHOPPING_RAW_DIRECT_OUTBOX_TABLE", defaultShoppingRawOutboxTable))
	gmarket := safeInsightIdentifierPath(envString("SHOPPING_GMARKET_RAW_INSERT_TABLE", defaultGmarketRawInsertTable))
	kurly := safeInsightIdentifierPath(envString("SHOPPING_KURLY_RAW_INSERT_TABLE", defaultKurlyRawInsertTable))
	if outbox == "" || gmarket == "" || kurly == "" {
		return "", fmt.Errorf("invalid raw ingestion target")
	}
	// Configured targets are validated identifiers; remove quoting only to match
	// earlier outbox records that used the same table without backticks.
	return fmt.Sprintf(`SELECT count() AS pending
FROM %s
WHERE replayed_at IS NULL
  AND replaceAll(target_table, char(96), '') IN ('%s', '%s')
SETTINGS max_threads = 1, max_execution_time = 10, max_memory_usage = 268435456, skip_unavailable_shards = 0
FORMAT JSONEachRow`, outbox, strings.ReplaceAll(gmarket, "`", ""), strings.ReplaceAll(kurly, "`", "")), nil
}
