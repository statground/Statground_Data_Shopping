package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInsightRawIngestionRequiresOwnerEndpointAndExactDrainedReadback(t *testing.T) {
	for _, test := range []struct {
		name, host, outbox string
		wantErr            bool
	}{
		{"drained", "clickhouse-s1-r1\n", "{\"pending\":0}\n", false},
		{"other_endpoint", "clickhouse-s1-r2\n", "{\"pending\":0}\n", true},
		{"pending", "clickhouse-s1-r1\n", "{\"pending\":1}\n", true},
		{"missing", "clickhouse-s1-r1\n", "{}\n", true},
		{"null", "clickhouse-s1-r1\n", "{\"pending\":null}\n", true},
		{"multiple", "clickhouse-s1-r1\n", "{\"pending\":0}\n{\"pending\":0}\n", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME", "clickhouse-s1-r1")
			t.Setenv("SHOPPING_RAW_DIRECT_OUTBOX_TABLE", defaultShoppingRawOutboxTable)
			t.Setenv("SHOPPING_GMARKET_RAW_INSERT_TABLE", defaultGmarketRawInsertTable)
			t.Setenv("SHOPPING_KURLY_RAW_INSERT_TABLE", defaultKurlyRawInsertTable)
			outboxQueries := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if string(body) == "SELECT hostName() FORMAT TabSeparatedRaw" {
					_, _ = io.WriteString(w, test.host)
					return
				}
				outboxQueries++
				query := strings.ReplaceAll(string(body), "`", "")
				for _, wanted := range []string{defaultShoppingRawOutboxTable, "replayed_at IS NULL", defaultGmarketRawInsertTable, defaultKurlyRawInsertTable, "max_execution_time = 10", "skip_unavailable_shards = 0"} {
					if !strings.Contains(query, wanted) {
						t.Errorf("query misses %q", wanted)
					}
				}
				if strings.Contains(query, "ALTER") || strings.Contains(query, "INSERT") {
					t.Error("drain check changed outbox state")
				}
				_, _ = io.WriteString(w, test.outbox)
			}))
			defer server.Close()
			client := &insightCHClient{url: server.URL, client: server.Client()}
			if err := client.verifyInsightRawIngestion(context.Background()); (err != nil) != test.wantErr {
				t.Fatalf("error=%v, want error=%t", err, test.wantErr)
			}
			if test.name == "other_endpoint" && outboxQueries != 0 {
				t.Fatal("checked empty outbox on the wrong endpoint")
			}
		})
	}
}

func TestInsightRefreshRetainsPublicationWhenRawDeliveryIsPending(t *testing.T) {
	for _, at := range []int{1, 2} {
		t.Run(fmt.Sprintf("raw_check_%d", at), func(t *testing.T) {
			client := &fakeInsightRefreshClient{products: testInsightStandardProducts(), rawErrAt: at}
			err := runShoppingInsightRefresh(context.Background(), client, insightRefreshTables{snapshot: "snapshot", keywordSearch: "keyword", publishedBatch: "published"})
			if err == nil || !strings.Contains(err.Error(), "raw ingestion") {
				t.Fatalf("error=%v", err)
			}
			for _, call := range client.calls {
				if call == "insert_published" || call == "verify_published" || (at == 1 && (call == "insert_keyword" || call == "insert_snapshot")) {
					t.Fatalf("published or wrote after unconfirmed raw delivery: %v", client.calls)
				}
			}
		})
	}
}
