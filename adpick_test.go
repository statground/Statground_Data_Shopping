package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const adpickTestRun = "01900000-0000-7000-8000-000000000001"

func adpickFixtureClient(t *testing.T, handler http.HandlerFunc) *adpickClient {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := newAdpickClient("fixture-secret")
	if err != nil {
		t.Fatal(err)
	}
	client.baseURL = server.URL + "/api/"
	client.interval = 0
	return client
}

func TestAdpickCatalogStrictMerchantRoutingAndLinkIdentity(t *testing.T) {
	queries := []adpickQuery{{"travel", "stays", "제주 호텔"}, {"services", "design", "로고 디자인"}}
	client := adpickFixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/malls") {
			if r.URL.Query().Get("rewardmalls") != "true" {
				t.Error("reward malls filter missing")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": []adpickMall{
				{Code: "ACTUAL_TRIP", Name: "트립닷컴", Link: "https://bitl.bz/trip"},
				{Code: "ACTUAL_KMONG", Name: "크몽", Link: "https://bitl.bz/kmong"},
				{Code: "COUPANG", Name: "쿠팡"}, {Code: "BOOK", Name: "교보문고"},
				{Code: "LECTURE", Name: "인프런"}, {Code: "KNOW", Name: "애드픽 지식마켓"},
				{Code: "EVIL", Name: "트립닷컴 사칭"},
			}})
			return
		}
		if !strings.HasSuffix(r.URL.Path, "/search") || r.URL.Query().Get("limit") != "20" {
			t.Errorf("unexpected request %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": []adpickOffer{
			{Code: "ACTUAL_TRIP", Name: "Trip.com", Title: "호텔 &amp; 숙박", Price: "19,900원", Link: "https://bitl.bz/hotel"},
			{Code: "ACTUAL_TRIP", Name: "트립닷컴", Title: "동일 링크 중복", Price: "29,900원", Link: "https://bitl.bz/hotel"},
			{Code: "ACTUAL_KMONG", Name: "크몽", Title: "로고 디자인", Price: "협의", Link: "https://deg.kr/design"},
			{Code: "COUPANG", Name: "쿠팡", Title: "제외", Link: "https://bitl.bz/coupang"},
			{Code: "EVIL", Name: "트립닷컴", Title: "코드 불일치", Link: "https://bitl.bz/evil"},
		}})
	})
	records, err := collectAdpickCatalog(context.Background(), client, queries, 20, adpickTestRun, NowKST())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 4 {
		t.Fatalf("records=%d want two merchants and two offers", len(records))
	}
	for _, record := range records {
		if record.Source != "adpick_biz" || len(record.ItemKey) != 64 {
			t.Fatalf("record identity=%+v", record)
		}
		if record.RecordType == "offer" && record.MerchantKey == "trip_com" {
			if record.CategorySlug != "stays" || record.PriceKRW == nil || *record.PriceKRW != 19900 || record.Title != "호텔 & 숙박" {
				t.Fatalf("travel offer=%+v", record)
			}
		}
		if record.MerchantKey == "kmong" && record.RecordType == "offer" && (record.CategorySlug != "design" || record.PriceKRW != nil) {
			t.Fatalf("service offer=%+v", record)
		}
	}
}

func TestAdpickCatalogRetainsDirectoryWithSuccessfulEmptySearch(t *testing.T) {
	client := adpickFixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		data := any([]adpickOffer{})
		if strings.HasSuffix(r.URL.Path, "/malls") {
			data = []adpickMall{{Code: "T", Name: "트립닷컴"}}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": data})
	})
	records, err := collectAdpickCatalog(context.Background(), client, defaultAdpickQueries[:1], 20, adpickTestRun, NowKST())
	if err != nil || len(records) != 1 || records[0].RecordType != "merchant" || records[0].AffiliateURL != "" {
		t.Fatalf("directory=%+v error=%v", records, err)
	}
}

func TestAdpickUnsuccessfulSearchRejectsWholeBatchWithoutSecrets(t *testing.T) {
	client := adpickFixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/malls") {
			_, _ = io.WriteString(w, `{"success":true,"data":[{"cp_code":"T","name":"트립닷컴"}]}`)
			return
		}
		_, _ = io.WriteString(w, `{"success":false,"message":"fixture-secret invalid","data":[]}`)
	})
	records, err := collectAdpickCatalog(context.Background(), client, defaultAdpickQueries[:1], 20, adpickTestRun, NowKST())
	if err == nil || records != nil || strings.Contains(err.Error(), "fixture-secret") {
		t.Fatalf("records=%v error=%v", records, err)
	}
}

func TestAdpickRejectsRedirectAndMerchantMismatch(t *testing.T) {
	t.Run("redirect", func(t *testing.T) {
		client := adpickFixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "https://example.com/fixture-secret", http.StatusFound)
		})
		_, err := collectAdpickCatalog(context.Background(), client, defaultAdpickQueries[:1], 20, adpickTestRun, NowKST())
		if err == nil || strings.Contains(err.Error(), "fixture-secret") {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("merchant mismatch", func(t *testing.T) {
		client := adpickFixtureClient(t, func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/malls") {
				_, _ = io.WriteString(w, `{"success":true,"data":[{"cp_code":"T","name":"트립닷컴"}]}`)
				return
			}
			_, _ = io.WriteString(w, `{"success":true,"data":[{"cp_code":"T","cp_name":"쿠팡","title":"bad","commissionlink":"https://bitl.bz/offer"}]}`)
		})
		records, err := collectAdpickCatalog(context.Background(), client, defaultAdpickQueries[:1], 20, adpickTestRun, NowKST())
		if err == nil || records != nil {
			t.Fatalf("records=%v error=%v", records, err)
		}
	})
}

func TestAdpickBoundedConfigAndSensitiveURLs(t *testing.T) {
	for _, raw := range []string{`[]`, `[{"vertical":"books","category":"design","keyword":"book"}]`, `[{"vertical":"travel","category":"design","keyword":"hotel"}]`} {
		t.Setenv("ADPICK_SEARCH_QUERIES_JSON", raw)
		if _, err := adpickQueriesFromEnv(); err == nil {
			t.Errorf("accepted config %s", raw)
		}
	}
	for _, raw := range []string{"http://bitl.bz/x", "https://bitl.bz@evil.com/x", "https://127.0.0.1/x", "https://bitl.bz/x?apikey=secret", "https://biz.adpick.co.kr/api/secret/directlink", "javascript:alert(1)", "https://evil.com/x"} {
		if safeAdpickURL(raw, true) != "" {
			t.Errorf("accepted %s", raw)
		}
	}
	if safeAdpickURL("https://biz.adpick.co.kr/api/secret/search", false) != "" {
		t.Fatal("image URL leaked API credential")
	}
	for _, raw := range []string{"10,000원~", "무료", "0원", "1,23원", "$20", "10원부터"} {
		if adpickPrice(raw) != nil {
			t.Errorf("invented price for %q", raw)
		}
	}
	client, err := newAdpickClient("secret")
	if err != nil || client.interval < 6100*time.Millisecond {
		t.Fatal("search pacing below quota")
	}
	client.lastSearch = time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.request(ctx, "search", nil, &[]adpickOffer{}); err != context.Canceled {
		t.Fatalf("pacing cancellation=%v", err)
	}
}

func TestAdpickPublicationRequiresReadbackBeforeMarker(t *testing.T) {
	for _, failure := range []string{"", "readback", "marker_parity", "node_switch"} {
		t.Run("failure="+failure, func(t *testing.T) {
			records := []adpickCatalogRecord{{Source: "adpick_biz", Vertical: "travel", RecordType: "merchant", MerchantCode: "TRIP", MerchantKey: "trip_com", MerchantName: "트립닷컴", ItemKey: strings.Repeat("a", 64), Title: "트립닷컴", AffiliateURL: "https://bitl.bz/trip", CollectedAt: "2026-09-06 01:02:03.004", CollectRunUUID: adpickTestRun, Version: 123}}
			markers := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, _ := io.ReadAll(r.Body)
				query := string(raw)
				if !strings.Contains(query, "SELECT hostName()") && !strings.Contains(query, "(SELECT throwIf(hostName() != 'clickhouse-s1-r1', 'adpick_catalog_endpoint_mismatch')) = 0") {
					t.Error("catalog write or readback missing executing-node guard")
				}
				switch {
				case strings.Contains(query, "SELECT hostName()"):
					_, _ = io.WriteString(w, "clickhouse-s1-r1\n")
				case strings.HasPrefix(query, "INSERT INTO "+adpickRawTable), strings.HasPrefix(query, "INSERT INTO "+adpickSnapshotTable):
					if !strings.Contains(query, `"source":"adpick_biz"`) || !strings.Contains(query, `"event_uuid"`) {
						t.Error("durable catalog identity missing")
					}
				case strings.HasPrefix(query, "INSERT INTO "+adpickPublishedTable):
					for _, predicate := range []string{"FROM input(", "CROSS JOIN", "FROM " + adpickSnapshotTable, "actual_count != 1", "actual_unique != 1", "actual_merchants != 1", "actual_offers != 0", "SHA256(toString(actual_rows)) != SHA256(toString(expected_rows))", "actual_rows != expected_rows", "LIMIT 2"} {
						if !strings.Contains(query, predicate) {
							t.Errorf("marker missing executing-node snapshot proof %s", predicate)
						}
					}
					if failure == "marker_parity" || failure == "node_switch" {
						w.WriteHeader(http.StatusInternalServerError)
						_, _ = io.WriteString(w, "Code: 395. assertion failed")
						return
					}
					markers++
				case strings.Contains(query, "FROM "+adpickRawTable), strings.Contains(query, "FROM "+adpickSnapshotTable):
					record := records[0]
					if failure == "readback" {
						record.Title = "corrupt"
					}
					_ = json.NewEncoder(w).Encode(record)
				case strings.Contains(query, "SELECT count() FROM "+adpickPublishedTable):
					_, _ = io.WriteString(w, "1\n")
				default:
					t.Errorf("unexpected ClickHouse request %q", query)
				}
			}))
			defer server.Close()
			pub := &ClickHouseRawPublisher{cfg: ClickHouseRawConfig{URL: server.URL, DirectEndpointHostname: "clickhouse-s1-r1", InsertChunkSize: 100, ProducerSource: "github_actions", AttemptTimeout: time.Second}, client: server.Client()}
			err := publishAdpickCatalog(context.Background(), pub, records)
			if failure != "" {
				if err == nil || markers != 0 {
					t.Fatalf("tampered catalog published markers=%d err=%v", markers, err)
				}
			} else if err != nil || markers != 1 {
				t.Fatalf("valid directory not published markers=%d err=%v", markers, err)
			}
		})
	}
}

func TestAdpickWorkflowGateAndDedicatedCollectionMode(t *testing.T) {
	body, err := os.ReadFile(".github/workflows/adpick-catalog.yml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	start := strings.Index(text, "  adpick_catalog:\n")
	if start < 0 {
		t.Fatal("missing Adpick job")
	}
	job := text[start:]
	for _, required := range []string{"vars.ADPICK_CATALOG_ENABLED != 'false'", "secrets.ADPICK_API", `ADPICK_COLLECT_ONLY: 'true'`, "ADPICK_QUERY_PROFILE: expanded", "adpick_coverage_summary.py", "actions/upload-artifact@v4", "local:" + adpickRawTable, "local:" + adpickSnapshotTable, "local:" + adpickPublishedTable, "Collect verify and publish travel and services catalogs"} {
		if !strings.Contains(job, required) {
			t.Errorf("missing workflow contract %s", required)
		}
	}
	if strings.Index(job, "Gate ClickHouse catalog writes") > strings.Index(job, "Collect verify and publish") {
		t.Fatal("writer starts before pressure gate")
	}
	pub := &ClickHouseRawPublisher{}
	if !pub.allowedRawReplayTarget(adpickRawTable) || !pub.allowedRawReplayTarget(adpickSnapshotTable) || pub.allowedRawReplayTarget(adpickPublishedTable) {
		t.Fatal("catalog outbox replay target boundaries wrong")
	}
}

func TestAdpickGrantRequiresAffirmativeScalar(t *testing.T) {
	for _, response := range []string{"1\n", "0\n", "", "1\n0\n", `{"result":1}`, "true\n", strings.Repeat("1", 20)} {
		t.Run("response="+strings.TrimSpace(response), func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				query := requestBody(t, r)
				if query != "CHECK GRANT INSERT ON "+adpickRawTable {
					t.Errorf("grant query must not append FORMAT or ignore the required target: %s", query)
				}
				_, _ = io.WriteString(w, response)
			}))
			defer server.Close()
			pub := rawPublisherForTest(server)
			err := pub.checkAdpickGrant(context.Background(), "INSERT", adpickRawTable)
			if (err == nil) != (response == "1\n") || calls != 1 {
				t.Fatalf("grant accepted=%t calls=%d response=%q", err == nil, calls, response)
			}
		})
	}
}

func TestAdpickDeniedGrantStopsBeforeAnyWrite(t *testing.T) {
	requests := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := requestBody(t, r)
		requests = append(requests, query)
		if query != "CHECK GRANT INSERT ON "+adpickRawTable {
			t.Errorf("denied grant allowed later preflight or write: %s", query)
		}
		_, _ = io.WriteString(w, "0\n")
	}))
	defer server.Close()
	pub := rawPublisherForTest(server)
	if err := preflightAdpickCatalog(context.Background(), pub); err == nil || len(requests) != 1 {
		t.Fatalf("denied preflight did not stop: requests=%d err=%v", len(requests), err)
	}
}

func TestAdpickReplayAndOutboxKeepExecutingNodeGuard(t *testing.T) {
	records := []adpickCatalogRecord{{Source: "adpick_biz", Vertical: "travel", RecordType: "merchant", MerchantCode: "TRIP", MerchantKey: "trip_com", MerchantName: "트립닷컴", ItemKey: strings.Repeat("a", 64), Title: "트립닷컴", CollectedAt: "2026-09-06 01:02:03.004", CollectRunUUID: adpickTestRun, Version: 123}}
	rows, err := adpickCatalogRows(records, "github_actions", records[0].CollectedAt, true)
	if err != nil {
		t.Fatal(err)
	}
	batches, err := canonicalRawBatches(adpickRawTable, rows, 100)
	if err != nil {
		t.Fatal(err)
	}
	batch := batches[0]
	outboxUUID := "01900000-0000-7000-8000-000000000011"
	var initialBody, replayBody, outboxBody, mutationBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := requestBody(t, r)
		switch {
		case strings.Contains(query, "toString(outbox_uuid) AS outbox_uuid"):
			_ = json.NewEncoder(w).Encode(rawOutboxHeader{OutboxUUID: outboxUUID, TargetTable: adpickRawTable, RowCount: 1, DeduplicationToken: batch.token})
		case strings.HasPrefix(query, "SELECT 1 FROM "+adpickRawTable):
		case strings.HasPrefix(query, "SELECT rows_json"):
			_ = json.NewEncoder(w).Encode(rawOutboxPayloadRecord{RowsJSON: batch.rowsJSON})
		case strings.Contains(query, "uniqExact(event_uuid) AS unique_events"):
			if !strings.Contains(query, "adpick_catalog_endpoint_mismatch") {
				t.Error("reconciliation read is not guarded")
			}
			_, _ = io.WriteString(w, `{"matched":0,"unique_events":0}`)
		case strings.HasPrefix(query, "INSERT INTO "+adpickRawTable):
			if initialBody == "" {
				initialBody = query
				w.WriteHeader(http.StatusServiceUnavailable)
			} else {
				replayBody = query
			}
		case strings.HasPrefix(query, "INSERT INTO "+defaultShoppingRawOutboxTable):
			outboxBody = query
		case strings.HasPrefix(query, "ALTER TABLE "+defaultShoppingRawOutboxTable):
			mutationBody = query
		default:
			t.Errorf("unexpected request %s", query)
		}
	}))
	defer server.Close()
	pub := rawPublisherForTest(server)
	if err := pub.insertRawBatchWithOutbox(context.Background(), adpickRawTable, batch); err != nil {
		t.Fatal(err)
	}
	if err := pub.replayRawOutbox(context.Background()); err != nil {
		t.Fatal(err)
	}
	if initialBody != replayBody {
		t.Fatal("guarded replay changed canonical SQL, payload or token")
	}
	for name, query := range map[string]string{"initial": initialBody, "replay": replayBody, "outbox": outboxBody, "acknowledgement": mutationBody} {
		if !strings.Contains(query, "(SELECT throwIf(hostName() != 'clickhouse-test', 'adpick_catalog_endpoint_mismatch')) = 0") {
			t.Errorf("%s missing executing-node guard", name)
		}
	}
	pub.cfg.DirectEndpointHostname = "unsafe'host"
	if _, err := pub.catalogRawInsertBody(adpickRawTable, batch); err == nil {
		t.Fatal("guard accepted unsafe hostname")
	}
}

func TestAdpickPublicationLocalClickHouse(t *testing.T) {
	binary := os.Getenv("SHOPPING_TEST_CLICKHOUSE_LOCAL")
	if binary == "" {
		t.Skip("optional isolated ClickHouse SQL integration")
	}
	run := func(path, query, payload string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, binary, "local", "--path", path, "--max_threads", "2", "--background_schedule_pool_size", "2", "--multiquery", "--query", query)
		command.Stdin = strings.NewReader(payload)
		return command.CombinedOutput()
	}
	host, err := run(t.TempDir(), "SELECT hostName() FORMAT TabSeparatedRaw", "")
	if err != nil {
		t.Fatalf("local ClickHouse startup: %v %s", err, host)
	}
	records := []adpickCatalogRecord{{Source: "adpick_biz", Vertical: "travel", RecordType: "merchant", MerchantCode: "TRIP", MerchantKey: "trip_com", MerchantName: "트립닷컴", ItemKey: strings.Repeat("a", 64), Title: "트립닷컴 <&'", CollectedAt: "2026-09-06 01:02:03.004", CollectRunUUID: adpickTestRun, Version: 123}}
	marker := map[string]any{"refresh_uuid": adpickTestRun, "generated_at": records[0].CollectedAt, "source_max_collected_at": records[0].CollectedAt, "published_at": records[0].CollectedAt, "expected_merchant_count": 1, "merchant_count": 1, "offer_count": 0, "content_sha256": strings.Repeat("b", 64)}
	pub := &ClickHouseRawPublisher{cfg: ClickHouseRawConfig{DirectEndpointHostname: strings.TrimSpace(string(host))}}
	rows, err := adpickCatalogRows(records, "github_actions", records[0].CollectedAt, false)
	if err != nil {
		t.Fatal(err)
	}
	batches, err := canonicalRawBatches(adpickSnapshotTable, rows, 100)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := pub.catalogRawInsertBody(adpickSnapshotTable, batches[0])
	if err != nil {
		t.Fatal(err)
	}
	ddl := "CREATE DATABASE Data_Shopping_Service; CREATE TABLE " + adpickSnapshotTable + " (" + adpickInputSchema + ", event_uuid UUID, snapshot_uuid UUID, generated_at DateTime64(3, 'Asia/Seoul')) ENGINE=MergeTree ORDER BY tuple(); CREATE TABLE " + adpickPublishedTable + " (refresh_uuid UUID, vertical String, version UInt64, collect_run_uuid UUID, generated_at DateTime64(3, 'Asia/Seoul'), source_max_collected_at DateTime64(3, 'Asia/Seoul'), expected_merchant_count UInt16, merchant_count UInt16, offer_count UInt64, row_count UInt64, content_sha256 FixedString(64), published_at DateTime64(3, 'Asia/Seoul')) ENGINE=MergeTree ORDER BY tuple();\n"
	for _, failure := range []string{"", "contents", "node", "raw_node"} {
		t.Run("failure="+failure, func(t *testing.T) {
			pub.cfg.DirectEndpointHostname = strings.TrimSpace(string(host))
			expected := append([]adpickCatalogRecord(nil), records...)
			if failure == "contents" {
				expected[0].Title = "changed after readback"
			}
			if failure == "node" || failure == "raw_node" {
				pub.cfg.DirectEndpointHostname = "different-node"
			}
			body, err := adpickPublicationBody(pub, expected, marker)
			if err != nil {
				t.Fatal(err)
			}
			insert := snapshot
			if failure == "raw_node" {
				insert, err = pub.catalogRawInsertBody(adpickSnapshotTable, batches[0])
				if err != nil {
					t.Fatal(err)
				}
			}
			path := t.TempDir()
			if output, err := run(path, ddl, ""); err != nil {
				t.Fatalf("local fixture schema: %v %s", err, output)
			}
			insertParts := strings.SplitN(string(insert), "FORMAT JSONEachRow\n", 2)
			output, err := run(path, insertParts[0]+"FORMAT JSONEachRow", insertParts[1])
			if failure == "raw_node" {
				if err == nil || !strings.Contains(string(output), "adpick_catalog_endpoint_mismatch") {
					t.Fatalf("local insert host guard: %v %s", err, output)
				}
				return
			}
			if err != nil {
				t.Fatalf("local fixture insert: %v %s", err, output)
			}
			markerParts := strings.SplitN(string(body), "FORMAT JSONEachRow\n", 2)
			output, err = run(path, markerParts[0]+"FORMAT JSONEachRow", markerParts[1])
			if failure == "" {
				if err != nil {
					t.Fatalf("valid local publication: %v %s", err, output)
				}
			} else if err == nil || !strings.Contains(string(output), "adpick_catalog_") {
				t.Fatalf("local guard did not reject %s: %v %s", failure, err, output)
			}
			output, err = run(path, "SELECT count() FROM "+adpickPublishedTable+" FORMAT TabSeparated", "")
			want := "0"
			if failure == "" {
				want = "1"
			}
			if err != nil || strings.TrimSpace(string(output)) != want {
				t.Fatalf("local marker rows want=%s: %v %s", want, err, output)
			}
		})
	}
}
