package main

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestShoppingPressureFailurePreventsLedgerAndProviderWork(t *testing.T) {
	events := []string{}
	blocked := errors.New("pressure_failed")
	err := runShoppingCollectorPhase(context.Background(), func(context.Context) error {
		events = append(events, "pressure")
		return blocked
	}, func(context.Context) error {
		events = append(events, "preflight")
		return nil
	}, func(context.Context) {
		events = append(events, "record", "merchant_fetch")
	})
	if !errors.Is(err, blocked) || !reflect.DeepEqual(events, []string{"pressure"}) {
		t.Fatalf("work started after pressure failure: %v %v", events, err)
	}
}

func TestShoppingCollectorPhaseOrdersGateBeforeLedgerAndProviders(t *testing.T) {
	events := []string{}
	err := runShoppingCollectorPhase(context.Background(), func(context.Context) error {
		events = append(events, "pressure")
		return nil
	}, func(context.Context) error {
		events = append(events, "preflight")
		return nil
	}, func(context.Context) {
		events = append(events, "record", "merchant_fetch")
	})
	if err != nil || !reflect.DeepEqual(events, []string{"pressure", "preflight", "record", "merchant_fetch"}) {
		t.Fatalf("unexpected collection phase: %v %v", events, err)
	}
}

func TestShoppingPreflightFailurePreventsLedgerAndProviderWork(t *testing.T) {
	err := runShoppingCollectorPhase(context.Background(), func(context.Context) error { return nil },
		func(context.Context) error { return errors.New("schema_failed") },
		func(context.Context) { t.Fatal("collection started after failed schema preflight") })
	if err == nil {
		t.Fatal("preflight failure ignored")
	}
}

func TestShoppingCollectionGateTargetsActiveRawAndPhysicalLedger(t *testing.T) {
	previousGmarket, previousKurly, previousMode, previousCount := GmarketCollectEnabled, KurlyCollectEnabled, CollectMode, RandomKeywordCount
	defer func() {
		GmarketCollectEnabled, KurlyCollectEnabled, CollectMode, RandomKeywordCount = previousGmarket, previousKurly, previousMode, previousCount
	}()
	GmarketCollectEnabled, KurlyCollectEnabled, CollectMode, RandomKeywordCount = true, false, "search_keywords", 2
	t.Setenv("CLICKHOUSE_PRESSURE_GATE_TARGETS", "local:unrelated.table")
	t.Setenv("CLICKHOUSE_PRESSURE_GATE_MAX_IOWAIT_NORMALIZED", "0.9")
	t.Setenv("ADPICK_BIZ_API_KEY", "fixture-secret")
	pub := &ClickHouseRawPublisher{cfg: ClickHouseRawConfig{
		URL: "http://fixture.invalid:8123/", User: "fixture", Password: "fixture",
		DirectEndpointHostname: "clickhouse-s1-r1", GmarketTable: "`Data_Shopping_Raw`.`gmarket_product_raw_endpoint_history`",
		KurlyTable: "`Data_Shopping_Raw`.`kurly_product_raw_endpoint_history`", OutboxTable: "`Data_Shopping_Log`.`shopping_raw_direct_insert_outbox`",
	}}
	values := map[string]string{}
	for _, entry := range shoppingCollectionGateEnv(pub) {
		name, value, _ := strings.Cut(entry, "=")
		values[name] = value
	}
	want := "local:Data_Shopping_Raw.gmarket_product_raw_endpoint_history,local:Data_Shopping_Log.shopping_raw_direct_insert_outbox,replica:Data_Content_Lexicon.keyword_selection_log_local"
	if values["CLICKHOUSE_PRESSURE_GATE_TARGETS"] != want || values["CLICKHOUSE_HOST"] != pub.cfg.URL ||
		values["CLICKHOUSE_DIRECT_ENDPOINT_HOSTNAME"] != "clickhouse-s1-r1" || values["CLICKHOUSE_PRESSURE_GATE_MAX_IOWAIT_NORMALIZED"] != "0.50" || values["ADPICK_BIZ_API_KEY"] != "" {
		t.Fatal("incorrect exact targets, endpoint, I/O cap, or secret isolation")
	}
	t.Setenv("CLICKHOUSE_PRESSURE_GATE_MAX_IOWAIT_NORMALIZED", "0.2")
	for _, entry := range shoppingCollectionGateEnv(pub) {
		if strings.HasPrefix(entry, "CLICKHOUSE_PRESSURE_GATE_MAX_IOWAIT_NORMALIZED=") && entry != "CLICKHOUSE_PRESSURE_GATE_MAX_IOWAIT_NORMALIZED=0.2" {
			t.Fatal("stricter I/O threshold was relaxed")
		}
	}
}
