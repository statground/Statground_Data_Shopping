#!/usr/bin/env bash
set -euo pipefail

url=$1
query="SELECT count() FROM Data_Shopping_Log.shopping_raw_direct_insert_outbox WHERE replayed_at IS NULL AND replaceAll(target_table, char(96), '') IN ('Data_Shopping_Raw.gmarket_product_raw_endpoint_history', 'Data_Shopping_Raw.kurly_product_raw_endpoint_history') SETTINGS max_threads=1, max_execution_time=10 FORMAT TabSeparatedRaw"

report() {
  printf '%s\n' "$1"
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    printf '%s\n' "$1" >> "$GITHUB_STEP_SUMMARY"
  fi
}

if ! pending="$(curl -fsS --max-time 20 -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" "$url" --data-binary "$query")"; then
  report 'Shopping outbox check failed: ClickHouse did not return a count. Publication remains gated.'
  exit 1
fi
if [[ ! "$pending" =~ ^[0-9]+$ ]]; then
  report 'Shopping outbox check failed: ClickHouse returned an invalid count. Publication remains gated.'
  exit 1
fi
if [[ "$pending" != '0' ]]; then
  report "Shopping raw outbox holds $pending Gmarket or Kurly batches. Run the bounded manual raw outbox replay before publication."
  exit 1
fi
report 'Shopping raw outbox verified: 0 pending Gmarket or Kurly batches.'
