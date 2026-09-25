#!/usr/bin/env bash
# Load test: Stripe to ERP cash application with webhooks, against the fake
# Stripe, a Temporal dev server and Postgres. Prints throughput and run
# latency percentiles.
#
#   scripts/loadtest.sh [payments] [extra turgon run flags...]
#
# Needs the temporal CLI, psql and a Postgres URL in LOADTEST_DATABASE_URL:
# a scratch database, where the test creates the demo ERP tables and
# empties erp.payments. Temporal
# runs in memory: the dev server's SQLite file limits throughput to a few
# runs a second and would measure the disk, not Turgon.
set -euo pipefail
n=${1:-500}
shift || true
db=${LOADTEST_DATABASE_URL:?set LOADTEST_DATABASE_URL}
root=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$work"; }
trap cleanup EXIT

go build -o "$work/turgon" "$root/cmd/turgon"
go build -o "$work/fakestripe" "$root/internal/tools/fakestripe"
cp -r "$root/examples" "$work/cat"
# The fake Stripe has no rate limit: lift the connection's, to measure Turgon.
sed -i 's|https://api.stripe.com|http://127.0.0.1:9400|; s|limits: {[^}]*}|limits: { maxConcurrentCalls: 64, requestsPerSecond: 1000 }|' \
  "$work/cat/connections/stripe-billing.yaml"
"$work/turgon" compile -c "$work/cat" stripe-payments-to-erp -o "$work/spec.json" >/dev/null
psql -q "$db" -f "$root/examples/sql/demo.sql" 2>/dev/null
psql -q "$db" -c "TRUNCATE erp.payments"

temporal server start-dev --headless --port 7233 --ui-port 8233 >"$work/temporal.log" 2>&1 & pids+=($!)
for _ in $(seq 60); do temporal operator cluster health >/dev/null 2>&1 && break; sleep 1; done

export TURGON_SECRET_ERP_DB_DSN=$db TURGON_SECRET_STRIPE_BILLING_RESTRICTED_KEY=rk_test_demo \
  TURGON_SECRET_STRIPE_BILLING_WEBHOOK_SECRET=whsec_demo
"$work/turgon" xref set --database-url "$db" --entity Customer --system stripe-billing --source cus_ada --master C-100 >/dev/null
"$work/turgon" run -s "$work/spec.json" --database-url "$db" --webhook-listen 127.0.0.1:8082 "$@" >"$work/run.log" 2>&1 & pids+=($!)
mkfifo "$work/cmds"
"$work/fakestripe" -webhook-url http://127.0.0.1:8082/webhooks/stripe-billing/Invoice.Paid -webhook-secret whsec_demo \
  <"$work/cmds" >"$work/fake.log" 2>&1 & pids+=($!)
exec 3>"$work/cmds"
sleep 3

start=$(date +%s.%N)
for i in $(seq "$n"); do echo "cus_ada $((10 + i % 90))"; done >&3
deadline=$((SECONDS + 600))
while (( $(psql -At "$db" -c "select count(*) from erp.payments") < n )); do
  if (( SECONDS > deadline )); then echo "not all payments were recorded in 10 minutes"; exit 1; fi
  sleep 0.5
done
# Then every run finishes: the ERP payment ID is written back to Stripe.
completed="WorkflowType='turgon.integration' AND ExecutionStatus='Completed'"
while (( $(temporal workflow count --query "$completed" -o json | python3 -c 'import json,sys; print(json.load(sys.stdin)["count"])') < n )); do
  if (( SECONDS > deadline )); then echo "not all runs completed in 10 minutes"; exit 1; fi
  sleep 0.5
done
elapsed=$(echo "$(date +%s.%N) - $start" | bc)
printf '%d payments recorded and linked in %.1fs: %.1f/s\n' "$n" "$elapsed" "$(echo "$n / $elapsed" | bc -l)"

temporal workflow list --query "WorkflowType='turgon.integration'" --limit "$n" -o json | python3 -c '
import datetime, json, sys
runs = json.load(sys.stdin)
t = lambda s: datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
lat = sorted((t(r["closeTime"]) - t(r["startTime"])).total_seconds() for r in runs if r.get("closeTime"))
done = sum(r["status"].endswith("COMPLETED") for r in runs)
q = lambda p: lat[int(p * (len(lat) - 1))]
print(f"{done}/{len(runs)} runs completed; latency p50 {q(.5):.2f}s p95 {q(.95):.2f}s p99 {q(.99):.2f}s")'
if grep -qi error "$work/run.log"; then
  echo "errors in the worker log:"
  grep -i error "$work/run.log" | head
  exit 1
fi
