#!/usr/bin/env bash
# End-to-end demo of the stream aligner. Assumes the compose stack is up:
#   docker compose up -d --build
# Exercises: normal alignment, duplicates, late-event correction, retraction,
# idle-stream window closing, watermark regression, the audit trail — and
# downstream delivery: subscription registration, in-order push of every
# version (NEW / CORRECTION / WITHDRAWAL), and delivery-status queries.
set -euo pipefail

A=${INGEST_A_URL:-http://localhost:8001}
B=${INGEST_B_URL:-http://localhost:8002}
R=${ALIGNER_URL:-http://localhost:8003}
WINDOW_MS=30000
RECV_PORT=${RECEIVER_PORT:-8901}
# how the aligner container reaches the receiver started below
RECV_BASE=${RECEIVER_BASE_URL:-http://host.docker.internal:$RECV_PORT}

say()  { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
json() { python3 -m json.tool; }
field() { python3 -c "import sys,json; d=json.load(sys.stdin); print(eval(sys.argv[1]))" "$1"; }

post() { # url payload
  curl -sf -X POST "$1" -H 'Content-Type: application/json' -d "$2"
}

wait_version() { # window_start key expected_version -> waits until head reaches it
  local ws=$1 key=$2 want=$3 tries=0 got
  while [ $tries -lt 60 ]; do
    got=$(curl -sf "$R/results/current?window_start=$ws&key=$key" | field "d['result']['version'] if d['result'] else 0" 2>/dev/null || echo 0)
    if [ "$got" -ge "$want" ]; then echo "  -> version $got"; return 0; fi
    tries=$((tries+1)); sleep 1
  done
  echo "  !! timed out waiting for version $want (got $got)"; return 1
}

NOW=$(python3 -c 'import time; print(int(time.time()*1000))')
WS=$(( NOW / WINDOW_MS * WINDOW_MS ))          # current 30s window
PUSH=$(( WS + WINDOW_MS + 8000 ))              # beyond window end + 5s grace

say "0. health"
curl -sf "$A/healthz" >/dev/null && curl -sf "$B/healthz" >/dev/null && curl -sf "$R/healthz" >/dev/null \
  && echo "all services healthy"
echo "demo window: [$WS, $((WS+WINDOW_MS)))"

say "0b. downstream receiver + subscription registration"
RECV_LOG=$(mktemp -t aligner-demo-recv.XXXXXX)
python3 "$(dirname "$0")/demo_receiver.py" "$RECV_PORT" >"$RECV_LOG" 2>&1 &
RECV_PID=$!
trap 'kill "$RECV_PID" 2>/dev/null || true' EXIT
sleep 1
post "$R/subscriptions" "{\"name\":\"demo-sink\",\"url\":\"$RECV_BASE/recv\"}" | json

say "1. normal events on both streams (window not yet closed)"
post "$A/events" '{"events":[
  {"event_id":"a1","event_time":'"$((WS+1000))"',"key":"order-1","payload":{"amount":100}},
  {"event_id":"a2","event_time":'"$((WS+2000))"',"key":"order-1","payload":{"amount":200}}]}' | json
post "$B/events" '{"event_id":"b1","event_time":'"$((WS+1500))"',"key":"order-1","payload":{"ship":"DHL"}}' | json
echo "result before watermarks pass (expect null):"
curl -sf "$R/results/current?window_start=$WS&key=order-1" | json

say "2. duplicate delivery is deduped, never double-counted"
post "$A/events" '{"event_id":"a1","event_time":'"$((WS+1000))"',"key":"order-1","payload":{"amount":100}}' | json

say "3. both watermarks pass the window -> INITIAL result"
post "$A/events" '{"event_id":"a-hb","event_time":'"$PUSH"',"key":"__hb__"}' >/dev/null
post "$B/events" '{"events":[
  {"event_id":"b2","event_time":'"$((WS+2500))"',"key":"order-1","payload":{"ship":"UPS"}},
  {"event_id":"b-hb","event_time":'"$PUSH"',"key":"__hb__"}]}' >/dev/null
wait_version "$WS" order-1 1
curl -sf "$R/results/current?window_start=$WS&key=order-1" | json

say "4. LATE event for the already-closed window -> correction v2"
post "$B/events" '{"event_id":"b3-late","event_time":'"$((WS+500))"',"key":"order-1","payload":{"ship":"late"}}' | json
wait_version "$WS" order-1 2
curl -sf "$R/results/current?window_start=$WS&key=order-1" | field "d['result']['payload']" | json

say "5. retraction of a1 -> correction v3"
post "$A/events" '{"event_id":"r1","event_time":'"$NOW"',"key":"order-1","type":"retract","retracts":"a1"}' | json
wait_version "$WS" order-1 3
curl -sf "$R/results/current?window_start=$WS&key=order-1" | field "d['result']['payload']" | json

say "6. WHY did the result change? full history + audit"
curl -sf "$R/results/history?window_start=$WS&key=order-1" | json

say "7. idle stream: no new events, watermark still advances past new windows"
echo "waiting ~25s for the 20s idle timeout..."
sleep 25
curl -sf "$A/watermark" | json
curl -sf "$R/windows" | field "[w for w in d['windows'] if w['key']=='__hb__']" | json

say "8. watermark regression (operator dials stream A back)"
post "$A/watermark/override" '{"watermark":'"$WS"'}' | json
sleep 3
echo "aligner watermark log (expect a 'regress' entry):"
curl -sf "$R/watermarks/history?stream=a" | json
echo "existing results are NOT silently rewritten:"
curl -sf "$R/results/current?window_start=$WS&key=order-1" | field "d['result']['version']"
say "9. late data still corrects results while watermark is regressed"
post "$B/events" '{"event_id":"b4-later","event_time":'"$((WS+800))"',"key":"order-1","payload":{"ship":"while-regressed"}}' >/dev/null
wait_version "$WS" order-1 4
curl -sf "$R/audit?window_start=$WS&key=order-1" | json
post "$A/watermark/override" '{"watermark":null}' >/dev/null
echo "override cleared"

say "10. downstream delivery: every version pushed in order, corrections flagged"
echo "waiting for the delivery queue to drain..."
sleep 5
echo "what demo-sink received for order-1 (kind / version / payload):"
python3 - "$RECV_LOG" <<'EOF'
import json, sys
rows = []
for line in open(sys.argv[1]):
    line = line.strip()
    if line.startswith("{"):
        rows.append(json.loads(line))
mine = [d for d in rows if d.get("key") == "order-1"]
for d in mine:
    print(f"  v{d['version']} {d['kind']:<10} pairs={len((d.get('payload') or {}).get('pairs', []))}")
print(f"  total deliveries received: {len(rows)}")
EOF
echo "per-result delivery ladder (which version delivered, which still retrying):"
curl -sf "$R/results/delivery?window_start=$WS&key=order-1" | json
echo "raw outbox view:"
curl -sf "$R/deliveries?window_start=$WS&key=order-1" | json
echo "subscription backlog counters:"
curl -sf "$R/subscriptions" | json

say "demo finished OK"
