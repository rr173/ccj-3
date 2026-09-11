#!/usr/bin/env bash
# End-to-end demo of the stream aligner. Assumes the compose stack is up:
#   docker compose up -d --build
# Exercises: normal alignment, duplicates, late-event correction, retraction,
# idle-stream watermarking (advances but never closes waiting businesses),
# watermark regression, the audit trail — and downstream delivery:
# subscription registration, in-order push of every version (NEW / CORRECTION
# / WITHDRAWAL), and delivery-status queries.
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

say "3. both watermarks pass the window -> INITIAL result (held: computed but not released)"
post "$A/events" '{"event_id":"a-hb","event_time":'"$PUSH"',"key":"__hb__"}' >/dev/null
post "$B/events" '{"events":[
  {"event_id":"b2","event_time":'"$((WS+2500))"',"key":"order-1","payload":{"ship":"UPS"}},
  {"event_id":"b-hb","event_time":'"$PUSH"',"key":"__hb__"}]}' >/dev/null
wait_version "$WS" order-1 1
echo "internal result exists, released=false and the outbox is empty:"
curl -sf "$R/results/current?window_start=$WS&key=order-1" | field "{'version': d['result']['version'], 'released': d['result']['released']}"
curl -sf "$R/deliveries?window_start=$WS&key=order-1" | field "len(d['deliveries'])"

say "3b. one-shot external release of order-1: the then-current head goes out as NEW"
post "$R/releases" '{"key":"order-1"}' | json
sleep 2
echo "released=true, one NEW delivery pushed, held backlog is empty:"
curl -sf "$R/results/delivery?window_start=$WS&key=order-1" \
  | field "{released: d['released'], released_version: d['released_version'], ups: {s['subscriber']: s['delivered_up_to'] for s in d['subscribers']}}"
curl -sf "$R/releases/backlog?key=order-1" | field "len(d['held'])"

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

say "7. idle stream: wall-clock watermark finalizes only sides that already have data"
echo "waiting ~25s for the 20s idle timeout..."
sleep 25
echo "watermark source flips to idle_timeout:"
curl -sf "$A/watermark" | json
echo "the __hb__ window's end is still ahead of the idle watermark — not closed yet:"
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

say "9b. release gate on a fresh key: held -> open/flush -> close holds new windows, released ones keep flowing"
GKEY="order-gate"
GWS=$(( $(python3 -c 'import time; print(int(time.time()*1000))') / WINDOW_MS * WINDOW_MS ))
GPUSH=$(( GWS + WINDOW_MS + 8000 ))
post "$A/watermark/override" '{"watermark":null}' >/dev/null
post "$B/watermark/override" '{"watermark":null}' >/dev/null
post "$A/events" '{"events":[
  {"event_id":"g-a1","event_time":'"$((GWS+1000))"',"key":"'"$GKEY"'","payload":{"n":1}},
  {"event_id":"g-a-hb","event_time":'"$GPUSH"',"key":"__hb__"}]}' >/dev/null
post "$B/events" '{"events":[
  {"event_id":"g-b1","event_time":'"$((GWS+1500))"',"key":"'"$GKEY"'","payload":{"s":"x"}},
  {"event_id":"g-b-hb","event_time":'"$GPUSH"',"key":"__hb__"}]}' >/dev/null
wait_version "$GWS" "$GKEY" 1
echo "gate defaults closed: result computed (queryable) but held, nothing pushed:"
curl -sf "$R/releases/backlog?key=$GKEY" | json
post "$A/events" '{"event_id":"g-a2-late","event_time":'"$((GWS+600))"',"key":"'"$GKEY"'","payload":{"n":2}}' >/dev/null
wait_version "$GWS" "$GKEY" 2
echo "a late event while held moved the internal head to v2 (v1 never goes out):"
curl -sf "$R/releases/backlog?key=$GKEY" | field "[(h['version'], h['releasable_now']) for h in d['held']]"
post "$R/release-gates" '{"key":"'"$GKEY"'","open":true}' | json
sleep 2
echo "opening flushed the head at THAT moment — first delivery is NEW v2, not v1:"
curl -sf "$R/results/delivery?window_start=$GWS&key=$GKEY" \
  | field "{ups: {s['subscriber']: s['delivered_up_to'] for s in d['subscribers']}, released_version: d['released_version']}"
post "$R/release-gates" '{"key":"'"$GKEY"'","open":false}' | json
post "$B/events" '{"event_id":"g-b2-late","event_time":'"$((GWS+700))"',"key":"'"$GKEY"'","payload":{"s":"y"}}' >/dev/null
wait_version "$GWS" "$GKEY" 3
sleep 2
echo "gate closed AFTER release: v3 correction still delivered, gate state does not swallow it:"
curl -sf "$R/results/delivery?window_start=$GWS&key=$GKEY" \
  | field "{ups: {s['subscriber']: s['delivered_up_to'] for s in d['subscribers']}}"
echo "release actions recorded for the key:"
curl -sf "$R/releases/history?key=$GKEY" | field "[(a['action'], a['windows']) for a in d['actions']]"

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

# ---------------------------------------------------------------------------
# downstream posting ledger: delivered != booked — the downstream reports
# which version it actually posted, and the ledger gates per result
# ---------------------------------------------------------------------------
say "10b. posting ledger: the downstream itself reports what it booked"
echo "demo-sink reports it posted order-1@WS only up to v1, while v4 is delivered:"
post "$R/postings" "{\"subscriber_name\":\"demo-sink\",\"window_start\":$WS,\"key\":\"order-1\",\"version\":1}" | json
echo "-> reported 1, delivered 4 = LAGGING:"
curl -sf "$R/postings?subscriber=demo-sink&key=order-1" \
  | field "[(p['key'], p['reported_version'], p['delivered_up_to'], p['status'], p['gated']) for p in d['postings']]"
echo "it catches up by reporting v4 -> ALIGNED:"
post "$R/postings" "{\"subscriber_name\":\"demo-sink\",\"window_start\":$WS,\"key\":\"order-1\",\"version\":4}" | json

say "10c. a new delivery on the aligned pair turns it LAGGING and holds later versions"
post "$B/events" '{"event_id":"b5-post","event_time":'"$((WS+900))"',"key":"order-1","payload":{"ship":"knocks-lagging"}}' >/dev/null
wait_version "$WS" order-1 5
sleep 3
echo "v5 delivered -> the pair is LAGGING again (reported 4, delivered 5):"
curl -sf "$R/postings?subscriber=demo-sink&key=order-1" \
  | field "[(p['reported_version'], p['delivered_up_to'], p['status'], p['gated']) for p in d['postings']]"
post "$B/events" '{"event_id":"b6-post","event_time":'"$((WS+1000))"',"key":"order-1","payload":{"ship":"held-while-lagging"}}' >/dev/null
wait_version "$WS" order-1 6
sleep 3
echo "v6 computed but HELD while the pair lags — still PENDING in the outbox:"
curl -sf "$R/deliveries?window_start=$WS&key=order-1&status=PENDING" \
  | field "[(d['version'], d['kind'], d['status']) for d in d['deliveries']]"
echo "reporting the still-held v6 is rejected too (never delivered, does not count):"
curl -s -o /dev/null -w '  http status: %{http_code}\n' -X POST "$R/postings" \
  -H 'Content-Type: application/json' \
  -d "{\"subscriber_name\":\"demo-sink\",\"window_start\":$WS,\"key\":\"order-1\",\"version\":6}"
echo "reporting a version we never sent (v99) is rejected and does not count:"
curl -s -o /dev/null -w '  http status: %{http_code}\n' -X POST "$R/postings" \
  -H 'Content-Type: application/json' \
  -d "{\"subscriber_name\":\"demo-sink\",\"window_start\":$WS,\"key\":\"order-1\",\"version\":99}"
echo "it reports catching up to v5 -> gate lifts -> held v6 flows:"
post "$R/postings" "{\"subscriber_name\":\"demo-sink\",\"window_start\":$WS,\"key\":\"order-1\",\"version\":5}" | json
sleep 3
curl -sf "$R/postings?subscriber=demo-sink&key=order-1" \
  | field "[(p['reported_version'], p['delivered_up_to'], p['status']) for p in d['postings']]"
post "$R/postings" "{\"subscriber_name\":\"demo-sink\",\"window_start\":$WS,\"key\":\"order-1\",\"version\":6}" | json

say "10d. the ledger trail: when it flipped aligned->lagging, and which version did it"
curl -sf "$R/postings/history?subscriber=demo-sink&window_start=$WS&key=order-1" \
  | field "[(e['event'], e['cause_version'], e['prev_status'], e['status']) for e in d['events']]"

# ---------------------------------------------------------------------------
# business orders: one business key spanning two windows, full lifecycle
# ---------------------------------------------------------------------------
say "11. business orders: one key across two windows (open -> waiting -> closed)"
WS2=$(( $(python3 -c 'import time; print(int(time.time()*1000))') / WINDOW_MS * WINDOW_MS ))
OKEY="order-2"
echo "order windows: [$WS2, $((WS2+WINDOW_MS))) and [$((WS2+WINDOW_MS)), $((WS2+2*WINDOW_MS)))"
post "$A/events" '{"events":[
  {"event_id":"o2-a1","event_time":'"$((WS2+1000))"',"key":"'"$OKEY"'","payload":{"amount":10}},
  {"event_id":"o2-a2","event_time":'"$((WS2+WINDOW_MS+1000))"',"key":"'"$OKEY"'","payload":{"amount":20}}]}' >/dev/null
post "$B/events" '{"event_id":"o2-b1","event_time":'"$((WS2+1500))"',"key":"'"$OKEY"'","payload":{"ship":"DHL"}}' >/dev/null
# pin both watermarks so only the first window closes (deterministic staging)
post "$A/watermark/override" "{\"watermark\":$((WS2+WINDOW_MS))}" >/dev/null
post "$B/watermark/override" "{\"watermark\":$((WS2+WINDOW_MS))}" >/dev/null
sleep 3
echo "first window closed, second still pending -> order is OPEN:"
curl -sf "$R/orders/current?key=$OKEY" | json

say "11b. second window closes single-sided -> WAITING (waiting for the other side)"
post "$A/watermark/override" "{\"watermark\":$((WS2+2*WINDOW_MS))}" >/dev/null
post "$B/watermark/override" "{\"watermark\":$((WS2+2*WINDOW_MS))}" >/dev/null
sleep 3
curl -sf "$R/orders/current?key=$OKEY" | json

say "11c. late B event matches the gap -> CLOSED (success order)"
post "$B/events" '{"event_id":"o2-b2","event_time":'"$((WS2+WINDOW_MS+1500))"',"key":"'"$OKEY"'","payload":{"ship":"UPS"}}' >/dev/null
sleep 3
curl -sf "$R/orders/current?key=$OKEY" | json
echo "close snapshot: each window bound at its result version"
curl -sf "$R/orders/history?key=$OKEY" | python3 -c '
import json, sys
h = json.load(sys.stdin)
for v in h["versions"]:
    if v["status"] == "CLOSED":
        print(json.dumps({"version": v["version"], "reason": v["reason"],
                          "snapshot": [(w["window_start"], w["result_version"]) for w in v["snapshot"]]},
                         ensure_ascii=False))
        break'

say "11d. benign correction after close -> REOPENED even though it still matches"
# add another matched pair to the already-closed first window: the window's
# content changes but both sides still match — the order must still reopen
post "$A/events" '{"event_id":"o2-a3","event_time":'"$((WS2+500))"',"key":"'"$OKEY"'","payload":{"amount":11}}' >/dev/null
post "$B/events" '{"event_id":"o2-b3","event_time":'"$((WS2+800))"',"key":"'"$OKEY"'","payload":{"ship":"DHL"}}' >/dev/null
sleep 3
echo "still fully matched, yet NOT closed:"
curl -sf "$R/orders/current?key=$OKEY" | json
echo "why was it reopened (which window, which result version):"
curl -sf "$R/orders/history?key=$OKEY" | python3 -c '
import json, sys
h = json.load(sys.stdin)
v = h["versions"][-1]
print(json.dumps({k: v[k] for k in ("version", "status", "reason",
      "trigger_window_start", "trigger_result_version")}, ensure_ascii=False))'
echo "and it re-closes when later activity finds it complete again:"
post "$A/events" '{"event_id":"o2-a4","event_time":'"$((WS2+600))"',"key":"'"$OKEY"'","payload":{"amount":12}}' >/dev/null
post "$B/events" '{"event_id":"o2-b4","event_time":'"$((WS2+900))"',"key":"'"$OKEY"'","payload":{"ship":"UPS"}}' >/dev/null
sleep 3
curl -sf "$R/orders/current?key=$OKEY" | python3 -c '
import json, sys
o = json.load(sys.stdin)["order"]
print("  status:", o["status"], " head_version:", o["head_version"])'

say "11e. withdraw ONE window of the closed order -> REOPENED, and it stays open"
# retract everything in the second window; the first window is still matched
post "$A/events" '{"event_id":"o2-r1","event_time":'"$(python3 -c 'import time; print(int(time.time()*1000))')"',"key":"'"$OKEY"'","type":"retract","retracts":"o2-a2"}' >/dev/null
post "$B/events" '{"event_id":"o2-r2","event_time":'"$(python3 -c 'import time; print(int(time.time()*1000))')"',"key":"'"$OKEY"'","type":"retract","retracts":"o2-b2"}' >/dev/null
sleep 3
echo "one window withdrawn, the other still matched — must NOT show closed:"
curl -sf "$R/orders/current?key=$OKEY" | json
sleep 3
echo "and it does not flip back to closed while the withdrawal is unresolved:"
curl -sf "$R/orders/current?key=$OKEY" | python3 -c '
import json, sys
o = json.load(sys.stdin)["order"]
print("  status:", o["status"])'

say "11f. withdraw the whole business -> VOID (never a success order)"
post "$A/events" '{"events":[
  {"event_id":"o2-r3","event_time":'"$(python3 -c 'import time; print(int(time.time()*1000))')"',"key":"'"$OKEY"'","type":"retract","retracts":"o2-a1"},
  {"event_id":"o2-r4","event_time":'"$(python3 -c 'import time; print(int(time.time()*1000))')"',"key":"'"$OKEY"'","type":"retract","retracts":"o2-a3"},
  {"event_id":"o2-r5","event_time":'"$(python3 -c 'import time; print(int(time.time()*1000))')"',"key":"'"$OKEY"'","type":"retract","retracts":"o2-a4"}]}' >/dev/null
post "$B/events" '{"events":[
  {"event_id":"o2-r6","event_time":'"$(python3 -c 'import time; print(int(time.time()*1000))')"',"key":"'"$OKEY"'","type":"retract","retracts":"o2-b1"},
  {"event_id":"o2-r7","event_time":'"$(python3 -c 'import time; print(int(time.time()*1000))')"',"key":"'"$OKEY"'","type":"retract","retracts":"o2-b3"},
  {"event_id":"o2-r8","event_time":'"$(python3 -c 'import time; print(int(time.time()*1000))')"',"key":"'"$OKEY"'","type":"retract","retracts":"o2-b4"}]}' >/dev/null
sleep 4
curl -sf "$R/orders/current?key=$OKEY" | json
echo "voided orders are not success orders:"
curl -sf "$R/orders?status=CLOSED" | python3 -c '
import json, sys
print("  CLOSED keys:", [o["key"] for o in json.load(sys.stdin)["orders"]])'
curl -sf "$R/orders?status=VOID" | python3 -c '
import json, sys
print("  VOID keys:  ", [o["key"] for o in json.load(sys.stdin)["orders"]])'

say "11g. new results after void: the SAME order continues (a voided order can never pose as new)"
post "$A/events" '{"events":[
  {"event_id":"o2-a5","event_time":'"$((WS2+700))"',"key":"'"$OKEY"'","payload":{"amount":13}},
  {"event_id":"o2-a6","event_time":'"$((WS2+WINDOW_MS+700))"',"key":"'"$OKEY"'","payload":{"amount":23}}]}' >/dev/null
post "$B/events" '{"events":[
  {"event_id":"o2-b5","event_time":'"$((WS2+1100))"',"key":"'"$OKEY"'","payload":{"ship":"SF"}},
  {"event_id":"o2-b6","event_time":'"$((WS2+WINDOW_MS+1100))"',"key":"'"$OKEY"'","payload":{"ship":"SF"}}]}' >/dev/null
sleep 3
curl -sf "$R/orders/current?key=$OKEY" | json
echo "full order history (the void chapter stays on record):"
curl -sf "$R/orders/history?key=$OKEY" | python3 -c '
import json, sys
h = json.load(sys.stdin)
for v in h["versions"]:
    print("  v%-2d %-8s %-18s trigger=(%d, result v%d)" % (
        v["version"], v["status"], v["reason"],
        v["trigger_window_start"], v["trigger_result_version"]))'
post "$A/watermark/override" '{"watermark":null}' >/dev/null
post "$B/watermark/override" '{"watermark":null}' >/dev/null
echo "overrides cleared"

say "demo finished OK"
