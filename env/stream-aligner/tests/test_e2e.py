#!/usr/bin/env python3
"""End-to-end test against a running compose stack (stdlib only).

Usage:  docker compose up -d --build && python3 tests/test_e2e.py

Covers the required behaviours: dual-watermark gating, duplicates, in-window
disorder, late-event correction, retraction, idle streams, watermark
regression, idempotent recompute, the audit trail — and downstream delivery:
subscription registration, in-order push of every version (NEW / CORRECTION /
WITHDRAWAL), retry-with-backoff on receiver failure, and the delivery-status
query APIs.
"""
import json
import os
import sys
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

A = os.environ.get("INGEST_A_URL", "http://localhost:8001")
B = os.environ.get("INGEST_B_URL", "http://localhost:8002")
R = os.environ.get("ALIGNER_URL", "http://localhost:8003")
WINDOW_MS = int(os.environ.get("WINDOW_SIZE_MS", "30000"))

# Receiver for downstream deliveries. Runs in-process; the aligner reaches it
# via RECEIVER_BASE_URL (host.docker.internal works with the compose stack).
RECV_PORT = int(os.environ.get("RECEIVER_PORT", "8901"))
RECV_BASE = os.environ.get("RECEIVER_BASE_URL", f"http://host.docker.internal:{RECV_PORT}")

FAILED = []
DELIVERIES = []          # every 200-acked delivery the receiver has seen, in arrival order
FLAKY = {"fail_next": 0}  # /flaky endpoint fails this many next requests with 500
LOCK = threading.Lock()


class Receiver(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(n) or b"{}")
        sub = "flaky" if self.path.startswith("/flaky") else "good"
        with LOCK:
            if sub == "flaky" and FLAKY["fail_next"] > 0:
                FLAKY["fail_next"] -= 1
                code = 500
            else:
                code = 200
            if code == 200:
                DELIVERIES.append(dict(body, _sub=sub))
        self.send_response(code)
        self.end_headers()

    def log_message(self, *args):
        pass


def recv(sub, key):
    with LOCK:
        return [d for d in DELIVERIES if d["_sub"] == sub and d.get("key") == key]


def check(name, cond, detail=""):
    status = "ok " if cond else "FAIL"
    print(f"[{status}] {name}" + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def req(method, base, path, body=None):
    url = base + path
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(r, timeout=15) as resp:
        return json.loads(resp.read())


def get(base, path, **params):
    if params:
        path += "?" + urllib.parse.urlencode(params)
    return req("GET", base, path)


def post(base, path, body):
    return req("POST", base, path, body)


def wait_for(name, fn, timeout=45):
    deadline = time.time() + timeout
    while time.time() < deadline:
        val = fn()
        if val:
            return val
        time.sleep(1)
    check(name, False, "timed out")
    return None


def ev(eid, t, key, typ="upsert", retracts=None, payload=None):
    e = {"event_id": eid, "event_time": t, "key": key, "type": typ, "payload": payload or {}}
    if retracts:
        e["retracts"] = retracts
    return e


def head(ws, key):
    return get(R, "/results/current", window_start=ws, key=key)["result"]


def main():
    now = int(time.time() * 1000)
    ws = now // WINDOW_MS * WINDOW_MS
    push = ws + WINDOW_MS + 8000  # beyond window end + 5s grace
    key = f"e2e-{now}"

    # -- downstream receivers + subscription registration --------------------
    server = ThreadingHTTPServer(("0.0.0.0", RECV_PORT), Receiver)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    r = post(R, "/subscriptions", {"name": "e2e-good", "url": f"{RECV_BASE}/good"})
    check("register downstream subscriber", r["subscription"]["active"] is True)
    r = post(R, "/subscriptions", {"name": "e2e-flaky", "url": f"{RECV_BASE}/flaky"})
    check("register second subscriber", r["subscription"]["name"] == "e2e-flaky")
    FLAKY["fail_next"] = 3  # first 3 delivery attempts to /flaky will fail

    # -- duplicates & in-window disorder ------------------------------------
    r = post(A, "/events", {"events": [
        ev(f"{key}-a2", ws + 2000, key, payload={"n": 2}),
        ev(f"{key}-a1", ws + 1000, key, payload={"n": 1}),  # out of order
    ]})
    check("ingest accepts batch", r["accepted"] == 2)
    r = post(A, "/events", {"events": [ev(f"{key}-a1", ws + 1000, key, payload={"n": 1})]})
    check("duplicate is deduped, not an error", r["accepted"] == 0 and r["deduped"] == [f"{key}-a1"])
    r = post(A, "/events", {"events": [ev(f"{key}-a1", ws + 1000, key, payload={"n": 1})]})
    check("repeated duplicate still deduped", r["accepted"] == 0)

    post(B, "/events", ev(f"{key}-b1", ws + 1500, key, payload={"s": "x"}))

    # -- window must NOT close before both watermarks pass -------------------
    post(A, "/events", ev(f"{key}-a-hb", push, "__hb__"))
    time.sleep(3)
    check("no result while stream B watermark lags", head(ws, key) is None)

    post(B, "/events", {"events": [
        ev(f"{key}-b2", ws + 2500, key, payload={"s": "y"}),
        ev(f"{key}-b-hb", push, "__hb__"),
    ]})
    v1 = wait_for("initial result once both watermarks pass",
                  lambda: head(ws, key))
    if v1:
        check("initial version is 1", v1["version"] == 1)
        pairs = [(p["a_event_id"], p["b_event_id"]) for p in v1["payload"]["pairs"]]
        check("pairs aligned in event-time order",
              pairs == [(f"{key}-a1", f"{key}-b1"), (f"{key}-a2", f"{key}-b2")], str(pairs))

    # -- failed delivery is visible as RETRYING, never dropped ---------------
    wait_for("failed delivery visible as RETRYING",
             lambda: get(R, "/deliveries", window_start=ws, key=key,
                         subscriber="e2e-flaky", status="RETRYING")["deliveries"] or None,
             timeout=30)

    # -- duplicate replay through the aligner must not double count ----------
    post(A, "/events", {"events": [ev(f"{key}-a1", ws + 1000, key, payload={"n": 1}),
                                   ev(f"{key}-a2", ws + 2000, key, payload={"n": 2})]})
    time.sleep(3)
    still = head(ws, key)
    check("duplicate replay does not bump version", still and still["version"] == 1)

    # -- late event for an already-closed window -> correction ---------------
    post(B, "/events", ev(f"{key}-b3-late", ws + 500, key, payload={"s": "late"}))
    v2 = wait_for("late event produces correction v2",
                  lambda: (h := head(ws, key)) and h["version"] >= 2 and h)
    if v2:
        check("correction reason is LATE_EVENT", v2["reason"] == "LATE_EVENT")
        check("late event reflected in payload",
              any(p["b_event_id"] == f"{key}-b3-late" for p in v2["payload"]["pairs"])
              or f"{key}-b3-late" in v2["payload"]["unmatched_b"])

    # -- retraction -> another correction ------------------------------------
    post(A, "/events", ev(f"{key}-r1", int(time.time() * 1000), key,
                          typ="retract", retracts=f"{key}-a1"))
    v3 = wait_for("retraction produces correction v3",
                  lambda: (h := head(ws, key)) and h["version"] >= 3 and h)
    if v3:
        check("retraction reason recorded", v3["reason"] == "RETRACTION")
        ids = [p["a_event_id"] for p in v3["payload"]["pairs"]]
        check("retracted event no longer paired", f"{key}-a1" not in ids, str(ids))

    # -- audit: why did the result change ------------------------------------
    hist = get(R, "/results/history", window_start=ws, key=key)
    versions = [v["version"] for v in hist["versions"]]
    check("all versions preserved", versions == [1, 2, 3], str(versions))
    reasons = [a["reason"] for a in hist["audit"]]
    check("audit explains every change",
          reasons == ["INITIAL", "LATE_EVENT", "RETRACTION"], str(reasons))
    superseded = [v for v in hist["versions"] if v["status"] == "SUPERSEDED"]
    check("old versions marked SUPERSEDED", len(superseded) == 2)

    # -- idle stream: watermark advances without new events ------------------
    print(".. waiting ~25s for idle timeout ..")
    time.sleep(25)
    wm = get(A, "/watermark")
    check("idle stream watermark advances via wall clock",
          wm["source"] in ("idle_timeout", "override") and wm["idle"], str(wm))

    # -- watermark regression: stable, logged, corrections keep working ------
    post(A, "/watermark/override", {"watermark": ws})
    time.sleep(3)
    log = get(R, "/watermarks/history", stream="a")["history"]
    check("watermark regression is logged",
          any(e["direction"] == "regress" for e in log), str(log[:2]))
    check("existing results survive regression", head(ws, key) is not None)
    post(B, "/events", ev(f"{key}-b4", ws + 800, key, payload={"s": "z"}))
    v4 = wait_for("corrections still work while watermark is regressed",
                  lambda: (h := head(ws, key)) and h["version"] >= 4 and h)
    post(A, "/watermark/override", {"watermark": None})

    # -- empty-result retraction ---------------------------------------------
    key2 = f"e2e-retract-{now}"
    post(A, "/events", {"events": [ev(f"{key2}-a1", ws + 1000, key2),
                                   ev(f"{key2}-a-hb", push, "__hb__")]})
    post(B, "/events", {"events": [ev(f"{key2}-b1", ws + 1000, key2),
                                   ev(f"{key2}-b-hb", push, "__hb__")]})
    wait_for("second key initial result", lambda: head(ws, key2))
    post(A, "/events", ev(f"{key2}-r1", int(time.time() * 1000), key2,
                          typ="retract", retracts=f"{key2}-a1"))
    post(B, "/events", ev(f"{key2}-r1", int(time.time() * 1000), key2,
                          typ="retract", retracts=f"{key2}-b1"))
    vr = wait_for("emptying a result emits RETRACTED version",
                  lambda: (h := head(ws, key2)) and h["status"] == "RETRACTED" and h)
    if vr:
        check("retracted result has null payload", vr["payload"] is None)

    # -- downstream delivery: order, kinds, retry, status queries ------------
    def versions_of(sub, k):
        return [d["version"] for d in recv(sub, k)]

    def kinds_of(sub, k):
        seen = {}
        for d in recv(sub, k):  # first occurrence per version
            seen.setdefault(d["version"], d["kind"])
        return [seen[v] for v in sorted(seen)]

    wait_for("good subscriber received every version of key",
             lambda: len(set(versions_of("good", key))) >= 4, timeout=60)
    wait_for("good subscriber received every version of key2",
             lambda: len(set(versions_of("good", key2))) >= 2, timeout=60)
    wait_for("flaky subscriber eventually received everything (retries)",
             lambda: len(set(versions_of("flaky", key))) >= 4
             and len(set(versions_of("flaky", key2))) >= 2, timeout=60)

    vs = versions_of("good", key)
    check("deliveries arrive in version order per result",
          vs == sorted(vs) and set(vs) == {1, 2, 3, 4}, str(vs))
    check("kinds: one NEW then CORRECTIONs (correction is not a new success)",
          kinds_of("good", key) == ["NEW", "CORRECTION", "CORRECTION", "CORRECTION"],
          str(kinds_of("good", key)))
    check("all versions carry the same result identity",
          all(d["window_start"] == ws and d["key"] == key for d in recv("good", key)))
    check("withdrawal of key2 delivered as WITHDRAWAL with null payload",
          kinds_of("good", key2) == ["NEW", "WITHDRAWAL"]
          and recv("good", key2)[-1]["payload"] is None)
    check("every delivery carries a dedup id and emitted_at",
          all(d.get("delivery_id") and d.get("emitted_at") for d in recv("good", key)))
    check("flaky subscriber also got versions in order",
          versions_of("flaky", key) == sorted(versions_of("flaky", key)))

    rows = get(R, "/deliveries", window_start=ws, key=key)["deliveries"]
    flaky_v1 = [r for r in rows if r["subscriber"] == "e2e-flaky" and r["version"] == 1]
    check("retried delivery eventually DELIVERED with attempts > 1",
          bool(flaky_v1) and flaky_v1[0]["status"] == "DELIVERED"
          and flaky_v1[0]["attempts"] >= 4 and flaky_v1[0]["last_error"] is None,
          str(flaky_v1))

    st = get(R, "/results/delivery", window_start=ws, key=key)
    ups = {s["subscriber"]: s["delivered_up_to"] for s in st["subscribers"]}
    check("per-result delivery status: delivered_up_to == head version",
          ups.get("e2e-good") == 4 and ups.get("e2e-flaky") == 4, str(ups))
    st2 = get(R, "/results/delivery", window_start=ws, key=key2)
    ups2 = {s["subscriber"]: s["delivered_up_to"] for s in st2["subscribers"]}
    check("withdrawn result fully delivered too",
          ups2.get("e2e-good") == 2 and ups2.get("e2e-flaky") == 2, str(ups2))

    subs = {s["name"]: s for s in get(R, "/subscriptions")["subscriptions"]}
    check("subscription list shows backlog counters, nothing left retrying",
          subs.get("e2e-good", {}).get("retrying") == 0
          and subs.get("e2e-flaky", {}).get("retrying") == 0
          and subs.get("e2e-good", {}).get("delivered", 0) > 0, str(subs))

    print()
    if FAILED:
        print(f"{len(FAILED)} check(s) FAILED: {FAILED}")
        sys.exit(1)
    print("all e2e checks passed")


if __name__ == "__main__":
    main()
