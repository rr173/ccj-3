#!/usr/bin/env python3
"""End-to-end test against a running compose stack (stdlib only).

Usage:  docker compose up -d --build && python3 tests/test_e2e.py

Covers the required behaviours: dual-watermark gating, duplicates, in-window
disorder, late-event correction, retraction, idle streams, watermark
regression, idempotent recompute, and the audit trail.
"""
import json
import os
import sys
import time
import urllib.parse
import urllib.request

A = os.environ.get("INGEST_A_URL", "http://localhost:8001")
B = os.environ.get("INGEST_B_URL", "http://localhost:8002")
R = os.environ.get("ALIGNER_URL", "http://localhost:8003")
WINDOW_MS = int(os.environ.get("WINDOW_SIZE_MS", "30000"))

FAILED = []


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

    print()
    if FAILED:
        print(f"{len(FAILED)} check(s) FAILED: {FAILED}")
        sys.exit(1)
    print("all e2e checks passed")


if __name__ == "__main__":
    main()
