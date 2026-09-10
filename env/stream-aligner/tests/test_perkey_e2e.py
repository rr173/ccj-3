#!/usr/bin/env python3
"""End-to-end test for per-key emission gating, against a running compose stack.

Usage:  docker compose up -d --build && python3 tests/test_perkey_e2e.py

Covers the per-business-key emission gate:
- a business whose own two sides have crossed the window end emits without
  waiting for the stream-wide watermark (its own newer events are enough);
- a business that never gets new events neither blocks others nor gets
  dropped — it simply keeps waiting;
- once the streams go idle, a window whose both sides have arrived IS
  finalized (idle wall-clock must not strand complete businesses), while a
  business still missing a side is NOT (idle must not force-close it);
- once the stream genuinely promises again (event-time watermark), the
  waiting business emits — even one-sided;
- a watermark regression never un-emits results that already went out.
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
W = int(os.environ.get("WINDOW_SIZE_MS", "30000"))

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


def ev(eid, t, key, payload=None):
    return {"event_id": eid, "event_time": t, "key": key,
            "type": "upsert", "payload": payload or {}}


def head(ws, key):
    return get(R, "/results/current", window_start=ws, key=key)["result"]


def main():
    now = int(time.time() * 1000)
    ws = now // W * W
    busy = f"pk-busy-{now}"   # keeps producing on both sides
    slow = f"pk-slow-{now}"   # one A-side event, then silence forever
    done = f"pk-done-{now}"   # both sides arrive in window ws, then silence

    # -- a quiet business cannot stall a flowing one --------------------------
    # busy has events in window ws AND beyond its end on both sides: its own
    # progress crosses the window end even though neither stream watermark
    # has reached it. slow only has one A-side event in window ws. done has
    # both sides in window ws but nothing beyond it.
    post(A, "/events", {"events": [ev(f"{busy}-a1", ws + 1000, busy),
                                   ev(f"{busy}-a2", ws + W + 1000, busy),
                                   ev(f"{slow}-a1", ws + 1000, slow),
                                   ev(f"{done}-a1", ws + 1000, done)]})
    post(B, "/events", {"events": [ev(f"{busy}-b1", ws + 1500, busy),
                                   ev(f"{busy}-b2", ws + W + 1500, busy),
                                   ev(f"{done}-b1", ws + 1500, done)]})

    v1 = wait_for("busy key emits on its own progress, watermark still short",
                  lambda: head(ws, busy))
    if v1:
        check("initial version is 1", v1["version"] == 1)
        pairs = [(p["a_event_id"], p["b_event_id"]) for p in v1["payload"]["pairs"]]
        check("only this window's events are paired",
              pairs == [(f"{busy}-a1", f"{busy}-b1")], str(pairs))
    wm = get(R, "/watermarks")
    check("stream watermarks had NOT reached the window end when it emitted",
          wm["min_watermark"] is not None and wm["min_watermark"] < ws + W,
          str(wm))
    audit = get(R, "/audit", window_start=ws, key=busy)["audit"]
    check("audit records per-side crossing evidence (own_progress)",
          audit and audit[0]["reason"] == "INITIAL"
          and audit[0]["detail"]["side_a"] == "own_progress"
          and audit[0]["detail"]["side_b"] == "own_progress",
          str(audit[:1]))

    time.sleep(3)  # let several ticks run — nothing more may happen
    check("slow key keeps waiting (its B side never crossed), not dropped",
          head(ws, slow) is None)
    check("done key waits while streams are live and the watermark is short",
          head(ws, done) is None)
    check("busy's next window waits too (nothing crosses its end yet)",
          head(ws + W, busy) is None)

    # -- once idle: complete businesses finalize, waiting ones do not ---------
    def both_idle():
        src = get(R, "/watermarks")["sources"]
        return src.get("a") == "idle_timeout" and src.get("b") == "idle_timeout"

    wait_for("both streams go idle (wall-clock watermark)", both_idle, timeout=60)
    v2 = wait_for("both-sides-arrived business emits once streams go idle",
                  lambda: head(ws, done), timeout=60)
    if v2:
        pairs = [(p["a_event_id"], p["b_event_id"]) for p in v2["payload"]["pairs"]]
        check("done key's pairs are complete",
              pairs == [(f"{done}-a1", f"{done}-b1")], str(pairs))
    audit = get(R, "/audit", window_start=ws, key=done)["audit"]
    check("done key's crossing evidence is idle_finalized on both sides",
          audit and audit[0]["reason"] == "INITIAL"
          and audit[0]["detail"]["side_a"] == "idle_finalized"
          and audit[0]["detail"]["side_b"] == "idle_finalized",
          str(audit[:1]))
    check("idle watermark did NOT close the still-waiting slow key",
          head(ws, slow) is None)

    # -- recovery: real stream progress emits the waiting businesses ----------
    post(A, "/events", ev(f"{busy}-a3", ws + 2 * W + 1000, busy))
    post(B, "/events", ev(f"{busy}-b3", ws + 2 * W + 1000, busy))

    v3 = wait_for("busy's second window emits once its own progress crosses",
                  lambda: head(ws + W, busy))
    if v3:
        pairs = [(p["a_event_id"], p["b_event_id"]) for p in v3["payload"]["pairs"]]
        check("second window pairs its own events",
              pairs == [(f"{busy}-a2", f"{busy}-b2")], str(pairs))
    v4 = wait_for("slow key finally emits once the stream watermark promises",
                  lambda: head(ws, slow))
    if v4:
        check("slow key's result is one-sided with the A event unmatched",
              v4["payload"]["match_count"] == 0
              and v4["payload"]["unmatched_a"] == [f"{slow}-a1"]
              and v4["payload"]["unmatched_b"] == [], str(v4["payload"]))

    # -- watermark regression must not un-emit anything -----------------------
    post(A, "/watermark/override", {"watermark": ws})  # dial A way back
    time.sleep(3)
    log = get(R, "/watermarks/history", stream="a")["history"]
    check("watermark regression is logged",
          any(e["direction"] == "regress" for e in log), str(log[:2]))
    check("emitted results survive the regression untouched",
          (head(ws, busy) or {}).get("version") == 1
          and (head(ws + W, busy) or {}).get("version") == 1
          and (head(ws, slow) or {}).get("version") == 1
          and (head(ws, done) or {}).get("version") == 1)
    post(A, "/watermark/override", {"watermark": None})

    print()
    if FAILED:
        print(f"{len(FAILED)} check(s) FAILED: {FAILED}")
        sys.exit(1)
    print("all per-key gating e2e checks passed")


if __name__ == "__main__":
    main()
