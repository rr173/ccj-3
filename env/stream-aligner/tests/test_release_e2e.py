#!/usr/bin/env python3
"""End-to-end test for the per-key external release gate (对外放行),
against a running compose stack.

Usage:  docker compose up -d --build && python3 tests/test_release_e2e.py

Covers, in a deterministic override-driven scenario:
- results are computed and queryable while the gate is closed, but NOTHING is
  pushed downstream (held);
- late events and retractions keep changing the held internal version;
- a one-shot release publishes the head AT RELEASE TIME as NEW — intermediate
  held versions are skipped, not sent;
- another key clicked nowhere stays held;
- opening a gate flushes the held head and lets later first versions flow;
- closing a gate holds back never-released windows but never stops
  corrections / withdrawals / revivals of already-released windows;
- a window fully retracted while held releases nothing; its later revival
  while the gate is open goes out as the first NEW;
- releasing an already-released window does not re-send it.
"""
import json
import os
import sys
import threading
import time
import urllib.parse
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

A = os.environ.get("INGEST_A_URL", "http://localhost:8001")
B = os.environ.get("INGEST_B_URL", "http://localhost:8002")
R = os.environ.get("ALIGNER_URL", "http://localhost:8003")
W = int(os.environ.get("WINDOW_SIZE_MS", "30000"))

RECV_PORT = int(os.environ.get("RECEIVER_PORT", "8902"))
RECV_BASE = os.environ.get("RECEIVER_BASE_URL", f"http://host.docker.internal:{RECV_PORT}")

FAILED = []
DELIVERIES = []
LOCK = threading.Lock()


class Receiver(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(n) or b"{}")
        with LOCK:
            DELIVERIES.append(dict(body))
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):
        pass


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
    try:
        with urllib.request.urlopen(r, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return {"_status": e.code, "_body": e.read().decode()}


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
    e = {"event_id": eid, "event_time": t, "key": key, "type": typ,
         "payload": payload or {}}
    if retracts:
        e["retracts"] = retracts
    return e


def head(ws, key):
    return get(R, "/results/current", window_start=ws, key=key)["result"]


def set_wm(t):
    post(A, "/watermark/override", {"watermark": t})
    post(B, "/watermark/override", {"watermark": t})


def upsert(stream, eid, t, key, payload=None):
    post(stream, "/events", ev(eid, t, key, payload=payload))


def retract(stream, eid, t, key, target):
    post(stream, "/events", ev(eid, t, key, typ="retract", retracts=target))


def delivered(key):
    with LOCK:
        return [(d["version"], d["kind"]) for d in DELIVERIES if d.get("key") == key]


def settle(seconds=3):
    time.sleep(seconds)


def main():
    now = int(time.time() * 1000)
    ws = now // W * W
    after = ws + W + 1000
    mid = ws + W // 2

    server = ThreadingHTTPServer(("0.0.0.0", RECV_PORT), Receiver)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    sub = post(R, "/subscriptions", {"name": "rel-good", "url": f"{RECV_BASE}/r"})
    check("receiver subscribed", sub.get("subscription", {}).get("active") is True, str(sub))

    set_wm(ws)  # nothing crosses yet -> deterministic staging

    k1 = f"rel-click-{now}"   # held, explicit one-shot release mid-stream
    k2 = f"rel-flow-{now}"    # released early, then gate closed: corrections must flow
    k3 = f"rel-open-{now}"    # gate opened: flush + auto-release later windows
    k4 = f"rel-other-{now}"   # never clicked: stays held the whole time
    k5 = f"rel-empty-{now}"   # fully retracted while held, later revives

    # two windows of events for k1/k2/k3/k5, one for k4
    for s, eid in ((A, "a1"), (B, "b1")):
        upsert(s, f"{k1}-{eid}", ws + 1000, k1, {"n": 1})
        upsert(s, f"{k2}-{eid}", ws + 1000, k2, {"n": 1})
        upsert(s, f"{k3}-{eid}", ws + 1000, k3, {"n": 1})
        upsert(s, f"{k4}-{eid}", ws + 1000, k4, {"n": 1})
        upsert(s, f"{k5}-{eid}", ws + 1000, k5, {"n": 1})

    # -- gates default closed: first window computes, nothing is delivered ----
    set_wm(after)
    for k in (k1, k2, k3, k4, k5):
        wait_for(f"{k} v1 computed internally", lambda k=k: head(ws, k))
    settle()
    check("no downstream delivery while all gates are closed",
          delivered(k1) == delivered(k2) == delivered(k3) == delivered(k4)
          == delivered(k5) == [], str(DELIVERIES))
    for k in (k1, k2, k3, k4, k5):
        h = head(ws, k)
        check(f"{k} internal v1 queryable and marked not released",
              h["version"] == 1 and h["released"] is False, str(h))
    backlog = {h["key"]: h for h in get(R, "/releases/backlog")["held"]}
    check("release backlog lists every held window",
          all(k in backlog and backlog[k]["releasable_now"] for k in (k1, k2, k3, k4, k5)),
          str(sorted(backlog)))

    # -- held window keeps changing internally (v2, v3) -----------------------
    upsert(B, f"{k1}-b-late", ws + 500, k1, {"n": 2})   # v2 correction
    retract(A, f"{k1}-ra1", mid, k1, f"{k1}-a1")        # v3 correction
    wait_for("k1 internal head reaches v3",
             lambda: (h := head(ws, k1)) and h["version"] == 3 and h)
    settle()
    check("still nothing delivered while held", delivered(k1) == [], str(delivered(k1)))

    # -- one-shot release: head AT RELEASE TIME (v3) goes out as NEW ----------
    rel = post(R, "/releases", {"key": k1})
    check("release publishes exactly one window at its current head",
          rel["count"] == 1 and rel["released"][0]["version"] == 3
          and rel["released"][0]["window_start"] == ws, str(rel))
    wait_for("k1 receives v3 as NEW", lambda: delivered(k1) == [(3, "NEW")])
    check("intermediate held versions v1/v2 are never sent", delivered(k1) == [(3, "NEW")],
          str(delivered(k1)))
    check("k1 now marked released at v3", head(ws, k1)["released"] is True)
    rel_again = post(R, "/releases", {"key": k1})
    check("releasing again is a no-op (nothing taken back / re-sent)",
          rel_again["count"] == 0 and delivered(k1) == [(3, "NEW")], str(rel_again))

    # -- releasing an explicit already-released window is a 409 ---------------
    bad = post(R, "/releases", {"key": k1, "window_starts": [ws]})
    check("explicit re-release of a released window conflicts",
          bad.get("_status") == 409, str(bad))

    # -- released window keeps flowing AFTER the gate is closed ---------------
    gate = post(R, "/release-gates", {"key": k2, "open": True})
    check("opening k2 gate flushed its held v1", gate["count"] == 1
          and gate["released"][0]["version"] == 1, str(gate))
    wait_for("k2 receives v1 NEW", lambda: delivered(k2) == [(1, "NEW")])
    post(R, "/release-gates", {"key": k2, "open": False})
    upsert(B, f"{k2}-b-late", ws + 500, k2, {"n": 9})  # correction after close
    wait_for("k2 correction v2 computed",
             lambda: (h := head(ws, k2)) and h["version"] == 2 and h)
    wait_for("closed gate still delivers correction of a RELEASED window",
             lambda: (2, "CORRECTION") in delivered(k2))
    retract(A, f"{k2}-ra1", mid + 1, k2, f"{k2}-a1")
    retract(B, f"{k2}-rb1", mid + 2, k2, f"{k2}-b1")
    wait_for("k2 withdrawal v3 computed",
             lambda: (h := head(ws, k2)) and h["status"] == "RETRACTED" and h)
    wait_for("withdrawal of a released window delivered with gate closed",
             lambda: (3, "WITHDRAWAL") in delivered(k2))
    check("k2 delivery ladder NEW -> CORRECTION -> WITHDRAWAL",
          delivered(k2) == [(1, "NEW"), (2, "CORRECTION"), (3, "WITHDRAWAL")],
          str(delivered(k2)))

    # -- gate open flushes held heads and auto-releases later windows ---------
    gate = post(R, "/release-gates", {"key": k3, "open": True})
    check("opening k3 gate flushed held v1", gate["count"] == 1
          and gate["released"][0]["version"] == 1, str(gate))
    wait_for("k3 receives v1 NEW", lambda: (1, "NEW") in delivered(k3))
    # k3 second window: gate open -> its INITIAL flows by itself
    for s, eid in ((A, "a2"), (B, "b2")):
        upsert(s, f"{k3}-{eid}", ws + W + 1000, k3, {"n": 2})
    set_wm(ws + 2 * W + 1000)

    def k3_second_auto_released():
        with LOCK:
            return any(d.get("window_start") == ws + W and d["kind"] == "NEW"
                       for d in DELIVERIES if d.get("key") == k3)

    wait_for("k3 second window auto-released while gate open", k3_second_auto_released)

    # -- the key never clicked stays held even though watermarks moved --------
    settle()
    check("un-clicked k4 stays held: no deliveries", delivered(k4) == [],
          str(delivered(k4)))
    check("k4 still in the held backlog",
          any(h["key"] == k4 for h in get(R, "/releases/backlog")["held"]))

    # -- fully retracted while held: nothing released; revival goes out NEW ---
    retract(A, f"{k5}-ra1", mid + 3, k5, f"{k5}-a1")
    retract(B, f"{k5}-rb1", mid + 4, k5, f"{k5}-b1")
    vr = wait_for("k5 held head becomes RETRACTED",
                  lambda: (h := head(ws, k5)) and h["status"] == "RETRACTED" and h)
    check("retracted held head is not releasable", vr["released"] is False)
    rel = post(R, "/releases", {"key": k5})
    check("releasing a fully-retracted held window sends nothing", rel["count"] == 0, str(rel))
    settle()
    check("nothing delivered for the retracted-while-held window",
          delivered(k5) == [], str(delivered(k5)))
    # revive with the gate open: first publication is NEW, not a correction
    post(R, "/release-gates", {"key": k5, "open": True})
    upsert(A, f"{k5}-a2", ws + 1100, k5, {"n": 3})
    wait_for("k5 revives internally",
             lambda: (h := head(ws, k5)) and h["status"] == "CURRENT" and h)

    def k5_first_new():
        got = delivered(k5)
        return got == [(got[0][0], "NEW")] if got else None

    wait_for("revived window released as first NEW (gate open)", k5_first_new)
    check("revival goes out NEW (the outside never saw the withdrawn version)",
          delivered(k5) and delivered(k5)[0][1] == "NEW", str(delivered(k5)))
    # -- gate listing shows state + held/released counters --------------------
    gates = {g["key"]: g for g in get(R, "/release-gates")["gates"]}
    check("gate states: k1/k4 closed, k3/k5 open, k2 closed",
          gates[k1]["open"] is False and gates[k2]["open"] is False
          and gates[k3]["open"] is True and gates[k5]["open"] is True
          and gates[k4]["open"] is False, str({k: gates[k]["open"] for k in (k1, k2, k3, k4, k5)}))
    check("k4 shows one held window, zero released",
          gates[k4]["held_windows"] == 1 and gates[k4]["released_windows"] == 0,
          str(gates[k4]))
    check("k2 shows one released window, none held",
          gates[k2]["released_windows"] == 1 and gates[k2]["held_windows"] == 0,
          str(gates[k2]))

    # -- release history is auditable -----------------------------------------
    hist = get(R, "/releases/history", key=k1)
    actions = [a["action"] for a in hist["actions"]]
    check("k1 release history records the explicit RELEASE",
          "RELEASE" in actions and len(hist["released_windows"]) == 1
          and hist["released_windows"][0]["result_version"] == 3, str(hist))
    hist3 = get(R, "/releases/history", key=k3)
    check("k3 gate-open flushes are audited as GATE_OPEN",
          any(a["action"] == "GATE_OPEN" for a in hist3["actions"]), str(hist3["actions"]))

    post(A, "/watermark/override", {"watermark": None})
    post(B, "/watermark/override", {"watermark": None})

    print()
    if FAILED:
        print(f"{len(FAILED)} check(s) FAILED: {FAILED}")
        sys.exit(1)
    print("all release-gate e2e checks passed")


if __name__ == "__main__":
    main()
