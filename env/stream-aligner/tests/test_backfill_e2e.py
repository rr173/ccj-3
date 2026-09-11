#!/usr/bin/env python3
"""End-to-end test for historical backfill (历史补推).

Usage:  docker compose up -d --build && python3 tests/test_backfill_e2e.py

Covers:
- only externally released versions are replayed; internal versions held
  before the first release are not;
- versions are copied with their original payload kind/version/timestamp and
  delivered in per-result version order;
- a realtime correction created while replay is paused still uses the
  REALTIME channel and is not rerouted into the backfill queue;
- per-result version order spans both channels: a realtime correction whose
  result's earliest version is still queued in the backfill waits BEHIND it
  (old version delivered first, no channel overtaking) — different results
  stay independent, and the cross-channel waits can never deadlock;
- a realtime correction whose result's earlier version is still queued in the
  backfill waits BEHIND that history (old version delivered first, no channel
  overtaking, no deadlock);
- a range whose bounds land mid-window still replays the whole containing
  window (bounds are snapped to window boundaries);
- replay can stop and resume at its current position;
- replaying the same downstream/range never enqueues a version twice.
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

RECV_PORT = int(os.environ.get("RECEIVER_PORT", "8903"))
RECV_BASE = os.environ.get("RECEIVER_BASE_URL", f"http://host.docker.internal:{RECV_PORT}")

FAILED = []
DELIVERIES = []
MODE = {"mode": "fail_all"}
LOCK = threading.Lock()


class Receiver(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(n) or b"{}")
        with LOCK:
            mode = MODE["mode"]
            fail = mode == "fail_all" or (
                mode == "fail_backfill" and body.get("channel") == "BACKFILL")
            if not fail:
                DELIVERIES.append(dict(body))
        self.send_response(500 if fail else 200)
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


def post(base, path, body=None):
    return req("POST", base, path, body or {})


def wait_for(name, fn, timeout=45):
    deadline = time.time() + timeout
    while time.time() < deadline:
        val = fn()
        if val:
            return val
        time.sleep(0.25)
    check(name, False, "timed out")
    return None


def ev(eid, t, key, typ="upsert", retracts=None, payload=None):
    e = {"event_id": eid, "event_time": t, "key": key, "type": typ,
         "payload": payload or {}}
    if retracts:
        e["retracts"] = retracts
    return e


def upsert(stream, eid, t, key, payload=None):
    post(stream, "/events", ev(eid, t, key, payload=payload))


def retract(stream, eid, t, key, target):
    post(stream, "/events", ev(eid, t, key, typ="retract", retracts=target))


def set_wm(value):
    post(A, "/watermark/override", {"watermark": value})
    post(B, "/watermark/override", {"watermark": value})


def head(ws, key):
    return get(R, "/results/current", window_start=ws, key=key)["result"]


def received(key=None, channel=None):
    with LOCK:
        return [d for d in DELIVERIES
                if (key is None or d.get("key") == key)
                and (channel is None or d.get("channel") == channel)]


def job(job_id):
    return get(R, f"/backfills/{job_id}")["backfill"]


def main():
    now = int(time.time() * 1000)
    ws = now // W * W
    tag = f"bf-{now}"
    hist = f"{tag}-hist"
    held = f"{tag}-held"
    mid = f"{tag}-mid"
    live = f"{tag}-live"
    live_ws = ws + 2 * W

    server = ThreadingHTTPServer(("0.0.0.0", RECV_PORT), Receiver)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    # Stage history before the downstream is registered. Watermarks are driven
    # by overrides for determinism.
    set_wm(ws)
    upsert(A, f"{hist}-a1", ws + 1000, hist, {"n": 1})
    upsert(B, f"{hist}-b1", ws + 1500, hist, {"n": 1})
    upsert(A, f"{held}-a1", ws + 1000, held)
    upsert(B, f"{held}-b1", ws + 1500, held)
    upsert(A, f"{mid}-a1", ws + 1000, mid, {"n": 1})
    upsert(B, f"{mid}-b1", ws + 1500, mid, {"n": 1})
    set_wm(ws + W + 1)
    wait_for("history v1 computed", lambda: head(ws, hist) and head(ws, hist)["version"] == 1)
    wait_for("held v1 computed", lambda: head(ws, held) and head(ws, held)["version"] == 1)
    wait_for("mid v1 computed", lambda: head(ws, mid) and head(ws, mid)["version"] == 1)
    post(R, "/releases", {"key": mid})  # externally released at v1

    # v1/v2 remain internal. v3 is the first externally released version.
    upsert(B, f"{hist}-b2", ws + 2500, hist, {"n": 2})
    wait_for("history v2 internal", lambda: head(ws, hist)["version"] == 2)
    retract(A, f"{hist}-ra1", ws + 3000, hist, f"{hist}-a1")
    wait_for("history v3 internal", lambda: head(ws, hist)["version"] == 3)
    rel = post(R, "/releases", {"key": hist})
    check("history released at current head v3; v1/v2 stay internal",
          rel["count"] == 1 and rel["released"][0]["version"] == 3, str(rel))

    # Later released versions must be replayed in order, including withdrawal.
    upsert(A, f"{hist}-a2", ws + 2000, hist, {"n": 3})
    wait_for("history v4 released correction", lambda: head(ws, hist)["version"] == 4)
    retract(A, f"{hist}-ra2", ws + 4000, hist, f"{hist}-a2")
    wait_for("history v5 correction", lambda: head(ws, hist)["version"] == 5)
    post(B, "/events", {"events": [
        ev(f"{hist}-rb1", ws + 4100, hist, typ="retract", retracts=f"{hist}-b1"),
        ev(f"{hist}-rb2", ws + 4200, hist, typ="retract", retracts=f"{hist}-b2"),
    ]})
    wait_for("history v6 withdrawal",
             lambda: (h := head(ws, hist)) and h["version"] == 6
             and h["status"] == "RETRACTED" and h)

    upsert(B, f"{held}-b2", ws + 2500, held, {"n": 9})
    wait_for("held key has an internal v2", lambda: head(ws, held)["version"] == 2)
    check("held key is not externally released", head(ws, held)["released"] is False)

    # A released correction of the mid-window-bounds fixture key.
    upsert(B, f"{mid}-b2", ws + 2500, mid, {"n": 7})
    wait_for("mid key has released v2", lambda: head(ws, mid)["version"] == 2)

    # Advance far enough for a separate realtime window used as live traffic.
    set_wm(live_ws + W + 1)
    sub_name = f"backfill-{now}"
    sub = post(R, "/subscriptions",
               {"name": sub_name, "url": f"{RECV_BASE}/recv"})
    sub_id = sub["subscription"]["id"]
    check("downstream registered", sub["subscription"]["active"] is True, str(sub))
    post(R, "/release-gates", {"key": live, "open": True})
    upsert(A, f"{live}-a1", live_ws + 1000, live)
    upsert(B, f"{live}-b1", live_ws + 1500, live)
    wait_for("realtime live v1 is retrying", lambda: next(
        (x for x in get(R, "/deliveries", subscriber=sub_name,
                        key=live, status="RETRYING")["deliveries"]
         if x["channel"] == "REALTIME"), None), timeout=20)

    # Range is [ws, ws+W): history is included; live_ws=ws+2W is not.
    created = post(R, "/backfills", {
        "subscriber_id": sub_id,
        "from_window_start": ws,
        "to_window_start": ws + W,
    })
    check("backfill enqueues four externally released versions (v3-v6)",
          created.get("backfill", {}).get("total_versions") == 4, str(created))
    check("new backfill is marked RUNNING",
          created.get("backfill", {}).get("status") == "RUNNING", str(created))
    job_id = created["backfill"]["id"]

    overlap = post(R, "/backfills", {
        "subscriber_id": sub_id,
        "from_window_start": ws - W,
        "to_window_start": ws + 2 * W,
    })
    check("cannot start an overlapping job while one is active",
          overlap.get("_status") == 409, str(overlap))

    # Let the live delivery through (fail only backfills now), but make the
    # first historical attempt fail so the job has a resumable retry position.
    with LOCK:
        MODE["mode"] = "fail_backfill"
    wait_for("realtime live v1 delivered",
             lambda: received(live, "REALTIME")
             and received(live, "REALTIME")[0]["version"] == 1)
    wait_for("first backfill attempt is retrying",
             lambda: job(job_id)["retrying_versions"] == 1, timeout=20)
    check("no backfill delivered while its first version is retrying",
          received(channel="BACKFILL") == [])

    stopped = post(R, f"/backfills/{job_id}/stop")
    check("backfill can be stopped", stopped["backfill"]["status"] == "PAUSED",
          str(stopped))
    time.sleep(2.5)  # longer than one retry interval
    check("stopped backfill remains at its checkpoint",
          received(channel="BACKFILL") == [] and job(job_id)["status"] == "PAUSED",
          str(job(job_id)))

    # A correction arriving while replay is paused still goes realtime and is
    # not rerouted/delayed by the backfill channel.
    upsert(B, f"{live}-b2", live_ws + 2500, live, {"n": 9})
    wait_for("realtime correction bypasses paused backfill",
             lambda: [(d["version"], d["kind"], d["channel"]) for d in received(live)]
             == [(1, "NEW", "REALTIME"), (2, "CORRECTION", "REALTIME")])

    with LOCK:
        MODE["mode"] = "ok"
    resumed = post(R, f"/backfills/{job_id}/resume")
    check("backfill can resume", resumed["backfill"]["status"] == "RUNNING", str(resumed))
    wait_for("backfill job completes",
             lambda: job(job_id)["status"] == "COMPLETED", timeout=30)
    final_job = job(job_id)
    check("all four copied versions delivered exactly once",
          final_job["delivered_versions"] == 4
          and final_job["pending_versions"] == 0, str(final_job))

    history = received(hist, "BACKFILL")
    check("backfilled versions preserve original order and kinds",
          [(d["version"], d["kind"]) for d in history]
          == [(3, "NEW"), (4, "CORRECTION"), (5, "CORRECTION"), (6, "WITHDRAWAL")],
          str([(d["version"], d["kind"]) for d in history]))
    check("withdrawal keeps null original payload", history[-1]["payload"] is None)
    check("backfill envelope carries channel/job identity",
          all(d["backfill_job_id"] == job_id and d["channel"] == "BACKFILL"
              for d in history))
    check("internal-only held key was never backfilled", received(held) == [])

    # --- mid-window bounds: a time landing inside the window must still
    # replay the whole window, for a different subscriber. Bounds here are
    # deliberately NOT window starts.
    mid_sub_name = f"backfill-mid-{now}"
    mid_sub = post(R, "/subscriptions",
                   {"name": mid_sub_name, "url": f"{RECV_BASE}/recv"})
    mid_sub_id = mid_sub["subscription"]["id"]
    mid_job = post(R, "/backfills", {
        "subscriber_id": mid_sub_id,
        "from_window_start": ws + 1000,   # inside the window starting at ws
        "to_window_start": ws + W - 1,    # still inside the same window
    })
    check("mid-window bounds snap onto the containing window",
          mid_job.get("backfill", {}).get("window_start_from") == ws
          and mid_job.get("backfill", {}).get("window_start_to") == ws + W,
          str(mid_job))
    check("mid-window range enqueues both released versions of the window",
          mid_job.get("backfill", {}).get("total_versions") == 2, str(mid_job))
    mid_job_id = mid_job["backfill"]["id"]
    wait_for("mid-window backfill completes",
             lambda: job(mid_job_id)["status"] == "COMPLETED", timeout=30)
    mid_got = received(mid, "BACKFILL")
    check("a time inside the window replays the whole window, in version order",
          [(d["version"], d["kind"]) for d in mid_got]
          == [(1, "NEW"), (2, "CORRECTION")],
          str([(d["version"], d["kind"]) for d in mid_got]))

    # --- cross-channel version order: while a third subscriber's replay is
    # held at its first version, a realtime correction of that SAME result must
    # wait behind the history instead of overtaking it (旧版先到, 订正后到).
    ord_sub_name = f"backfill-ord-{now}"
    ord_sub = post(R, "/subscriptions",
                   {"name": ord_sub_name, "url": f"{RECV_BASE}/recv"})
    ord_sub_id = ord_sub["subscription"]["id"]
    ord_job = post(R, "/backfills", {
        "subscriber_id": ord_sub_id,
        "from_window_start": ws,
        "to_window_start": ws + W,
    })
    ord_job_id = ord_job["backfill"]["id"]
    # The range copies v3..v6; hold the whole chain at v3 by failing backfills.
    with LOCK:
        MODE["mode"] = "fail_backfill"
    wait_for("ordering backfill v3 is retrying",
             lambda: job(ord_job_id)["retrying_versions"] >= 1, timeout=20)

    # A fresh correction of the same (ws, hist) result goes REALTIME. It must
    # NOT even be attempted while v3 of the history is still undelivered: the
    # cross-channel barrier keeps its outbox row PENDING.
    upsert(B, f"{hist}-b3", ws + 2600, hist, {"n": 11})
    wait_for("ordering realtime v7 correction exists",
             lambda: head(ws, hist) and head(ws, hist)["version"] == 7)
    wait_for("ordering realtime correction is queued behind history", lambda: next(
        (x for x in get(R, "/deliveries", subscriber=ord_sub_name,
                        key=hist)["deliveries"]
         if x["channel"] == "REALTIME" and x["version"] == 7), None), timeout=20)
    time.sleep(2.5)
    blocked_rows = get(R, "/deliveries", subscriber=ord_sub_name,
                       key=hist, limit=50)["deliveries"]
    v7_row = next((x for x in blocked_rows
                   if x["channel"] == "REALTIME" and x["version"] == 7), None)
    check("realtime correction is held PENDING and never overtakes the history",
          v7_row is not None and v7_row["status"] == "PENDING"
          and all(x["status"] != "DELIVERED" for x in blocked_rows),
          str([(x["version"], x["channel"], x["status"]) for x in blocked_rows]))

    with LOCK:
        MODE["mode"] = "ok"
    wait_for("ordering backfill completes",
             lambda: job(ord_job_id)["status"] == "COMPLETED", timeout=30)
    # realtime v7 only went to the ordering subscriber — read its ladder.
    ord_rows = get(R, "/deliveries", subscriber=ord_sub_name,
                   key=hist, status="DELIVERED", limit=50)["deliveries"]
    check("ordering subscriber got v3..v7 strictly in version order",
          [d["version"] for d in ord_rows] == [3, 4, 5, 6, 7]
          and [d["channel"] for d in ord_rows]
          == ["BACKFILL", "BACKFILL", "BACKFILL", "BACKFILL", "REALTIME"],
          str([(d["version"], d["channel"]) for d in ord_rows]))
    check("correction arrived AFTER the earliest version, not before it",
          next(i for i, d in enumerate(ord_rows) if d["version"] == 7)
          > next(i for i, d in enumerate(ord_rows) if d["version"] == 3),
          "")

    # The ordering guarantee is per (key): one result's history never overtakes
    # that same result's versions across channels; different results stay
    # independent and never block each other.
    arrival = [(d["key"], d["version"], d["channel"]) for d in DELIVERIES]
    by_key = {}
    for k, v, ch in arrival:
        by_key.setdefault(k, []).append((v, ch))
    for k, seq in by_key.items():
        versions = [v for v, ch in seq]
        check(f"{k}: delivered in strictly ascending version order",
              versions == sorted(versions) and len(versions) == len(set(versions)),
              str(seq))
    check("realtime live correction still flowed while backfill was paused",
          by_key.get(live) == [(1, "REALTIME"), (2, "REALTIME")],
          str(by_key.get(live)))

    # Same subscriber + same range again: existing delivery identity wins, so
    # this creates a completed zero-version job and causes no duplicate POST.
    before = len(DELIVERIES)
    again = post(R, "/backfills", {
        "subscriber_name": sub_name,
        "from_window_start": ws,
        "to_window_start": ws + W,
    })
    check("repeating a completed backfill enqueues no version twice",
          again.get("backfill", {}).get("total_versions") == 0
          and again.get("backfill", {}).get("status") == "COMPLETED", str(again))
    time.sleep(2)
    with LOCK:
        check("repeated backfill sends no duplicate HTTP delivery",
              len(DELIVERIES) == before)

    second_stop = post(R, f"/backfills/{again['backfill']['id']}/stop")
    check("stopping a completed job is idempotent",
          second_stop["backfill"]["status"] == "COMPLETED", str(second_stop))

    set_wm(None)
    print()
    if FAILED:
        print(f"{len(FAILED)} check(s) FAILED: {FAILED}")
        sys.exit(1)
    print("all backfill e2e checks passed")


if __name__ == "__main__":
    main()
