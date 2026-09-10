#!/usr/bin/env python3
"""End-to-end test for the downstream posting ledger (下游入账台账),
against a running compose stack.

Usage:  docker compose up -d --build && python3 tests/test_posting_e2e.py

Covers, in a deterministic override-driven scenario:
- a downstream reports the version it posted; the ledger aligns it with what
  we confirmed delivered (ALIGNED);
- a downstream that never reported is listed with a null ledger status and is
  NOT gated (pre-ledger behaviour preserved);
- a new delivery on an aligned pair turns it LAGGING and holds that result's
  later versions for that downstream — while other results (another key AND
  another window of the same key) keep flowing;
- reporting the catch-up version re-aligns the pair and releases the held
  versions, which then knock it lagging again;
- reporting a version we never sent to that downstream is rejected (409),
  changes nothing, and is traced;
- a rollback report moves the ledger backwards; delivery records are kept;
- a downstream claiming a version we are still retrying shows
  AHEAD_UNCONFIRMED;
- re-registering the same downstream under a new URL keeps the ledger
  attached to the same subscriber;
- every report and every lagging transition is in the append-only history.
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

RECV_PORT = int(os.environ.get("RECEIVER_PORT", "8904"))
RECV_BASE = os.environ.get("RECEIVER_BASE_URL", f"http://host.docker.internal:{RECV_PORT}")
RECV2_PORT = int(os.environ.get("RECEIVER2_PORT", "8905"))
RECV2_BASE = os.environ.get("RECEIVER2_BASE_URL", f"http://host.docker.internal:{RECV2_PORT}")
DEAD_URL = os.environ.get("DEAD_RECEIVER_URL", "http://host.docker.internal:8998/nope")

FAILED = []
DELIVERIES = []    # receiver 1
DELIVERIES2 = []   # receiver 2 (after the URL change)
LOCK = threading.Lock()


class Receiver(BaseHTTPRequestHandler):
    box = DELIVERIES

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(n) or b"{}")
        with LOCK:
            self.box.append(dict(body))
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):
        pass


class Receiver2(Receiver):
    box = DELIVERIES2


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


def received(box, key):
    """[(window_start, version, kind)] received by one receiver, in order."""
    with LOCK:
        return [(d["window_start"], d["version"], d["kind"])
                for d in box if d.get("key") == key]


def report(sub, ws, key, version):
    return post(R, "/postings", {"subscriber_name": sub, "window_start": ws,
                                 "key": key, "version": version})


def posting(sub, ws, key):
    rows = get(R, "/postings", subscriber=sub, window_start=ws, key=key)["postings"]
    return rows[0] if rows else None


def delivered_up_to(sub, ws, key):
    """The pair's delivered_up_to as committed in the DB (None if no row)."""
    p = posting(sub, ws, key)
    return p["delivered_up_to"] if p else None


def wait_delivered(sub, ws, key, version, timeout=45):
    """Wait until the pair's DELIVERED watermark is committed and visible —
    the state a following report will be evaluated against."""
    return wait_for(f"{sub} {key}@{ws} delivered_up_to={version}",
                    lambda: delivered_up_to(sub, ws, key) == version
                    and delivered_up_to(sub, ws, key), timeout)


def history(sub, ws, key):
    return get(R, "/postings/history", subscriber=sub,
               window_start=ws, key=key)["events"]


def settle(seconds=3):
    time.sleep(seconds)


def main():
    now = int(time.time() * 1000)
    ws = now // W * W
    ws2 = ws + W
    after = ws + W + 1000

    server = ThreadingHTTPServer(("0.0.0.0", RECV_PORT), Receiver)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    server2 = ThreadingHTTPServer(("0.0.0.0", RECV2_PORT), Receiver2)
    threading.Thread(target=server2.serve_forever, daemon=True).start()

    GOOD = "post-good"
    DEAD = "post-dead"
    sub = post(R, "/subscriptions", {"name": GOOD, "url": f"{RECV_BASE}/r"})
    check("reporting downstream subscribed",
          sub.get("subscription", {}).get("active") is True, str(sub))
    good_id = sub["subscription"]["id"]
    sub = post(R, "/subscriptions", {"name": DEAD, "url": DEAD_URL})
    check("unreachable downstream subscribed (stays RETRYING)",
          sub.get("subscription", {}).get("active") is True, str(sub))

    set_wm(ws)  # nothing crosses yet -> deterministic staging

    k1 = f"post-lag-{now}"    # reported on; exercised through the whole lifecycle
    k2 = f"post-free-{now}"   # never reported on: flows freely, ungated

    for s, eid in ((A, "a1"), (B, "b1")):
        upsert(s, f"{k1}-{eid}", ws + 1000, k1, {"n": 1})
        upsert(s, f"{k2}-{eid}", ws + 1000, k2, {"n": 1})

    # -- v1 computed held, then released and delivered -------------------------
    set_wm(after)
    for k in (k1, k2):
        wait_for(f"{k} v1 computed", lambda k=k: head(ws, k))
    post(R, "/releases", {"key": k1})
    post(R, "/releases", {"key": k2})
    wait_delivered(GOOD, ws, k1, 1)
    wait_delivered(GOOD, ws, k2, 1)
    wait_for("k1 v1 received by post-good",
             lambda: received(DELIVERIES, k1) == [(ws, 1, "NEW")])
    wait_for("k2 v1 received by post-good",
             lambda: received(DELIVERIES, k2) == [(ws, 1, "NEW")])

    # -- first report establishes the ledger pair: ALIGNED ---------------------
    p = report(GOOD, ws, k1, 1)
    check("reporting the delivered v1 aligns the pair",
          p.get("posting", {}).get("status") == "ALIGNED"
          and p["posting"]["reported_version"] == 1
          and p["posting"]["delivered_up_to"] == 1, str(p))
    row = posting(GOOD, ws, k1)
    check("ledger view shows ALIGNED, not gated",
          row and row["status"] == "ALIGNED" and row["gated"] is False, str(row))

    # -- a downstream claiming a version we are still retrying -----------------
    p = report(DEAD, ws, k1, 1)
    check("reporting a version still being retried -> AHEAD_UNCONFIRMED",
          p.get("posting", {}).get("status") == "AHEAD_UNCONFIRMED", str(p))
    row = posting(DEAD, ws, k1)
    check("ledger shows it posted v1 while nothing is confirmed delivered",
          row and row["status"] == "AHEAD_UNCONFIRMED"
          and row["reported_version"] == 1
          and row["delivered_up_to"] is None
          and row["inflight_version"] == 1
          and row["gated"] is False, str(row))

    # -- a new delivery on the aligned pair turns it LAGGING -------------------
    upsert(B, f"{k1}-b-late", ws + 500, k1, {"n": 2})   # v2 correction
    wait_delivered(GOOD, ws, k1, 2)
    check("receiver got k1 v2 CORRECTION",
          received(DELIVERIES, k1) == [(ws, 1, "NEW"), (ws, 2, "CORRECTION")],
          str(received(DELIVERIES, k1)))
    row = posting(GOOD, ws, k1)
    check("delivery of v2 knocked the pair LAGGING (reported 1, delivered 2)",
          row and row["status"] == "LAGGING" and row["gated"] is True
          and row["reported_version"] == 1 and row["delivered_up_to"] == 2, str(row))
    lag = get(R, "/postings", status="LAGGING")["postings"]
    check("the lagging pair is listed by the status filter",
          any(r["subscriber"] == GOOD and r["key"] == k1 for r in lag), str(lag))

    # -- while LAGGING, later versions of THIS result are held ------------------
    upsert(B, f"{k1}-b-late2", ws + 600, k1, {"n": 3})  # v3 computed, must be held
    wait_for("k1 v3 computed internally",
             lambda: (h := head(ws, k1)) and h["version"] == 3 and h)
    settle()
    check("v3 is NOT sent while the pair lags",
          received(DELIVERIES, k1) == [(ws, 1, "NEW"), (ws, 2, "CORRECTION")],
          str(received(DELIVERIES, k1)))
    pend = get(R, "/deliveries", key=k1, status="PENDING")["deliveries"]
    check("v3 sits PENDING in the outbox, held by the ledger gate",
          any(d["version"] == 3 and d["subscriber"] == GOOD for d in pend), str(pend))

    # -- other results of the same downstream are NOT held ----------------------
    upsert(B, f"{k2}-b-late", ws + 500, k2, {"n": 2})   # k2 v2
    wait_delivered(GOOD, ws, k2, 2)
    check("k2 (never reported on) keeps flowing: v2 received",
          received(DELIVERIES, k2) == [(ws, 1, "NEW"), (ws, 2, "CORRECTION")],
          str(received(DELIVERIES, k2)))
    row = posting(GOOD, ws, k2)
    check("unreported pair shows delivered_up_to with null ledger status, ungated",
          row and row["status"] is None and row["reported_version"] is None
          and row["delivered_up_to"] == 2 and row["gated"] is False, str(row))
    # a different window of the SAME key is a different result: also not held
    for s, eid in ((A, "a2"), (B, "b2")):
        upsert(s, f"{k1}-{eid}", ws2 + 1000, k1, {"n": 1})
    set_wm(ws2 + W + 1000)
    wait_for("k1 second window computed", lambda: head(ws2, k1))
    rel = post(R, "/releases", {"key": k1})
    check("release publishes the second window of k1",
          any(w["window_start"] == ws2 for w in rel["released"]), str(rel))
    wait_for("k1 window 2 v1 delivered although window 1 lags",
             lambda: (ws2, 1, "NEW") in received(DELIVERIES, k1))

    # -- reporting the catch-up version re-aligns and releases the gate --------
    p = report(GOOD, ws, k1, 2)
    check("reporting v2 re-aligns the pair",
          p.get("posting", {}).get("status") == "ALIGNED", str(p))
    wait_delivered(GOOD, ws, k1, 3)
    check("held v3 flowed once aligned",
          (ws, 3, "CORRECTION") in received(DELIVERIES, k1),
          str(received(DELIVERIES, k1)))
    row = posting(GOOD, ws, k1)
    check("the new delivery knocked it LAGGING again (reported 2, delivered 3)",
          row and row["status"] == "LAGGING"
          and row["reported_version"] == 2 and row["delivered_up_to"] == 3, str(row))
    p = report(GOOD, ws, k1, 3)
    check("reporting v3 aligns again",
          p.get("posting", {}).get("status") == "ALIGNED", str(p))

    # -- reporting a version we never sent does not count -----------------------
    bad = report(GOOD, ws, k1, 99)
    check("reporting a never-sent version is rejected (409)",
          bad.get("_status") == 409, str(bad))
    row = posting(GOOD, ws, k1)
    check("the rejected report changed nothing",
          row and row["status"] == "ALIGNED" and row["reported_version"] == 3, str(row))
    bad = report(GOOD, ws2, k1, 2)
    check("reporting a version that exists nowhere in the outbox is rejected",
          bad.get("_status") == 409, str(bad))
    bad = report(GOOD, ws, k1, 0)
    check("version 0 is not a valid report (422)",
          bad.get("_status") == 422, str(bad))

    # -- rollback: the ledger regresses, deliveries are never erased ------------
    p = report(GOOD, ws, k1, 1)
    check("rollback report moves the ledger back to LAGGING",
          p.get("posting", {}).get("status") == "LAGGING"
          and p["posting"]["reported_version"] == 1
          and p["posting"]["delivered_up_to"] == 3, str(p))
    upsert(B, f"{k1}-b-late3", ws + 700, k1, {"n": 4})  # v4 computed, must be held
    wait_for("k1 v4 computed internally",
             lambda: (h := head(ws, k1)) and h["version"] == 4 and h)
    settle()
    check("v4 held while the rolled-back pair lags",
          (ws, 4, "CORRECTION") not in received(DELIVERIES, k1),
          str(received(DELIVERIES, k1)))
    p = report(GOOD, ws, k1, 2)
    check("catching up only to v2 keeps it LAGGING (delivered is 3)",
          p.get("posting", {}).get("status") == "LAGGING", str(p))
    settle(2)
    check("v4 still held", (ws, 4, "CORRECTION") not in received(DELIVERIES, k1),
          str(received(DELIVERIES, k1)))
    p = report(GOOD, ws, k1, 3)
    check("reporting back up to the delivered version re-aligns",
          p.get("posting", {}).get("status") == "ALIGNED", str(p))
    wait_delivered(GOOD, ws, k1, 4)
    check("v4 flowed after the rollback was caught up",
          (ws, 4, "CORRECTION") in received(DELIVERIES, k1),
          str(received(DELIVERIES, k1)))
    report(GOOD, ws, k1, 4)
    sent = get(R, "/deliveries", key=k1, window_start=ws, subscriber=GOOD)["deliveries"]
    check("delivery records were never erased by the rollback",
          sorted(d["version"] for d in sent) == [1, 2, 3, 4]
          and all(d["status"] == "DELIVERED" for d in sent), str(sent))

    # -- a new receiver URL is NOT a new downstream ------------------------------
    sub = post(R, "/subscriptions", {"name": GOOD, "url": f"{RECV2_BASE}/r"})
    check("re-registering under a new URL keeps the same subscriber id",
          sub.get("subscription", {}).get("id") == good_id, str(sub))
    row = posting(GOOD, ws, k1)
    check("the ledger stays attached to the same downstream",
          row and row["reported_version"] == 4 and row["status"] == "ALIGNED", str(row))
    upsert(B, f"{k1}-b-late4", ws + 800, k1, {"n": 5})  # v5
    wait_delivered(GOOD, ws, k1, 5)
    check("v5 delivered to the NEW address",
          (ws, 5, "CORRECTION") in received(DELIVERIES2, k1),
          str(received(DELIVERIES2, k1)))
    check("nothing more went to the old address",
          (ws, 5, "CORRECTION") not in received(DELIVERIES, k1),
          str(received(DELIVERIES, k1)))
    report(GOOD, ws, k1, 5)

    # -- the trail: every report and every lagging transition -------------------
    events = history(GOOD, ws, k1)
    kinds = [(e["event"], e["cause_version"], e["prev_status"], e["status"])
             for e in events]
    check("history is chronological (id ascending)",
          all(events[i]["id"] < events[i + 1]["id"] for i in range(len(events) - 1)),
          str(kinds))
    check("first event is the accepted v1 report that opened the pair",
          kinds and kinds[0] == ("REPORT_ACCEPTED", 1, None, "ALIGNED"), str(kinds[:1]))
    knocked = [e for e in events if e["event"] == "DELIVERY_ADVANCED"
               and e["prev_status"] == "ALIGNED" and e["status"] == "LAGGING"]
    check("the ALIGNED->LAGGING moments and their culprit versions are recorded",
          [e["cause_version"] for e in knocked] == [2, 3, 4, 5], str(kinds))
    check("the rollback report is traced (reported 3 -> 1)",
          any(e["event"] == "REPORT_ACCEPTED" and e["cause_version"] == 1
              and e["prev_reported"] == 3 and e["reported_version"] == 1
              and e["status"] == "LAGGING" for e in events), str(kinds))
    check("the rejected report is traced",
          any(e["event"] == "REPORT_REJECTED" and e["cause_version"] == 99
              for e in events), str(kinds))

    post(A, "/watermark/override", {"watermark": None})
    post(B, "/watermark/override", {"watermark": None})

    print()
    if FAILED:
        print(f"{len(FAILED)} check(s) FAILED: {FAILED}")
        sys.exit(1)
    print("all posting-ledger e2e checks passed")


if __name__ == "__main__":
    main()
