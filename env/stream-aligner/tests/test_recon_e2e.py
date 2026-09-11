#!/usr/bin/env python3
"""End-to-end test for reconciliation batches (对账批次),
against a running compose stack.

Usage:  docker compose up -d --build && python3 tests/test_recon_e2e.py

Covers the full lifecycle:
- opening a batch photographs every sent result once: the four buckets
  ALIGNED / LAGGING / AHEAD_UNCONFIRMED / NOT_REPORTED, the per-version
  delivery ladder (including a still-PENDING held version) and rejected
  reports;
- the photograph is immutable: deliveries/reports after opening move only
  the live_* columns, never the frozen snapshot (drifted=true);
- mid-window range bounds are snapped to window boundaries;
- two OPEN batches of the SAME subscriber cannot overlap (409); adjacent /
  disjoint ranges are fine; a different subscriber is never blocked; a
  CLOSED batch never blocks reopening the same range;
- non-aligned items must be adjudicated one by one (CONFIRMED/REJECTED):
  aligned items reject verdicts, one undecided item blocks closing, verdicts
  can be changed while open (traced) and the same verdict is idempotent;
- after closing, verdicts can no longer be recorded or changed (409),
  closing twice is an idempotent no-op;
- the append-only event trail (open / decisions / close);
- an empty batch (no sent results in range) can close at once.
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

RECV_PORT = int(os.environ.get("RECEIVER_PORT", "8906"))
RECV_BASE = os.environ.get("RECEIVER_BASE_URL", f"http://host.docker.internal:{RECV_PORT}")
DEAD_URL = os.environ.get("DEAD_RECEIVER_URL", "http://host.docker.internal:8998/nope")

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


def upsert(stream, eid, t, key, payload=None):
    post(stream, "/events", ev(eid, t, key, payload=payload))


def set_wm(t):
    post(A, "/watermark/override", {"watermark": t})
    post(B, "/watermark/override", {"watermark": t})


def head(ws, key):
    return get(R, "/results/current", window_start=ws, key=key)["result"]


def delivered_up_to(sub, ws, key):
    rows = get(R, "/postings", subscriber=sub, window_start=ws, key=key)["postings"]
    return rows[0]["delivered_up_to"] if rows else None


def wait_delivered(sub, ws, key, version, timeout=45):
    return wait_for(f"{sub} {key}@{ws} delivered_up_to={version}",
                    lambda: delivered_up_to(sub, ws, key) == version, timeout)


def report(sub, ws, key, version):
    return post(R, "/postings", {"subscriber_name": sub, "window_start": ws,
                                 "key": key, "version": version})


def open_batch(sub, from_ws, to_ws, operator="tester"):
    return post(R, "/reconciliations",
                {"subscriber_name": sub, "from_window_start": from_ws,
                 "to_window_start": to_ws, "operator": operator})


def batch_items(bid, **params):
    return get(R, f"/reconciliations/{bid}/items", **params)["items"]


def item_map(bid, **params):
    return {(i["window_start"], i["key"]): i
            for i in batch_items(bid, **params)}


def decide(bid, ws, key, decision, operator="tester", note=None):
    body = {"window_start": ws, "key": key, "decision": decision,
            "operator": operator}
    if note:
        body["note"] = note
    return post(R, f"/reconciliations/{bid}/decisions", body)


def main():
    now = int(time.time() * 1000)
    ws = now // W * W
    after = ws + W + 1000

    server = ThreadingHTTPServer(("0.0.0.0", RECV_PORT), Receiver)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    GOOD = "recon-good"
    DEAD = "recon-dead"
    sub = post(R, "/subscriptions", {"name": GOOD, "url": f"{RECV_BASE}/r"})
    check("good downstream subscribed",
          sub.get("subscription", {}).get("active") is True, str(sub))

    set_wm(ws)  # deterministic staging: nothing crosses yet

    suffix = str(now)
    k_aligned = f"rc-aligned-{suffix}"
    k_lag = f"rc-lag-{suffix}"
    k_norep = f"rc-norep-{suffix}"
    k_ahead = f"rc-ahead-{suffix}"
    keys = (k_aligned, k_lag, k_norep, k_ahead)

    for key in keys:
        upsert(A, f"{key}-a1", ws + 1000, key, {"n": 1})
        upsert(B, f"{key}-b1", ws + 1500, key, {"n": 1})

    set_wm(after)
    for key in keys:
        wait_for(f"{key} v1 computed", lambda key=key: head(ws, key))

    # release three keys before the dead downstream exists
    for key in (k_aligned, k_lag, k_norep):
        post(R, "/releases", {"key": key})
    for key in (k_aligned, k_lag, k_norep):
        wait_delivered(GOOD, ws, key, 1)

    # the dead downstream registers BEFORE k_ahead is released -> it gets a
    # retrying v1 delivery (post-registered subscribers receive no backfill)
    sub = post(R, "/subscriptions", {"name": DEAD, "url": DEAD_URL})
    check("dead downstream subscribed",
          sub.get("subscription", {}).get("active") is True, str(sub))
    post(R, "/releases", {"key": k_ahead})
    wait_delivered(GOOD, ws, k_ahead, 1)
    wait_for("dead v1 dispatched and being retried",
             lambda: any(d["version"] == 1 for d in get(
                 R, "/deliveries", subscriber=DEAD, key=k_ahead,
                 status="RETRYING")["deliveries"]))

    # -- shape the four ledger states ------------------------------------------
    report(GOOD, ws, k_aligned, 1)
    report(GOOD, ws, k_ahead, 1)
    report(GOOD, ws, k_lag, 1)
    upsert(B, f"{k_lag}-b-late", ws + 500, k_lag, {"n": 2})   # v2 delivered -> LAGGING
    wait_delivered(GOOD, ws, k_lag, 2)
    upsert(B, f"{k_lag}-b-late2", ws + 600, k_lag, {"n": 3})  # v3 held PENDING by the gate
    wait_for("lag key v3 computed",
             lambda: (h := head(ws, k_lag)) and h["version"] == 3 and h)
    # a rejected report must end up in the batch snapshot
    bad = report(GOOD, ws, k_norep, 99)
    check("never-sent v99 report rejected before opening",
          bad.get("_status") == 409, str(bad))
    # the dead downstream genuinely may have booked the retrying v1
    p = report(DEAD, ws, k_ahead, 1)
    check("dead reports the retrying v1 -> AHEAD_UNCONFIRMED",
          p.get("posting", {}).get("status") == "AHEAD_UNCONFIRMED", str(p))

    # -- open the GOOD batch with mid-window bounds (snapping test) ------------
    opened = open_batch(GOOD, ws + 100, ws + W - 100)
    check("batch opened", opened.get("_status") is None, str(opened))
    bid = opened["reconciliation"]["id"]
    check("range snapped to window boundaries",
          opened["reconciliation"]["window_start_from"] == ws
          and opened["reconciliation"]["window_start_to"] == ws + W,
          str(opened["reconciliation"]))
    rc = opened["reconciliation"]
    check("batch counters: 2 aligned / 1 lagging / 0 ahead / 1 not reported",
          (rc["total_items"], rc["aligned_items"], rc["lagging_items"],
           rc["ahead_items"], rc["not_reported_items"], rc["unresolved_items"])
          == (4, 2, 1, 0, 1, 2), str(rc))

    items = item_map(bid)
    check("all four results photographed", set(items) == {(ws, k) for k in keys},
          str({(k[1]): v["item_status"] for k, v in items.items()}))
    check("k_aligned snapshot ALIGNED at v1",
          items[(ws, k_aligned)]["item_status"] == "ALIGNED"
          and items[(ws, k_aligned)]["delivered_version"] == 1
          and items[(ws, k_aligned)]["reported_version"] == 1, str(items[(ws, k_aligned)]))
    lag = items[(ws, k_lag)]
    check("k_lag snapshot LAGGING: reported 1, delivered 2, sent 3 (v3 held)",
          lag["item_status"] == "LAGGING" and lag["reported_version"] == 1
          and lag["delivered_version"] == 2 and lag["sent_version"] == 3
          and lag["inflight_version"] == 3, str(lag))
    ladder = [(v["version"], v["status"]) for v in lag["delivery_ladder"]]
    check("snapshot delivery ladder freezes v1/v2 DELIVERED + v3 PENDING",
          ladder == [(1, "DELIVERED"), (2, "DELIVERED"), (3, "PENDING")], str(ladder))
    norep = items[(ws, k_norep)]
    check("k_norep snapshot NOT_REPORTED with the rejected v99 report attached",
          norep["item_status"] == "NOT_REPORTED"
          and norep["reported_version"] is None
          and [r["version"] for r in norep["rejected_reports"]] == [99], str(norep))
    check("aligned/lag rows have no rejected reports",
          items[(ws, k_aligned)]["rejected_reports"] == []
          and lag["rejected_reports"] == [], "unexpected rejected reports")

    # -- the dead downstream's own batch sees AHEAD_UNCONFIRMED ----------------
    # DEAD registered before k_lag's v2 correction, so post-registration
    # versions fan out to it too (never its missing v1 — that is backfill's
    # job): its batch also holds a never-reported, never-delivered k_lag.
    opened_dead = open_batch(DEAD, ws + 100, ws + W - 100)
    check("different subscriber can open an overlapping batch",
          opened_dead.get("_status") is None, str(opened_dead))
    dbid = opened_dead["reconciliation"]["id"]
    check("dead batch photographed 2 results (k_ahead + post-registration k_lag)",
          opened_dead["reconciliation"]["total_items"] == 2
          and opened_dead["reconciliation"]["not_reported_items"] == 1
          and opened_dead["reconciliation"]["ahead_items"] == 1
          and opened_dead["reconciliation"]["unresolved_items"] == 2,
          str(opened_dead["reconciliation"]))
    d_items = item_map(dbid)
    check("dead batch: k_ahead AHEAD_UNCONFIRMED, nothing delivered",
          (ws, k_ahead) in d_items
          and d_items[(ws, k_ahead)]["item_status"] == "AHEAD_UNCONFIRMED"
          and d_items[(ws, k_ahead)]["delivered_version"] is None
          and d_items[(ws, k_ahead)]["inflight_version"] == 1
          and d_items[(ws, k_ahead)]["reported_version"] == 1,
          str(d_items))
    check("dead batch: k_lag NOT_REPORTED (v2/v3 on the wire, none confirmed)",
          d_items[(ws, k_lag)]["item_status"] == "NOT_REPORTED"
          and d_items[(ws, k_lag)]["sent_version"] == 3
          and d_items[(ws, k_lag)]["inflight_version"] == 2
          and d_items[(ws, k_lag)]["reported_version"] is None,
          str(d_items.get((ws, k_lag))))

    # -- overlap guard ----------------------------------------------------------
    dup = open_batch(GOOD, ws, ws + W)
    check("overlapping OPEN batch for the same subscriber -> 409",
          dup.get("_status") == 409, str(dup))
    far = open_batch(GOOD, ws + 10 * W + 100, ws + 11 * W - 100)
    check("disjoint (snapped, future) range opens fine",
          far.get("_status") is None, str(far))
    far_id = far["reconciliation"]["id"]
    check("an empty range photographs zero items",
          far["reconciliation"]["total_items"] == 0
          and far["reconciliation"]["unresolved_items"] == 0, str(far["reconciliation"]))
    closed = post(R, f"/reconciliations/{far_id}/close", {"operator": "tester"})
    check("an empty batch closes at once",
          closed.get("reconciliation", {}).get("status") == "CLOSED", str(closed))

    # -- the snapshot is frozen while the live ledger keeps moving --------------
    upsert(B, f"{k_aligned}-b-late", ws + 500, k_aligned, {"n": 2})  # v2 delivered
    wait_delivered(GOOD, ws, k_aligned, 2)
    report(GOOD, ws, k_aligned, 2)
    upsert(B, f"{k_norep}-b-late", ws + 500, k_norep, {"n": 2})       # v2 delivered
    wait_delivered(GOOD, ws, k_norep, 2)
    items = item_map(bid)
    a_now, n_now = items[(ws, k_aligned)], items[(ws, k_norep)]
    check("frozen snapshot of k_aligned still says delivered/reported v1",
          a_now["delivered_version"] == 1 and a_now["reported_version"] == 1
          and a_now["item_status"] == "ALIGNED", str(a_now))
    check("live columns show k_aligned at v2 and flag the drift",
          a_now["live_delivered_version"] == 2 and a_now["live_reported_version"] == 2
          and a_now["live_item_status"] == "ALIGNED" and a_now["drifted"] is True, str(a_now))
    check("k_norep frozen at sent v1, live at v2, still NOT_REPORTED + drifted",
          n_now["sent_version"] == 1 and n_now["live_sent_version"] == 2
          and n_now["live_item_status"] == "NOT_REPORTED"
          and n_now["drifted"] is True, str(n_now))

    # -- adjudication one by one ------------------------------------------------
    bad = decide(bid, ws, k_aligned, "CONFIRMED")
    check("verdict on an ALIGNED item -> 409", bad.get("_status") == 409, str(bad))
    bad = decide(bid, ws, "rc-no-such-key", "CONFIRMED")
    check("verdict on an unknown item -> 404", bad.get("_status") == 404, str(bad))
    bad = post(R, f"/reconciliations/{bid}/decisions",
               {"window_start": ws, "key": k_lag, "decision": "WAT"})
    check("invalid decision -> 422", bad.get("_status") == 422, str(bad))
    bad = decide(999999, ws, k_lag, "CONFIRMED")
    check("decision on an unknown batch -> 404", bad.get("_status") == 404, str(bad))

    close_attempt = post(R, f"/reconciliations/{bid}/close", {"operator": "tester"})
    check("cannot close while two items are undecided",
          close_attempt.get("_status") == 409, str(close_attempt))

    d = decide(bid, ws, k_lag, "CONFIRMED", note="they will catch up")
    check("CONFIRMED on the lagging item is recorded and counted",
          d.get("item", {}).get("decision") == "CONFIRMED"
          and d["reconciliation"]["confirmed_items"] == 1
          and d["reconciliation"]["unresolved_items"] == 1, str(d))
    d = decide(bid, ws, k_lag, "CONFIRMED")
    check("repeating the same verdict is an idempotent no-op",
          d.get("_status") is None and d["reconciliation"]["confirmed_items"] == 1, str(d))
    d = decide(bid, ws, k_lag, "REJECTED")
    check("changing the verdict while open is allowed",
          d.get("item", {}).get("decision") == "REJECTED"
          and d["reconciliation"]["confirmed_items"] == 0
          and d["reconciliation"]["rejected_items"] == 1, str(d))
    d = decide(bid, ws, k_norep, "REJECTED", note="we never sent that; actually we did, rejected")
    check("REJECTED on the not-reported item clears the last open item",
          d.get("item", {}).get("decision") == "REJECTED"
          and d["reconciliation"]["unresolved_items"] == 0, str(d))

    # filters
    undecided = {(i["window_start"], i["key"])
                 for i in batch_items(bid, undecided_only=True)}
    check("undecided_only is now empty", undecided == set(), str(undecided))
    lag_rows = batch_items(bid, item_status="LAGGING")
    check("item_status filter finds the (frozen) lagging row even after drift",
          [(i["window_start"], i["key"]) for i in lag_rows] == [(ws, k_lag)], str(lag_rows))
    decided = batch_items(bid, decision="REJECTED")
    check("decision filter returns both rejected rows",
          sorted(i["key"] for i in decided) == sorted([k_lag, k_norep]), str(decided))

    closed = post(R, f"/reconciliations/{bid}/close", {"operator": "tester"})
    check("batch closes once every non-aligned item is adjudicated",
          closed.get("reconciliation", {}).get("status") == "CLOSED"
          and closed["reconciliation"]["closed_by"] == "tester", str(closed))

    # -- frozen after close -----------------------------------------------------
    bad = decide(bid, ws, k_lag, "CONFIRMED")
    check("after close a verdict cannot be changed -> 409",
          bad.get("_status") == 409, str(bad))
    items = item_map(bid)
    check("the rejected verdict itself is untouched",
          items[(ws, k_lag)]["decision"] == "REJECTED", str(items[(ws, k_lag)]))
    events_before = get(R, f"/reconciliations/{bid}/events")["events"]
    closed2 = post(R, f"/reconciliations/{bid}/close", {"operator": "tester"})
    check("closing an already-closed batch is an idempotent no-op",
          closed2.get("reconciliation", {}).get("status") == "CLOSED", str(closed2))
    events_after = get(R, f"/reconciliations/{bid}/events")["events"]
    check("no duplicate BATCH_CLOSED event",
          len(events_after) == len(events_before), str(events_after))
    kinds = [(e["event"], e.get("window_start"), e.get("decision"),
              e.get("prev_decision"), e.get("operator"), e.get("note"))
             for e in events_after]
    check("event trail: open, 3 decisions (same verdict no-op, change traced), close",
          kinds[0][0] == "BATCH_OPENED" and kinds[-1][0] == "BATCH_CLOSED"
          and sum(1 for k in kinds if k[0] == "ITEM_DECIDED") == 3
          and any(k[0] == "ITEM_DECIDED" and k[1] == ws and k[2] == "REJECTED"
                  and k[3] == "CONFIRMED" and k[4] == "tester" for k in kinds)
          and any(k[0] == "ITEM_DECIDED" and k[5] == "they will catch up"
                  for k in kinds), str(kinds))
    check("events come back chronological by id",
          [e["id"] for e in events_after] == sorted(e["id"] for e in events_after),
          str(kinds))

    # -- closing frees the range: another OPEN batch may now cover it -----------
    reopened = open_batch(GOOD, ws, ws + W, operator="tester2")
    check("a CLOSED batch never blocks reopening the same range",
          reopened.get("_status") is None and reopened["reconciliation"]["status"] == "OPEN",
          str(reopened))
    nid = reopened["reconciliation"]["id"]
    n_items = item_map(nid)
    check("the new batch photographs the CURRENT state (k_norep sent v2)",
          n_items[(ws, k_norep)]["sent_version"] == 2
          and n_items[(ws, k_norep)]["item_status"] == "NOT_REPORTED",
          str(n_items.get((ws, k_norep))))

    # list view
    lst = get(R, "/reconciliations", subscriber=GOOD, status="OPEN")["reconciliations"]
    check("list filter: only OPEN batches of the good subscriber",
          {b["id"] for b in lst} == {nid}
          and all(b["status"] == "OPEN" for b in lst), str(lst))
    one = get(R, f"/reconciliations/{bid}")["reconciliation"]
    check("batch detail carries counts and unresolved_items=0",
          one["status"] == "CLOSED" and one["unresolved_items"] == 0
          and one["subscriber"] == GOOD, str(one))
    missing = get(R, "/reconciliations/999999")
    check("unknown batch detail -> 404", missing.get("_status") == 404, str(missing))

    # -- the dead batch follows the same lifecycle ------------------------------
    # every undecided item (ahead + not-reported) must be adjudicated one by one
    undecided_dead = {(i["window_start"], i["key"])
                      for i in batch_items(dbid, undecided_only=True)}
    check("dead batch lists two undecided non-aligned items",
          undecided_dead == {(ws, k_ahead), (ws, k_lag)}, str(undecided_dead))
    for ws0, key0 in sorted(undecided_dead):
        d = decide(dbid, ws0, key0, "CONFIRMED")
        check(f"dead: {key0} adjudicated CONFIRMED",
              d.get("item", {}).get("decision") == "CONFIRMED", str(d))
    d_batch = get(R, f"/reconciliations/{dbid}")["reconciliation"]
    check("dead batch has no unresolved items left",
          d_batch["unresolved_items"] == 0, str(d_batch))
    closed = post(R, f"/reconciliations/{dbid}/close", {"operator": "tester"})
    check("dead batch closes",
          closed.get("reconciliation", {}).get("status") == "CLOSED", str(closed))

    # -- input validation --------------------------------------------------------
    bad = post(R, "/reconciliations",
               {"subscriber_name": GOOD, "from_window_start": ws + W,
                "to_window_start": ws})
    check("inverted range -> 422", bad.get("_status") == 422, str(bad))
    bad = post(R, "/reconciliations",
               {"subscriber_name": "rc-no-such-sub",
                "from_window_start": ws, "to_window_start": ws + W})
    check("unknown subscriber -> 404", bad.get("_status") == 404, str(bad))
    bad = get(R, "/reconciliations", status="WAT")
    check("bad status filter -> 422", bad.get("_status") == 422, str(bad))

    post(A, "/watermark/override", {"watermark": None})
    post(B, "/watermark/override", {"watermark": None})

    print()
    if FAILED:
        print(f"{len(FAILED)} check(s) FAILED: {FAILED}")
        sys.exit(1)
    print("all reconciliation-batch e2e checks passed")


if __name__ == "__main__":
    main()
