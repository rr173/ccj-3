#!/usr/bin/env python3
"""End-to-end test for reconciliation settlements (对账落账),
against a running stack (local: ports 8001/8002/8003).

Verifies the four rules from the settlement semantics:
- 开账认驳不动投递；只有结账才落 (verdicts are inert while OPEN);
- LAGGING + 认 -> FREEZE: pin at its reported version, later versions are no
  longer sent to THIS downstream, reports above the pin are rejected;
- LAGGING + 驳 -> CONTINUE: keep sending what we sent, the lagging gate never
  holds that pair again;
- NOT_REPORTED + 认 -> SUPPRESS: that version is never (re)delivered, a later
  backfill never copies it, every report of it is rejected;
- NOT_REPORTED + 驳 -> REDRIVE: the version is RE-POSTED after close (a brand
  new delivery_id, same result version — even though the records already
  showed DELIVERED: "送成功了但它没报，结账后也要再送一次"), until the
  downstream reports it, after which normal flow resumes;
- AHEAD_UNCONFIRMED + 认 -> the retrying version is confirmed DELIVERED at
  close and the ledger aligns;
- 同一条只能落到一次: another batch adjudicating an already-settled pair
  fails close with 409;
- 别的下游 / 别的笔不受拖累; every settled row is queryable (pinned version).
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

RECV_PORT = int(os.environ.get("RECEIVER_PORT", "8910"))
RECV_BASE = os.environ.get("RECEIVER_BASE_URL", f"http://127.0.0.1:{RECV_PORT}")
OTHER_PORT = int(os.environ.get("RECEIVER2_PORT", "8911"))
OTHER_BASE = os.environ.get("RECEIVER2_BASE_URL", f"http://127.0.0.1:{OTHER_PORT}")
DEAD_URL = os.environ.get("DEAD_RECEIVER_URL", "http://127.0.0.1:8998/nope")

FAILED = []
DELIVERIES = []
OTHER_DELIVERIES = []
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


class OtherReceiver(Receiver):
    box = OTHER_DELIVERIES


def check(name, cond, detail=""):
    status = "ok " if cond else "FAIL"
    print(f"[{status}] {name}" + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def req(method, base, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(base + path, data=data, method=method,
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


def wait_for(name, fn, timeout=40):
    deadline = time.time() + timeout
    while time.time() < deadline:
        val = fn()
        if val:
            return val
        time.sleep(0.5)
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


def sub(name, base):
    return post(R, "/subscriptions",
                {"name": name, "url": f"{base}/r"})["subscription"]["id"]


def received(box, ws, key):
    """[(version, delivery_id, redelivery_seq)] received for one result."""
    with LOCK:
        return [(d["version"], d["delivery_id"], d.get("redelivery_seq", 0))
                for d in box
                if d.get("window_start") == ws and d.get("key") == key]


def delivered_versions(name, ws, key):
    rows = get(R, "/deliveries", subscriber=name, window_start=ws, key=key,
               limit=100)["deliveries"]
    return {r["version"]: r["status"] for r in rows}


def report(name, ws, key, version):
    return post(R, "/postings", {"subscriber_name": name, "window_start": ws,
                                 "key": key, "version": version})


def wait_delivered(name, ws, key, version, timeout=30):
    return wait_for(
        f"{name} {key}@{ws} v{version} delivered",
        lambda: delivered_versions(name, ws, key).get(version) == "DELIVERED",
        timeout)


def main():
    now = int(time.time() * 1000)
    ws = now // W * W
    after = ws + W + 1000

    ThreadingHTTPServer  # noqa: B018 (import kept for parity with other suites)
    srv = ThreadingHTTPServer(("0.0.0.0", RECV_PORT), Receiver)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    srv2 = ThreadingHTTPServer(("0.0.0.0", OTHER_PORT), OtherReceiver)
    threading.Thread(target=srv2.serve_forever, daemon=True).start()

    S1, S2 = f"set-s1-{now}", f"set-s2-{now}"
    sub(S1, RECV_BASE)
    sub(S2, OTHER_BASE)
    DEAD = f"set-dead-{now}"
    post(R, "/subscriptions", {"name": DEAD, "url": DEAD_URL})

    set_wm(ws)
    suffix = str(now)
    k_freeze = f"set-freeze-{suffix}"    # LAGGING, 认 -> FREEZE at reported
    k_continue = f"set-cont-{suffix}"   # LAGGING, 驳 -> CONTINUE
    k_suppress = f"set-supp-{suffix}"   # NOT_REPORTED delivered, 认 -> SUPPRESS
    k_redrive = f"set-redr-{suffix}"    # NOT_REPORTED delivered, 驳 -> REDRIVE
    k_ahead = f"set-ahead-{suffix}"     # only the dead downstream gets it
    keys = (k_freeze, k_continue, k_suppress, k_redrive, k_ahead)
    for key in keys:
        upsert(A, f"{key}-a1", ws + 1000, key, {"n": 1})
        upsert(B, f"{key}-b1", ws + 1500, key, {"n": 1})
    set_wm(after)
    for key in keys:
        wait_for(f"{key} v1", lambda key=key: head(ws, key))
    for key in (k_freeze, k_continue, k_suppress, k_redrive, k_ahead):
        post(R, "/releases", {"key": key})
    for key in (k_freeze, k_continue, k_suppress, k_redrive, k_ahead):
        wait_delivered(S1, ws, key, 1)
        wait_delivered(S2, ws, key, 1)

    # freeze/continue: both report v1, then get v2 delivered -> LAGGING
    report(S1, ws, k_freeze, 1)
    report(S1, ws, k_continue, 1)
    upsert(B, f"{k_freeze}-b2", ws + 500, k_freeze, {"n": 2})
    upsert(B, f"{k_continue}-b2", ws + 500, k_continue, {"n": 2})
    wait_delivered(S1, ws, k_freeze, 2)
    wait_delivered(S1, ws, k_continue, 2)
    # v3 exists and sits PENDING behind the lagging gate for both. Wait for
    # the row first: LAGGING guarantees v3 can never be sent before close.
    upsert(B, f"{k_freeze}-b3", ws + 600, k_freeze, {"n": 3})
    upsert(B, f"{k_continue}-b3", ws + 600, k_continue, {"n": 3})
    wait_for("freeze v3 computed",
             lambda: head(ws, k_freeze)["version"] == 3)
    wait_for("continue v3 computed",
             lambda: head(ws, k_continue)["version"] == 3)
    wait_for("freeze/continue v3 parked PENDING by the lagging gate",
             lambda: delivered_versions(S1, ws, k_freeze).get(3) == "PENDING"
             and delivered_versions(S1, ws, k_continue).get(3) == "PENDING")
    time.sleep(1)
    # suppress/redrive: v1 delivered but NEVER reported -> NOT_REPORTED
    # the dead downstream has k_ahead v1 retrying (its response is lost)
    wait_for("dead k_ahead v1 retrying",
             lambda: delivered_versions(DEAD, ws, k_ahead).get(1) == "RETRYING")
    dead_report = report(DEAD, ws, k_ahead, 1)
    check("dead reports the retrying v1 -> AHEAD_UNCONFIRMED",
          dead_report.get("posting", {}).get("status") == "AHEAD_UNCONFIRMED",
          str(dead_report))

    # -- open the batch: verdicts recorded now must NOT change delivery yet ---
    opened = post(R, "/reconciliations",
                  {"subscriber_name": S1, "from_window_start": ws,
                   "to_window_start": ws + W, "operator": "tester"})
    check("S1 batch opened", opened.get("_status") is None, str(opened))
    bid = opened["reconciliation"]["id"]
    imap = {(i["window_start"], i["key"]): i
            for i in get(R, f"/reconciliations/{bid}/items")["items"]}
    check("photographed four LAGGING/NOT_REPORTED items",
          imap[(ws, k_freeze)]["item_status"] == "LAGGING"
          and imap[(ws, k_continue)]["item_status"] == "LAGGING"
          and imap[(ws, k_suppress)]["item_status"] == "NOT_REPORTED"
          and imap[(ws, k_redrive)]["item_status"] == "NOT_REPORTED",
          str({k[1]: v["item_status"] for k, v in imap.items()}))

    post(R, f"/reconciliations/{bid}/decisions",
         {"window_start": ws, "key": k_freeze, "decision": "CONFIRMED"})
    post(R, f"/reconciliations/{bid}/decisions",
         {"window_start": ws, "key": k_continue, "decision": "REJECTED"})
    post(R, f"/reconciliations/{bid}/decisions",
         {"window_start": ws, "key": k_suppress, "decision": "CONFIRMED"})
    redrive_decided = post(R, f"/reconciliations/{bid}/decisions",
                           {"window_start": ws, "key": k_redrive,
                            "decision": "REJECTED"})
    # S1 also received k_ahead (fan-out is to every subscriber) but never
    # reported it: confirm it as well (SUPPRESS) so the batch can close.
    post(R, f"/reconciliations/{bid}/decisions",
         {"window_start": ws, "key": k_ahead, "decision": "CONFIRMED"})
    unresolved = get(R, f"/reconciliations/{bid}")["reconciliation"]["unresolved_items"]
    check("all five adjudicated, unresolved=0", unresolved == 0, str(unresolved))
    time.sleep(2)
    check("verdict while OPEN does not redrive (still exactly one v1 POST)",
          [(d[0], d[2]) for d in received(DELIVERIES, ws, k_redrive)] == [(1, 0)],
          str(received(DELIVERIES, ws, k_redrive)))

    # a separate dead batch: adjudicate every non-aligned item generically;
    # its AHEAD k_ahead verdict is the one that re-confirms the retrying row.
    opened_dead = post(R, "/reconciliations",
                       {"subscriber_name": DEAD, "from_window_start": ws,
                        "to_window_start": ws + W})
    dbid = opened_dead["reconciliation"]["id"]
    dmap = {(i["window_start"], i["key"]): i
            for i in get(R, f"/reconciliations/{dbid}/items")["items"]}
    check("dead batch has the AHEAD k_ahead",
          dmap.get((ws, k_ahead), {}).get("item_status") == "AHEAD_UNCONFIRMED",
          str(dmap))
    for it in dmap.values():
        if it["item_status"] != "ALIGNED":
            decision = ("CONFIRMED" if it["item_status"] == "AHEAD_UNCONFIRMED"
                        else "CONFIRMED")
            post(R, f"/reconciliations/{dbid}/decisions",
                 {"window_start": it["window_start"], "key": it["key"],
                  "decision": decision})

    # -- close: the verdicts land now ------------------------------------------
    closed = post(R, f"/reconciliations/{bid}/close", {"operator": "tester"})
    check("S1 batch CLOSED and 5 items settled",
          closed.get("reconciliation", {}).get("status") == "CLOSED"
          and closed["reconciliation"]["settled_items"] == 5, str(closed))
    closed_dead = post(R, f"/reconciliations/{dbid}/close", {"operator": "tester"})
    check("dead batch closed (AHEAD + 认 -> MARK_DELIVERED)",
          closed_dead.get("reconciliation", {}).get("status") == "CLOSED",
          str(closed_dead))

    # -- REDRIVE: the already-DELIVERED v1 is re-POSTed with a NEW delivery id
    redriven = wait_for(
        "REDRIVE re-posts v1 (new delivery_id, same version, redelivery_seq=1)",
        lambda: (v1 := [d for d in received(DELIVERIES, ws, k_redrive) if d[0] == 1])
                and len(v1) == 2 and v1[1][2] == 1 and v1[1][1] != v1[0][1]
                and v1[1], timeout=20)
    check("redriven v1 is a new delivery_id, not the old row",
          redriven and redriven[1] != received(DELIVERIES, ws, k_redrive)[0][1],
          str(received(DELIVERIES, ws, k_redrive)))
    dr_rows = get(R, "/deliveries", subscriber=S1, window_start=ws,
                  key=k_redrive, limit=20)["deliveries"]
    check("outbox keeps both generations of v1 (seq 0 DELIVERED, seq 1 DELIVERED)",
          sorted((r["version"], r["redelivery_seq"], r["status"]) for r in dr_rows)
          == [(1, 0, "DELIVERED"), (1, 1, "DELIVERED")], str(dr_rows))

    # once it reports the redriven version, the settlement is FULFILLED
    rp = report(S1, ws, k_redrive, 1)
    check("reporting the redriven v1 is accepted and settles REDRIVE",
          rp.get("posting", {}).get("status") == "ALIGNED", str(rp))
    st = get(R, "/settlements", subscriber=S1, window_start=ws,
             key=k_redrive)["settlements"]
    check("REDRIVE settlement is FULFILLED at pin v1",
          st and st[0]["status"] == "FULFILLED" and st[0]["pinned_version"] == 1
          and st[0]["effect"] == "REDRIVE", str(st))

    # -- SUPPRESS: never re-sent; a later backfill never copies it -------------
    time.sleep(2)
    got = received(DELIVERIES, ws, k_suppress)
    check("SUPPRESS pair received only the original v1 (no re-post)",
          [(d[0], d[2]) for d in got] == [(1, 0)], str(got))
    bad = report(S1, ws, k_suppress, 1)
    check("SUPPRESS rejects every later report (409, does not count)",
          bad.get("_status") == 409, str(bad))
    bf = post(R, "/backfills", {"subscriber_name": S1,
                                "from_window_start": ws,
                                "to_window_start": ws + W})
    all_bf = get(R, "/deliveries", subscriber=S1, channel="BACKFILL",
                 limit=500)["deliveries"]
    supp_copies = [r for r in all_bf
                   if r["window_start"] == ws and r["key"] == k_suppress]
    check("backfill never copies a SUPPRESS-pinned result",
          supp_copies == [],
          f"job={bf.get('backfill', {}).get('id')} copies={len(supp_copies)}")

    # -- FREEZE: pinned at v1; v2 stays the delivered fact, the held v3 stays
    # parked forever and never arrives; no v4 sent either --------------------
    upsert(B, f"{k_freeze}-b4", ws + 650, k_freeze, {"n": 4})
    wait_for("freeze v4 computed", lambda: head(ws, k_freeze)["version"] == 4)
    time.sleep(2)
    got_freeze = [d[0] for d in received(DELIVERIES, ws, k_freeze)]
    check("FREEZE pair received v1,v2 before close, but never v3/v4 after",
          got_freeze == [1, 2], str(got_freeze))
    parked = {r["version"]: r["status"] for r in
              get(R, "/deliveries", subscriber=S1, window_start=ws,
                  key=k_freeze, limit=20)["deliveries"]}
    check("the not-yet-sent v3/v4 rows stay PENDING (delivered v2 untouched)",
          parked.get(1) == "DELIVERED" and parked.get(2) == "DELIVERED"
          and parked.get(3) == "PENDING" and parked.get(4) == "PENDING",
          str(parked))
    bad = report(S1, ws, k_freeze, 3)
    check("FREEZE rejects a report above the pin (409)",
          bad.get("_status") == 409, str(bad))
    same = report(S1, ws, k_freeze, 1)
    check("re-reporting the pinned version stays accepted (idempotent)",
          same.get("posting", {}).get("reported_version") == 1, str(same))

    # -- CONTINUE: the lagging gate never holds the pair again -----------------
    wait_for("CONTINUE held v3 dispatched despite never reporting v2",
             lambda: delivered_versions(S1, ws, k_continue).get(3) == "DELIVERED")
    check("CONTINUE pair received v1,v2,v3 (not held by the lag)",
          sorted(set(d[0] for d in received(DELIVERIES, ws, k_continue))) == [1, 2, 3],
          str(received(DELIVERIES, ws, k_continue)))
    # reporting beyond the pinned v1 fulfils the CONTINUE settlement
    report(S1, ws, k_continue, 3)
    st = get(R, "/settlements", subscriber=S1, window_start=ws,
             key=k_continue)["settlements"]
    check("CONTINUE settlement FULFILLED after reports move past v1",
          st and st[0]["status"] == "FULFILLED", str(st))

    # -- MARK_DELIVERED: the dead v1 is now confirmed, ledger ALIGNED ----------
    wait_for("dead retrying v1 marked DELIVERED at close",
             lambda: delivered_versions(DEAD, ws, k_ahead).get(1) == "DELIVERED")
    prow = get(R, "/postings", subscriber=DEAD, window_start=ws,
               key=k_ahead)["postings"]
    check("dead ledger aligned to v1 by MARK_DELIVERED",
          prow and prow[0]["status"] == "ALIGNED"
          and prow[0]["reported_version"] == 1
          and prow[0]["delivered_up_to"] == 1, str(prow))

    # -- other downstream and other results are not dragged --------------------
    # S2 has no settlements: its corrections kept/keep flowing normally.
    upsert(B, f"{k_freeze}-s2-b2", ws + 700, k_freeze, {"n": 9})
    wait_for("S2 still receives the frozen result's v2 (other downstream)",
             lambda: (ws, 2) in [(d.get("window_start"), d["version"])
                                 for d in OTHER_DELIVERIES]
             and 2 in [d[0] for d in received(OTHER_DELIVERIES, ws, k_freeze)])
    # another window of the SAME key flows to S1 even though window 1 is frozen
    ws2 = ws + W
    upsert(A, f"{k_freeze}-w2-a1", ws2 + 1000, k_freeze, {"n": 1})
    upsert(B, f"{k_freeze}-w2-b1", ws2 + 1500, k_freeze, {"n": 1})
    set_wm(ws2 + W + 1)
    wait_for("frozen key's second window computed",
             lambda: head(ws2, k_freeze))
    post(R, "/releases", {"key": k_freeze})
    wait_for("frozen key window 2 v1 delivered (the settlement is per window)",
             lambda: delivered_versions(S1, ws2, k_freeze).get(1) == "DELIVERED")

    # -- settlements query: this batch, every pair landed with its pin ---------
    allst = get(R, f"/settlements?batch_id={bid}") if False else \
        get(R, "/settlements", batch_id=bid)["settlements"]
    by_key = {s["key"]: s for s in allst}
    expected = {k_freeze: ("FREEZE", "ACTIVE", 1),
                k_continue: ("CONTINUE", "FULFILLED", 1),
                k_suppress: ("SUPPRESS", "ACTIVE", 0),
                k_redrive: ("REDRIVE", "FULFILLED", 1),
                k_ahead: ("SUPPRESS", "ACTIVE", 0)}
    for key, (effect, status, pin) in expected.items():
        s = by_key.get(key)
        check(f"{key} settlement {effect}@{pin} {status}",
              s and (s["effect"], s["status"], s["pinned_version"])
              == (effect, status, pin), str(s))
    check("settlement list count for the batch is 5", len(allst) == 5, str(len(allst)))

    # -- 同一条只能落到一次 ------------------------------------------------------
    dup = post(R, "/reconciliations",
               {"subscriber_name": S1, "from_window_start": ws,
                "to_window_start": ws + W})
    nid = dup["reconciliation"]["id"]
    # every current state is non-aligned for the frozen/suppressed pair; adjudicate
    items = get(R, f"/reconciliations/{nid}/items")["items"]
    for it in items:
        if it["item_status"] != "ALIGNED" and it["decision"] is None:
            post(R, f"/reconciliations/{nid}/decisions",
                 {"window_start": it["window_start"], "key": it["key"],
                  "decision": "CONFIRMED"})
    bad_close = post(R, f"/reconciliations/{nid}/close", {"operator": "tester"})
    check("closing a batch that would re-settle a pair -> 409 (同一条只能落到一次)",
          bad_close.get("_status") == 409, str(bad_close)[:300])
    # items rows show which batch pinned them
    shown = get(R, f"/reconciliations/{bid}/items", key=k_redrive)["items"]
    check("batch items carry the settlement effect + pin",
          shown and shown[0]["settlement_effect"] == "REDRIVE"
          and shown[0]["pinned_version"] == 1
          and shown[0]["settlement_status"] == "FULFILLED", str(shown))

    post(A, "/watermark/override", {"watermark": None})
    post(B, "/watermark/override", {"watermark": None})

    print()
    if FAILED:
        print(f"{len(FAILED)} check(s) FAILED: {FAILED}")
        sys.exit(1)
    print("all settlement e2e checks passed")


if __name__ == "__main__":
    main()
