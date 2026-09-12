#!/usr/bin/env python3
"""End-to-end test for gap carry-forwards (缺口结转), against a running stack.

Usage:  docker compose up -d --build && python3 tests/test_carry_e2e.py

Covers the hard semantics:
- one side's leftovers of a CLOSED window are routed to a later same-key window
  that has NOT emitted yet; a target that already emitted rejects (409);
- the source window is corrected (CARRY_FORWARD): its leftovers leave its own
  unmatched gap (carried-a/b), they cannot be carried a second time;
- when the target closes, carried events pair its opposite leftovers and the
  pairs are visibly CARRY pairs (carry id + source window), never the target's
  own native pairs; a partial match keeps the carry OPEN with only the matched
  items MATCHED;
- GET /gap-carries answers while open: open status, source -> target window,
  which side and which events; closing freezes matched_snapshot ("当时对上的
  样子") and closes the carry;
- a correction/retraction of the SOURCE (a carried event retracted, or paired
  at the source by a late opposite event) VOIDS the carry — void carries can
  never match again and their events can never be carried a second time;
- a correction/withdrawal of the TARGET after close REOPENS a closed carry
  (it must not keep showing matched); it closes again when the pairs return;
- different keys cannot share a carry (409/404), one event cannot be in two
  open carries (409), a target earlier than / equal to the source is 422.
"""
import json
import os
import sys
import time
import urllib.error
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
    try:
        with urllib.request.urlopen(r, timeout=15) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def get(base, path, **params):
    if params:
        path += "?" + urllib.parse.urlencode(params)
    return req("GET", base, path)


def post(base, path, body):
    return req("POST", base, path, body)


def wait_for(name, fn, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        val = fn()
        if val:
            return val
        time.sleep(0.5)
    check(name, False, "timed out")
    return None


def ev(eid, t, key, payload=None):
    return {"event_id": eid, "event_time": t, "key": key,
            "type": "upsert", "payload": payload or {}}


def retract(eid, t, key, target):
    return {"event_id": eid, "event_time": t, "key": key,
            "type": "retract", "retracts": target}


def head(ws, key):
    return get(R, "/results/current", window_start=ws, key=key)[1]["result"]


def wait_head(ws, key):
    return wait_for(f"head for window {ws} key {key}", lambda: head(ws, key))


def carry_get(cid):
    return get(R, f"/gap-carries/{cid}")[1]["carry"]


def wait_status(cid, status):
    def f():
        c = carry_get(cid)
        return c if c["status"] == status else None
    return wait_for(f"carry {cid} -> {status}", f)


def wait_order(key, status):
    def f():
        rows = get(R, "/orders", key=key, status=status)[1]["orders"]
        return rows[0] if rows else None
    return wait_for(f"order {key} -> {status}", f)


def main():
    now = int(time.time() * 1000)
    ws = now // W * W + 3 * W
    key = f"carry-{now}"
    other = f"carry-other-{now}"

    # ------------------------------------------------------------------
    # 1. source window closes one-sided (A longer): a1/b1 pair, a2 leftover
    post(A, "/events", {"events": [ev(f"{key}-a1", ws + 1000, key),
                                   ev(f"{key}-a2", ws + 2000, key)]})
    post(B, "/events", ev(f"{key}-b1", ws + 1500, key))
    v = wait_head(ws, key)
    check("source v1 is one pair + one A leftover",
          v["version"] == 1
          and [(p["a_event_id"], p["b_event_id"]) for p in v["payload"]["pairs"]]
              == [(f"{key}-a1", f"{key}-b1")]
          and v["payload"]["unmatched_a"] == [f"{key}-a2"],
          str(v["payload"]))

    tgt = ws + W

    # --- validation before opening any carry ----------------------------
    code, _ = post(R, "/gap-carries",
                   {"key": key, "source_window_start": ws,
                    "target_window_start": ws, "side": "a"})
    check("target must be later than source (422)", code == 422, str(code))
    code, _ = post(R, "/gap-carries",
                   {"key": key, "source_window_start": ws,
                    "target_window_start": tgt + 7, "side": "a"})
    check("non-boundary target window_start is 422", code == 422, str(code))
    code, _ = post(R, "/gap-carries",
                   {"key": key, "source_window_start": ws,
                    "target_window_start": tgt, "side": "z"})
    check("bad side is 422", code == 422, str(code))
    code, _ = post(R, "/gap-carries",
                   {"key": other, "source_window_start": ws + 10 * W,
                    "target_window_start": ws + 11 * W, "side": "a"})
    check("a source with no result for this key is 404", code == 404, str(code))
    code, _ = post(R, "/gap-carries",
                   {"key": key, "source_window_start": ws,
                    "target_window_start": tgt, "side": "b"})
    check("carrying the side without leftovers is 409", code == 409, str(code))

    # --- target that already emitted can never receive a carry ----------
    other_ws = ws + 6 * W
    other_tgt = other_ws + W
    post(A, "/events", ev(f"{other}-a0", other_ws + 100, other))
    post(A, "/events", ev(f"{other}-a1", other_tgt + 1000, other))
    post(B, "/events", ev(f"{other}-b1", other_tgt + 1000, other))
    wait_head(other_tgt, other)
    code, _ = post(R, "/gap-carries",
                   {"key": other, "source_window_start": other_ws,
                    "target_window_start": other_tgt, "side": "a"})
    check("an already-emitted target rejects the carry (409)", code == 409, str(code))

    # ------------------------------------------------------------------
    # 2. open the carry for the real key
    code, resp = post(R, "/gap-carries",
                      {"key": key, "source_window_start": ws,
                       "target_window_start": tgt, "side": "a",
                       "operator": "e2e", "note": "route a2 onward"})
    check("carry opens (201)", code == 201, str(resp))
    cid = resp["carry"]["id"]

    # source window corrected: the leftover left its own gap, visibly carried
    def source_routed():
        p = head(ws, key)["payload"]
        return p.get("carried", {}).get("a") == [f"{key}-a2"] and not p["unmatched_a"]
    wait_for("source payload shows a2 routed out (carried-a, not unmatched)",
             source_routed)
    src = head(ws, key)
    check("source correction reason is CARRY_FORWARD and version bumped",
          src["version"] == 2, str(src["version"]))

    # the events cannot be carried a second time
    code, _ = post(R, "/gap-carries",
                   {"key": key, "source_window_start": ws,
                    "target_window_start": ws + 2 * W, "side": "a",
                    "event_ids": [f"{key}-a2"]})
    check("an event already on an OPEN carry cannot be carried again (409)",
          code == 409, str(code))
    code, _ = post(R, "/gap-carries",
                   {"key": key, "source_window_start": ws,
                    "target_window_start": ws + 2 * W, "side": "a"})
    check("a source with no remaining leftover cannot open another carry (409)",
          code == 409, str(code))

    # open carry is queryable: from where, to where, carrying which events
    c = carry_get(cid)
    check("GET carry shows OPEN, source->target, side and the carried event",
          c["status"] == "OPEN" and c["source_window_start"] == ws
          and c["target_window_start"] == tgt and c["side"] == "a"
          and [i["event_id"] for i in c["items"]] == [f"{key}-a2"]
          and c["items"][0]["item_status"] == "CARRIED", str(c))
    rows = get(R, "/gap-carries", open_only="true")[1]["carries"]
    check("open_only list contains the carry", any(x["id"] == cid for x in rows))
    events = get(R, f"/gap-carries/{cid}/events")[1]["events"]
    check("the open is on the append-only trail",
          events[0]["event"] == "CARRY_OPENED"
          and events[0]["detail"]["events"] == [f"{key}-a2"], str(events[:1]))
    # target has not emitted yet — nothing paired
    check("target still has no result while events are missing",
          head(tgt, key) is None)

    # ------------------------------------------------------------------
    # 3. target closes: native pair + the carry fills its B leftover
    post(A, "/events", ev(f"{key}-a3", tgt + 1000, key))
    post(B, "/events", {"events": [ev(f"{key}-b3", tgt + 1500, key),
                                   ev(f"{key}-b4", tgt + 2500, key)]})
    tv = wait_head(tgt, key)

    def carry_pair(cid_=cid):
        p = head(tgt, key)["payload"]
        return [pp for pp in p["pairs"] if pp.get("carry")
                and pp["carry"]["carry_id"] == cid_]
    cp = wait_for("carry pair appears in the target result", lambda: carry_pair() or None)
    if tv and cp:
        native = [p for p in tv["payload"]["pairs"] if p["carry"] is None]
        check("native pair is the target's own (a3-b3)",
              [(p["a_event_id"], p["b_event_id"]) for p in native]
              == [(f"{key}-a3", f"{key}-b3")], str(native))
        check("carry pair is tagged (a2 from source -> b4), never a native pair",
              [(p["a_event_id"], p["b_event_id"]) for p in cp]
              == [(f"{key}-a2", f"{key}-b4")]
              and cp[0]["carry"]["source_window_start"] == ws, str(cp))
        check("target has no leftovers after the carry",
              tv["payload"]["unmatched_a"] == [] and tv["payload"]["unmatched_b"] == [],
              str(tv["payload"]))
    c = wait_status(cid, "CLOSED")
    if c:
        check("all items MATCHED, frozen snapshot preserves the close-time match",
              c["matched_count"] == 1 and c["items"][0]["item_status"] == "MATCHED"
              and c["items"][0]["event_id"] == f"{key}-a2"
              and c["items"][0]["matched_against"] == f"{key}-b4"
              and c["target_version"] == head(tgt, key)["version"]
              and c["matched_snapshot"]["matches"]
              == [{"event_id": f"{key}-a2", "matched_against": f"{key}-b4"}],
              str(c))
        # GET items must carry the real submitted event id, never the literal
        # column name (a dict-row zip bug wrote field names as values).
        listed = [x for x in get(R, "/gap-carries", key=key)[1]["carries"]
                  if x["id"] == cid][0]
        check("list endpoint also returns the real carried event id (not the field name)",
              [i["event_id"] for i in listed["items"]] == [f"{key}-a2"]
              and {i["item_status"] for i in listed["items"]} == {"MATCHED"},
              str(listed["items"]))
    trail = get(R, f"/gap-carries/{cid}/events")[1]["events"]
    kinds = [e["event"] for e in trail]
    check("trail has ITEM_MATCHED then CARRY_CLOSED",
          "ITEM_MATCHED" in kinds and kinds[-1] == "CARRY_CLOSED", str(kinds))

    # a CLOSED carry's event is history — still cannot be carried again
    code, _ = post(R, "/gap-carries",
                   {"key": key, "source_window_start": ws,
                    "target_window_start": ws + 2 * W, "side": "a"})
    check("a CLOSED carry's event still cannot be carried a second time (409)",
          code == 409, str(code))

    # ------------------------------------------------------------------
    # 4. TARGET correction after close must REOPEN the closed carry
    #    (an A-side event arriving later changes positional pairing; we force
    #     the carry pair to disappear by retracting the B it matched)
    post(B, "/events", retract(f"{key}-rb4", tgt + 3000, key, f"{key}-b4"))
    c = wait_status(cid, "REOPENED")
    if c:
        check("closed carry reopens when the target stops pairing it",
              c["target_version"] is None and c["matched_snapshot"] is None
              and c["items"][0]["item_status"] == "CARRIED", str(c))
    kinds = [e["event"] for e in get(R, f"/gap-carries/{cid}/events")[1]["events"]]
    check("CARRY_REOPENED (target_corrected) is on the trail",
          "CARRY_REOPENED" in kinds, str(kinds))
    check("it must not keep showing matched anywhere",
          all(x["status"] != "CLOSED"
              for x in get(R, "/gap-carries", key=key)[1]["carries"]
              if x["id"] == cid))

    # the carried event comes back into play: a fresh B at the target closes it
    post(B, "/events", ev(f"{key}-b5", tgt + 4000, key))
    c = wait_status(cid, "CLOSED")
    if c:
        check("carry closes again against the new B with a NEW snapshot",
              c["items"][0]["matched_against"] == f"{key}-b5"
              and c["matched_snapshot"]["matches"]
              == [{"event_id": f"{key}-a2", "matched_against": f"{key}-b5"}], str(c))

    # ------------------------------------------------------------------
    # 5. business order integration: wait for the carry then close the order
    o = wait_order(key, "CLOSED")
    if o is not None:
        check("order reaches CLOSED only after the carry resolved",
              o["open_carries"] == 0 and o["gap_windows"] == 0, str(o))
    hist = get(R, "/orders/history", key=key)[1]["versions"]
    check("a CARRY_RESOLVED order version exists",
          any(v["reason"] == "CARRY_RESOLVED" for v in hist),
          str([v["reason"] for v in hist]))

    # ------------------------------------------------------------------
    # 6. SOURCE retraction after (re)close VOIDS — dead forever
    post(A, "/events", retract(f"{key}-ra2", ws + 5000, key, f"{key}-a2"))
    c = wait_status(cid, "VOID")
    if c:
        check("source retraction voids the carry (reason recorded, items DEAD)",
              c["void_reason"] == "source_retracted"
              and all(i["item_status"] == "DEAD" for i in c["items"]), str(c))
        check("voided target loses the carry pair from its result",
              not carry_pair(), str(head(tgt, key)["payload"]))
    code, _ = post(R, "/gap-carries",
                   {"key": key, "source_window_start": ws,
                    "target_window_start": ws + 2 * W, "side": "a",
                    "event_ids": [f"{key}-a2"]})
    check("a VOID carry's event can never be carried again (409)",
          code == 409, str(code))
    rows = get(R, "/gap-carries", status="VOID", key=key)[1]["carries"]
    check("void carries stay on the ledger", any(x["id"] == cid for x in rows))

    # ------------------------------------------------------------------
    # 7. source-paired death: a late opposite event pairs the leftover at the
    #    source window -> its open carry is voided (source_paired).
    ws2 = ws + 4 * W
    tgt2 = ws2 + W
    k2 = f"{key}-p{now}"
    post(A, "/events", ev(f"{k2}-a1", ws2 + 1000, k2))
    wait_head(ws2, k2)
    code, c2resp = post(R, "/gap-carries",
                        {"key": k2, "source_window_start": ws2,
                         "target_window_start": tgt2, "side": "a"})
    check("second carry opens for the new key", code == 201, str(c2resp))
    c2 = c2resp["carry"]["id"]
    # late B pairs a1 AT THE SOURCE
    post(B, "/events", ev(f"{k2}-b1", ws2 + 4000, k2))
    c2r = wait_status(c2, "VOID")
    if c2r:
        check("a carried event paired at the source by a late event voids "
              "the carry (source_paired)",
              c2r["void_reason"] == "source_paired", str(c2r))
        # the target — even once it later closes — must not pair a dead event
        post(A, "/events", ev(f"{k2}-a2", tgt2 + 1000, k2))
        post(B, "/events", ev(f"{k2}-b2", tgt2 + 1000, k2))
        tv2 = wait_head(tgt2, k2)
        tagged = [p for p in tv2["payload"]["pairs"] if p.get("carry")] if tv2 else []
        check("the dead carry never matches in the later target",
              not tagged and tv2["payload"]["match_count"] == 1, str(tv2["payload"]))

    # ------------------------------------------------------------------
    # 8. B-side carry regression: the carried events are B leftovers. This is
    #    the exact failure reported (carry stays OPEN, empty matched_snapshot,
    #    item ids wrong): it must close with a non-empty snapshot carrying the
    #    real submitted event ids on BOTH sides of the pair.
    ws3 = ws + 8 * W
    tgt3 = ws3 + W
    k3 = f"{key}-b{now}"
    # source closes B-long: b1 pairs a1, b2 is the B leftover
    post(A, "/events", ev(f"{k3}-a1", ws3 + 1000, k3))
    post(B, "/events", {"events": [ev(f"{k3}-b1", ws3 + 1500, k3),
                                   ev(f"{k3}-b2", ws3 + 2500, k3)]})
    s3 = wait_head(ws3, k3)
    check("b-side source has one B leftover to carry",
          s3 and s3["payload"]["unmatched_b"] == [f"{k3}-b2"], str(s3))
    code, c3resp = post(R, "/gap-carries",
                        {"key": k3, "source_window_start": ws3,
                         "target_window_start": tgt3, "side": "b"})
    check("b-side carry opens (201)", code == 201, str(c3resp))
    c3 = c3resp["carry"]["id"]
    check("the OPEN carry already carries the real submitted event id",
          [i["event_id"] for i in c3resp["carry"]["items"]] == [f"{k3}-b2"],
          str(c3resp["carry"]))
    # target closes A-long: native a4-b3, the carried b2 fills the A leftover a5
    post(A, "/events", {"events": [ev(f"{k3}-a4", tgt3 + 1000, k3),
                                   ev(f"{k3}-a5", tgt3 + 2500, k3)]})
    post(B, "/events", ev(f"{k3}-b3", tgt3 + 1500, k3))
    tv3 = wait_head(tgt3, k3)
    tagged3 = wait_for("b-side carry pair appears in the target",
                       lambda: [p for p in head(tgt3, k3)["payload"]["pairs"]
                                if p.get("carry")
                                and p["carry"]["carry_id"] == c3] or None)
    if tv3 and tagged3:
        check("b-side carry pairs the real carried B id against the A leftover",
              [(p["a_event_id"], p["b_event_id"]) for p in tagged3]
              == [(f"{k3}-a5", f"{k3}-b2")]
              and tagged3[0]["carry"]["source_window_start"] == ws3, str(tagged3))
    c3f = wait_status(c3, "CLOSED")
    if c3f:
        check("b-side carry CLOSED with a NON-EMPTY snapshot of real ids",
              c3f["target_version"] is not None
              and c3f["matched_snapshot"] is not None
              and c3f["matched_snapshot"]["matches"]
              == [{"event_id": f"{k3}-b2", "matched_against": f"{k3}-a5"}]
              and c3f["items"][0]["matched_against"] == f"{k3}-a5",
              str(c3f))

    print()
    if FAILED:
        print(f"{len(FAILED)} check(s) FAILED: {FAILED}")
        sys.exit(1)
    print("all gap-carry e2e checks passed")


if __name__ == "__main__":
    main()
