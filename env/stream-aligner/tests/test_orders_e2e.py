#!/usr/bin/env python3
"""End-to-end test for business orders (业务单) against a running compose stack.

Usage:  docker compose up -d --build && python3 tests/test_orders_e2e.py

Covers the order lifecycle on top of the aligned window results: multi-window
assembly (OPEN while windows are pending), one-sided gaps (WAITING), close
once every window is in and matched (CLOSED, with a full per-window snapshot),
corrections after close (REOPENED, recording which window/result version
triggered it), full withdrawal (VOID — never a success order), revival
continuing the *same* order (a voided order never masquerades as a new one),
eventless middle windows, and late earlier windows.

Watermarks are driven by overrides so the scenario is deterministic.
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


def ev(eid, t, key, typ="upsert", retracts=None, payload=None):
    e = {"event_id": eid, "event_time": t, "key": key, "type": typ, "payload": payload or {}}
    if retracts:
        e["retracts"] = retracts
    return e


def order(key):
    return get(R, "/orders/current", key=key)["order"]


def versions(key, status=None):
    vs = get(R, "/orders/history", key=key)["versions"]
    return [v for v in vs if status is None or v["status"] == status]


def snapshot_versions(order_obj):
    return {w["window_start"]: w["result_version"] for w in order_obj["windows"]}


def main():
    now = int(time.time() * 1000)
    ws0 = now // W * W
    ws1, ws2 = ws0 + W, ws0 + 2 * W
    tag = f"ord-{now}"
    k1, k2, k3 = f"{tag}-full", f"{tag}-hole", f"{tag}-late"

    def set_wm(value):
        post(A, "/watermark/override", {"watermark": value})
        post(B, "/watermark/override", {"watermark": value})

    def retract(stream, eid, target, key):
        post(stream, "/events", ev(eid, int(time.time() * 1000), key,
                                   typ="retract", retracts=target))

    # =========================================================================
    # K1: full lifecycle OPEN -> WAITING -> CLOSED -> REOPENED -> CLOSED
    #     -> VOID -> revived (same order, never a new one)
    # =========================================================================
    post(A, "/events", {"events": [ev(f"{k1}-a1", ws0 + 1000, k1),
                                   ev(f"{k1}-a2", ws1 + 1000, k1)]})
    post(B, "/events", ev(f"{k1}-b1", ws0 + 1500, k1))
    post(A, "/events", {"events": [ev(f"{k3}-a1", ws1 + 1000, k3)]})
    post(B, "/events", {"events": [ev(f"{k3}-b1", ws1 + 1500, k3)]})

    # -- only ws0 due: K1 opens with the second window still pending ----------
    set_wm(ws1)
    o = wait_for("K1 order opens once the first window closes", lambda: order(k1))
    if o:
        check("order is OPEN while a known business window is still pending",
              o["status"] == "OPEN" and o["pending_windows"] == [ws1], str(o))
    h = get(R, "/orders/history", key=k1)
    check("first version is ORDER_OPENED",
          h["versions"] and h["versions"][0]["version"] == 1
          and h["versions"][0]["reason"] == "ORDER_OPENED", str(h["versions"][:1]))
    check("no order before its first window result",
          order(k3) is None)

    # -- ws1 due but single-sided: WAITING (长期只有单边) ----------------------
    set_wm(ws2)
    o = wait_for("one-sided window keeps the order WAITING",
                 lambda: (x := order(k1)) and x["status"] == "WAITING" and x)
    if o:
        gaps = [w["window_start"] for w in o["windows"] if w["has_gap"]]
        check("the one-sided window is the gap", gaps == [ws1], str(o["windows"]))
    o3 = wait_for("single matched window closes K3 directly",
                  lambda: (x := order(k3)) and x["status"] == "CLOSED" and x)

    # -- late B event completes ws1: CLOSED, snapshot records each version ----
    post(B, "/events", ev(f"{k1}-b2", ws1 + 1500, k1))
    o = wait_for("all windows matched -> CLOSED",
                 lambda: (x := order(k1)) and x["status"] == "CLOSED" and x)
    if o:
        check("each window entered at its current result version",
              snapshot_versions(o) == {ws0: 1, ws1: 2}, str(o["windows"]))
    closed = versions(k1, "CLOSED")
    if closed:
        snap = {w["window_start"]: w["result_version"] for w in closed[-1]["snapshot"]}
        check("close version keeps a full-order snapshot of window versions",
              snap == {ws0: 1, ws1: 2}, str(closed[-1]["snapshot"]))

    # -- correction after close: REOPENED, trigger recorded -------------------
    retract(A, f"{k1}-r1", f"{k1}-a1", k1)
    o = wait_for("correction after close reopens the order",
                 lambda: (x := order(k1)) and x["status"] == "REOPENED" and x)
    reopened = versions(k1, "REOPENED")
    if reopened:
        v = reopened[0]
        check("reopen version names the triggering window and result version",
              v["reason"] == "WINDOW_CORRECTED" and v["trigger_window_start"] == ws0
              and v["trigger_result_version"] == 2, str(v))

    # -- gap repaired: CLOSED again -------------------------------------------
    post(A, "/events", ev(f"{k1}-a3", ws0 + 500, k1))
    wait_for("order closes again once the gap is repaired",
             lambda: (x := order(k1)) and x["status"] == "CLOSED" and x)

    # -- full withdrawal: VOID, not a success ---------------------------------
    retract(A, f"{k1}-r2", f"{k1}-a3", k1)
    retract(A, f"{k1}-r3", f"{k1}-a2", k1)
    retract(B, f"{k1}-r4", f"{k1}-b1", k1)
    retract(B, f"{k1}-r5", f"{k1}-b2", k1)
    o = wait_for("withdrawing the whole business voids the order",
                 lambda: (x := order(k1)) and x["status"] == "VOID" and x)
    voided = versions(k1, "VOID")
    if voided:
        v = voided[-1]
        check("void version explains itself (withdrawal + trigger window/version)",
              v["reason"] == "WINDOW_WITHDRAWN"
              and v["trigger_window_start"] in (ws0, ws1)
              and v["trigger_result_version"] > 0, str(v))
    closed_keys = {x["key"] for x in get(R, "/orders", status="CLOSED")["orders"]}
    void_keys = {x["key"] for x in get(R, "/orders", status="VOID")["orders"]}
    check("a voided order never counts as a success order",
          k1 not in closed_keys and k1 in void_keys,
          f"closed={closed_keys} void={void_keys}")
    first_close = versions(k1, "CLOSED")[0]
    snap0 = {w["window_start"]: w["result_version"] for w in first_close["snapshot"]}
    check("the original close snapshot survives reopen and void untouched",
          snap0 == {ws0: 1, ws1: 2}, str(first_close["snapshot"]))

    # -- revival continues the SAME order; a voided order can't pose as new ---
    order_id = order(k1)["id"]
    void_head = order(k1)["head_version"]
    post(A, "/events", ev(f"{k1}-a4", ws0 + 800, k1))
    post(B, "/events", ev(f"{k1}-b4", ws0 + 1200, k1))
    o = wait_for("new results after void revive the same order",
                 lambda: (x := order(k1)) and x["status"] == "CLOSED" and x)
    if o:
        check("revival keeps the same order id and continues its version chain",
              o["id"] == order_id and o["head_version"] > void_head,
              f"id={o['id']} head={o['head_version']}")
        check("void chapter stays in the order's history",
              bool(versions(k1, "VOID")) and versions(k1)[0]["version"] == 1)
    all_k1 = [x for x in get(R, "/orders", key=k1)["orders"]]
    check("one order per business key, always", len(all_k1) == 1, str(all_k1))

    # =========================================================================
    # K2: eventless middle window must not wedge or flap the order (中间缺窗)
    # =========================================================================
    post(A, "/events", {"events": [ev(f"{k2}-a1", ws0 + 1000, k2),
                                   ev(f"{k2}-a2", ws2 + 1000, k2)]})
    post(B, "/events", {"events": [ev(f"{k2}-b1", ws0 + 1500, k2),
                                   ev(f"{k2}-b2", ws2 + 1500, k2)]})
    o = wait_for("K2 opens with the far window pending",
                 lambda: (x := order(k2)) and x["status"] == "OPEN"
                 and x["pending_windows"] == [ws2] and x)
    set_wm(ws0 + 3 * W)
    o = wait_for("eventless middle window does not block closing",
                 lambda: (x := order(k2)) and x["status"] == "CLOSED" and x)
    if o:
        check("order spans the hole with exactly its two real windows",
              sorted(snapshot_versions(o)) == [ws0, ws2]
              and o["missing_windows"] == [] and o["pending_windows"] == [],
              str(o["windows"]))

    # =========================================================================
    # K3: earlier window arrives after the later one already closed (前窗后到)
    # =========================================================================
    post(A, "/events", ev(f"{k3}-a2", ws0 + 600, k3))
    post(B, "/events", ev(f"{k3}-b2", ws0 + 900, k3))
    o = wait_for("late earlier window joins the closed order",
                 lambda: (x := order(k3)) and x["head_version"] >= 2 and x)
    if o:
        check("order re-closes as a new version spanning both windows",
              o["status"] == "CLOSED" and sorted(snapshot_versions(o)) == [ws0, ws1],
              str(o["windows"]))
    h3 = versions(k3)
    check("the late join is audited as WINDOW_JOINED with its trigger",
          len(h3) >= 2 and h3[1]["reason"] == "WINDOW_JOINED"
          and h3[1]["trigger_window_start"] == ws0
          and h3[1]["trigger_result_version"] == 1, str(h3))

    # -- every window bound to exactly one order ------------------------------
    for key in (k1, k2, k3):
        o = order(key)
        wss = [w["window_start"] for w in o["windows"]]
        check(f"windows of {key} bound to exactly one order, no duplicates",
              len(wss) == len(set(wss)), str(wss))

    set_wm(None)  # release the overrides
    print()
    if FAILED:
        print(f"{len(FAILED)} check(s) FAILED: {FAILED}")
        sys.exit(1)
    print("all order e2e checks passed")


if __name__ == "__main__":
    main()
