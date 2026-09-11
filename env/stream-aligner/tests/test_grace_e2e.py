#!/usr/bin/env python3
"""End-to-end test for per-(window, key) close grace (关窗宽限),
against a running compose stack.

Usage:  docker compose up -d --build && python3 tests/test_grace_e2e.py

Covers:
- granting a grace holds exactly one (window, key) past its ordinary close —
  even once both sides would otherwise cross — while another key's window
  keeps its original window end and is never blocked;
- before the deadline nothing comes out (宽限没到点，这一窗就先别出);
- at the deadline the THEN-current version fires: a late opposite-side event
  arriving during the wait is paired into it (到期出的就是当时那一版);
- still one-sided at the deadline -> the one-sided result fires anyway, no
  more waiting; a retraction during the wait shapes that same version;
- an emitted window can never be graced again (409); an ACTIVE grace can
  never be stacked (409); a window with no data is 404; a non-boundary
  window_start is 422;
- a grace that elapsed with all data retracted away is EXPIRED (no result),
  and once data returns the window can be graced again;
- GET /window-graces answers which windows are held, their deadlines and
  which key they belong to.
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


def head(ws, key):
    return get(R, "/results/current", window_start=ws, key=key)[1]["result"]


def grace_rows(**params):
    return get(R, "/window-graces", **params)[1]["graces"]


def find_grace(ws, key, status=None):
    rows = grace_rows(window_start=ws, key=key)
    if status is not None:
        rows = [g for g in rows if g["status"] == status]
    return rows[0] if rows else None


def main():
    now = int(time.time() * 1000)
    ws = now // W * W
    hold = f"gr-hold-{now}"   # granted a grace, B arrives during it
    pal = f"gr-pal-{now}"    # no grace: closes normally, never blocked

    # --- validation: no data yet / non-boundary start ----------------------
    code, _ = post(R, "/window-graces",
                   {"window_start": ws, "key": hold, "extra_ms": 2000})
    check("grace on a window with no data is 404", code == 404, str(code))
    code, _ = post(R, "/window-graces",
                   {"window_start": ws + 1, "key": hold, "extra_ms": 2000})
    check("grace with a non-boundary window_start is 422", code == 422, str(code))

    # --- grant: A-side data only for hold, both sides for pal --------------
    post(A, "/events", {"events": [ev(f"{hold}-a1", ws + 1000, hold),
                                   ev(f"{pal}-a1", ws + 1000, pal)]})
    post(B, "/events", ev(f"{pal}-b1", ws + 1500, pal))

    code, resp = post(R, "/window-graces",
                      {"window_start": ws, "key": hold,
                       "extra_ms": 2200, "operator": "e2e", "note": "wait for B"})
    check("first grace is 201", code == 201, str(resp))
    gid = resp["grace"]["id"]
    due_at = resp["grace"]["due_at"]

    # --- no stacking --------------------------------------------------------
    code, resp = post(R, "/window-graces",
                      {"window_start": ws, "key": hold, "extra_ms": 5000})
    check("stacking a second ACTIVE grace is 409", code == 409, str(code))

    # --- before the deadline: held, queryable ------------------------------
    check("head does not exist while the grace is active", head(ws, hold) is None)
    g = find_grace(ws, hold, "ACTIVE")
    check("GET shows the ACTIVE grace, its deadline and its key",
          g is not None and g["id"] == gid and g["due_at"] == due_at
          and g["key"] == hold, str(g))
    rows = grace_rows(active_only=True)
    check("active_only list contains the held window",
          any(x["id"] == gid for x in rows), str(len(rows)))
    wins = [w for w in get(R, "/windows")[1]["windows"]
            if w["window_start"] == ws and w["key"] == hold][0]
    check("/windows flags the held pair (grace_active, not closed_effective)",
          wins["grace_active"] and wins["grace_due_at"] is not None
          and wins["head_version"] is None and not wins["closed_effective"],
          str(wins))

    # --- the other key is unaffected: closes on its own window end ---------
    palv = wait_for("pal emits on its own without waiting for the graced key",
                    lambda: head(ws, pal))
    if palv:
        check("pal result is the matched pair",
              palv["version"] == 1
              and [(p["a_event_id"], p["b_event_id"]) for p in palv["payload"]["pairs"]]
              == [(f"{pal}-a1", f"{pal}-b1")], str(palv["payload"]))
    # already-emitted pal can never be graced
    code, _ = post(R, "/window-graces",
                   {"window_start": ws, "key": pal, "extra_ms": 1000})
    check("gracing an already-emitted window is 409", code == 409, str(code))

    # the graced window stays held past pal's close
    time.sleep(1.0)
    check("hold still not emitted before its deadline", head(ws, hold) is None)

    # --- late opposite side during the grace -> included in the v1 ---------
    post(B, "/events", ev(f"{hold}-b1", ws + 2000, hold))
    time.sleep(0.5)  # plenty of ticks, but the deadline is not there yet
    check("late B arriving during the grace does NOT release it early",
          head(ws, hold) is None)
    v = wait_for("at the deadline the THEN-current version fires (now matched)",
                 lambda: head(ws, hold), timeout=10)
    if v:
        check("deadline version is v1 carrying the late B event",
              v["version"] == 1
              and [(p["a_event_id"], p["b_event_id"]) for p in v["payload"]["pairs"]]
              == [(f"{hold}-a1", f"{hold}-b1")], str(v["payload"]))
    g = find_grace(ws, hold, "FIRED")
    check("the grace is FIRED with that version",
          g is not None and g["fired_version"] == 1 and g["finished_at"], str(g))
    audit = get(R, "/audit", window_start=ws, key=hold)[1]["audit"]
    check("the INITIAL audit records the grace deadline firing",
          audit and audit[0]["reason"] == "INITIAL"
          and audit[0]["detail"].get("grace", {}).get("fired_at_deadline") is True,
          str(audit[:1]))
    # fired windows can't be graced again either
    code, _ = post(R, "/window-graces",
                   {"window_start": ws, "key": hold, "extra_ms": 1000})
    check("re-gracing a fired/emitted window is 409", code == 409, str(code))

    # --- one-sided deadline: it fires one-sided, no more waiting -----------
    ws2 = ws + 3 * W
    one = f"gr-one-{now}"
    post(A, "/events", ev(f"{one}-a1", ws2 + 1000, one))
    post(R, "/window-graces",
         {"window_start": ws2, "key": one, "extra_ms": 1500})
    check("one-sided grace window starts held", head(ws2, one) is None)
    v = wait_for("one-sided window fires at its deadline anyway",
                 lambda: head(ws2, one), timeout=10)
    if v:
        check("the deadline result is one-sided (A unmatched)",
              v["payload"]["match_count"] == 0
              and v["payload"]["unmatched_a"] == [f"{one}-a1"]
              and v["payload"]["unmatched_b"] == [], str(v["payload"]))
    check("the one-sided grace is FIRED",
          find_grace(ws2, one, "FIRED") is not None)

    # --- retraction during the wait shapes the version at the deadline -----
    ws3 = ws + 6 * W
    rt = f"gr-retract-{now}"
    post(A, "/events", ev(f"{rt}-a1", ws3 + 1000, rt))
    post(R, "/window-graces",
         {"window_start": ws3, "key": rt, "extra_ms": 2000})
    post(A, "/events", {"event_id": f"{rt}-r1", "event_time": ws3 + 1500,
                        "key": rt, "type": "retract", "retracts": f"{rt}-a1"})
    # nothing at the deadline: everything was retracted while held
    time.sleep(3.0)
    check("no result when all data was retracted before the deadline",
          head(ws3, rt) is None)
    g = find_grace(ws3, rt, "EXPIRED")
    check("the grace is EXPIRED (lapsed, no result)", g is not None, str(g))
    # data returns after expiry -> ordinary waiting, and a fresh grace is legal
    post(A, "/events", ev(f"{rt}-a2", ws3 + 2000, rt))
    code, resp = post(R, "/window-graces",
                      {"window_start": ws3, "key": rt, "extra_ms": 1500})
    check("an EXPIRED window with new data can be graced again",
          code == 201, str(resp))
    check("still held under the new grace", head(ws3, rt) is None)
    v = wait_for("the re-granted window fires at its new deadline",
                 lambda: head(ws3, rt), timeout=10)
    if v:
        check("the fired version only contains the new event (old one retracted)",
              v["payload"]["unmatched_a"] == [f"{rt}-a2"], str(v["payload"]))
    check("second grace is FIRED", find_grace(ws3, rt, "FIRED") is not None)

    print()
    if FAILED:
        print(f"{len(FAILED)} check(s) FAILED: {FAILED}")
        sys.exit(1)
    print("all close-grace e2e checks passed")


if __name__ == "__main__":
    main()
