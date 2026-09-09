"""Alignment service.

Continuously pulls both ingest streams, maintains a per-stream watermark, and
emits aligned business results per (window, key) once *both* watermarks have
passed the window end. Late events and retractions recompute already-emitted
windows and produce new, fully audited versions — old versions are kept, never
overwritten. All recomputation is idempotent: identical content never yields a
new version, so replays and restarts cannot double-count.

Every emitted version is also fanned out to registered downstreams via a
transactional outbox: the delivery rows are inserted in the same transaction
as the result version, so a version can never exist without its deliveries.
A dispatcher pushes deliveries to each subscriber in per-result version order
(a later version is never sent before an earlier one), retries failures with
exponential backoff, and never gives up — nothing is silently dropped.
"""
import json
import logging
import os
import threading
import time
import urllib.parse
import urllib.request
from contextlib import asynccontextmanager
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from app.core import compute_payload, decide, delivery_kind, retry_delay_ms, window_of

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("aligner")

DSN = os.environ["DATABASE_DSN"]
INGESTS = {
    "a": os.environ.get("INGEST_A_URL", "http://localhost:8001"),
    "b": os.environ.get("INGEST_B_URL", "http://localhost:8002"),
}
WINDOW_MS = int(os.environ.get("WINDOW_SIZE_MS", "60000"))
POLL_MS = int(os.environ.get("POLL_INTERVAL_MS", "1000"))
BATCH = int(os.environ.get("PULL_BATCH_SIZE", "500"))
DELIVERY_POLL_MS = int(os.environ.get("DELIVERY_POLL_MS", "1000"))
DELIVERY_TIMEOUT_MS = int(os.environ.get("DELIVERY_TIMEOUT_MS", "5000"))
RETRY_BASE_MS = int(os.environ.get("DELIVERY_RETRY_BASE_MS", "2000"))
RETRY_MAX_MS = int(os.environ.get("DELIVERY_RETRY_MAX_MS", "60000"))
DISPATCH_BATCH = int(os.environ.get("DELIVERY_DISPATCH_BATCH", "100"))

DDL = """
CREATE TABLE IF NOT EXISTS stream_events (
    stream     TEXT NOT NULL,
    event_id   TEXT NOT NULL,
    event_time BIGINT NOT NULL,
    key        TEXT NOT NULL,
    type       TEXT NOT NULL,
    retracts   TEXT,
    payload    JSONB,
    seq        BIGINT NOT NULL,
    PRIMARY KEY (stream, event_id)
);
CREATE INDEX IF NOT EXISTS stream_events_window_idx ON stream_events (key, event_time);
CREATE TABLE IF NOT EXISTS offsets (
    stream   TEXT PRIMARY KEY,
    last_seq BIGINT NOT NULL DEFAULT 0
);
INSERT INTO offsets (stream, last_seq) VALUES ('a', 0), ('b', 0) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS watermarks (
    stream     TEXT PRIMARY KEY,
    watermark  BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS watermark_log (
    id            BIGSERIAL PRIMARY KEY,
    stream        TEXT NOT NULL,
    old_watermark BIGINT,
    new_watermark BIGINT,
    direction     TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS results (
    id           BIGSERIAL PRIMARY KEY,
    window_start BIGINT NOT NULL,
    window_end   BIGINT NOT NULL,
    key          TEXT NOT NULL,
    version      INT NOT NULL,
    payload      JSONB,
    payload_hash TEXT,
    status       TEXT NOT NULL CHECK (status IN ('CURRENT', 'SUPERSEDED', 'RETRACTED')),
    reason       TEXT NOT NULL CHECK (reason IN ('INITIAL', 'LATE_EVENT', 'RETRACTION')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (window_start, key, version)
);
CREATE TABLE IF NOT EXISTS audit (
    id           BIGSERIAL PRIMARY KEY,
    window_start BIGINT NOT NULL,
    window_end   BIGINT NOT NULL,
    key          TEXT NOT NULL,
    from_version INT,
    to_version   INT NOT NULL,
    reason       TEXT NOT NULL,
    detail       JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Delivery outbox. One row per (subscriber, result version); rows are created
-- in the same transaction as the result version itself, so an emitted version
-- can never be missing its deliveries. The dispatcher only ever *updates*
-- these rows, so a crash mid-delivery is recovered by simply retrying.
CREATE TABLE IF NOT EXISTS subscribers (
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    url        TEXT NOT NULL,
    active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS deliveries (
    id              BIGSERIAL PRIMARY KEY,
    subscriber_id   BIGINT NOT NULL REFERENCES subscribers(id),
    window_start    BIGINT NOT NULL,
    window_end      BIGINT NOT NULL,
    key             TEXT NOT NULL,
    version         INT NOT NULL,
    kind            TEXT NOT NULL CHECK (kind IN ('NEW', 'CORRECTION', 'WITHDRAWAL')),
    payload         JSONB,           -- result snapshot as of this version
    status          TEXT NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'RETRYING', 'DELIVERED')),
    attempts        INT NOT NULL DEFAULT 0,
    last_error      TEXT,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_attempt_at TIMESTAMPTZ,
    delivered_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (subscriber_id, window_start, key, version)
);
CREATE INDEX IF NOT EXISTS deliveries_due_idx
    ON deliveries (next_attempt_at) WHERE status <> 'DELIVERED';
"""


def connect(retries=60, delay=1.0):
    for attempt in range(retries):
        try:
            return psycopg2.connect(DSN)
        except psycopg2.OperationalError:
            if attempt == retries - 1:
                raise
            time.sleep(delay)


def http_get(url, params=None, timeout=10):
    if params:
        url += "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read())


def http_post(url, body, timeout=5):
    """POST a JSON body; raises unless the endpoint answers 2xx."""
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        resp.read()


# ---------------------------------------------------------------------------
# core state machine
# ---------------------------------------------------------------------------

def get_head(cur, window_start, key):
    cur.execute(
        """SELECT id, version, status, payload_hash FROM results
           WHERE window_start = %s AND key = %s ORDER BY version DESC LIMIT 1""",
        (window_start, key),
    )
    row = cur.fetchone()
    return {"id": row[0], "version": row[1], "status": row[2], "payload_hash": row[3]} if row else None


def build_payload(cur, window_start, key):
    """Recompute the result for (window, key) from the stored events.

    Retractions are applied via NOT EXISTS over the whole stream, so a retract
    event landing in a *different* window still filters its target.
    """
    window_end = window_start + WINDOW_MS
    cur.execute(
        """SELECT e.stream, e.event_id, e.event_time, e.payload
           FROM stream_events e
           WHERE e.type = 'upsert' AND e.key = %s
             AND e.event_time >= %s AND e.event_time < %s
             AND NOT EXISTS (
                 SELECT 1 FROM stream_events r
                 WHERE r.type = 'retract' AND r.stream = e.stream AND r.retracts = e.event_id
             )""",
        (key, window_start, window_end),
    )
    by_stream = {"a": [], "b": []}
    for stream, event_id, event_time, payload in cur.fetchall():
        by_stream[stream].append(
            {"event_id": event_id, "event_time": event_time, "payload": payload}
        )
    return compute_payload(key, window_start, window_end, by_stream["a"], by_stream["b"])


def emit(conn, window_start, key, reason, detail):
    """Recompute (window, key) and persist a new version iff the content changed.

    The version row, its audit entry and one outbox delivery per subscriber
    are written in a single transaction: a version never exists without its
    audit trail, and never without the deliveries that push it downstream.
    """
    window_end = window_start + WINDOW_MS
    with conn, conn.cursor() as cur:
        payload = build_payload(cur, window_start, key)
        head = get_head(cur, window_start, key)
        nxt = decide(head, payload)
        if nxt is None:
            return False
        if head is not None:
            cur.execute("UPDATE results SET status = 'SUPERSEDED' WHERE id = %s", (head["id"],))
        cur.execute(
            """INSERT INTO results (window_start, window_end, key, version, payload,
                                    payload_hash, status, reason)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (window_start, window_end, key, nxt["version"],
             psycopg2.extras.Json(payload) if payload is not None else None,
             nxt["payload_hash"], nxt["status"], reason),
        )
        cur.execute(
            """INSERT INTO audit (window_start, window_end, key, from_version, to_version, reason, detail)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (window_start, window_end, key,
             head["version"] if head else None, nxt["version"], reason,
             psycopg2.extras.Json(detail) if detail is not None else None),
        )
        # Outbox fan-out. kind tells the downstream how to book this version:
        # NEW = first sight of the result, CORRECTION = amend the same
        # (window, key) booking (never a new success), WITHDRAWAL = reverse it.
        kind = delivery_kind(reason, nxt["status"])
        cur.execute(
            """INSERT INTO deliveries (subscriber_id, window_start, window_end, key,
                                       version, kind, payload)
               SELECT s.id, %s, %s, %s, %s, %s, %s FROM subscribers s
               ON CONFLICT (subscriber_id, window_start, key, version) DO NOTHING""",
            (window_start, window_end, key, nxt["version"], kind,
             psycopg2.extras.Json(payload) if payload is not None else None),
        )
    log.info("window=%d key=%s -> v%d (%s, %s)", window_start, key, nxt["version"],
             nxt["status"], reason)
    return True


def pull_stream(conn, stream, base_url):
    """Fetch new events from one ingest service and store them idempotently.

    The offset is committed in the same transaction as the inserts, so a crash
    mid-batch replays at most the uncommitted tail, and the PRIMARY KEY makes
    that replay a no-op. Returns a list of (window_start, key, reason, detail)
    for windows that may already have an emitted result.
    """
    with conn, conn.cursor() as cur:
        cur.execute("SELECT last_seq FROM offsets WHERE stream = %s", (stream,))
        last = cur.fetchone()[0]

    dirty = []
    while True:
        batch = http_get(f"{base_url}/events", {"after_seq": last, "limit": BATCH})["events"]
        if not batch:
            break
        with conn, conn.cursor() as cur:
            for ev in batch:
                cur.execute(
                    """INSERT INTO stream_events (stream, event_id, event_time, key, type, retracts, payload, seq)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (stream, event_id) DO NOTHING""",
                    (stream, ev["event_id"], ev["event_time"], ev["key"], ev["type"],
                     ev.get("retracts"),
                     psycopg2.extras.Json(ev["payload"]) if ev.get("payload") is not None else None,
                     ev["seq"]),
                )
                if cur.rowcount == 0:
                    continue  # replayed row, already applied
                if ev["type"] == "upsert":
                    ws, _ = window_of(ev["event_time"], WINDOW_MS)
                    dirty.append((ws, ev["key"], "LATE_EVENT",
                                  {"stream": stream, "event_id": ev["event_id"],
                                   "event_time": ev["event_time"]}))
                else:  # retract: the *target's* window is the one affected
                    cur.execute(
                        "SELECT event_time, key FROM stream_events WHERE stream = %s AND event_id = %s",
                        (stream, ev["retracts"]),
                    )
                    target = cur.fetchone()
                    if target:
                        ws, _ = window_of(target[0], WINDOW_MS)
                        dirty.append((ws, target[1], "RETRACTION",
                                      {"stream": stream, "retract_event_id": ev["event_id"],
                                       "retracted_event_id": ev["retracts"]}))
                    # else: target not arrived yet; when it does, the retract is
                    # already stored and the NOT EXISTS filter excludes it.
            cur.execute("UPDATE offsets SET last_seq = %s WHERE stream = %s",
                        (batch[-1]["seq"], stream))
        last = batch[-1]["seq"]
        if len(batch) < BATCH:
            break
    return dirty


def refresh_watermarks(conn):
    for stream, base_url in INGESTS.items():
        info = http_get(f"{base_url}/watermark")
        new_wm = info["watermark"]
        with conn, conn.cursor() as cur:
            cur.execute("SELECT watermark FROM watermarks WHERE stream = %s", (stream,))
            row = cur.fetchone()
            old_wm = row[0] if row else None
            if old_wm == new_wm:
                continue
            if old_wm is not None and (new_wm is None or new_wm < old_wm):
                direction = "regress"
                log.warning("stream %s watermark regressed: %s -> %s", stream, old_wm, new_wm)
            else:
                direction = "advance"
            cur.execute(
                "INSERT INTO watermark_log (stream, old_watermark, new_watermark, direction) VALUES (%s, %s, %s, %s)",
                (stream, old_wm, new_wm, direction),
            )
            cur.execute(
                """INSERT INTO watermarks (stream, watermark) VALUES (%s, %s)
                   ON CONFLICT (stream) DO UPDATE SET watermark = EXCLUDED.watermark,
                                                      updated_at = now()""",
                (stream, new_wm),
            )


def current_watermarks(cur):
    cur.execute("SELECT stream, watermark FROM watermarks")
    return {stream: wm for stream, wm in cur.fetchall()}


def close_windows(conn):
    """Emit INITIAL results for windows both watermarks have passed."""
    with conn.cursor() as cur:
        wms = current_watermarks(cur)
        if len(wms) < len(INGESTS) or any(w is None for w in wms.values()):
            return
        min_wm = min(wms.values())
        cur.execute(
            """SELECT DISTINCT key, (event_time / %s) * %s AS ws
               FROM stream_events WHERE type = 'upsert'""",
            (WINDOW_MS, WINDOW_MS),
        )
        candidates = cur.fetchall()
    for key, ws in candidates:
        if ws + WINDOW_MS > min_wm:
            continue
        with conn.cursor() as cur:
            head = get_head(cur, ws, key)
        if head is None:
            emit(conn, ws, key, "INITIAL", {"min_watermark": min_wm})


def tick(conn):
    dirty = []
    for stream, base_url in INGESTS.items():
        dirty.extend(pull_stream(conn, stream, base_url))
    refresh_watermarks(conn)
    # Late data first: recompute only windows that already have a result —
    # windows not yet emitted will be covered by close_windows below.
    seen = set()
    for ws, key, reason, detail in dirty:
        if (ws, key, reason) in seen:
            continue
        seen.add((ws, key, reason))
        with conn.cursor() as cur:
            head = get_head(cur, ws, key)
        if head is not None:
            emit(conn, ws, key, reason, detail)
    close_windows(conn)


# ---------------------------------------------------------------------------
# delivery dispatcher (transactional outbox)
# ---------------------------------------------------------------------------

# A delivery is due only when every earlier version of the *same result* for
# the *same subscriber* is DELIVERED — version N+1 must never reach a
# downstream before version N. Different results proceed independently.
DUE_SQL = """
SELECT d.id, d.subscriber_id, s.name AS subscriber, s.url,
       d.window_start, d.window_end, d.key, d.version, d.kind, d.payload,
       d.attempts, d.created_at
FROM deliveries d
JOIN subscribers s ON s.id = d.subscriber_id
WHERE s.active
  AND d.status IN ('PENDING', 'RETRYING')
  AND d.next_attempt_at <= now()
  AND NOT EXISTS (
      SELECT 1 FROM deliveries p
      WHERE p.subscriber_id = d.subscriber_id
        AND p.window_start = d.window_start
        AND p.key = d.key
        AND p.version < d.version
        AND p.status <> 'DELIVERED'
  )
ORDER BY d.id
LIMIT %s
"""


def deliver_one(conn, row):
    """Attempt one delivery; record the outcome on the outbox row.

    At-least-once by design: a lost response after a successful receive yields
    a duplicate, which the downstream dedups by delivery_id / version. A
    failure only ever schedules another attempt — rows are never deleted, so
    nothing is dropped.
    """
    envelope = {
        "delivery_id": row["id"],
        "kind": row["kind"],
        "window_start": row["window_start"],
        "window_end": row["window_end"],
        "key": row["key"],
        "version": row["version"],
        "payload": row["payload"],
        "emitted_at": row["created_at"].isoformat() if row["created_at"] else None,
    }
    attempts = row["attempts"] + 1
    try:
        http_post(row["url"], envelope, timeout=DELIVERY_TIMEOUT_MS / 1000.0)
    except Exception as exc:
        delay = retry_delay_ms(attempts, RETRY_BASE_MS, RETRY_MAX_MS)
        err = f"{type(exc).__name__}: {exc}"[:500]
        with conn, conn.cursor() as cur:
            cur.execute(
                """UPDATE deliveries
                   SET status = 'RETRYING', attempts = %s, last_attempt_at = now(),
                       next_attempt_at = now() + %s * INTERVAL '1 millisecond',
                       last_error = %s
                   WHERE id = %s""",
                (attempts, delay, err, row["id"]),
            )
        log.warning("delivery %d (%s %s v%d) to %s failed (%s); retry %d in %dms",
                    row["id"], row["kind"], row["key"], row["version"],
                    row["subscriber"], err, attempts, delay)
        return False
    with conn, conn.cursor() as cur:
        cur.execute(
            """UPDATE deliveries
               SET status = 'DELIVERED', attempts = %s, last_attempt_at = now(),
                   delivered_at = now(), last_error = NULL
               WHERE id = %s""",
            (attempts, row["id"]),
        )
    log.info("delivery %d (%s %s v%d) to %s ok (attempt %d)",
             row["id"], row["kind"], row["key"], row["version"],
             row["subscriber"], attempts)
    return True


def dispatch_due(conn):
    """Deliver everything currently due, draining version chains in one pass."""
    for _ in range(20):  # a success may make the next version eligible
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(DUE_SQL, (DISPATCH_BATCH,))
            rows = cur.fetchall()
        if not rows:
            return
        progressed = False
        for row in rows:
            if deliver_one(conn, row):
                progressed = True
        if not progressed:
            return


def delivery_loop():
    log.info("delivery dispatcher started (poll=%dms timeout=%dms backoff=%d..%dms)",
             DELIVERY_POLL_MS, DELIVERY_TIMEOUT_MS, RETRY_BASE_MS, RETRY_MAX_MS)
    while not _stop.is_set():
        try:
            conn = connect()
            try:
                dispatch_due(conn)
            finally:
                conn.close()
        except Exception:
            log.exception("dispatch failed; will retry")
        _stop.wait(DELIVERY_POLL_MS / 1000.0)


# ---------------------------------------------------------------------------
# service plumbing
# ---------------------------------------------------------------------------

_stop = threading.Event()


def loop():
    log.info("aligner loop started (window=%dms poll=%dms)", WINDOW_MS, POLL_MS)
    while not _stop.is_set():
        try:
            conn = connect()
            try:
                tick(conn)
            finally:
                conn.close()
        except Exception:
            log.exception("tick failed; will retry")
        _stop.wait(POLL_MS / 1000.0)


@asynccontextmanager
async def lifespan(app):
    conn = connect()
    with conn, conn.cursor() as cur:
        cur.execute(DDL)
    conn.close()
    t = threading.Thread(target=loop, daemon=True)
    t.start()
    d = threading.Thread(target=delivery_loop, daemon=True)
    d.start()
    yield
    _stop.set()
    t.join(timeout=5)
    d.join(timeout=5)


app = FastAPI(title="aligner", lifespan=lifespan)


@app.get("/healthz")
def healthz():
    conn = connect(retries=1)
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
    finally:
        conn.close()
    return {"status": "ok"}


@app.get("/watermarks")
def watermarks():
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            wms = current_watermarks(cur)
    finally:
        conn.close()
    ready = len(wms) == len(INGESTS) and all(w is not None for w in wms.values())
    return {
        "streams": wms,
        "min_watermark": min(wms.values()) if ready else None,
        "window_size_ms": WINDOW_MS,
    }


@app.get("/watermarks/history")
def watermark_history(stream: Optional[str] = None, limit: int = 100):
    sql = "SELECT * FROM watermark_log"
    args = []
    if stream:
        sql += " WHERE stream = %s"
        args.append(stream)
    sql += " ORDER BY id DESC LIMIT %s"
    args.append(min(max(limit, 1), 1000))
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, args)
            rows = cur.fetchall()
    finally:
        conn.close()
    return {"history": rows}


@app.get("/results/current")
def results_current(window_start: Optional[int] = None, key: Optional[str] = None):
    """Head version of results. With window_start+key, returns a single result."""
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT DISTINCT ON (window_start, key) *
                   FROM results ORDER BY window_start, key, version DESC"""
            )
            heads = cur.fetchall()
    finally:
        conn.close()
    heads = [h for h in heads if h["status"] != "SUPERSEDED"]
    if window_start is not None:
        heads = [h for h in heads if h["window_start"] == window_start]
    if key is not None:
        heads = [h for h in heads if h["key"] == key]
    if window_start is not None and key is not None:
        return {"result": heads[0] if heads else None}
    return {"results": heads}


@app.get("/results/history")
def results_history(window_start: int, key: str):
    """Every version of one result plus the audit trail explaining each change."""
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM results WHERE window_start = %s AND key = %s ORDER BY version",
                (window_start, key),
            )
            versions = cur.fetchall()
            cur.execute(
                "SELECT * FROM audit WHERE window_start = %s AND key = %s ORDER BY id",
                (window_start, key),
            )
            audits = cur.fetchall()
    finally:
        conn.close()
    return {"window_start": window_start, "key": key,
            "versions": versions, "audit": audits}


@app.get("/audit")
def audit_trail(window_start: Optional[int] = None, key: Optional[str] = None,
                limit: int = Query(default=100)):
    sql, args = "SELECT * FROM audit", []
    conds = []
    if window_start is not None:
        conds.append("window_start = %s")
        args.append(window_start)
    if key is not None:
        conds.append("key = %s")
        args.append(key)
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY id DESC LIMIT %s"
    args.append(min(max(limit, 1), 1000))
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, args)
            rows = cur.fetchall()
    finally:
        conn.close()
    return {"audit": rows}


@app.get("/windows")
def windows():
    """All known (window, key) pairs with their alignment state."""
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            wms = current_watermarks(cur)
            min_wm = min(wms.values()) if len(wms) == len(INGESTS) and all(
                w is not None for w in wms.values()) else None
            cur.execute(
                """SELECT key, (event_time / %s) * %s AS window_start,
                          count(*) FILTER (WHERE type = 'upsert') AS upserts,
                          count(*) FILTER (WHERE type = 'retract') AS retracts
                   FROM stream_events GROUP BY key, window_start ORDER BY window_start, key""",
                (WINDOW_MS, WINDOW_MS),
            )
            rows = cur.fetchall()
            cur.execute(
                """SELECT DISTINCT ON (window_start, key) window_start, key, version, status
                   FROM results ORDER BY window_start, key, version DESC"""
            )
            heads = {(r["window_start"], r["key"]): r for r in cur.fetchall()}
    finally:
        conn.close()
    out = []
    for r in rows:
        ws = r["window_start"]
        head = heads.get((ws, r["key"]))
        out.append({
            "window_start": ws,
            "window_end": ws + WINDOW_MS,
            "key": r["key"],
            "upserts": r["upserts"],
            "retracts": r["retracts"],
            "closed": min_wm is not None and ws + WINDOW_MS <= min_wm,
            "head_version": head["version"] if head else None,
            "head_status": head["status"] if head else None,
        })
    return {"windows": out, "min_watermark": min_wm}


# ---------------------------------------------------------------------------
# downstream subscriptions & delivery status
# ---------------------------------------------------------------------------

class SubscriptionIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    url: str = Field(min_length=1, max_length=2000,
                     description="endpoint that receives POSTed result versions")


@app.post("/subscriptions")
def subscribe(body: SubscriptionIn):
    """Register (or re-register) a downstream receiver.

    Re-posting an existing name updates its URL and re-activates it. Only
    versions emitted *after* registration are delivered — a downstream that
    needs history should first read it via /results/* (that is how existing
    consumers already booked their state).
    """
    if not body.url.startswith(("http://", "https://")):
        raise HTTPException(422, "url must be an http(s) endpoint")
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """INSERT INTO subscribers (name, url) VALUES (%s, %s)
                   ON CONFLICT (name) DO UPDATE SET url = EXCLUDED.url, active = TRUE
                   RETURNING id, name, url, active, created_at""",
                (body.name, body.url),
            )
            row = cur.fetchone()
    finally:
        conn.close()
    log.info("subscriber registered: %s -> %s", body.name, body.url)
    return {"subscription": row}


@app.get("/subscriptions")
def subscriptions():
    """All registered downstreams with their delivery backlog counts."""
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT s.id, s.name, s.url, s.active, s.created_at,
                          count(d.id) FILTER (WHERE d.status = 'DELIVERED') AS delivered,
                          count(d.id) FILTER (WHERE d.status = 'RETRYING')  AS retrying,
                          count(d.id) FILTER (WHERE d.status = 'PENDING')   AS pending
                   FROM subscribers s
                   LEFT JOIN deliveries d ON d.subscriber_id = s.id
                   GROUP BY s.id ORDER BY s.id"""
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    return {"subscriptions": rows}


@app.delete("/subscriptions/{subscriber_id}")
def unsubscribe(subscriber_id: int):
    """Deactivate a subscriber: stops dispatch, keeps the delivery records."""
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """UPDATE subscribers SET active = FALSE WHERE id = %s
                   RETURNING id, name, url, active, created_at""",
                (subscriber_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()
    if row is None:
        raise HTTPException(404, "no such subscriber")
    log.info("subscriber deactivated: %s", row["name"])
    return {"subscription": row}


DELIVERY_COLS = """d.id, s.name AS subscriber, s.url, d.window_start, d.window_end, d.key,
                   d.version, d.kind, d.status, d.attempts, d.last_error,
                   d.next_attempt_at, d.last_attempt_at, d.delivered_at, d.created_at"""


@app.get("/deliveries")
def deliveries(window_start: Optional[int] = None, key: Optional[str] = None,
               subscriber: Optional[str] = None, status: Optional[str] = None,
               limit: int = Query(default=200)):
    """Raw outbox view: every (subscriber, result version) delivery row."""
    sql = f"SELECT {DELIVERY_COLS} FROM deliveries d JOIN subscribers s ON s.id = d.subscriber_id"
    conds, args = [], []
    if window_start is not None:
        conds.append("d.window_start = %s")
        args.append(window_start)
    if key is not None:
        conds.append("d.key = %s")
        args.append(key)
    if subscriber is not None:
        conds.append("s.name = %s")
        args.append(subscriber)
    if status is not None:
        if status not in ("PENDING", "RETRYING", "DELIVERED"):
            raise HTTPException(422, "status must be PENDING, RETRYING or DELIVERED")
        conds.append("d.status = %s")
        args.append(status)
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY d.id DESC LIMIT %s"
    args.append(min(max(limit, 1), 1000))
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, args)
            rows = cur.fetchall()
    finally:
        conn.close()
    return {"deliveries": rows}


@app.get("/results/delivery")
def result_delivery(window_start: int, key: str):
    """Per-result delivery ladder: for one result, where each version stands
    with each downstream — which version is delivered, which is still being
    retried, and the last contiguously delivered version (delivered_up_to)."""
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""SELECT {DELIVERY_COLS}, s.active
                    FROM deliveries d JOIN subscribers s ON s.id = d.subscriber_id
                    WHERE d.window_start = %s AND d.key = %s
                    ORDER BY s.name, d.version""",
                (window_start, key),
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    by_sub = {}
    for r in rows:
        sub = by_sub.setdefault(r["subscriber"], {
            "subscriber": r["subscriber"], "url": r["url"], "active": r["active"],
            "delivered_up_to": None, "versions": [],
        })
        sub["versions"].append({
            "version": r["version"], "kind": r["kind"], "status": r["status"],
            "attempts": r["attempts"], "last_error": r["last_error"],
            "next_attempt_at": r["next_attempt_at"], "delivered_at": r["delivered_at"],
        })
    for sub in by_sub.values():
        for v in sub["versions"]:
            if v["status"] != "DELIVERED":
                break
            sub["delivered_up_to"] = v["version"]
    return {"window_start": window_start, "key": key,
            "subscribers": list(by_sub.values())}
