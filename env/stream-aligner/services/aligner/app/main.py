"""Alignment service.

Continuously pulls both ingest streams, maintains a per-stream watermark, and
emits aligned business results per (window, key) once *that key's own two
sides* have both crossed the window end — each side crosses by its own newer
events, by a real (non-idle) watermark promise, or, once the stream has gone
idle, by simply already having data on that side in that window. The gate is
per business key: one quiet business never stalls the others, a slow business
simply keeps waiting (never dropped, never force-closed one-sided by an idle
stream), and a business whose both sides have arrived is never stuck behind
a silent stream. Late events and retractions recompute already-emitted windows
and produce new, fully audited versions — old versions are kept, never
overwritten, and a watermark regression can never un-emit them. All
recomputation is idempotent: identical content never yields a new version, so
replays and restarts cannot double-count.

Computed results are internal until *externally released* (对外放行): a
per-business-key gate (closed by default) holds fresh windows back — versions,
audit and order folds still happen and remain queryable, but no delivery rows
are created. A one-shot release (POST /releases) or opening the key's gate
(POST /release-gates) publishes the window's head version AT THAT MOMENT as
NEW; intermediate held versions are never sent. Release is one-shot per
(window, key) and recorded in an immutable ledger, so later corrections,
withdrawals and revivals keep flowing regardless of the gate — closing it only
holds back windows that have never been out, and nothing released is reclaimed.

Every released version is fanned out to registered downstreams via a
transactional outbox: the release ledger row and deliveries are inserted in
the same transaction as the result version, so a released version can never
exist without its deliveries. A dispatcher pushes deliveries to each
subscriber in per-result version order (a later version is never sent before
an earlier one), retries failures with exponential backoff, and never gives up
— nothing is silently dropped.

Historical replays are separate BACKFILL-channel deliveries: a per-subscriber
job copies only versions that already crossed the release gate, preserving
each version's original kind, payload, timestamp and order. The live REALTIME
channel is dispatched first on every poll, so live traffic is never starved
behind a large replay; the decisive guarantee is per-result: versions of one
(window, key) always reach the downstream in version order across both
channels — a realtime correction born while that result's earlier version is
still queued in the replay waits behind the history. Different results never
block one another and the wait graph only points at strictly smaller versions,
so the two channels cannot deadlock. A replay job can be paused and later
resumed, and the outbox identity prevents the same subscriber/version from
being replayed twice. Replay ranges speak event time and are snapped to window
boundaries, so a bound landing mid-window never drops the window containing it.

On top of delivery sits the downstream posting ledger (下游入账台账): a
delivered version is only "booked" once the downstream itself reports it
posted. Per (downstream, result) the ledger tracks reported_version against
delivered_up_to — ALIGNED, LAGGING, or AHEAD_UNCONFIRMED (it posted a version
we are still retrying). A new delivery on an aligned pair turns it LAGGING
and holds that result's later versions for that downstream until its posting
catches up; other results keep flowing. Reports only count for versions that
actually went out (DELIVERED, or RETRYING with the response lost); a version
never dispatched — never sent, or still held by the gate — is rejected (and
traced); rollback reports move the ledger back without erasing delivery
records. Every report and every delivery advance appends an immutable event,
so the moment and the version that flipped a pair from aligned to lagging is
always queryable.

Reconciliation batches (对账批次) sit on top of the delivery outbox and the
posting ledger: a batch is opened for ONE downstream over ONE event-time range
and, at opening, takes a single transactional photograph — under the
subscriber row lock, so it is one consistent state — of every result the
downstream has any delivery row for: which version we sent / confirmed /
still have in flight, which version it reported, the full per-version
delivery ladder and any reports of ours it had rejected. Those snapshot
columns are frozen forever; later ledger movement is visible only as separate
live_* columns beside them. Each photographed pair is bucketed ALIGNED,
LAGGING, AHEAD_UNCONFIRMED or NOT_REPORTED (sent but never reported);
ALIGNED rows need no action while every other row must be adjudicated
one by one — CONFIRMED (认账) or REJECTED (驳) — before the batch can close,
and a verdict never moves the ledger or the outbox. Two OPEN batches of the
same subscriber cannot cover overlapping windows; after closing, verdicts
are frozen too, and a new batch may reopen the same range and photograph the
then-current state.
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

from app.core import (ORDER_STATUSES, POSTING_STATUSES, build_order_snapshot,
                      compute_payload, decide, delivery_kind, evaluate_order,
                      normalize_backfill_range, order_reason, posting_status,
                      ranges_overlap, reconciliation_can_close,
                      reconciliation_item_status, released_version_kind,
                      reportable_delivery_status, retry_delay_ms, should_deliver,
                      side_evidence, window_of, window_ready)

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
-- The per-key emission gate needs to know whether a watermark is a real
-- progress promise (event_time / override) or idle wall-clock advancement.
ALTER TABLE watermarks ADD COLUMN IF NOT EXISTS source TEXT;
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

-- Historical replay (历史补推). REALTIME rows are the normal release outbox;
-- BACKFILL rows are versions copied from the immutable release history into a
-- per-subscriber replay job. They share the same delivery identity
-- (subscriber, window, key, version), so a version can never be enqueued twice.
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS channel TEXT NOT NULL DEFAULT 'REALTIME';
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS backfill_job_id BIGINT;
DO $$
BEGIN
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_channel_check
        CHECK (channel IN ('REALTIME', 'BACKFILL'));
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;
CREATE INDEX IF NOT EXISTS deliveries_subscriber_channel_idx
    ON deliveries (subscriber_id, channel, status)
    INCLUDE (window_start, key, version);
CREATE INDEX IF NOT EXISTS deliveries_backfill_job_idx
    ON deliveries (backfill_job_id, id)
    WHERE backfill_job_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS backfill_jobs (
    id                 BIGSERIAL PRIMARY KEY,
    subscriber_id      BIGINT NOT NULL REFERENCES subscribers(id),
    window_start_from  BIGINT NOT NULL,
    window_start_to    BIGINT NOT NULL,
    status             TEXT NOT NULL DEFAULT 'RUNNING'
                       CHECK (status IN ('RUNNING', 'PAUSED', 'COMPLETED')),
    total_versions     INT NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    paused_at          TIMESTAMPTZ,
    completed_at       TIMESTAMPTZ,
    CHECK (window_start_from <= window_start_to)
);
CREATE INDEX IF NOT EXISTS backfill_jobs_subscriber_idx
    ON backfill_jobs (subscriber_id, id DESC);
-- Only one runnable/paused replay may exist for a subscriber. A completed job
-- is immutable; resume creates no duplicate rows because delivery identity is
-- globally unique per subscriber and version.
CREATE UNIQUE INDEX IF NOT EXISTS backfill_jobs_one_active_uq
    ON backfill_jobs (subscriber_id) WHERE status IN ('RUNNING', 'PAUSED');
DO $$
BEGIN
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_backfill_job_fk
        FOREIGN KEY (backfill_job_id) REFERENCES backfill_jobs(id);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;
-- Business orders (业务单): one order per business key, assembled from the
-- aligned window results above. The order row is only a denormalized head;
-- every state change is an append-only row in biz_order_versions carrying a
-- full snapshot of each window's bound result version, so the shape of the
-- order at close time is never overwritten by a later reopen or void.
CREATE TABLE IF NOT EXISTS biz_orders (
    id           BIGSERIAL PRIMARY KEY,
    key          TEXT NOT NULL UNIQUE,   -- one order per business key, forever
    status       TEXT NOT NULL CHECK (status IN ('OPEN','WAITING','CLOSED','REOPENED','VOID')),
    head_version INT NOT NULL,
    ever_closed  BOOLEAN NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Current window -> order bindings. UNIQUE(key, window_start) is the hard
-- guarantee that one window's result can never feed two orders.
CREATE TABLE IF NOT EXISTS biz_order_windows (
    order_id       BIGINT NOT NULL REFERENCES biz_orders(id),
    key            TEXT NOT NULL,
    window_start   BIGINT NOT NULL,
    window_end     BIGINT NOT NULL,
    result_version INT NOT NULL,
    result_status  TEXT NOT NULL CHECK (result_status IN ('CURRENT','RETRACTED')),
    has_gap        BOOLEAN NOT NULL,
    match_count    INT NOT NULL,
    unmatched_a    INT NOT NULL,
    unmatched_b    INT NOT NULL,
    payload_hash   TEXT,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, window_start),
    UNIQUE (key, window_start)
);
CREATE TABLE IF NOT EXISTS biz_order_versions (
    id                     BIGSERIAL PRIMARY KEY,
    order_id               BIGINT NOT NULL REFERENCES biz_orders(id),
    version                INT NOT NULL,
    status                 TEXT NOT NULL CHECK (status IN ('OPEN','WAITING','CLOSED','REOPENED','VOID')),
    reason                 TEXT NOT NULL CHECK (reason IN
                           ('ORDER_OPENED','WINDOW_JOINED','WINDOW_CORRECTED',
                            'WINDOW_WITHDRAWN','WINDOW_REVIVED')),
    trigger_window_start   BIGINT NOT NULL,   -- which window caused this version
    trigger_result_version INT NOT NULL,      -- ... at which result version
    trigger_result_id      BIGINT NOT NULL,
    snapshot               JSONB NOT NULL,    -- every window's bound result version
    missing_windows        JSONB NOT NULL,    -- due business windows not yet in the order
    pending_windows        JSONB NOT NULL,    -- business windows known from events, not yet due
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (order_id, version)
);
-- Order-builder checkpoint: results.id up to which orders have been built.
-- Updated in the same transaction as each order version, so a crash mid-build
-- replays at most the uncommitted tail and that replay is a no-op.
CREATE TABLE IF NOT EXISTS biz_order_state (
    id             SMALLINT PRIMARY KEY,
    last_result_id BIGINT NOT NULL DEFAULT 0
);
INSERT INTO biz_order_state (id) VALUES (1) ON CONFLICT DO NOTHING;
-- ---------------------------------------------------------------------------
-- External release gate (对外放行), per business key.
--
-- release_gates: the per-key switch (closed unless opened). It only gates the
-- FIRST external publication of each window.
-- release_actions: append-only audit of every release operation (explicit one-
-- shot releases, gate opens flushing the backlog, gate closes).
-- window_releases: the release ledger — one immutable row per (window, key)
-- that has crossed the gate. Its mere existence is the "already given
-- externally" fact: once inserted, corrections/withdrawals keep flowing no
-- matter what the gate does afterwards. Rows are never deleted, so a close can
-- never reclaim what went out.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS release_gates (
    key        TEXT PRIMARY KEY,
    open       BOOLEAN NOT NULL DEFAULT FALSE,
    opened_at  TIMESTAMPTZ,
    closed_at  TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS release_actions (
    id         BIGSERIAL PRIMARY KEY,
    key        TEXT NOT NULL,
    action     TEXT NOT NULL CHECK (action IN ('RELEASE', 'GATE_OPEN', 'GATE_CLOSE')),
    window_start BIGINT,
    result_version INT,
    windows    JSONB NOT NULL,   -- [{window_start, window_end, version}, ...]
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS release_actions_key_idx ON release_actions (key, id);
CREATE TABLE IF NOT EXISTS window_releases (
    window_start  BIGINT NOT NULL,
    window_end    BIGINT NOT NULL,
    key           TEXT NOT NULL,
    result_version INT NOT NULL,         -- the head version as released
    action        TEXT NOT NULL CHECK (action IN ('RELEASE', 'GATE_OPEN')),
    released_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, key)      -- one-shot per window; insert-only
);
-- Migration: windows that already had deliveries before the gate existed are
-- retroactively "released", so their later corrections keep flowing (old
-- subscribers' behaviour is preserved). Earliest delivered version = the
-- version that first crossed the gate. Gates stay closed by default — only
-- NEW windows of a key are held until it is released.
INSERT INTO release_gates (key, open, updated_at)
SELECT DISTINCT key, FALSE, now() FROM results
ON CONFLICT (key) DO NOTHING;
INSERT INTO window_releases (window_start, window_end, key, result_version, action, released_at)
SELECT d.window_start,
       (SELECT window_end FROM results r0
         WHERE r0.window_start = d.window_start AND r0.key = d.key LIMIT 1),
       d.key, MIN(d.version), 'RELEASE', now()
FROM deliveries d
GROUP BY d.window_start, d.key
ON CONFLICT (window_start, key) DO NOTHING;
-- ---------------------------------------------------------------------------
-- Downstream posting ledger (下游入账台账), per (subscriber, window, key).
--
-- "We delivered it" never means "it booked it": the downstream itself reports
-- the version it has posted, and the ledger holds the comparison —
-- reported_version (它报到哪一版) against delivered_up_to (我送到哪一版):
-- ALIGNED (对齐) / LAGGING (落后) / AHEAD_UNCONFIRMED (它对上了一个我们还在
-- 重试的版本). A pair enters the ledger at its first accepted report; pairs
-- never reported on keep flowing exactly as before — the flow-control gate
-- only ever engages on a pair the downstream itself started reporting.
--
-- posting_ledger is the current head; posting_events is the append-only
-- trail: every report (accepted AND rejected) and every delivery advance
-- lands one row carrying the state before and after, so the moment a pair
-- flipped from ALIGNED to LAGGING — and which version knocked it there — is
-- never overwritten by later states. Rows are keyed by subscriber_id, so
-- re-registering a downstream under a new URL keeps the ledger attached to
-- the same downstream.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS posting_ledger (
    subscriber_id     BIGINT NOT NULL REFERENCES subscribers(id),
    window_start      BIGINT NOT NULL,
    key               TEXT NOT NULL,
    reported_version  INT NOT NULL,            -- last version it reported posted
    delivered_up_to   INT,                     -- max version confirmed DELIVERED (NULL = none yet)
    status            TEXT NOT NULL CHECK (status IN ('ALIGNED', 'LAGGING', 'AHEAD_UNCONFIRMED')),
    first_reported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (subscriber_id, window_start, key)
);
CREATE TABLE IF NOT EXISTS posting_events (
    id               BIGSERIAL PRIMARY KEY,
    subscriber_id    BIGINT NOT NULL REFERENCES subscribers(id),
    window_start     BIGINT NOT NULL,
    key              TEXT NOT NULL,
    event            TEXT NOT NULL CHECK (event IN
                     ('REPORT_ACCEPTED', 'REPORT_REJECTED', 'DELIVERY_ADVANCED')),
    cause_version    INT NOT NULL,             -- the version reported / delivered
    prev_reported    INT,
    prev_delivered   INT,
    prev_status      TEXT CHECK (prev_status IN ('ALIGNED', 'LAGGING', 'AHEAD_UNCONFIRMED')),
    reported_version INT,                      -- ledger state after this event
    delivered_up_to  INT,
    status           TEXT CHECK (status IN ('ALIGNED', 'LAGGING', 'AHEAD_UNCONFIRMED')),
    detail           JSONB,                    -- e.g. the rejection reason
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS posting_events_pair_idx
    ON posting_events (subscriber_id, window_start, key, id);
CREATE INDEX IF NOT EXISTS posting_events_subscriber_idx
    ON posting_events (subscriber_id, id);
-- ---------------------------------------------------------------------------
-- Reconciliation batches (对账批次), per subscriber over a window range.
--
-- Opening a batch takes ONE transactional point-in-time photograph, under the
-- subscriber row lock, of every result the downstream has any delivery row
-- for in range: sent/delivered/in-flight versions, its last reported version,
-- the full per-version delivery ladder and any reports of ours that were
-- rejected (posting_events REPORT_REJECTED). Those columns are the frozen
-- snapshot — later ledger/delivery movement never updates them; item rows are
-- only ever touched by a CONFIRMED/REJECTED adjudication before close, and
-- not even after. Every adjudication, the opening and the close append an
-- immutable reconciliation_events row.
--
-- At most one OPEN batch per subscriber may cover any given window at a time
-- (checked in create_reconciliation under the same subscriber row lock, so
-- two concurrent opens cannot both miss the check). CLOSED batches never
-- block a new batch, even over the exact same range.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reconciliation_batches (
    id                    BIGSERIAL PRIMARY KEY,
    subscriber_id         BIGINT NOT NULL REFERENCES subscribers(id),
    window_start_from     BIGINT NOT NULL,   -- snapped, inclusive
    window_start_to       BIGINT NOT NULL,   -- snapped, exclusive
    status                TEXT NOT NULL DEFAULT 'OPEN'
                          CHECK (status IN ('OPEN', 'CLOSED')),
    total_items           INT NOT NULL DEFAULT 0,
    aligned_items         INT NOT NULL DEFAULT 0,
    lagging_items         INT NOT NULL DEFAULT 0,
    ahead_items           INT NOT NULL DEFAULT 0,
    not_reported_items    INT NOT NULL DEFAULT 0,
    confirmed_items       INT NOT NULL DEFAULT 0,
    rejected_items        INT NOT NULL DEFAULT 0,
    created_by            TEXT,
    closed_by             TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at             TIMESTAMPTZ,
    CHECK (window_start_from <= window_start_to)
);
CREATE INDEX IF NOT EXISTS reconciliation_batches_subscriber_idx
    ON reconciliation_batches (subscriber_id, id DESC);
CREATE INDEX IF NOT EXISTS reconciliation_batches_open_idx
    ON reconciliation_batches (subscriber_id, window_start_from, window_start_to)
    WHERE status = 'OPEN';
CREATE TABLE IF NOT EXISTS reconciliation_items (
    batch_id          BIGINT NOT NULL REFERENCES reconciliation_batches(id),
    window_start      BIGINT NOT NULL,
    key               TEXT NOT NULL,
    window_end        BIGINT NOT NULL,
    -- Frozen snapshot as of batch opening (never updated afterwards):
    sent_version      INT NOT NULL,        -- highest delivery row of any status
    delivered_version INT,                 -- highest DELIVERED (NULL = none)
    inflight_version  INT,                 -- lowest non-DELIVERED (NULL = none)
    reported_version  INT,                 -- downstream's last reported posting
    item_status       TEXT NOT NULL CHECK (item_status IN
                       ('ALIGNED', 'LAGGING', 'AHEAD_UNCONFIRMED', 'NOT_REPORTED')),
    delivery_ladder   JSONB NOT NULL,      -- [{version, kind, status, channel}]
    rejected_reports  JSONB NOT NULL,      -- posting-events REPORT_REJECTED rows
    -- The only mutable columns: the adjudication, and only while the batch is
    -- OPEN. ALIGNED rows never carry one ("对上的不用管").
    decision          TEXT CHECK (decision IN ('CONFIRMED', 'REJECTED')),
    decided_by        TEXT,
    decided_at        TIMESTAMPTZ,
    PRIMARY KEY (batch_id, window_start, key)
);
CREATE INDEX IF NOT EXISTS reconciliation_items_status_idx
    ON reconciliation_items (batch_id, item_status);
CREATE TABLE IF NOT EXISTS reconciliation_events (
    id            BIGSERIAL PRIMARY KEY,
    batch_id      BIGINT NOT NULL REFERENCES reconciliation_batches(id),
    event         TEXT NOT NULL CHECK (event IN
                   ('BATCH_OPENED', 'ITEM_DECIDED', 'BATCH_CLOSED')),
    window_start  BIGINT,
    key           TEXT,
    item_status   TEXT,
    decision      TEXT,
    prev_decision TEXT,
    operator      TEXT,
    note          TEXT,
    detail        JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS reconciliation_events_batch_idx
    ON reconciliation_events (batch_id, id);
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

    The version row and its audit entry are always written: the internal
    result exists (and is queryable) the moment it is computed. Outbox
    deliveries, however, are gated by the per-key external release:

    - the window has been released once (a window_releases row exists): every
      later version is delivered, gate open or closed — closing never holds
      back what already crossed;
    - never released, gate open: this live version is the first publication —
      the release ledger row is written in this same transaction and the
      version goes out as NEW;
    - never released, gate closed: the version is held internally — no
      deliveries exist until an explicit release (or a later gate open) takes
      the then-current head.

    Everything above commits in one transaction, so a delivered version can
    never be missing its release record, and a held version can never leak an
    outbox row.
    """
    window_end = window_start + WINDOW_MS
    with conn, conn.cursor() as cur:
        # Serialize against a concurrent explicit release / gate flip for the
        # same key: both paths take the gate row lock first.
        cur.execute(
            """INSERT INTO release_gates (key) VALUES (%s) ON CONFLICT (key) DO NOTHING""",
            (key,),
        )
        cur.execute("SELECT open FROM release_gates WHERE key = %s FOR UPDATE", (key,))
        gate_open = cur.fetchone()[0]

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
        cur.execute(
            "SELECT 1 FROM window_releases WHERE window_start = %s AND key = %s",
            (window_start, key),
        )
        ever_released = cur.fetchone() is not None
        publish = should_deliver(gate_open, ever_released, nxt["status"])
        if not publish:
            log.info("window=%d key=%s -> v%d (%s, %s) HELD (not released externally)",
                     window_start, key, nxt["version"], nxt["status"], reason)
            return True
        if not ever_released:
            # First external publication: the release ledger row is born in the
            # same transaction as the deliveries — one-shot, insert-only, never
            # revocable by closing the gate.
            cur.execute(
                """INSERT INTO window_releases
                       (window_start, window_end, key, result_version, action)
                   VALUES (%s, %s, %s, %s, 'GATE_OPEN')""",
                (window_start, window_end, key, nxt["version"]),
            )
            cur.execute(
                """INSERT INTO release_actions (key, action, window_start, result_version, windows)
                   VALUES (%s, 'GATE_OPEN', %s, %s, %s)""",
                (key, window_start, nxt["version"],
                 psycopg2.extras.Json([{"window_start": window_start,
                                        "window_end": window_end,
                                        "version": nxt["version"]}])),
            )
        # Outbox fan-out. kind tells the downstream how to book this version:
        # NEW = first sight of the result, CORRECTION = amend the same
        # (window, key) booking (never a new success), WITHDRAWAL = reverse it.
        # A held window's first release is NEW regardless of its internal reason.
        kind = "NEW" if not ever_released else delivery_kind(reason, nxt["status"])
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
        new_wm, new_src = info["watermark"], info.get("source")
        with conn, conn.cursor() as cur:
            cur.execute("SELECT watermark, source FROM watermarks WHERE stream = %s",
                        (stream,))
            row = cur.fetchone()
            old_wm, old_src = (row[0], row[1]) if row else (None, None)
            if old_wm == new_wm and old_src == new_src:
                continue
            if old_wm != new_wm:
                if old_wm is not None and (new_wm is None or new_wm < old_wm):
                    direction = "regress"
                    log.warning("stream %s watermark regressed: %s -> %s",
                                stream, old_wm, new_wm)
                else:
                    direction = "advance"
                cur.execute(
                    "INSERT INTO watermark_log (stream, old_watermark, new_watermark, direction) VALUES (%s, %s, %s, %s)",
                    (stream, old_wm, new_wm, direction),
                )
            cur.execute(
                """INSERT INTO watermarks (stream, watermark, source) VALUES (%s, %s, %s)
                   ON CONFLICT (stream) DO UPDATE SET watermark = EXCLUDED.watermark,
                                                      source = EXCLUDED.source,
                                                      updated_at = now()""",
                (stream, new_wm, new_src),
            )


def current_watermarks(cur):
    """{stream: {"watermark", "source"}} — the source decides whether the
    watermark may serve as crossing evidence (see core.side_evidence)."""
    cur.execute("SELECT stream, watermark, source FROM watermarks")
    return {stream: {"watermark": wm, "source": src}
            for stream, wm, src in cur.fetchall()}


def key_max_event_times(cur, key=None):
    """Latest event_time seen per key and stream: each key's own progress on
    each side. Retracts count too — any event shows the business acted on
    that stream at that time."""
    sql = "SELECT key, stream, MAX(event_time) FROM stream_events"
    args = []
    if key is not None:
        sql += " WHERE key = %s"
        args.append(key)
    sql += " GROUP BY key, stream"
    cur.execute(sql, args)
    out = {}
    for k, stream, mx in cur.fetchall():
        out.setdefault(k, {})[stream] = mx
    return out


def effective_data_sides(cur, key=None):
    """{(key, window_start): {streams}} — which sides have at least one
    effective (non-retracted) upsert in that window. This is the "has data
    on this side in this window" fact the idle-finalization rule uses: an
    idle stream may only finalize a side that already has data."""
    sql = """SELECT e.key, (e.event_time / %s) * %s AS ws, e.stream
             FROM stream_events e
             WHERE e.type = 'upsert'
               AND NOT EXISTS (
                   SELECT 1 FROM stream_events r
                   WHERE r.type = 'retract' AND r.stream = e.stream
                         AND r.retracts = e.event_id
               )"""
    args = [WINDOW_MS, WINDOW_MS]
    if key is not None:
        sql += " AND e.key = %s"
        args.append(key)
    sql += " GROUP BY e.key, ws, e.stream"
    cur.execute(sql, args)
    sides = {}
    for k, ws, stream in cur.fetchall():
        sides.setdefault((k, ws), set()).add(stream)
    return sides


def emission_marks(wms, key_max, sides, key, ws):
    """Per-stream crossing evidence for one (key, window)
    (core.side_evidence)."""
    present = sides.get((key, ws), ())
    marks = {}
    for stream in INGESTS:
        info = wms.get(stream) or {}
        marks[stream] = {
            "watermark": info.get("watermark"),
            "source": info.get("source"),
            "key_max_event_time": key_max.get(key, {}).get(stream),
            "key_has_data_in_window": stream in present,
        }
    return marks


def close_windows(conn):
    """Emit INITIAL results for (window, key) pairs whose own two sides have
    both crossed the window end.

    The gate is per business key: one quiet business cannot stall the
    others; a slow business simply keeps waiting — its events and the
    absence of a result are the waiting state, re-evaluated every tick,
    never dropped; an idle stream's wall-clock watermark never finalizes
    a side that has no data in the window, but a window whose both sides
    have arrived is finalized once the streams go idle. Already-emitted
    results are never revisited here, so a watermark regression cannot
    un-emit them.
    """
    with conn.cursor() as cur:
        wms = current_watermarks(cur)
        key_max = key_max_event_times(cur)
        sides = effective_data_sides(cur)
    for key, ws in sides:
        end = ws + WINDOW_MS
        marks = emission_marks(wms, key_max, sides, key, ws)
        if not window_ready(marks["a"], marks["b"], end):
            continue
        with conn.cursor() as cur:
            head = get_head(cur, ws, key)
        if head is None:
            emit(conn, ws, key, "INITIAL", {
                "side_a": side_evidence(marks["a"], end),
                "side_b": side_evidence(marks["b"], end),
                "watermark_a": marks["a"]["watermark"],
                "watermark_b": marks["b"]["watermark"],
            })


# ---------------------------------------------------------------------------
# business orders (业务单)
# ---------------------------------------------------------------------------
# One order per business key, built by folding every committed result version
# into the key's order in results.id order. The order's status is derived from
# its window bindings: OPEN while known business windows are still missing or
# pending, WAITING when only one-sided gaps remain, CLOSED when every window
# is in and fully matched, REOPENED when a closed order is undone by a later
# correction/withdrawal, VOID when the whole business is withdrawn. Every
# transition appends a full-snapshot version — the close-time shape of the
# order is never overwritten by a later reopen or void.

def business_windows(cur, key):
    """Windows holding at least one effective (non-retracted) upsert for key."""
    cur.execute(
        """SELECT DISTINCT (e.event_time / %s) * %s AS ws
           FROM stream_events e
           WHERE e.key = %s AND e.type = 'upsert'
             AND NOT EXISTS (
                 SELECT 1 FROM stream_events r
                 WHERE r.type = 'retract' AND r.stream = e.stream
                       AND r.retracts = e.event_id
             )""",
        (WINDOW_MS, WINDOW_MS, key),
    )
    return {row[0] for row in cur.fetchall()}


def order_window_gaps(cur, key, bound):
    """Unbound business windows, split into missing (both sides crossed, so
    the result is due) and pending (still waiting for at least one side) —
    the same per-key gate that drives emission."""
    wms = current_watermarks(cur)
    key_max = key_max_event_times(cur, key)
    sides = effective_data_sides(cur, key)
    missing, pending = [], []
    for ws in business_windows(cur, key) - bound:
        marks = emission_marks(wms, key_max, sides, key, ws)
        due = window_ready(marks["a"], marks["b"], ws + WINDOW_MS)
        (missing if due else pending).append(ws)
    return sorted(missing), sorted(pending)


def apply_result_to_order(conn, result):
    """Fold one committed result version into its key's order.

    Binding upsert + new order version (full snapshot) + head update + builder
    checkpoint all commit in one transaction; replaying the same result row
    after a crash redoes the identical write, so the builder is idempotent.
    """
    key, ws, we = result["key"], result["window_start"], result["window_end"]
    payload = result["payload"]
    # The row's status may later be flipped to SUPERSEDED; the status it was
    # *emitted* with is derivable from the payload: live iff payload present.
    result_status = "CURRENT" if payload is not None else "RETRACTED"
    has_gap = bool(payload and (payload["unmatched_a"] or payload["unmatched_b"]))
    match_count = payload["match_count"] if payload else 0
    unmatched_a = len(payload["unmatched_a"]) if payload else 0
    unmatched_b = len(payload["unmatched_b"]) if payload else 0
    with conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO biz_orders (key, status, head_version)
               VALUES (%s, 'OPEN', 0) ON CONFLICT (key) DO NOTHING""",
            (key,),
        )
        cur.execute(
            "SELECT id, head_version, ever_closed FROM biz_orders WHERE key = %s",
            (key,),
        )
        order_id, head_version, ever_closed = cur.fetchone()

        cur.execute(
            "SELECT result_status FROM biz_order_windows WHERE key = %s AND window_start = %s",
            (key, ws),
        )
        prev = cur.fetchone()
        cur.execute(
            """INSERT INTO biz_order_windows
                   (order_id, key, window_start, window_end, result_version, result_status,
                    has_gap, match_count, unmatched_a, unmatched_b, payload_hash)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (key, window_start) DO UPDATE SET
                   result_version = EXCLUDED.result_version,
                   result_status  = EXCLUDED.result_status,
                   has_gap        = EXCLUDED.has_gap,
                   match_count    = EXCLUDED.match_count,
                   unmatched_a    = EXCLUDED.unmatched_a,
                   unmatched_b    = EXCLUDED.unmatched_b,
                   payload_hash   = EXCLUDED.payload_hash,
                   updated_at     = now()""",
            (order_id, key, ws, we, result["version"], result_status, has_gap,
             match_count, unmatched_a, unmatched_b, result["payload_hash"]),
        )
        cur.execute(
            """SELECT window_start, window_end, result_version, result_status, has_gap,
                      match_count, unmatched_a, unmatched_b, payload_hash
               FROM biz_order_windows WHERE order_id = %s ORDER BY window_start""",
            (order_id,),
        )
        cols = [d[0] for d in cur.description]
        bindings = [dict(zip(cols, row)) for row in cur.fetchall()]
        missing, pending = order_window_gaps(cur, key, {b["window_start"] for b in bindings})
        reason = "ORDER_OPENED" if head_version == 0 else order_reason(
            prev[0] if prev else None, result_status)
        status = evaluate_order(bindings, missing, pending, ever_closed, reason)
        version = head_version + 1
        cur.execute(
            """INSERT INTO biz_order_versions
                   (order_id, version, status, reason, trigger_window_start,
                    trigger_result_version, trigger_result_id, snapshot,
                    missing_windows, pending_windows)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (order_id, version, status, reason, ws, result["version"], result["id"],
             psycopg2.extras.Json(build_order_snapshot(bindings)),
             psycopg2.extras.Json(missing), psycopg2.extras.Json(pending)),
        )
        cur.execute(
            """UPDATE biz_orders
               SET status = %s, head_version = %s,
                   ever_closed = ever_closed OR %s, updated_at = now()
               WHERE id = %s""",
            (status, version, status == "CLOSED", order_id),
        )
        cur.execute("UPDATE biz_order_state SET last_result_id = %s WHERE id = 1",
                    (result["id"],))
    log.info("order key=%s -> v%d (%s, %s) by window=%d result v%d",
             key, version, status, reason, ws, result["version"])


def build_orders(conn):
    """Drain newly committed result versions into orders, in results.id order."""
    with conn, conn.cursor() as cur:
        cur.execute("SELECT last_result_id FROM biz_order_state WHERE id = 1")
        last = cur.fetchone()[0]
    while True:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM results WHERE id > %s ORDER BY id LIMIT %s",
                        (last, BATCH))
            rows = cur.fetchall()
        if not rows:
            return
        for row in rows:
            apply_result_to_order(conn, row)
        last = rows[-1]["id"]
        if len(rows) < BATCH:
            return


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
    build_orders(conn)


# ---------------------------------------------------------------------------
# external release (对外放行)
# ---------------------------------------------------------------------------
# Internal results exist as soon as they are computed, but they are only given
# externally through here. Two operations, both per business key:
#
# release_key   — one-shot: publish exactly the windows of this key that are
#                 computed but have never been released (or just the requested
#                 subset). Each window goes out at the version that is head AT
#                 RELEASE TIME; intermediate versions held internally are
#                 skipped, not sent. Gate state is unchanged.
# set_gate      — flip the key's switch. Opening flushes that same backlog
#                 atomically and lets later first versions flow by themselves;
#                 closing only holds back windows that have never been out —
#                 corrections/withdrawals of released windows keep flowing, and
#                 release-ledger rows are never deleted.
#
# Both serialize against emit() for the key via the release_gates row lock, so
# a late event landing during a release can never split "the version at release
# time" between the ledger and the outbox.

def held_heads(cur, key):
    """Live-or-empty head results of ``key`` that have never been externally
    released. Status is the head status as emitted (payload null = RETRACTED)."""
    cur.execute(
        """SELECT r.window_start, r.window_end, r.version, r.payload
           FROM results r
           WHERE r.key = %s
             AND r.status IN ('CURRENT', 'RETRACTED')
             AND NOT EXISTS (
                 SELECT 1 FROM window_releases w
                 WHERE w.window_start = r.window_start AND w.key = r.key
             )
           ORDER BY r.window_start""",
        (key,),
    )
    return cur.fetchall()


def publish_heads(cur, key, rows, action):
    """Create release ledger rows + outbox deliveries for the given held heads.

    Runs inside the caller's transaction (which already holds the key's gate
    lock). Only live heads are published — a fully-retracted window that never
    went out releases nothing (a later revival releases it as the first NEW).
    Returns the [{"window_start", "window_end", "version"}] list released and
    writes one summary release_actions row.
    """
    released = []
    for ws, we, version, payload in rows:
        if payload is None:
            continue  # fully retracted and never out: nothing to release
        cur.execute(
            """INSERT INTO window_releases
                   (window_start, window_end, key, result_version, action)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (window_start, key) DO NOTHING""",
            (ws, we, key, version, action),
        )
        cur.execute(
            """INSERT INTO deliveries (subscriber_id, window_start, window_end, key,
                                       version, kind, payload)
               SELECT s.id, %s, %s, %s, %s, 'NEW', %s FROM subscribers s
               ON CONFLICT (subscriber_id, window_start, key, version) DO NOTHING""",
            (ws, we, key, version, psycopg2.extras.Json(payload)),
        )
        released.append({"window_start": ws, "window_end": we, "version": version})
    if released:
        cur.execute(
            """INSERT INTO release_actions (key, action, window_start, result_version, windows)
               VALUES (%s, %s, NULL, NULL, %s)""",
            (key, action, psycopg2.extras.Json(released)),
        )
    return released


def release_key(conn, key, window_starts=None):
    """One-shot external release for one business key.

    Publishes exactly the key's windows that are computed but have never been
    released — or the requested subset — each at the version that is head at
    this moment. Gate state is untouched: a later click releases only windows
    that are still held. Idempotent: an already-released window is not sent
    twice (it simply isn't in the held set anymore); a held window whose head
    is fully retracted releases nothing.
    """
    with conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO release_gates (key) VALUES (%s) ON CONFLICT (key) DO NOTHING",
            (key,),
        )
        cur.execute("SELECT 1 FROM release_gates WHERE key = %s FOR UPDATE", (key,))
        cur.fetchone()  # row was just ensured above; the lock serializes with emit()
        rows = held_heads(cur, key)
        if window_starts is not None:
            wanted = set(window_starts)
            chosen = [r for r in rows if r[0] in wanted]
            found = {r[0] for r in rows}
            unknown = sorted(wanted - found)
            if unknown:
                raise HTTPException(
                    409,
                    f"window(s) {unknown} for key {key!r} are not releasable now "
                    "(already released, not computed, or fully retracted)")
        else:
            chosen = rows
        released = publish_heads(cur, key, chosen, "RELEASE")
    log.info("release key=%s windows=%d versions=%s", key, len(released),
             [w["version"] for w in released])
    return released


def set_gate(conn, key, open_):
    """Open or close a key's external release switch.

    Opening flushes the current held backlog atomically and is idempotent
    (a repeat open finds nothing held). Closing only affects windows that have
    never been released: released windows' corrections/withdrawals keep
    flowing, and the ledger is never deleted — nothing out is reclaimed.
    """
    action = "GATE_OPEN" if open_ else "GATE_CLOSE"
    with conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO release_gates (key, open, opened_at, closed_at)
               VALUES (%s, %s, now(), CASE WHEN %s THEN NULL ELSE now() END)
               ON CONFLICT (key) DO UPDATE
                   SET open = EXCLUDED.open,
                       opened_at = CASE WHEN EXCLUDED.open THEN now()
                                        ELSE release_gates.opened_at END,
                       closed_at = CASE WHEN EXCLUDED.open THEN NULL ELSE now() END,
                       updated_at = now()""",
            (key, open_, open_),
        )
        cur.execute("SELECT 1 FROM release_gates WHERE key = %s FOR UPDATE", (key,))
        cur.fetchone()
        released = []
        if open_:
            released = publish_heads(cur, key, held_heads(cur, key), "GATE_OPEN")
            if not released:
                # an open click with an empty backlog still leaves a trace
                cur.execute(
                    """INSERT INTO release_actions (key, action, window_start, result_version, windows)
                       VALUES (%s, 'GATE_OPEN', NULL, NULL, '[]'::jsonb)""",
                    (key,),
                )
        else:
            cur.execute(
                """INSERT INTO release_actions (key, action, window_start, result_version, windows)
                   VALUES (%s, 'GATE_CLOSE', NULL, NULL, '[]'::jsonb)""",
                (key,),
            )
    log.info("gate key=%s -> %s (flushed %d held windows)", key,
             "OPEN" if open_ else "CLOSED", len(released))
    return released


# ---------------------------------------------------------------------------
# historical backfill (历史补推)
# ---------------------------------------------------------------------------
# Replay is a point-in-time copy of versions that have already crossed the
# global release gate — never internal/held versions. For every released
# window, replay starts at the version recorded in window_releases (the first
# version the outside world was allowed to see) and follows later versions in
# result-id/version order. Versions before that first release are deliberately
# invisible here.
#
# Copies are ordinary deliveries with channel='BACKFILL'. The delivery primary
# identity (subscriber, window, key, version) stays unique, so the same
# downstream can never replay a version it already has, whether the existing
# row is realtime or a previous backfill. REALTIME rows are dispatched first on
# every poll, so live traffic that is already due is never starved behind a
# large replay; corrections created during replay keep their REALTIME channel
# and are not rerouted into the backfill queue. A replay job can be paused; its
# delivery rows and retry timers stay exactly where they were, so resume
# continues at the same place.
#
# Version ordering across channels is per (window, key): a version is only due
# once every EARLIER version of the same result is DELIVERED, no matter which
# channel it rides. During a replay a fresh correction (REALTIME v2) therefore
# waits behind the historical v1 still in the BACKFILL queue instead of
# overtaking it — the earliest version always reaches the downstream first.
# The barrier is deliberately scoped to one result and the dispatch loops are
# acyclic (a row only waits on strictly smaller versions), so the two channels
# can never deadlock; different results simply never wait on each other.

BACKFILL_CANDIDATE_SQL = """
WITH first_release AS (
    SELECT window_start, key, MIN(result_version) AS first_version
    FROM window_releases
    WHERE window_start >= %s AND window_start < %s
    GROUP BY window_start, key
)
SELECT r.id, r.window_start, r.window_end, r.key, r.version, r.reason,
       r.payload, r.created_at, f.first_version
FROM results r
JOIN first_release f
  ON f.window_start = r.window_start AND f.key = r.key
WHERE r.version >= f.first_version
ORDER BY r.id
"""


def create_backfill(conn, subscriber_id, window_start_from, window_start_to):
    """Create and materialize one subscriber's historical replay job.

    The requested range speaks event time and is snapped to tumbling-window
    boundaries first (normalize_backfill_range): the window containing the
    lower bound is included, and the window the range reaches into at the top
    is included too — a bound landing mid-window must never drop the whole
    window. Bounds already aligned to window starts are unchanged, so the
    stored half-open range is exactly [from_window_start, to_window_start).

    Candidate selection and delivery copies are one repeatable-read
    transaction, making the replay a snapshot of externally released versions
    at creation time. A later release/correction stays realtime and is not
    silently added to this job. Existing delivery rows are never overwritten:
    that is the database-level guarantee that one downstream never gets the
    same version through two backfills.
    """
    from_ws, to_ws = normalize_backfill_range(
        window_start_from, window_start_to, WINDOW_MS)
    with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        cur.execute(
            """SELECT id, name, url, active FROM subscribers
               WHERE id = %s FOR UPDATE""",
            (subscriber_id,),
        )
        subscriber = cur.fetchone()
        if subscriber is None:
            raise HTTPException(404, "no such subscriber")
        if not subscriber["active"]:
            raise HTTPException(409, "subscriber is deactivated; re-register it before backfill")
        cur.execute(
            """SELECT id, status FROM backfill_jobs
               WHERE subscriber_id = %s AND status IN ('RUNNING', 'PAUSED')""",
            (subscriber_id,),
        )
        active = cur.fetchone()
        if active is not None:
            raise HTTPException(
                409,
                f"subscriber {subscriber['name']!r} already has backfill job "
                f"{active['id']} in status {active['status']}; stop or wait for it first")

        cur.execute(
            """INSERT INTO backfill_jobs
                   (subscriber_id, window_start_from, window_start_to, status)
               VALUES (%s, %s, %s, 'RUNNING')
               RETURNING *""",
            (subscriber_id, from_ws, to_ws),
        )
        job = cur.fetchone()
        cur.execute(
            BACKFILL_CANDIDATE_SQL,
            (from_ws, to_ws),
        )
        candidates = cur.fetchall()
        enqueued = 0
        for r in candidates:
            status = "RETRACTED" if r["payload"] is None else "CURRENT"
            kind = released_version_kind(
                r["version"], r["first_version"], r["reason"], status)
            cur.execute(
                """INSERT INTO deliveries
                       (subscriber_id, window_start, window_end, key, version,
                        kind, payload, channel, backfill_job_id, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, 'BACKFILL', %s, %s)
                   ON CONFLICT (subscriber_id, window_start, key, version) DO NOTHING""",
                (subscriber_id, r["window_start"], r["window_end"], r["key"],
                 r["version"], kind,
                 psycopg2.extras.Json(r["payload"]) if r["payload"] is not None else None,
                 job["id"], r["created_at"]),
            )
            enqueued += cur.rowcount
        cur.execute(
            """UPDATE backfill_jobs
               SET total_versions = %s,
                   status = CASE WHEN %s = 0 THEN 'COMPLETED' ELSE status END,
                   completed_at = CASE WHEN %s = 0 THEN now() ELSE completed_at END
               WHERE id = %s
               RETURNING *""",
            (enqueued, enqueued, enqueued, job["id"]),
        )
        return cur.fetchone()


def get_backfill_job(cur, job_id, for_update=False):
    cur.execute(
        f"SELECT * FROM backfill_jobs WHERE id = %s{' FOR UPDATE' if for_update else ''}",
        (job_id,),
    )
    return cur.fetchone()


def stop_backfill(conn, job_id):
    with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        job = get_backfill_job(cur, job_id, for_update=True)
        if job is None:
            raise HTTPException(404, "no such backfill job")
        if job["status"] == "RUNNING":
            cur.execute(
                """UPDATE backfill_jobs
                   SET status = 'PAUSED', paused_at = now()
                   WHERE id = %s RETURNING *""",
                (job_id,),
            )
            job = cur.fetchone()
    return job


def resume_backfill(conn, job_id):
    with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        job = get_backfill_job(cur, job_id, for_update=True)
        if job is None:
            raise HTTPException(404, "no such backfill job")
        if job["status"] == "PAUSED":
            cur.execute(
                """UPDATE backfill_jobs
                   SET status = 'RUNNING', paused_at = NULL
                   WHERE id = %s RETURNING *""",
                (job_id,),
            )
            job = cur.fetchone()
    return job


def complete_finished_backfill_jobs(cur):
    """Mark runnable jobs completed once every copied version is settled.

    Paused jobs are deliberately left PAUSED even if an in-flight request
    completed their final row: resuming a finished job is a harmless no-op and
    the explicit stop state remains auditable.
    """
    cur.execute(
        """UPDATE backfill_jobs j
           SET status = 'COMPLETED', completed_at = now()
           WHERE j.status = 'RUNNING'
             AND NOT EXISTS (
                 SELECT 1 FROM deliveries d
                 WHERE d.backfill_job_id = j.id
                   AND d.status IN ('PENDING', 'RETRYING')
             )
         """
    )


# ---------------------------------------------------------------------------
# downstream posting ledger (下游入账台账)
# ---------------------------------------------------------------------------
# The ledger is fed from two sides, always in the writer's own transaction:
#
# - a successful delivery advances delivered_up_to (advance_posting_ledger,
#   called from deliver_one). If the pair was ALIGNED, the new delivery turns
#   it LAGGING right there — the DELIVERY_ADVANCED event records which
#   version knocked it lagging;
# - a downstream report (POST /postings) moves reported_version — up as it
#   catches up, DOWN when it says it rolled back (the ledger regresses with
#   it; delivery rows are never touched). A report only counts for a version
#   that actually went out to this downstream (DELIVERED, or RETRYING with
#   the response lost); a version never dispatched — never sent, or still
#   PENDING (held by the gate) — is rejected: it changes nothing, but the
#   rejection itself is traced as REPORT_REJECTED.
#
# While a pair is LAGGING the dispatcher holds that result's later versions
# for that downstream (see the NOT EXISTS gate in DUE_SQL /
# BACKFILL_DUE_SQL); every other result of the same downstream flows on.

def record_posting_event(cur, subscriber_id, window_start, key, event,
                         cause_version, prev, new, detail=None):
    """Append one posting-ledger event. ``prev``/``new`` are
    (reported_version, delivered_up_to, status) triples; ``prev`` is None
    when the pair had no ledger row before this event."""
    cur.execute(
        """INSERT INTO posting_events
               (subscriber_id, window_start, key, event, cause_version,
                prev_reported, prev_delivered, prev_status,
                reported_version, delivered_up_to, status, detail)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (subscriber_id, window_start, key, event, cause_version,
         prev[0] if prev else None, prev[1] if prev else None,
         prev[2] if prev else None, new[0], new[1], new[2],
         psycopg2.extras.Json(detail) if detail is not None else None),
    )


def advance_posting_ledger(cur, delivery):
    """Fold one successful delivery into the posting ledger.

    Runs inside deliver_one's transaction, which already holds the
    subscriber row lock — so a concurrent first report for the same
    subscriber cannot seed the ledger from a stale delivered watermark.
    Pairs the downstream never reported on are untracked: there is nothing
    to knock lagging, and their flow is exactly the pre-ledger behaviour.
    """
    cur.execute(
        """SELECT reported_version, delivered_up_to, status FROM posting_ledger
           WHERE subscriber_id = %s AND window_start = %s AND key = %s
           FOR UPDATE""",
        (delivery["subscriber_id"], delivery["window_start"], delivery["key"]),
    )
    row = cur.fetchone()
    if row is None:
        return
    reported, delivered, prev_status = row
    if delivered is not None and delivery["version"] <= delivered:
        return  # non-advancing (a redelivery of an older version): no change
    new = (reported, delivery["version"],
           posting_status(reported, delivery["version"]))
    cur.execute(
        """UPDATE posting_ledger
           SET delivered_up_to = %s, status = %s, updated_at = now()
           WHERE subscriber_id = %s AND window_start = %s AND key = %s""",
        (new[1], new[2], delivery["subscriber_id"],
         delivery["window_start"], delivery["key"]),
    )
    record_posting_event(cur, delivery["subscriber_id"], delivery["window_start"],
                         delivery["key"], "DELIVERY_ADVANCED", delivery["version"],
                         (reported, delivered, prev_status), new)


def report_posting(conn, subscriber_id, window_start, key, version):
    """Apply one downstream posting report; returns the resulting ledger row.

    The reported version must be one we actually DELIVERED or at least put on
    the wire (RETRYING — the response was lost, so it may genuinely have
    booked it). A version never dispatched to this downstream — never sent at
    all, or still PENDING (e.g. held by the ledger gate) — cannot have been
    posted by it: the report is rejected (the caller answers 409) and the
    rejection is traced. Reports may move the posted position backwards (the
    downstream rolled its books back): the ledger regresses accordingly while
    every delivery row stays put. Both the ledger write and its event commit
    in one transaction.
    """
    with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Serialize against a concurrent delivery advancing the same
        # subscriber's ledger (deliver_one locks the subscriber row too).
        cur.execute("SELECT 1 FROM subscribers WHERE id = %s FOR UPDATE",
                    (subscriber_id,))
        cur.execute(
            """SELECT status FROM deliveries
               WHERE subscriber_id = %s AND window_start = %s
                 AND key = %s AND version = %s""",
            (subscriber_id, window_start, key, version),
        )
        sent = cur.fetchone()
        dispatched = sent is not None and reportable_delivery_status(sent["status"])
        cur.execute(
            """SELECT reported_version, delivered_up_to, status FROM posting_ledger
               WHERE subscriber_id = %s AND window_start = %s AND key = %s
               FOR UPDATE""",
            (subscriber_id, window_start, key),
        )
        row = cur.fetchone()
        prev = ((row["reported_version"], row["delivered_up_to"], row["status"])
                if row else None)
        if not dispatched:
            # A version this downstream never got from us (never sent, or
            # still held undelivered): the report cannot count. Trace the
            # rejection, change nothing.
            detail = ({"reason": "version_never_sent_to_subscriber"}
                      if sent is None else
                      {"reason": "version_not_yet_delivered",
                       "delivery_status": sent["status"]})
            record_posting_event(
                cur, subscriber_id, window_start, key, "REPORT_REJECTED",
                version, prev, prev or (None, None, None), detail)
            return None
        if row is None:
            # First report for this pair: seed delivered_up_to from the
            # outbox as it stands now (the subscriber lock above makes this
            # read race-free against concurrent deliveries).
            cur.execute(
                """SELECT MAX(version) AS d FROM deliveries
                   WHERE subscriber_id = %s AND window_start = %s
                     AND key = %s AND status = 'DELIVERED'""",
                (subscriber_id, window_start, key),
            )
            delivered = cur.fetchone()["d"]
            status = posting_status(version, delivered)
            cur.execute(
                """INSERT INTO posting_ledger
                       (subscriber_id, window_start, key,
                        reported_version, delivered_up_to, status)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (subscriber_id, window_start, key, version, delivered, status),
            )
        else:
            delivered = row["delivered_up_to"]
            status = posting_status(version, delivered)
            cur.execute(
                """UPDATE posting_ledger
                   SET reported_version = %s, status = %s, updated_at = now()
                   WHERE subscriber_id = %s AND window_start = %s AND key = %s""",
                (version, status, subscriber_id, window_start, key),
            )
        new = (version, delivered, status)
        record_posting_event(cur, subscriber_id, window_start, key,
                             "REPORT_ACCEPTED", version, prev, new)
        return {"subscriber_id": subscriber_id, "window_start": window_start,
                "key": key, "reported_version": version,
                "delivered_up_to": delivered, "status": status}


# ---------------------------------------------------------------------------
# reconciliation batches (对账批次)
# ---------------------------------------------------------------------------
# A batch is opened for one subscriber over one event-time range. Opening takes
# a single transactional photograph, under the subscriber row lock, of every
# result that has any delivery row for that subscriber in range: the snapshot
# columns on reconciliation_items (sent / delivered / in-flight / reported
# versions, the full delivery ladder, the downstream's rejected reports) are
# written once and never updated — later ledger or outbox movement lives only
# in the query-layer "live_*" columns, never overwrites the photograph.
#
# Every non-aligned item must be adjudicated one by one (CONFIRMED/REJECTED)
# before the batch can close; adjudication only writes the verdict columns and
# an append-only event — it never touches the posting ledger or the outbox.
# Closing is idempotent; once CLOSED the verdict columns are frozen too.

# Per-result snapshot aggregates, plus the version ladder and the downstream's
# rejected reports (the posting_events trail) frozen at opening time. All
# reads run in the caller's single transaction, which holds the subscriber row
# lock taken in create_reconciliation, so the photograph is one consistent
# state — a delivery cannot land between the ladder read and the ledger read.
RECON_SNAPSHOT_SQL = """
WITH sent AS (
    SELECT d.window_start, d.key, MAX(d.window_end) AS window_end,
           MAX(d.version) AS sent_version,
           MAX(d.version) FILTER (WHERE d.status = 'DELIVERED') AS delivered_version,
           MIN(d.version) FILTER (WHERE d.status <> 'DELIVERED') AS inflight_version
    FROM deliveries d
    WHERE d.subscriber_id = %s
      AND d.window_start >= %s AND d.window_start < %s
    GROUP BY d.window_start, d.key
),
ladder AS (
    SELECT d.window_start, d.key,
           jsonb_agg(jsonb_build_object(
                        'version', d.version, 'kind', d.kind,
                        'status', d.status, 'channel', d.channel,
                        'attempts', d.attempts, 'last_error', d.last_error,
                        'delivered_at', d.delivered_at)
                    ORDER BY d.version) AS delivery_ladder
    FROM deliveries d
    WHERE d.subscriber_id = %s
      AND d.window_start >= %s AND d.window_start < %s
    GROUP BY d.window_start, d.key
),
rej AS (
    SELECT e.window_start, e.key,
           jsonb_agg(jsonb_build_object(
                        'version', e.cause_version, 'detail', e.detail,
                        'created_at', e.created_at)
                    ORDER BY e.id) AS rejected_reports
    FROM posting_events e
    WHERE e.subscriber_id = %s AND e.event = 'REPORT_REJECTED'
      AND e.window_start >= %s AND e.window_start < %s
    GROUP BY e.window_start, e.key
)
SELECT s.window_start, s.key, s.window_end, s.sent_version,
       s.delivered_version, s.inflight_version, pl.reported_version,
       l.delivery_ladder, COALESCE(r.rejected_reports, '[]'::jsonb) AS rejected_reports
FROM sent s
JOIN ladder l ON l.window_start = s.window_start AND l.key = s.key
LEFT JOIN posting_ledger pl
  ON pl.subscriber_id = %s AND pl.window_start = s.window_start AND pl.key = s.key
LEFT JOIN rej r ON r.window_start = s.window_start AND r.key = s.key
ORDER BY s.window_start, s.key
"""


def create_reconciliation(conn, subscriber_id, from_ms, to_ms, created_by=None):
    """Open one reconciliation batch and materialize its immutable snapshot.

    The event-time range is snapped to tumbling-window boundaries exactly
    like backfill (normalize_backfill_range). Materialization, the overlap
    guard and the BATCH_OPENED event commit in one transaction that first
    locks the subscriber row — two concurrent opens for the same downstream
    serialize here and cannot both pass the overlap check. Only an OPEN batch
    blocks: a CLOSED batch over the same range never stops a new one.
    """
    from_ws, to_ws = normalize_backfill_range(from_ms, to_ms, WINDOW_MS)
    with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT id, name, url, active FROM subscribers WHERE id = %s FOR UPDATE",
            (subscriber_id,),
        )
        subscriber = cur.fetchone()
        if subscriber is None:
            raise HTTPException(404, "no such subscriber")
        cur.execute(
            """SELECT id, window_start_from, window_start_to
               FROM reconciliation_batches
               WHERE subscriber_id = %s AND status = 'OPEN'
               FOR UPDATE""",
            (subscriber_id,),
        )
        for row in cur.fetchall():
            if ranges_overlap(from_ws, to_ws,
                              row["window_start_from"], row["window_start_to"]):
                raise HTTPException(
                    409,
                    f"open reconciliation batch {row['id']} for this subscriber already "
                    f"covers [{row['window_start_from']}, {row['window_start_to']}), which "
                    f"overlaps the requested [{from_ws}, {to_ws}); close it first")
        cur.execute(
            """INSERT INTO reconciliation_batches
                   (subscriber_id, window_start_from, window_start_to, created_by)
               VALUES (%s, %s, %s, %s) RETURNING *""",
            (subscriber_id, from_ws, to_ws, created_by),
        )
        batch = cur.fetchone()
        cur.execute(RECON_SNAPSHOT_SQL,
                    (subscriber_id, from_ws, to_ws) * 3 + (subscriber_id,))
        items = cur.fetchall()
        counts = {s: 0 for s in ("ALIGNED", "LAGGING",
                                 "AHEAD_UNCONFIRMED", "NOT_REPORTED")}
        for r in items:
            item_status = reconciliation_item_status(
                r["sent_version"], r["delivered_version"],
                r["inflight_version"], r["reported_version"])
            counts[item_status] += 1
            cur.execute(
                """INSERT INTO reconciliation_items
                       (batch_id, window_start, key, window_end, sent_version,
                        delivered_version, inflight_version, reported_version,
                        item_status, delivery_ladder, rejected_reports)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (batch["id"], r["window_start"], r["key"], r["window_end"],
                 r["sent_version"], r["delivered_version"], r["inflight_version"],
                 r["reported_version"], item_status,
                 psycopg2.extras.Json(r["delivery_ladder"]),
                 psycopg2.extras.Json(r["rejected_reports"])),
            )
        cur.execute(
            """UPDATE reconciliation_batches
               SET total_items = %s, aligned_items = %s, lagging_items = %s,
                   ahead_items = %s, not_reported_items = %s
               WHERE id = %s RETURNING *""",
            (len(items), counts["ALIGNED"], counts["LAGGING"],
             counts["AHEAD_UNCONFIRMED"], counts["NOT_REPORTED"], batch["id"]),
        )
        batch = cur.fetchone()
        cur.execute(
            """INSERT INTO reconciliation_events (batch_id, event, operator, detail)
               VALUES (%s, 'BATCH_OPENED', %s, %s)""",
            (batch["id"], created_by,
             psycopg2.extras.Json({"window_start_from": from_ws,
                                   "window_start_to": to_ws,
                                   "total_items": len(items), **counts})),
        )
        # Snapshot rows as persisted — the immutable photograph the response
        # hands back, status classification included.
        cur.execute(
            """SELECT * FROM reconciliation_items WHERE batch_id = %s
               ORDER BY window_start, key""",
            (batch["id"],),
        )
        snapshot = cur.fetchall()
    log.info("reconciliation batch %s opened for subscriber=%s range=[%s,%s): %d items "
             "(aligned=%d lagging=%d ahead=%d not_reported=%d)",
             batch["id"], subscriber["name"], from_ws, to_ws, len(items),
             counts["ALIGNED"], counts["LAGGING"], counts["AHEAD_UNCONFIRMED"],
             counts["NOT_REPORTED"])
    return batch, snapshot


def get_reconciliation(cur, batch_id, for_update=False):
    cur.execute(
        f"""SELECT b.*, s.name AS subscriber, s.url,
                   b.total_items - b.aligned_items
                       - b.confirmed_items - b.rejected_items AS unresolved_items
            FROM reconciliation_batches b
            JOIN subscribers s ON s.id = b.subscriber_id
            WHERE b.id = %s{' FOR UPDATE' if for_update else ''}""",
        (batch_id,),
    )
    return cur.fetchone()


def set_reconciliation_decision(conn, batch_id, window_start, key, decision,
                                operator=None, note=None):
    """Adjudicate one non-aligned item: CONFIRMED (认账) or REJECTED (驳).

    Allowed only while the batch is OPEN; an ALIGNED item is never adjudicated
    ("对上的不用管"). A different verdict re-decides the item (every change is
    an event); the same verdict is an idempotent no-op. The batch counters are
    recomputed from the items table in the same transaction.
    """
    with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT status FROM reconciliation_batches WHERE id = %s FOR UPDATE",
                    (batch_id,))
        batch = cur.fetchone()
        if batch is None:
            raise HTTPException(404, "no such reconciliation batch")
        if batch["status"] != "OPEN":
            raise HTTPException(
                409,
                f"reconciliation batch {batch_id} is CLOSED — verdicts can no longer "
                "be recorded or changed (结掉之后认驳不能再改)")
        cur.execute(
            """SELECT item_status, decision FROM reconciliation_items
               WHERE batch_id = %s AND window_start = %s AND key = %s FOR UPDATE""",
            (batch_id, window_start, key),
        )
        item = cur.fetchone()
        if item is None:
            raise HTTPException(404, "no such item in this reconciliation batch")
        if item["item_status"] == "ALIGNED":
            raise HTTPException(
                409,
                f"({window_start}, {key!r}) was ALIGNED at batch opening — aligned items "
                "need no adjudication (对上的不用管)")
        prev_decision = item["decision"]
        if prev_decision != decision:
            cur.execute(
                """UPDATE reconciliation_items
                   SET decision = %s, decided_by = %s, decided_at = now()
                   WHERE batch_id = %s AND window_start = %s AND key = %s""",
                (decision, operator, batch_id, window_start, key),
            )
            cur.execute(
                """UPDATE reconciliation_batches b SET
                       confirmed_items = (SELECT count(*) FROM reconciliation_items i
                                          WHERE i.batch_id = b.id AND i.decision = 'CONFIRMED'),
                       rejected_items  = (SELECT count(*) FROM reconciliation_items i
                                          WHERE i.batch_id = b.id AND i.decision = 'REJECTED')
                   WHERE b.id = %s""",
                (batch_id,),
            )
            cur.execute(
                """INSERT INTO reconciliation_events
                       (batch_id, event, window_start, key, item_status,
                        decision, prev_decision, operator, note)
                   VALUES (%s, 'ITEM_DECIDED', %s, %s, %s, %s, %s, %s, %s)""",
                (batch_id, window_start, key, item["item_status"], decision,
                 prev_decision, operator, note),
            )
        batch = get_reconciliation(cur, batch_id)
        cur.execute(
            """SELECT * FROM reconciliation_items
               WHERE batch_id = %s AND window_start = %s AND key = %s""",
            (batch_id, window_start, key),
        )
        updated_item = cur.fetchone()
    return batch, updated_item


def close_reconciliation(conn, batch_id, operator=None):
    """Close a batch once every non-aligned item carries a verdict.

    Fails 409 while any LAGGING / AHEAD_UNCONFIRMED / NOT_REPORTED item is
    still undecided — one undecided row keeps the whole batch open. Closing an
    already-closed batch is an idempotent no-op (its frozen verdicts are
    returned unchanged, no extra event is written).
    """
    with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM reconciliation_batches WHERE id = %s FOR UPDATE",
                    (batch_id,))
        batch = cur.fetchone()
        if batch is None:
            raise HTTPException(404, "no such reconciliation batch")
        if batch["status"] == "CLOSED":
            return get_reconciliation(cur, batch_id)
        cur.execute(
            """SELECT window_start, key, item_status, decision
               FROM reconciliation_items WHERE batch_id = %s FOR UPDATE""",
            (batch_id,),
        )
        items = cur.fetchall()
        unresolved = [
            (r["window_start"], r["key"], r["item_status"])
            for r in items
            if r["item_status"] != "ALIGNED" and r["decision"] is None
        ]
        if unresolved:
            raise HTTPException(
                409,
                f"reconciliation batch {batch_id} still has {len(unresolved)} undecided "
                f"non-aligned item(s): every LAGGING / AHEAD_UNCONFIRMED / NOT_REPORTED "
                "row must be CONFIRMED or REJECTED before closing "
                "(有一条没处理完这批不能结)")
        cur.execute(
            """UPDATE reconciliation_batches
               SET status = 'CLOSED', closed_at = now(), closed_by = %s
               WHERE id = %s RETURNING *""",
            (operator, batch_id),
        )
        cur.execute(
            """INSERT INTO reconciliation_events (batch_id, event, operator, detail)
               VALUES (%s, 'BATCH_CLOSED', %s, %s)""",
            (batch_id, operator,
             psycopg2.extras.Json({"total_items": batch["total_items"],
                                   "confirmed_items": batch["confirmed_items"],
                                   "rejected_items": batch["rejected_items"]})),
        )
        closed = get_reconciliation(cur, batch_id)
    log.info("reconciliation batch %s closed by %s (%d items)",
             batch_id, operator, batch["total_items"])
    return closed


# Live (post-opening) state shown next to the frozen snapshot in item queries.
# Correlated subqueries read the outbox/ledger as they stand NOW; they never
# write back to reconciliation_items, so the photograph cannot be retaken.
RECON_ITEMS_SQL = """
SELECT i.*, b.subscriber_id, s.name AS subscriber,
       (SELECT max(d.version) FROM deliveries d
         WHERE d.subscriber_id = b.subscriber_id
           AND d.window_start = i.window_start AND d.key = i.key) AS live_sent_version,
       (SELECT max(d.version) FILTER (WHERE d.status = 'DELIVERED') FROM deliveries d
         WHERE d.subscriber_id = b.subscriber_id
           AND d.window_start = i.window_start AND d.key = i.key) AS live_delivered_version,
       (SELECT min(d.version) FILTER (WHERE d.status <> 'DELIVERED') FROM deliveries d
         WHERE d.subscriber_id = b.subscriber_id
           AND d.window_start = i.window_start AND d.key = i.key) AS live_inflight_version,
       pl.reported_version AS live_reported_version
FROM reconciliation_items i
JOIN reconciliation_batches b ON b.id = i.batch_id
JOIN subscribers s ON s.id = b.subscriber_id
LEFT JOIN posting_ledger pl
  ON pl.subscriber_id = b.subscriber_id
 AND pl.window_start = i.window_start AND pl.key = i.key
WHERE i.batch_id = %s
{extra_conds}
ORDER BY i.window_start, i.key
LIMIT %s
"""


def reconcile_item_live(row):
    """Annotate a persisted item row with the live state beside its frozen
    snapshot: the live four-bucket classification and whether either the raw
    versions or the bucket moved since opening."""
    live_status = reconciliation_item_status(
        row["live_sent_version"], row["live_delivered_version"],
        row["live_inflight_version"], row["live_reported_version"])
    frozen = (row["sent_version"], row["delivered_version"],
              row["inflight_version"], row["reported_version"])
    live = (row["live_sent_version"], row["live_delivered_version"],
            row["live_inflight_version"], row["live_reported_version"])
    row["live_item_status"] = live_status
    row["drifted"] = frozen != live
    return row


# ---------------------------------------------------------------------------
# delivery dispatcher (transactional outbox)
# ---------------------------------------------------------------------------

# A delivery is due only when every earlier version of the same result for the
# same subscriber is DELIVERED — version N+1 must never reach a downstream
# before version N. The ordering barrier spans BOTH channels: a realtime
# correction born while a historical replay is queued waits behind the
# backfilled earlier versions of that same result, so the earliest version
# always arrives first. The barrier is scoped to one (window, key) on purpose:
# different results proceed independently and must never block one another —
# adding a subscriber-wide live barrier on top of cross-channel ordering would
# close a cycle (a backfilled later version waiting behind live traffic that
# is itself waiting on an earlier backfill), deadlocking every chain involved.
# REALTIME is still dispatched first on every poll, so history never jumps
# ahead of live traffic that is already due; it simply does not wait on live
# rows of a *different* result.
DUE_SQL = """
SELECT d.id, d.subscriber_id, s.name AS subscriber, s.url,
       d.window_start, d.window_end, d.key, d.version, d.kind, d.payload,
       d.attempts, d.created_at, d.channel, d.backfill_job_id
FROM deliveries d
JOIN subscribers s ON s.id = d.subscriber_id
WHERE s.active
  AND d.channel = 'REALTIME'
  AND d.status IN ('PENDING', 'RETRYING')
  AND d.next_attempt_at <= now()
  AND NOT EXISTS (
      -- cross-channel version order: an earlier version of this result,
      -- whether REALTIME or BACKFILL, must be delivered first
      SELECT 1 FROM deliveries p
      WHERE p.subscriber_id = d.subscriber_id
        AND p.window_start = d.window_start
        AND p.key = d.key
        AND p.version < d.version
        AND p.status <> 'DELIVERED'
  )
  AND NOT EXISTS (
      -- posting-ledger flow control: this downstream's posted position lags
      -- what we already delivered for THIS result — hold its later versions
      -- until it reports catching up. Scoped to one (window, key): other
      -- results of the same downstream are never held back by this one.
      SELECT 1 FROM posting_ledger pl
      WHERE pl.subscriber_id = d.subscriber_id
        AND pl.window_start = d.window_start
        AND pl.key = d.key
        AND pl.status = 'LAGGING'
  )
ORDER BY d.id
LIMIT %s
"""

BACKFILL_DUE_SQL = """
SELECT d.id, d.subscriber_id, s.name AS subscriber, s.url,
       d.window_start, d.window_end, d.key, d.version, d.kind, d.payload,
       d.attempts, d.created_at, d.channel, d.backfill_job_id
FROM deliveries d
JOIN subscribers s ON s.id = d.subscriber_id
JOIN backfill_jobs j ON j.id = d.backfill_job_id
WHERE s.active
  AND j.status = 'RUNNING'
  AND d.channel = 'BACKFILL'
  AND d.status IN ('PENDING', 'RETRYING')
  AND d.next_attempt_at <= now()
  -- Per-result version order across both channels: an earlier version of this
  -- same result still pending (including a realtime one) must go first. The
  -- barrier stays scoped to one (window, key): a different result's live
  -- traffic neither overtakes nor blocks this one, and cycles across results
  -- are impossible.
  AND NOT EXISTS (
      SELECT 1 FROM deliveries p
      WHERE p.subscriber_id = d.subscriber_id
        AND p.window_start = d.window_start
        AND p.key = d.key
        AND p.version < d.version
        AND p.status <> 'DELIVERED'
  )
  AND NOT EXISTS (
      -- the posting-ledger gate applies to replays exactly as to live
      -- traffic: a downstream behind on posting this result gets no further
      -- versions of it, whichever channel they ride
      SELECT 1 FROM posting_ledger pl
      WHERE pl.subscriber_id = d.subscriber_id
        AND pl.window_start = d.window_start
        AND pl.key = d.key
        AND pl.status = 'LAGGING'
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
        "channel": row.get("channel", "REALTIME"),
        "backfill_job_id": row.get("backfill_job_id"),
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
        # Lock the subscriber row before touching the ledger: a concurrent
        # first report for this subscriber seeds its ledger row under the
        # same lock, so the two can never miss each other's write.
        cur.execute("SELECT 1 FROM subscribers WHERE id = %s FOR UPDATE",
                    (row["subscriber_id"],))
        advance_posting_ledger(cur, row)
    log.info("delivery %d (%s %s v%d) to %s ok (attempt %d)",
             row["id"], row["kind"], row["key"], row["version"],
             row["subscriber"], attempts)
    return True


def dispatch_due(conn):
    """Deliver everything currently due, draining version chains in one pass.

    REALTIME is dispatched first on every iteration (backfill is only reached
    when no live row is due), so a large replay never starves live traffic.
    Version order is enforced per (window, key) across both channels — a
    success may unlock the next version of the same result even if that next
    version rides the other channel; different results never block each other.
    """
    for _ in range(20):
        progressed = False
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(DUE_SQL, (DISPATCH_BATCH,))
            live_rows = cur.fetchall()
        for row in live_rows:
            if deliver_one(conn, row):
                progressed = True
        if live_rows:
            continue

        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(BACKFILL_DUE_SQL, (DISPATCH_BATCH,))
            backfill_rows = cur.fetchall()
        for row in backfill_rows:
            if deliver_one(conn, row):
                progressed = True
        with conn, conn.cursor() as cur:
            complete_finished_backfill_jobs(cur)
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
        with conn, conn.cursor() as cur:
            wms = current_watermarks(cur)
    finally:
        conn.close()
    values = {s: i["watermark"] for s, i in wms.items()}
    ready = len(values) == len(INGESTS) and all(v is not None for v in values.values())
    return {
        "streams": values,
        "sources": {s: i["source"] for s, i in wms.items()},
        "min_watermark": min(values.values()) if ready else None,
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
    """Head version of results. With window_start+key, returns a single result.

    ``released`` says whether this (window, key) has been given externally at
    least once — a held result is computed, audited and queryable, but has no
    outbox deliveries yet."""
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT DISTINCT ON (r.window_start, r.key) r.*,
                          (w.window_start IS NOT NULL) AS released
                   FROM results r
                   LEFT JOIN window_releases w
                     ON w.window_start = r.window_start AND w.key = r.key
                   ORDER BY r.window_start, r.key, r.version DESC"""
            )
            heads = cur.fetchall()
    finally:
        conn.close()
    heads = [h for h in heads if h["status"] != "SUPERSEDED"]
    if window_start is not None:
        heads = [h for h in heads if h["window_start"] == window_start]
    if key is not None:
        heads = [h for h in heads if h["key"] == key]
    for h in heads:
        h["released"] = bool(h["released"])
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
    """All known (window, key) pairs with their alignment state. ``closed``
    is per key: both of that key's own sides have crossed the window end."""
    conn = connect()
    try:
        with conn:
            with conn.cursor() as cur:
                wms = current_watermarks(cur)
                key_max = key_max_event_times(cur)
                sides = effective_data_sides(cur)
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """SELECT key, (event_time / %s) * %s AS window_start,
                              count(*) FILTER (WHERE type = 'upsert') AS upserts,
                              count(*) FILTER (WHERE type = 'retract') AS retracts
                       FROM stream_events GROUP BY key, window_start ORDER BY window_start, key""",
                    (WINDOW_MS, WINDOW_MS),
                )
                rows = cur.fetchall()
                cur.execute(
                    """SELECT DISTINCT ON (r.window_start, r.key)
                              r.window_start, r.key, r.version, r.status,
                              (w.window_start IS NOT NULL) AS released
                       FROM results r
                       LEFT JOIN window_releases w
                         ON w.window_start = r.window_start AND w.key = r.key
                       ORDER BY r.window_start, r.key, r.version DESC"""
                )
                heads = {(r["window_start"], r["key"]): r for r in cur.fetchall()}
                cur.execute("SELECT key, open FROM release_gates")
                gates = {r[0]: r[1] for r in cur.fetchall()}
    finally:
        conn.close()
    values = [i["watermark"] for i in wms.values()]
    min_wm = min(values) if len(values) == len(INGESTS) and all(
        v is not None for v in values) else None
    out = []
    for r in rows:
        ws = r["window_start"]
        marks = emission_marks(wms, key_max, sides, r["key"], ws)
        head = heads.get((ws, r["key"]))
        out.append({
            "window_start": ws,
            "window_end": ws + WINDOW_MS,
            "key": r["key"],
            "upserts": r["upserts"],
            "retracts": r["retracts"],
            "closed": window_ready(marks["a"], marks["b"], ws + WINDOW_MS),
            "head_version": head["version"] if head else None,
            "head_status": head["status"] if head else None,
            "released": bool(head and head["released"]),
            "gate_open": gates.get(r["key"], False),
        })
    return {"windows": out, "min_watermark": min_wm}


# ---------------------------------------------------------------------------
# business order queries
# ---------------------------------------------------------------------------

@app.get("/orders")
def orders(status: Optional[str] = None, key: Optional[str] = None):
    """Head state of every business order. ``status`` filters the lifecycle
    state (OPEN / WAITING / CLOSED / REOPENED / VOID) — success orders are
    exactly the CLOSED ones; VOID orders never count as successes."""
    if status is not None and status not in ORDER_STATUSES:
        raise HTTPException(422, f"status must be one of {ORDER_STATUSES}")
    sql = """SELECT o.id, o.key, o.status, o.head_version, o.ever_closed,
                    o.created_at, o.updated_at,
                    count(w.order_id) AS windows,
                    count(w.order_id) FILTER (WHERE w.result_status = 'CURRENT') AS live_windows,
                    count(w.order_id) FILTER (WHERE w.has_gap) AS gap_windows
             FROM biz_orders o
             LEFT JOIN biz_order_windows w ON w.order_id = o.id"""
    conds, args = [], []
    if status is not None:
        conds.append("o.status = %s")
        args.append(status)
    if key is not None:
        conds.append("o.key = %s")
        args.append(key)
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " GROUP BY o.id ORDER BY o.id"
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, args)
            rows = cur.fetchall()
    finally:
        conn.close()
    return {"orders": rows}


@app.get("/orders/current")
def orders_current(key: str):
    """One order as it stands now: which result version each window entered
    at, plus the business windows still missing or pending."""
    conn = connect()
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM biz_orders WHERE key = %s", (key,))
                order = cur.fetchone()
                if order is None:
                    return {"order": None}
                cur.execute(
                    """SELECT window_start, window_end, result_version, result_status,
                              has_gap, match_count, unmatched_a, unmatched_b, payload_hash
                       FROM biz_order_windows WHERE order_id = %s ORDER BY window_start""",
                    (order["id"],),
                )
                windows = cur.fetchall()
            # gap helpers expect tuple rows — use a plain cursor here
            with conn.cursor() as cur:
                missing, pending = order_window_gaps(
                    cur, key, {w["window_start"] for w in windows})
    finally:
        conn.close()
    return {"order": {**order, "windows": windows,
                      "missing_windows": missing, "pending_windows": pending}}


@app.get("/orders/history")
def orders_history(key: str):
    """Every version of one order: status, why it changed, which window and
    which result version triggered it, and the full per-window snapshot as of
    that version (the close-time shape is preserved across reopens/voids)."""
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM biz_orders WHERE key = %s", (key,))
            order = cur.fetchone()
            if order is None:
                return {"order": None, "versions": []}
            cur.execute(
                """SELECT version, status, reason, trigger_window_start,
                          trigger_result_version, trigger_result_id, snapshot,
                          missing_windows, pending_windows, created_at
                   FROM biz_order_versions WHERE order_id = %s ORDER BY version""",
                (order["id"],),
            )
            versions = cur.fetchall()
    finally:
        conn.close()
    return {"order": order, "versions": versions}


# ---------------------------------------------------------------------------
# external release gate (对外放行)
# ---------------------------------------------------------------------------

class ReleaseIn(BaseModel):
    key: str = Field(min_length=1, max_length=200)
    window_starts: Optional[list[int]] = Field(
        default=None,
        description="only release these held windows; default = every computed-"
                    "but-unreleased window of this key")


class GateIn(BaseModel):
    key: str = Field(min_length=1, max_length=200)
    open: bool


class BackfillIn(BaseModel):
    subscriber_id: Optional[int] = None
    subscriber_name: Optional[str] = Field(default=None, min_length=1, max_length=200)
    from_window_start: int = Field(
        ge=0,
        description="event-time lower bound (inclusive); snapped DOWN to the "
                    "start of the window containing it")
    to_window_start: int = Field(
        ge=0,
        description="event-time upper bound (exclusive); snapped UP to the end "
                    "of the last window it reaches into")


@app.post("/releases")
def release(body: ReleaseIn):
    """One-shot external release for ONE business key.

    Sends exactly the version that is head now for each still-unreleased,
    currently-live window of the key (intermediate held versions are skipped,
    not sent). Other keys stay held; windows fully retracted before ever being
    released send nothing. Releasing the same key again only flushes windows
    that are still held — what already went out is never taken back.
    """
    ws = body.window_starts
    if ws is not None:
        if not ws:
            raise HTTPException(422, "window_starts must be non-empty when given")
        if len(ws) != len(set(ws)):
            raise HTTPException(422, "window_starts must not repeat windows")
    conn = connect()
    try:
        released = release_key(conn, body.key, ws)
    finally:
        conn.close()
    return {"key": body.key, "released": released,
            "count": len(released)}


@app.post("/release-gates")
def gate(body: GateIn):
    """Open/close the per-key release switch.

    Opening immediately flushes the key's current held backlog (at its current
    heads) and lets subsequent first versions flow on their own; closing holds
    back only windows that have never been released — corrections and
    withdrawals of released windows keep being delivered, and the open/close
    history is recorded, never reclaimed.
    """
    conn = connect()
    try:
        released = set_gate(conn, body.key, body.open)
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM release_gates WHERE key = %s", (body.key,))
            gate_row = cur.fetchone()
    finally:
        conn.close()
    return {"gate": gate_row, "released": released, "count": len(released)}


@app.get("/release-gates")
def release_gates_list(key: Optional[str] = None):
    """Per-key gate state plus counters of held (computed but not released)
    windows and windows already given externally."""
    sql = """
    SELECT g.key, g.open, g.opened_at, g.closed_at, g.updated_at,
           (SELECT count(*) FROM window_releases w WHERE w.key = g.key) AS released_windows,
           (SELECT count(*) FROM results r
             WHERE r.key = g.key AND r.status IN ('CURRENT','RETRACTED')
               AND NOT EXISTS (SELECT 1 FROM window_releases w
                               WHERE w.window_start = r.window_start AND w.key = r.key))
             AS held_windows
    FROM release_gates g"""
    args = []
    if key is not None:
        sql += " WHERE g.key = %s"
        args.append(key)
    sql += " ORDER BY g.key"
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, args)
            rows = cur.fetchall()
    finally:
        conn.close()
    return {"gates": rows}


@app.get("/releases/backlog")
def release_backlog(key: Optional[str] = None):
    """What the gate is currently holding: the head version of every computed
    window with no external release yet (live heads are releasable now; heads
    retracted while held send nothing until they revive)."""
    sql = """
    SELECT r.key, r.window_start, r.window_end, r.version, r.status,
           g.open AS gate_open,
           (r.payload IS NOT NULL) AS releasable_now
    FROM results r
    JOIN release_gates g ON g.key = r.key
    WHERE r.status IN ('CURRENT','RETRACTED')
      AND NOT EXISTS (SELECT 1 FROM window_releases w
                      WHERE w.window_start = r.window_start AND w.key = r.key)"""
    args = []
    if key is not None:
        sql += " AND r.key = %s"
        args.append(key)
    sql += " ORDER BY r.key, r.window_start"
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, args)
            rows = cur.fetchall()
    finally:
        conn.close()
    return {"held": rows}


@app.get("/releases/history")
def release_history(key: str, limit: int = Query(default=100)):
    """Audit of every release action for a key (explicit releases, gate-open
    flushes, gate closes) plus the immutable per-window release ledger."""
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM release_actions WHERE key = %s ORDER BY id DESC LIMIT %s",
                (key, min(max(limit, 1), 1000)),
            )
            actions = cur.fetchall()
            cur.execute(
                """SELECT window_start, window_end, result_version, action, released_at
                   FROM window_releases WHERE key = %s ORDER BY window_start""",
                (key,),
            )
            ledger = cur.fetchall()
    finally:
        conn.close()
    return {"key": key, "actions": actions, "released_windows": ledger}


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

    Re-posting an existing name updates its URL and re-activates it. New
    versions are delivered in the REALTIME channel. Already-released history
    can be replayed explicitly with POST /backfills; held/internal versions
    are never part of that replay.
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


def backfill_job_json(cur, job):
    """A job row plus live counts from its copied delivery rows."""
    cur.execute(
        """SELECT count(*) FILTER (WHERE status = 'DELIVERED') AS delivered,
                  count(*) FILTER (WHERE status IN ('PENDING', 'RETRYING')) AS pending,
                  count(*) FILTER (WHERE status = 'RETRYING') AS retrying
           FROM deliveries WHERE backfill_job_id = %s""",
        (job["id"],),
    )
    counts = cur.fetchone()
    return {
        **job,
        "delivered_versions": counts["delivered"],
        "pending_versions": counts["pending"],
        "retrying_versions": counts["retrying"],
        "unfinished_versions": counts["pending"],
    }


@app.post("/backfills")
def create_backfill_endpoint(body: BackfillIn):
    """Replay externally released history for one registered downstream.

    Both bounds speak event time and are snapped to tumbling-window
    boundaries: the lower bound is floored to its window's start and the
    exclusive upper bound is ceiled to the end of the last window it reaches
    into, so asking for a time *inside* a window replays the whole window.
    The stored job range is the resulting half-open
    ``[from_window_start, to_window_start)``. Only versions at or after each
    window's first externally released version are copied; internal versions
    held before release are not. The copied versions keep their original
    payload, kind and version timestamp, and are dispatched in the BACKFILL
    channel behind any unfinished live delivery for this subscriber. A later
    realtime correction of a result whose earlier version is still in this
    queue waits behind that history — versions of one result always reach the
    downstream in version order.
    """
    if body.to_window_start <= body.from_window_start:
        raise HTTPException(422, "to_window_start must be greater than from_window_start")
    if (body.subscriber_id is None) == (body.subscriber_name is None):
        raise HTTPException(422, "provide exactly one of subscriber_id or subscriber_name")
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if body.subscriber_id is not None:
                cur.execute("SELECT id FROM subscribers WHERE id = %s",
                            (body.subscriber_id,))
            else:
                cur.execute("SELECT id FROM subscribers WHERE name = %s",
                            (body.subscriber_name,))
            row = cur.fetchone()
            if row is None:
                raise HTTPException(404, "no such subscriber")
            subscriber_id = row["id"]
        try:
            job = create_backfill(conn, subscriber_id,
                                  body.from_window_start, body.to_window_start)
        except psycopg2.errors.UniqueViolation:
            raise HTTPException(409, "an active backfill job already exists for this subscriber")
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            payload = backfill_job_json(cur, job)
    finally:
        conn.close()
    log.info("backfill job %s created for subscriber=%s range=[%s,%s): %d versions",
             payload["id"], payload["subscriber_id"],
             payload["window_start_from"], payload["window_start_to"],
             payload["total_versions"])
    return {"backfill": payload}


@app.get("/backfills")
def list_backfills(subscriber_id: Optional[int] = None,
                   subscriber: Optional[str] = None,
                   status: Optional[str] = None,
                   limit: int = Query(default=100)):
    if status is not None and status not in ("RUNNING", "PAUSED", "COMPLETED"):
        raise HTTPException(422, "status must be RUNNING, PAUSED or COMPLETED")
    sql = """SELECT j.* FROM backfill_jobs j
             JOIN subscribers s ON s.id = j.subscriber_id"""
    conds, args = [], []
    if subscriber_id is not None:
        conds.append("j.subscriber_id = %s")
        args.append(subscriber_id)
    if subscriber is not None:
        conds.append("s.name = %s")
        args.append(subscriber)
    if status is not None:
        conds.append("j.status = %s")
        args.append(status)
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY j.id DESC LIMIT %s"
    args.append(min(max(limit, 1), 1000))
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, args)
            jobs = [backfill_job_json(cur, row) for row in cur.fetchall()]
    finally:
        conn.close()
    return {"backfills": jobs}


@app.get("/backfills/{job_id}")
def get_backfill(job_id: int):
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            job = get_backfill_job(cur, job_id)
            payload = backfill_job_json(cur, job) if job else None
    finally:
        conn.close()
    if payload is None:
        raise HTTPException(404, "no such backfill job")
    return {"backfill": payload}


@app.post("/backfills/{job_id}/stop")
def stop_backfill_endpoint(job_id: int):
    conn = connect()
    try:
        job = stop_backfill(conn, job_id)
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            payload = backfill_job_json(cur, job)
    finally:
        conn.close()
    return {"backfill": payload}


@app.post("/backfills/{job_id}/resume")
def resume_backfill_endpoint(job_id: int):
    conn = connect()
    try:
        job = resume_backfill(conn, job_id)
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            payload = backfill_job_json(cur, job)
    finally:
        conn.close()
    return {"backfill": payload}


DELIVERY_COLS = """d.id, s.name AS subscriber, s.url, d.window_start, d.window_end, d.key,
                   d.version, d.kind, d.status, d.attempts, d.last_error,
                   d.next_attempt_at, d.last_attempt_at, d.delivered_at, d.created_at,
                   d.channel, d.backfill_job_id"""


@app.get("/deliveries")
def deliveries(window_start: Optional[int] = None, key: Optional[str] = None,
               subscriber: Optional[str] = None, status: Optional[str] = None,
               channel: Optional[str] = None, backfill_job_id: Optional[int] = None,
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
    if channel is not None:
        if channel not in ("REALTIME", "BACKFILL"):
            raise HTTPException(422, "channel must be REALTIME or BACKFILL")
        conds.append("d.channel = %s")
        args.append(channel)
    if backfill_job_id is not None:
        conds.append("d.backfill_job_id = %s")
        args.append(backfill_job_id)
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
            cur.execute(
                """SELECT result_version, action, released_at FROM window_releases
                   WHERE window_start = %s AND key = %s""",
                (window_start, key),
            )
            released = cur.fetchone()
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
            "channel": r["channel"], "backfill_job_id": r["backfill_job_id"],
        })
    for sub in by_sub.values():
        for v in sub["versions"]:
            if v["status"] != "DELIVERED":
                break
            sub["delivered_up_to"] = v["version"]
    return {"window_start": window_start, "key": key,
            "released": released is not None,
            "released_version": released["result_version"] if released else None,
            "released_at": released["released_at"] if released else None,
            "subscribers": list(by_sub.values())}


# ---------------------------------------------------------------------------
# downstream posting ledger (下游入账台账)
# ---------------------------------------------------------------------------

class PostingIn(BaseModel):
    subscriber_id: Optional[int] = None
    subscriber_name: Optional[str] = Field(default=None, min_length=1, max_length=200)
    window_start: int = Field(ge=0)
    key: str = Field(min_length=1, max_length=200)
    version: int = Field(
        ge=1,
        description="the result version the downstream says it has posted (入账)")


@app.post("/postings", status_code=200)
def posting_report(body: PostingIn):
    """A downstream reports which version of one result it has posted.

    The report only counts when it names a version that actually went out to
    this downstream: DELIVERED, or RETRYING (dispatched, the response was
    lost — it may genuinely have posted it). Reporting a version that never
    reached it — never sent at all, or still PENDING (held by the ledger
    gate) — is rejected with 409: it changes nothing, though the rejection
    itself is traced in the ledger history. A report below the current posted
    position is a rollback: the ledger regresses with it (already-delivered
    records are never erased), and the pair turns LAGGING until its posting
    catches up again. Re-registering the downstream under a new URL does not
    move the ledger — it is keyed by the subscriber, not the address.
    """
    if (body.subscriber_id is None) == (body.subscriber_name is None):
        raise HTTPException(422, "provide exactly one of subscriber_id or subscriber_name")
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            if body.subscriber_id is not None:
                cur.execute("SELECT id FROM subscribers WHERE id = %s",
                            (body.subscriber_id,))
            else:
                cur.execute("SELECT id FROM subscribers WHERE name = %s",
                            (body.subscriber_name,))
            row = cur.fetchone()
            if row is None:
                raise HTTPException(404, "no such subscriber")
            subscriber_id = row[0]
        posting = report_posting(conn, subscriber_id, body.window_start,
                                 body.key, body.version)
    finally:
        conn.close()
    if posting is None:
        raise HTTPException(
            409,
            f"version {body.version} of ({body.window_start}, {body.key!r}) "
            "has not been delivered to this subscriber (never sent, or still "
            "held undelivered); the report does not count")
    return {"posting": posting}


@app.get("/postings")
def postings(subscriber: Optional[str] = None, key: Optional[str] = None,
             window_start: Optional[int] = None, status: Optional[str] = None,
             limit: int = Query(default=200)):
    """The posting ledger, one row per (downstream, result) ever sent to:
    which version the downstream reported posted, which version we confirmed
    delivered, and whether the pair is ALIGNED, LAGGING (its later versions
    are currently held for this downstream) or AHEAD_UNCONFIRMED (it posted a
    version we are still retrying). Pairs with no report yet have a null
    status/reported_version — the gate only engages once a downstream starts
    reporting."""
    if status is not None and status not in POSTING_STATUSES:
        raise HTTPException(422, f"status must be one of {POSTING_STATUSES}")
    sql = """
    WITH sent AS (
        SELECT d.subscriber_id, d.window_start, d.key,
               MAX(d.window_end) AS window_end,
               MAX(d.version) AS sent_up_to,
               MAX(d.version) FILTER (WHERE d.status = 'DELIVERED') AS delivered_up_to,
               MIN(d.version) FILTER (WHERE d.status <> 'DELIVERED') AS inflight_version
        FROM deliveries d
        GROUP BY d.subscriber_id, d.window_start, d.key
    )
    SELECT s.id AS subscriber_id, s.name AS subscriber, s.url, s.active,
           sent.window_start, sent.window_end, sent.key,
           pl.reported_version,
           COALESCE(pl.delivered_up_to, sent.delivered_up_to) AS delivered_up_to,
           sent.sent_up_to, sent.inflight_version,
           pl.status,
           (pl.status = 'LAGGING') AS gated,
           pl.first_reported_at, pl.updated_at AS ledger_updated_at
    FROM sent
    JOIN subscribers s ON s.id = sent.subscriber_id
    LEFT JOIN posting_ledger pl
      ON pl.subscriber_id = sent.subscriber_id
     AND pl.window_start = sent.window_start
     AND pl.key = sent.key"""
    conds, args = [], []
    if subscriber is not None:
        conds.append("s.name = %s")
        args.append(subscriber)
    if key is not None:
        conds.append("sent.key = %s")
        args.append(key)
    if window_start is not None:
        conds.append("sent.window_start = %s")
        args.append(window_start)
    if status is not None:
        conds.append("pl.status = %s")
        args.append(status)
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY s.name, sent.key, sent.window_start LIMIT %s"
    args.append(min(max(limit, 1), 1000))
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, args)
            rows = cur.fetchall()
    finally:
        conn.close()
    for r in rows:
        r["gated"] = bool(r["gated"])
    return {"postings": rows}


@app.get("/postings/history")
def posting_history(subscriber: Optional[str] = None,
                    subscriber_id: Optional[int] = None,
                    window_start: Optional[int] = None, key: Optional[str] = None,
                    limit: int = Query(default=200)):
    """The append-only ledger trail: every report (accepted or rejected) and
    every delivery advance, each with the ledger state before and after. This
    is where "when did it flip from ALIGNED to LAGGING, and which version
    knocked it lagging" is answered — look for DELIVERY_ADVANCED rows with
    prev_status='ALIGNED' and status='LAGGING'; cause_version is the culprit.
    Fully filtered (one downstream, one result) the trail comes back
    chronological; otherwise it is the global feed, newest first."""
    sql = """SELECT e.id, s.name AS subscriber, e.subscriber_id, e.window_start,
                    e.key, e.event, e.cause_version,
                    e.prev_reported, e.prev_delivered, e.prev_status,
                    e.reported_version, e.delivered_up_to, e.status,
                    e.detail, e.created_at
             FROM posting_events e JOIN subscribers s ON s.id = e.subscriber_id"""
    conds, args = [], []
    if subscriber is not None:
        conds.append("s.name = %s")
        args.append(subscriber)
    if subscriber_id is not None:
        conds.append("e.subscriber_id = %s")
        args.append(subscriber_id)
    if window_start is not None:
        conds.append("e.window_start = %s")
        args.append(window_start)
    if key is not None:
        conds.append("e.key = %s")
        args.append(key)
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    pair_scoped = ((subscriber is not None or subscriber_id is not None)
                   and window_start is not None and key is not None)
    sql += " ORDER BY e.id" + ("" if pair_scoped else " DESC") + " LIMIT %s"
    args.append(min(max(limit, 1), 1000))
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, args)
            rows = cur.fetchall()
    finally:
        conn.close()
    return {"events": rows}


# ---------------------------------------------------------------------------
# reconciliation batches (对账批次)
# ---------------------------------------------------------------------------

class ReconciliationIn(BaseModel):
    subscriber_id: Optional[int] = None
    subscriber_name: Optional[str] = Field(default=None, min_length=1, max_length=200)
    from_window_start: int = Field(
        ge=0,
        description="event-time lower bound (inclusive); snapped DOWN to the "
                    "start of the window containing it")
    to_window_start: int = Field(
        ge=0,
        description="event-time upper bound (exclusive); snapped UP to the end "
                    "of the last window it reaches into")
    operator: Optional[str] = Field(default=None, max_length=200)


class ReconciliationDecisionIn(BaseModel):
    window_start: int = Field(ge=0)
    key: str = Field(min_length=1, max_length=200)
    decision: str = Field(description="CONFIRMED = 认账, REJECTED = 驳")
    operator: Optional[str] = Field(default=None, max_length=200)
    note: Optional[str] = Field(default=None, max_length=2000)


class ReconciliationCloseIn(BaseModel):
    operator: Optional[str] = Field(default=None, max_length=200)


def resolve_subscriber(cur, subscriber_id=None, subscriber_name=None):
    """Resolve the subscriber id from the id/name pair, validating that
    exactly one was given. Raises 404/422 like the posting/backfill paths."""
    if (subscriber_id is None) == (subscriber_name is None):
        raise HTTPException(422, "provide exactly one of subscriber_id or subscriber_name")
    if subscriber_id is not None:
        cur.execute("SELECT id FROM subscribers WHERE id = %s", (subscriber_id,))
    else:
        cur.execute("SELECT id FROM subscribers WHERE name = %s", (subscriber_name,))
    row = cur.fetchone()
    if row is None:
        raise HTTPException(404, "no such subscriber")
    return row["id"]


@app.post("/reconciliations", status_code=201)
def reconciliation_open(body: ReconciliationIn):
    """Open a reconciliation batch for one downstream over an event-time range.

    At opening, every result the downstream has a delivery row for in range is
    photographed once — sent / delivered / in-flight / reported versions, the
    per-version delivery ladder and the downstream's rejected reports. That
    photograph is immutable: later posting-ledger or outbox movement never
    rewrites it (item queries additionally show the live state beside it). The
    range is snapped to window boundaries the same way backfill ranges are.
    Two OPEN batches of the same subscriber may not cover overlapping windows
    (409); a CLOSED batch never blocks a new one.
    """
    if body.to_window_start <= body.from_window_start:
        raise HTTPException(422, "to_window_start must be greater than from_window_start")
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            subscriber_id = resolve_subscriber(
                cur, body.subscriber_id, body.subscriber_name)
        try:
            batch, items = create_reconciliation(
                conn, subscriber_id, body.from_window_start,
                body.to_window_start, body.operator)
        except psycopg2.errors.UniqueViolation:
            raise HTTPException(409, "a conflicting reconciliation batch already exists")
    finally:
        conn.close()
    return {"reconciliation": batch, "items": items}


@app.get("/reconciliations")
def reconciliation_list(subscriber_id: Optional[int] = None,
                        subscriber: Optional[str] = None,
                        status: Optional[str] = None,
                        limit: int = Query(default=100)):
    """Reconciliation batches, newest first, with item/verdict/unresolved counts."""
    if status is not None and status not in ("OPEN", "CLOSED"):
        raise HTTPException(422, "status must be OPEN or CLOSED")
    sql = """
    SELECT b.*, s.name AS subscriber, s.url,
           b.total_items - b.aligned_items
               - b.confirmed_items - b.rejected_items AS unresolved_items
    FROM reconciliation_batches b
    JOIN subscribers s ON s.id = b.subscriber_id"""
    conds, args = [], []
    if subscriber_id is not None:
        conds.append("b.subscriber_id = %s")
        args.append(subscriber_id)
    if subscriber is not None:
        conds.append("s.name = %s")
        args.append(subscriber)
    if status is not None:
        conds.append("b.status = %s")
        args.append(status)
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY b.id DESC LIMIT %s"
    args.append(min(max(limit, 1), 1000))
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, args)
            batches = cur.fetchall()
    finally:
        conn.close()
    return {"reconciliations": batches}


@app.get("/reconciliations/{batch_id}")
def reconciliation_get(batch_id: int):
    """One batch with counts; ``unresolved_items`` is the number of non-aligned
    rows still missing a verdict — 0 means the batch is closeable."""
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            batch = get_reconciliation(cur, batch_id)
    finally:
        conn.close()
    if batch is None:
        raise HTTPException(404, "no such reconciliation batch")
    return {"reconciliation": batch}


@app.get("/reconciliations/{batch_id}/items")
def reconciliation_items(batch_id: int, key: Optional[str] = None,
                         window_start: Optional[int] = None,
                         item_status: Optional[str] = None,
                         decision: Optional[str] = None,
                         undecided_only: bool = False,
                         limit: int = Query(default=500)):
    """The batch's frozen per-result photographs.

    Each row carries the immutable snapshot as of opening plus live_* columns
    computed from the current outbox/ledger — ``drifted`` marks rows whose
    state moved since the photograph was taken; nothing live ever rewrites the
    snapshot. Filters: key / window_start / item_status (ALIGNED, LAGGING,
    AHEAD_UNCONFIRMED, NOT_REPORTED) / decision (CONFIRMED, REJECTED) /
    undecided_only (non-aligned rows without a verdict — what still blocks
    closing).
    """
    if item_status is not None and item_status not in (
            "ALIGNED", "LAGGING", "AHEAD_UNCONFIRMED", "NOT_REPORTED"):
        raise HTTPException(422,
                            "item_status must be ALIGNED, LAGGING, "
                            "AHEAD_UNCONFIRMED or NOT_REPORTED")
    if decision is not None and decision not in ("CONFIRMED", "REJECTED"):
        raise HTTPException(422, "decision must be CONFIRMED or REJECTED")
    conds, args = [], []
    if key is not None:
        conds.append("i.key = %s")
        args.append(key)
    if window_start is not None:
        conds.append("i.window_start = %s")
        args.append(window_start)
    if item_status is not None:
        conds.append("i.item_status = %s")
        args.append(item_status)
    if decision is not None:
        conds.append("i.decision = %s")
        args.append(decision)
    if undecided_only:
        conds.append("i.item_status <> 'ALIGNED' AND i.decision IS NULL")
    # The template already carries `WHERE i.batch_id = %s`; appended filters
    # must be AND-joined, never a second WHERE (which is a syntax error that
    # breaks every filtered/single-item lookup).
    sql = RECON_ITEMS_SQL.format(
        extra_conds=(" AND " + " AND ".join(conds)) if conds else "")
    args.append(min(max(limit, 1), 5000))
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT status FROM reconciliation_batches WHERE id = %s",
                (batch_id,))
            exists = cur.fetchone()
            if exists is None:
                raise HTTPException(404, "no such reconciliation batch")
            cur.execute(sql, [batch_id] + args)
            rows = [reconcile_item_live(r) for r in cur.fetchall()]
            batch_status = exists["status"]
    finally:
        conn.close()
    return {"batch_id": batch_id, "status": batch_status, "items": rows}


@app.post("/reconciliations/{batch_id}/decisions")
def reconciliation_decide(batch_id: int, body: ReconciliationDecisionIn):
    """Adjudicate one non-aligned item: CONFIRMED (认账) or REJECTED (驳).

    Only an OPEN batch accepts verdicts; after closing both recording and
    changing a verdict return 409 (结掉之后认驳不能再改). ALIGNED items cannot
    be adjudicated (对上的不用管). Re-deciding with a different verdict while
    still open is allowed and every change is traced; the same verdict is an
    idempotent no-op. A verdict never moves the posting ledger or the outbox —
    it is the reconciliation outcome for that frozen row only.
    """
    if body.decision not in ("CONFIRMED", "REJECTED"):
        raise HTTPException(422, "decision must be CONFIRMED or REJECTED")
    conn = connect()
    try:
        batch, item = set_reconciliation_decision(
            conn, batch_id, body.window_start, body.key, body.decision,
            body.operator, body.note)
    finally:
        conn.close()
    return {"reconciliation": batch, "item": item}


@app.post("/reconciliations/{batch_id}/close")
def reconciliation_close(batch_id: int, body: ReconciliationCloseIn = None):
    """Close a batch.

    Every LAGGING / AHEAD_UNCONFIRMED / NOT_REPORTED item must carry a verdict
    first — a single undecided row keeps the batch OPEN (409). ALIGNED rows
    never need one. Closing an already-closed batch is an idempotent no-op.
    """
    operator = body.operator if body is not None else None
    conn = connect()
    try:
        batch = close_reconciliation(conn, batch_id, operator)
    finally:
        conn.close()
    return {"reconciliation": batch}


@app.get("/reconciliations/{batch_id}/events")
def reconciliation_events(batch_id: int, limit: int = Query(default=1000)):
    """The batch's append-only trail, chronological: BATCH_OPENED, every
    ITEM_DECIDED (with the previous verdict) and BATCH_CLOSED."""
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id FROM reconciliation_batches WHERE id = %s", (batch_id,))
            if cur.fetchone() is None:
                raise HTTPException(404, "no such reconciliation batch")
            cur.execute(
                """SELECT * FROM reconciliation_events
                   WHERE batch_id = %s ORDER BY id LIMIT %s""",
                (batch_id, min(max(limit, 1), 5000)),
            )
            events = cur.fetchall()
    finally:
        conn.close()
    return {"batch_id": batch_id, "events": events}
