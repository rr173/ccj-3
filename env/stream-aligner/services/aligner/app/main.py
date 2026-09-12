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

A per-(window, key) close grace (关窗宽限, POST /window-graces) lets an
operator hold ONE not-yet-emitted window of ONE business key for an extra
wall-clock interval to wait for its opposite side: while the grace is ACTIVE
that pair's INITIAL result is withheld even after both of that key's sides
cross the window end, while every other key and every other window of the same
key keeps its original window end — the scope is exactly the one pair, and one
pair's wait can never hold another pair back. Two ACTIVE graces cannot stack
on a window and a window that already emitted can never be graced again (the
grant fails 409 in both cases; ACTIVE uniqueness is also a partial index).
At the deadline the deadline sweeper (the first step of the same per-tick
close pass) emits the THEN-current recomputation in one transaction with the
grace row's FIRED marker: a late opposite-side event arriving during the wait
is folded into that very first version, and a window still one-sided at the
deadline fires one-sided rather than waiting forever; if all data was
retracted away meanwhile the grace lapses (EXPIRED) without a result and
ordinary per-key waiting resumes. Grace rows are append-only and queryable
via GET /window-graces (which windows are held, their due_at, which key).

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
and a verdict never moves the ledger or the outbox WHILE THE BATCH IS OPEN.
Closing a batch *settles* every adjudicated item (对账落账): one immutable
reconciliation_settlements row per (subscriber, window, key), so the same
pair can land exactly once ("同一条只能落到一次"), and the verdict only then
starts driving delivery and reports. A LAGGING item CONFIRMED is FROZEN at
the version it reported (later versions are parked, reports above the pin
rejected); REJECTED it CONTINUES (the lagging gate never holds that pair
again). A NOT_REPORTED item CONFIRMED is SUPPRESSED at version 0 (never
re-delivered, never copied by a later backfill, every report rejected);
REJECTED it is RE-DRIVEN — the pinned sent version is POSTED again as a new
outbox generation (new delivery_id, same result version, redelivery_seq>=1,
even when the old row already showed DELIVERED, since "sent" never means "it
booked it") until the downstream reports it, then normal flow resumes. An
AHEAD_UNCONFIRMED item CONFIRMED marks its retrying version DELIVERED at
close and aligns the ledger. Two OPEN batches of the same subscriber cannot
cover overlapping windows; after closing, verdicts are frozen too, and a new
batch may reopen the same range and photograph the then-current state.

Gap carry-forwards (缺口结转) route one side's unmatched leftovers of a CLOSED
source window to one LATER, not-yet-emitted window of the SAME business key:
POST /gap-carries. The source window is corrected in the same logical
operation (reason CARRY_FORWARD) so its events visibly leave its own gap
(payload carried-a/carried-b); when the target window later emits, carried
events pair only against the target's own opposite-side leftovers, and every
such pair is tagged {carry_id, source_window_start} (a carry match is never
written as the target window's own native pair; native pairs are formed from
the native pool first and are never displaced by a routed event). Carries are
OPEN while any event is still unpaired (partial matches record per-item
MATCHED rows), CLOSED once everything pairs with an immutable matched_snapshot
("当时对上的样子"), and REOPENED or VOID after a later source/target correction:
a carried event retracted or paired at the source voids the carry (dead
forever — its events can never be carried again or match anywhere; the global
unique index on gap_carry_items also enforces "one event rides at most one
carry, ever"), while a target correction/withdrawal reopens a closed carry the
same tick. A carry can only target an unemitted window; different keys can
never share one.
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

from app.core import (MAX_GRACE_EXTRA_MS, MIGRATION_STATUSES, ORDER_STATUSES,
                      POSTING_STATUSES, build_order_snapshot,
                      annotate_source_payload,
                      compute_payload, compute_payload_with_carries,
                      compute_payload_with_routes,
                      carry_item_fate, decide,
                      delivery_kind, evaluate_order, grace_grant_error,
                      migration_create_error,
                      migration_matched_event_ids,
                      migration_unpaired_event_ids,
                      normalize_backfill_range, opposite_side, order_reason,
                      posting_status, ranges_overlap, reconciliation_can_close,
                      reconciliation_item_status, released_version_kind,
                      retry_delay_ms, settlement_blocks_report, settlement_effect,
                      settlement_fulfils, settlement_pin_version, should_deliver,
                      side_evidence, valid_window_start, window_of, window_ready)

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
    reason       TEXT NOT NULL CHECK (reason IN ('INITIAL', 'LATE_EVENT', 'RETRACTION', 'CARRY_FORWARD')),
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
-- Re-delivery generations (再送). A CLOSED reconciliation batch's REDRIVE
-- settlement ("不认它没入过") must RE-POST a version the records already show
-- as DELIVERED: at-least-once re-delivery is a brand-new outbox row with a new
-- delivery_id (otherwise a downstream de-duplicating by delivery_id drops it),
-- while keeping the same result version. seq 0 = the original send; a redrive
-- copies the same version as seq 1. Redrive copies never block or get blocked
-- by the version-order barrier (they are a re-presentation of an already-sent
-- version, not the next version), so later versions keep flowing.
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS redelivery_seq INT NOT NULL DEFAULT 0;
ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS
    deliveries_subscriber_id_window_start_key_version_key;
DO $$
BEGIN
    ALTER TABLE deliveries ADD CONSTRAINT deliveries_identity_uq
        UNIQUE (subscriber_id, window_start, key, version, redelivery_seq);
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;
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
                     ('REPORT_ACCEPTED', 'REPORT_REJECTED', 'DELIVERY_ADVANCED',
                      'SETTLEMENT_ADVANCED')),
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
-- ---------------------------------------------------------------------------
-- Settlements (对账落账), the closed-batch verdicts applied to delivery.
--
-- One row per (subscriber, window, key), inserted ONCE, when the first batch
-- adjudicating that pair closes ("同一条只能落到一次"): the verdict's effect,
-- the version the pair is pinned at, and the batch that settled it. A later
-- batch cannot re-settle the same pair — its close fails 409 until the
-- disputing items are resolved differently; FULFILLED rows stay (the history
-- of where the pin was), they never block a new settlement, and the effect
-- on delivery/reports is decided by effect+status together.
--
-- effect:
--   FREEZE         (LAGGING + 认): stop at the pinned reported version —
--                  later versions are never sent to this downstream, reports
--                  above the pin are rejected;
--   CONTINUE       (LAGGING + 驳): keep sending what we sent — the posting
--                  gate never holds this pair again; discharged (FULFILLED)
--                  once its reports move past the pinned version;
--   SUPPRESS       (NOT_REPORTED + 认): pin 0 — the version is never
--                  (re)delivered (re-drive or backfill), every report is
--                  rejected;
--   REDRIVE        (NOT_REPORTED + 驳): the pinned lowest sent version is
--                  re-driven until reported; discharging the report settles
--                  it (FULFILLED) and normal flow resumes;
--   MARK_DELIVERED (AHEAD_UNCONFIRMED + 认): the in-flight versions up to the
--                  reported pin are confirmed DELIVERED at close, the ledger
--                  is aligned — born FULFILLED;
--   NONE           (AHEAD_UNCONFIRMED + 驳): ordinary retries continue —
--                  born FULFILLED, the row is the record that it was judged.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reconciliation_settlements (
    id                 BIGSERIAL PRIMARY KEY,
    batch_id           BIGINT NOT NULL REFERENCES reconciliation_batches(id),
    subscriber_id      BIGINT NOT NULL REFERENCES subscribers(id),
    window_start       BIGINT NOT NULL,
    key                TEXT NOT NULL,
    effect             TEXT NOT NULL CHECK (effect IN
                       ('FREEZE', 'CONTINUE', 'SUPPRESS', 'REDRIVE',
                        'MARK_DELIVERED', 'NONE')),
    status             TEXT NOT NULL DEFAULT 'ACTIVE'
                       CHECK (status IN ('ACTIVE', 'FULFILLED')),
    pinned_version     INT NOT NULL,
    -- frozen snapshot references carried for the record
    snapshot_item_status TEXT NOT NULL,
    snapshot_decision    TEXT NOT NULL,
    settled_by         TEXT,
    settled_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    fulfilled_at       TIMESTAMPTZ,
    UNIQUE (subscriber_id, window_start, key)
);
CREATE INDEX IF NOT EXISTS reconciliation_settlements_batch_idx
    ON reconciliation_settlements (batch_id, id);
CREATE INDEX IF NOT EXISTS reconciliation_settlements_active_idx
    ON reconciliation_settlements (subscriber_id, window_start, key)
    WHERE status = 'ACTIVE';
-- ---------------------------------------------------------------------------
-- Per-(window, key) close grace (关窗宽限).
--
-- A not-yet-emitted (window, key) may be granted one ACTIVE grace: hold its
-- INITIAL result past the ordinary per-key window end and wait a little
-- longer for the opposite side. Rows are append-only — a granted grace is
-- never deleted. ACTIVE = still holding (due_at is the wall-clock deadline);
-- FIRED = the deadline elapsed with effective data left, the then-current
-- version was emitted in the same transaction that flipped it; EXPIRED = the
-- deadline elapsed after all data was retracted away, nothing was emitted and
-- the window went back to ordinary waiting (a fresh grace may be granted
-- later once new data arrives). The partial unique index makes "同一窗不能
-- 叠两条还没到期的宽限" a database-level guarantee under concurrent grants.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS window_graces (
    id             BIGSERIAL PRIMARY KEY,
    window_start   BIGINT NOT NULL,
    window_end     BIGINT NOT NULL,
    key            TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'ACTIVE'
                   CHECK (status IN ('ACTIVE', 'FIRED', 'EXPIRED')),
    extra_ms       BIGINT NOT NULL,
    due_at         TIMESTAMPTZ NOT NULL,
    fired_version  INT,                 -- version emitted at the deadline (FIRED)
    created_by     TEXT,
    note           TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS window_graces_one_active_uq
    ON window_graces (window_start, key) WHERE status = 'ACTIVE';
CREATE INDEX IF NOT EXISTS window_graces_active_due_idx
    ON window_graces (due_at) WHERE status = 'ACTIVE';
CREATE INDEX IF NOT EXISTS window_graces_key_idx
    ON window_graces (key, window_start);
-- ---------------------------------------------------------------------------
-- Gap carry-forwards (缺口结转).
--
-- One carry routes the leftover events of ONE side of a CLOSED source window
-- to one LATER, not-yet-emitted window of the SAME business key, where they
-- are injected into the carry side's ordering and paired against the target's
-- own leftovers. Lifecycle:
--   OPEN     created, none/part of the events matched yet;
--   CLOSED   every event matched at the target — matched_snapshot freezes
--            "当时对上的样子" (which target version paired each event);
--   REOPENED a CLOSED carry whose target later corrected/withdrew so a pair
--            no longer exists — never keeps showing CLOSED; the carry waits
--            (and re-matches) like OPEN;
--   VOID     dead forever: an event was retracted at the source or got paired
--            at the source by a late opposite-side event. A void carry can
--            never match again.
--
-- gap_carry_events is the append-only trail (open / item match / close /
-- reopen / void). The global UNIQUE(key, side, event_id) on gap_carry_items
-- is the hard form of two rules at once: "结转出去的那几条，原来那一窗不能
-- 再拿去结第二次" (a void carry's events stay in history and cannot be carried
-- again) and "同一条对不上的不能同时待在两张还开着的结转里" (one event rides
-- at most one carry, period). A carry can only target an unemitted window —
-- enforced by create_carry under the key's gate-row lock, the same lock every
-- emit() takes, so an INITIAL result racing the grant cannot slip past it.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gap_carries (
    id                    BIGSERIAL PRIMARY KEY,
    key                   TEXT NOT NULL,
    side                  TEXT NOT NULL CHECK (side IN ('a', 'b')),
    source_window_start   BIGINT NOT NULL,
    source_window_end     BIGINT NOT NULL,
    target_window_start   BIGINT NOT NULL,
    target_window_end     BIGINT NOT NULL,
    status                TEXT NOT NULL DEFAULT 'OPEN'
                          CHECK (status IN ('OPEN', 'CLOSED', 'REOPENED', 'VOID')),
    source_version        INT NOT NULL,   -- source result version at creation
    target_version        INT,            -- target version that closed the carry
    matched_snapshot      JSONB,          -- frozen close-time match picture
    void_reason           TEXT CHECK (void_reason IN ('source_retracted', 'source_paired')),
    created_by            TEXT,
    note                  TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at             TIMESTAMPTZ,
    reopened_at           TIMESTAMPTZ,
    voided_at             TIMESTAMPTZ,
    CHECK (target_window_start > source_window_start)
);
CREATE INDEX IF NOT EXISTS gap_carries_key_idx ON gap_carries (key, id);
CREATE INDEX IF NOT EXISTS gap_carries_target_idx
    ON gap_carries (target_window_start, key) WHERE status <> 'VOID';
CREATE INDEX IF NOT EXISTS gap_carries_source_idx
    ON gap_carries (source_window_start, key);
CREATE INDEX IF NOT EXISTS gap_carries_open_idx
    ON gap_carries (key) WHERE status IN ('OPEN', 'REOPENED');
CREATE TABLE IF NOT EXISTS gap_carry_items (
    carry_id      BIGINT NOT NULL REFERENCES gap_carries(id),
    key           TEXT NOT NULL,
    side          TEXT NOT NULL CHECK (side IN ('a', 'b')),
    event_id      TEXT NOT NULL,
    event_time    BIGINT NOT NULL,
    payload       JSONB,
    item_status   TEXT NOT NULL DEFAULT 'CARRIED'
                  CHECK (item_status IN ('CARRIED', 'MATCHED', 'DEAD')),
    matched_version    INT,             -- target result version that paired it
    matched_against    TEXT,            -- the opposite-side event it paired with
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (carry_id, event_id)
);
-- One event rides at most one carry across the whole history: this rejects a
-- second carry whether the first one is OPEN, CLOSED, REOPENED or VOID
-- ("作废的不能再拿去对", "原来那一窗不能再拿去结第二次").
CREATE UNIQUE INDEX IF NOT EXISTS gap_carry_items_event_uq
    ON gap_carry_items (key, side, event_id);
CREATE INDEX IF NOT EXISTS gap_carry_items_carry_idx
    ON gap_carry_items (carry_id, item_status);
CREATE TABLE IF NOT EXISTS gap_carry_events (
    id               BIGSERIAL PRIMARY KEY,
    carry_id         BIGINT NOT NULL REFERENCES gap_carries(id),
    key              TEXT NOT NULL,
    event            TEXT NOT NULL CHECK (event IN
                     ('CARRY_OPENED', 'ITEM_MATCHED', 'CARRY_CLOSED',
                      'CARRY_REOPENED', 'CARRY_VOIDED')),
    target_version   INT,
    detail           JSONB,
    operator         TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS gap_carry_events_carry_idx
    ON gap_carry_events (carry_id, id);
CREATE INDEX IF NOT EXISTS gap_carry_events_key_idx
    ON gap_carry_events (key, id);
-- CARRY_FORWARD is the reason of every result version the carry machinery
-- produces: the source window's "these leftovers are routed elsewhere"
-- correction, and the target window's recomputation as carries open/close/
-- void. It delivers to downstream as an ordinary CORRECTION.
ALTER TABLE results DROP CONSTRAINT IF EXISTS results_reason_check;
ALTER TABLE results ADD CONSTRAINT results_reason_check
    CHECK (reason IN ('INITIAL', 'LATE_EVENT', 'RETRACTION', 'CARRY_FORWARD'));
ALTER TABLE audit DROP CONSTRAINT IF EXISTS audit_reason_check;
ALTER TABLE audit ADD CONSTRAINT audit_reason_check
    CHECK (reason IN ('INITIAL', 'LATE_EVENT', 'RETRACTION', 'CARRY_FORWARD'));
-- A carry finally closing (CARRY_RESOLVED) is genuine new progress that may
-- close an order that was WAITING on the in-flight events.
ALTER TABLE biz_order_versions DROP CONSTRAINT IF EXISTS biz_order_versions_reason_check;
ALTER TABLE biz_order_versions ADD CONSTRAINT biz_order_versions_reason_check
    CHECK (reason IN
           ('ORDER_OPENED','WINDOW_JOINED','WINDOW_CORRECTED',
            'WINDOW_WITHDRAWN','WINDOW_REVIVED','CARRY_RESOLVED'));
-- Source windows record which leftovers are currently routed out, per side.
ALTER TABLE biz_order_windows ADD COLUMN IF NOT EXISTS carried_a INT NOT NULL DEFAULT 0;
ALTER TABLE biz_order_windows ADD COLUMN IF NOT EXISTS carried_b INT NOT NULL DEFAULT 0;
-- ---------------------------------------------------------------------------
-- Business-key migrations (业务键迁出).
--
-- One row per declared migration of a whole key tail: from_key windows >=
-- start_window_start no longer produce their own results; their events route
-- into to_key windows from that window on. Lifecycle:
--   OPEN     declared, not cut yet — the start window may still be changed
--            and the declaration voided; the from_key's windows >= S are held;
--   CUT      the start window became due while OPEN: every held from_key event
--            (effective upserts with event_time >= S) started routing into the
--            to_key, the cut snapshots the two sides' result versions, and the
--            from_key is sealed (later upserts are rejected at pull time);
--   REOPENED a later correction/retraction at either side of the start window
--            invalidated the cut — it never keeps showing CUT. The routed
--            events keep routing (to-key results stay correct); the derivation
--            settles it back to CUT with a FRESH snapshot (MIGRATION_RECUT) or
--            auto-voids it when every routed event was retracted at source;
--   VOID     terminal. Manual void is only possible while OPEN ("作废只能在还
--            没切过去时做"); a reopened cut also auto-voids once nothing routed
--            survives. After a void the from_key emits its own windows again
--            and the same row can never cut ("不能再拿这笔去切").
--
-- The partial unique indexes are the hard form of "同一迁出键不能同时开着两笔
-- 还没结束的迁出" (and, conservatively, of never entangling one key in two
-- unfinished migrations on either side): only non-VOID rows participate, so a
-- voided migration never blocks a fresh one. key_migration_events is the
-- append-only trail; to-key result versions produced by migration routing use
-- reason MIGRATION (delivered downstream as an ordinary CORRECTION / NEW).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS key_migrations (
    id                      BIGSERIAL PRIMARY KEY,
    from_key                TEXT NOT NULL,
    to_key                  TEXT NOT NULL,
    start_window_start      BIGINT NOT NULL,   -- S: first window routed over
    start_window_end        BIGINT NOT NULL,
    status                  TEXT NOT NULL DEFAULT 'OPEN'
                            CHECK (status IN ('OPEN', 'CUT', 'REOPENED', 'VOID')),
    -- versions at the cut moment ("两边当时各是哪一版"):
    cut_from_version        INT,   -- from_key head strictly before S at cut
    cut_from_window_start   BIGINT,
    cut_to_version          INT,   -- to_key head at S produced by the cut
    cut_at                  TIMESTAMPTZ,
    recut_count             INT NOT NULL DEFAULT 0,
    void_reason             TEXT CHECK (void_reason IN
                            ('operator_void', 'source_retracted')),
    -- Frozen routing picture at the (re)cut: every surviving routed event and
    -- the origin window it belongs to. A later derivation compares this to
    -- the live routed set: a change in the start window invalidates the cut
    -- (REOPENED), so a cut can never keep showing a stale snapshot.
    cut_signature           JSONB,
    last_block_reason       TEXT,  -- why an OPEN migration cannot cut right now
    created_by              TEXT,
    note                    TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    voided_at               TIMESTAMPTZ,
    CHECK (from_key <> to_key)
);
-- At most one unfinished migration OUT of a key.
CREATE UNIQUE INDEX IF NOT EXISTS key_migrations_from_open_uq
    ON key_migrations (from_key) WHERE status <> 'VOID';
-- Conservatively also: one unfinished migration INTO a key (the to_key cannot
-- be two migrations' destination at once, nor another migration's source).
CREATE UNIQUE INDEX IF NOT EXISTS key_migrations_to_open_uq
    ON key_migrations (to_key) WHERE status <> 'VOID';
CREATE INDEX IF NOT EXISTS key_migrations_from_idx
    ON key_migrations (from_key, id);
CREATE INDEX IF NOT EXISTS key_migrations_to_idx
    ON key_migrations (to_key, id) WHERE status <> 'VOID';
-- Upgrade path for databases created while the migration feature was being
-- built with the table but no frozen-signature column.
ALTER TABLE key_migrations ADD COLUMN IF NOT EXISTS cut_signature JSONB;
CREATE TABLE IF NOT EXISTS key_migration_events (
    id            BIGSERIAL PRIMARY KEY,
    migration_id  BIGINT NOT NULL REFERENCES key_migrations(id),
    from_key      TEXT NOT NULL,
    to_key        TEXT NOT NULL,
    event         TEXT NOT NULL CHECK (event IN
                   ('MIGRATION_OPENED', 'START_WINDOW_CHANGED', 'MIGRATION_CUT',
                    'MIGRATION_RECUT', 'MIGRATION_REOPENED', 'MIGRATION_VOIDED')),
    window_start  BIGINT,
    detail        JSONB,
    operator      TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS key_migration_events_migration_idx
    ON key_migration_events (migration_id, id);
CREATE INDEX IF NOT EXISTS key_migration_events_from_idx
    ON key_migration_events (from_key, id);
-- Rejected upserts: after the cut the from_key is sealed — a late upsert must
-- FAIL rather than silently route, but the event is durably recorded so the
-- rejection is auditable ("切过去之后，迁出键再来新事件要失败"). Retractions
-- keep flowing (they correct already-routed events and can reopen the cut).
ALTER TABLE stream_events ADD COLUMN IF NOT EXISTS rejected BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE stream_events ADD COLUMN IF NOT EXISTS rejected_reason TEXT;
ALTER TABLE stream_events ADD COLUMN IF NOT EXISTS migration_id BIGINT;
CREATE INDEX IF NOT EXISTS stream_events_rejected_idx
    ON stream_events (key, event_time) WHERE rejected;
-- MIGRATION is a result reason like CARRY_FORWARD (to-key versions the routing
-- produced), delivered downstream as CORRECTION (or NEW for a first release).
ALTER TABLE results DROP CONSTRAINT IF EXISTS results_reason_check;
ALTER TABLE results ADD CONSTRAINT results_reason_check
    CHECK (reason IN ('INITIAL', 'LATE_EVENT', 'RETRACTION', 'CARRY_FORWARD',
                      'MIGRATION'));
ALTER TABLE audit DROP CONSTRAINT IF EXISTS audit_reason_check;
ALTER TABLE audit ADD CONSTRAINT audit_reason_check
    CHECK (reason IN ('INITIAL', 'LATE_EVENT', 'RETRACTION', 'CARRY_FORWARD',
                      'MIGRATION'));
-- Order folds driven by migration lifecycle events (a cut re-folds both keys;
-- a void re-folds the from_key as its tail comes back).
ALTER TABLE biz_order_versions DROP CONSTRAINT IF EXISTS biz_order_versions_reason_check;
ALTER TABLE biz_order_versions ADD CONSTRAINT biz_order_versions_reason_check
    CHECK (reason IN
           ('ORDER_OPENED','WINDOW_JOINED','WINDOW_CORRECTED',
            'WINDOW_WITHDRAWN','WINDOW_REVIVED','CARRY_RESOLVED',
            'MIGRATION_CUT','MIGRATION_VOIDED'));
-- CARRY_RESOLVED/MIGRATION folds may have no single triggering result row
-- (e.g. a source-only void fold). Keep the FK column nullable for those.
ALTER TABLE biz_order_versions ALTER COLUMN trigger_result_id DROP NOT NULL;
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


def get_head_full(cur, window_start, key):
    """Like get_head, but also carries the stored payload (needed by carry
    derivation, which reads the target result's pairs). get_head stays lean —
    most callers never need the payload blob."""
    cur.execute(
        """SELECT id, version, status, payload_hash, payload FROM results
           WHERE window_start = %s AND key = %s ORDER BY version DESC LIMIT 1""",
        (window_start, key),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {"id": row[0], "version": row[1], "status": row[2],
            "payload_hash": row[3], "payload": row[4]}


def load_effective_events(cur, window_start, key):
    """Effective (non-retracted, non-rejected) upserts of (window, key), per
    stream.

    Retractions are applied via NOT EXISTS over the whole stream, so a retract
    event landing in a *different* window still filters its target. Upserts
    rejected by a sealed (already-cut) migration are excluded everywhere — the
    old key must never produce them, routed or own (see key_migrations).
    """
    window_end = window_start + WINDOW_MS
    cur.execute(
        """SELECT e.stream, e.event_id, e.event_time, e.payload
           FROM stream_events e
           WHERE e.type = 'upsert' AND e.key = %s AND NOT e.rejected
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
    return by_stream


def carries_for_window(cur, window_start, key):
    """All non-VOID carries aimed at (window, key), with their live item
    events. Matched items MUST stay injected: a CLOSED carry's pair is part of
    the target result, so an unrelated later correction of the target must not
    silently drop the pair (that would reopen the carry every tick). Only
    DEAD items (a voided carry) are excluded."""
    cur.execute(
        """SELECT c.id, c.side, c.source_window_start,
                  i.event_id, i.event_time, i.payload
           FROM gap_carries c
           JOIN gap_carry_items i ON i.carry_id = c.id AND i.item_status <> 'DEAD'
           WHERE c.key = %s AND c.target_window_start = %s
             AND c.status IN ('OPEN', 'REOPENED', 'CLOSED')
           ORDER BY c.id, i.event_time, i.event_id""",
        (key, window_start),
    )
    carries = {}
    for cid, side, source_ws, event_id, event_time, payload in cur.fetchall():
        c = carries.setdefault(cid, {"id": cid, "side": side,
                                     "source_window_start": source_ws, "events": []})
        c["events"].append({"event_id": event_id, "event_time": event_time,
                            "payload": payload})
    return list(carries.values())


def migrations_for_window(cur, window_start, key):
    """All active (non-VOID) migrations routing INTO (window, key), with their
    currently-routed events that land in this window.

    A migration routes from ``start_window_start`` on, so it contributes to
    every to_key window at/after its start. Events whose origin window is the
    queried one are attached (event_time bucketed by the ORIGIN event time, the
    old key's window they belonged to). Events retracted at the from_key drop
    out via the same effective-event filter own events use.
    """
    cur.execute(
        """SELECT m.id, m.from_key, m.start_window_start, e.stream,
                  e.event_id, e.event_time, e.payload
           FROM key_migrations m
           JOIN stream_events e
             ON e.key = m.from_key AND e.type = 'upsert' AND NOT e.rejected
                AND (e.event_time / %s) * %s = %s
                AND e.event_time / %s * %s >= m.start_window_start
                AND NOT EXISTS (
                    SELECT 1 FROM stream_events r
                    WHERE r.type = 'retract' AND r.stream = e.stream
                      AND r.retracts = e.event_id)
           WHERE m.to_key = %s AND m.status IN ('CUT', 'REOPENED')
           ORDER BY m.id, e.stream, e.event_time, e.event_id""",
        (WINDOW_MS, WINDOW_MS, window_start,
         WINDOW_MS, WINDOW_MS, key),
    )
    groups = {}
    for mid, from_key, start_ws, stream, event_id, event_time, payload in cur.fetchall():
        g = groups.setdefault(mid, {"id": mid, "from_key": from_key,
                                    "start_window_start": start_ws,
                                    "by_side": {"a": [], "b": []}})
        g["by_side"][stream].append(
            {"event_id": event_id, "event_time": event_time, "payload": payload})
    out = []
    for mid in sorted(groups):
        g = groups[mid]
        for side in ("a", "b"):
            if g["by_side"][side]:
                out.append({"id": mid, "from_key": g["from_key"],
                            "origin_window_start": window_start,
                            "side": side, "events": g["by_side"][side]})
    return out


def render_window_payload(cur, window_start, key, carries=None, migrations=None,
                          autoload=True):
    """The window's payload as it must be stored now: native events paired by
    the ordinary rule, with active migrations routing whole-key-tail events in
    (every such pair visibly tagged with its migration provenance) and live
    carries routing same-key leftovers in. When ``autoload`` is True (the
    default) the active migrations and carries aimed at this window are loaded
    here; callers that already loaded them pass them in, and callers wanting a
    deliberately plain render pass ``autoload=False``.
    """
    window_end = window_start + WINDOW_MS
    by_stream = load_effective_events(cur, window_start, key)
    if autoload:
        if migrations is None:
            migrations = migrations_for_window(cur, window_start, key)
        if carries is None:
            carries = carries_for_window(cur, window_start, key)
    migrations = migrations or []
    carries = carries or []
    if migrations or carries:
        return compute_payload_with_routes(
            key, window_start, window_end,
            by_stream["a"], by_stream["b"], migrations, carries)
    return compute_payload(key, window_start, window_end,
                           by_stream["a"], by_stream["b"])


def build_payload(cur, window_start, key):
    """Recompute the plain (carry-free) result for (window, key)."""
    return render_window_payload(cur, window_start, key)


def outgoing_carries_for_window(cur, window_start, key):
    """Live carries ROUTING OUT of (window, key): their non-DEAD items,
    grouped per carry. A source window re-emitted after a correction keeps
    showing every routed event as "carried" rather than as its own unmatched
    gap — a MATCHED item still left through the carry, only a DEAD item (void
    carry, event retracted / paired at the source) returns to the window.
    CLOSED carries thus keep annotating the source; a VOID carry drops out, so
    its surviving events return to unmatched."""
    cur.execute(
        """SELECT c.id, c.side, i.event_id
           FROM gap_carries c
           JOIN gap_carry_items i ON i.carry_id = c.id AND i.item_status <> 'DEAD'
           WHERE c.key = %s AND c.source_window_start = %s
             AND c.status <> 'VOID'
           ORDER BY c.id, i.event_time, i.event_id""",
        (key, window_start),
    )
    carries = {}
    for cid, side, event_id in cur.fetchall():
        carries.setdefault(cid, {"id": cid, "side": side, "event_ids": []})
        carries[cid]["event_ids"].append(event_id)
    return list(carries.values())


def emit(conn, window_start, key, reason, detail, grace=None):
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

    A per-(window, key) close grace (关窗宽限) additionally holds the FIRST
    version: while an ACTIVE grace exists, an INITIAL emission is skipped even
    if both sides crossed the window end ("宽限没到点，这一窗就先别出"). The
    deadline sweeper calls this function with ``grace`` (the ACTIVE row locked
    FOR UPDATE): the row is re-checked under the key gate lock and the fate is
    committed in the same transaction as the version — FIRED with the
    then-current version (one-sided included, "到点了还是一边，按单边出"), or
    EXPIRED without a version when the recomputation is empty (everything was
    retracted during the wait).

    Everything above commits in one transaction, so a delivered version can
    never be missing its release record, a FIRED grace can never exist without
    its version, and a held version can never leak an outbox row.
    """
    window_end = window_start + WINDOW_MS
    with conn, conn.cursor() as cur:
        # Serialize against a concurrent explicit release / gate flip / grace
        # grant for the same key: every writing path takes the gate row lock
        # first, then (if it touches one) the grace row lock.
        cur.execute(
            """INSERT INTO release_gates (key) VALUES (%s) ON CONFLICT (key) DO NOTHING""",
            (key,),
        )
        cur.execute("SELECT open FROM release_gates WHERE key = %s FOR UPDATE", (key,))
        gate_open = cur.fetchone()[0]

        if grace is not None:
            # Deadline sweeper path: pin the grace row and re-validate it
            # under the gate lock. The sweeper selected due ACTIVE rows
            # before taking this lock, so the row must still be exactly that.
            cur.execute(
                """SELECT id, status, due_at FROM window_graces
                   WHERE id = %s AND window_start = %s AND key = %s FOR UPDATE""",
                (grace["id"], window_start, key),
            )
            locked = cur.fetchone()
            if locked is None or locked[1] != "ACTIVE":
                log.warning("grace %s for window=%d key=%s changed before firing; skipping",
                            grace["id"], window_start, key)
                return False
        else:
            # Ordinary path: an ACTIVE grace holds the first version of this
            # window regardless of the crossing evidence ("只宽这一键这一窗"
            # — the hold itself is the grace's whole effect; other windows
            # are simply not listed here).
            cur.execute(
                "SELECT 1 FROM window_graces WHERE window_start = %s AND key = %s "
                "AND status = 'ACTIVE' LIMIT 1",
                (window_start, key),
            )
            if cur.fetchone() is not None:
                log.info("window=%d key=%s HELD by active close grace (not emitted yet)",
                         window_start, key)
                return False

        payload = render_window_payload(cur, window_start, key)
        # Leftovers this window routed elsewhere via a same-key gap carry stay
        # visibly routed: they are not this window's own unmatched gap anymore.
        # Applied AFTER the incoming-routes render so chained windows work.
        for oc in outgoing_carries_for_window(cur, window_start, key):
            payload = annotate_source_payload(
                payload, oc["side"], oc["id"], oc["event_ids"])
        head = get_head(cur, window_start, key)
        nxt = decide(head, payload)

        if grace is not None:
            # The deadline is here: an empty recomputation (all events
            # retracted during the wait) emits nothing — the grace lapses and
            # the window returns to ordinary per-key waiting; a fresh grace
            # can be granted once new data arrives.
            if nxt is None:
                if payload is None and head is None:
                    cur.execute(
                        """UPDATE window_graces
                           SET status = 'EXPIRED', finished_at = now()
                           WHERE id = %s AND status = 'ACTIVE'""",
                        (grace["id"],),
                    )
                    log.info("window=%d key=%s grace %s EXPIRED with no data left",
                             window_start, key, grace["id"])
                    return False
                # A non-initial no-op at the deadline means another path
                # already produced identical content under the same gate
                # lock — it owns the FIRE transition; do nothing.
                log.warning("window=%d key=%s grace %s due but recompute is a no-op "
                            "(head=%s)", window_start, key, grace["id"], head)
                return False

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
        if grace is not None:
            # The deadline elapsed: the version just committed is exactly the
            # then-current recomputation ("到期出的就是当时那一版"). Mark the
            # grace FIRED right here — before the external-release decision —
            # because grace concerns the INTERNAL emission: a version held by
            # the (orthogonal) release gate still closes the grace, otherwise
            # the sweeper would keep re-selecting this ACTIVE row forever.
            # Same transaction either way, so a crash leaves neither a version
            # without a FIRED row nor a FIRED row without its version.
            cur.execute(
                """UPDATE window_graces
                   SET status = 'FIRED', fired_version = %s, finished_at = now()
                   WHERE id = %s AND status = 'ACTIVE'""",
                (nxt["version"], grace["id"]),
            )
            if cur.rowcount == 0:
                raise RuntimeError(
                    f"grace {grace['id']} vanished during FIRE for window={window_start} key={key}")
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
               ON CONFLICT (subscriber_id, window_start, key, version, redelivery_seq) DO NOTHING""",
            (window_start, window_end, key, nxt["version"], kind,
             psycopg2.extras.Json(payload) if payload is not None else None),
        )
    log.info("window=%d key=%s -> v%d (%s, %s)%s", window_start, key, nxt["version"],
             nxt["status"], reason, " [grace FIRED]" if grace is not None else "")
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
                rejected_reason = None
                migration_id = None
                if ev["type"] == "upsert":
                    # The from_key is sealed once a migration has cut: a late
                    # upsert at/after the start window must FAIL rather than
                    # silently route or produce its own result ("切过去之后，
                    # 迁出键再来新事件要失败"). The row is still stored and
                    # marked rejected so the rejection is auditable; retractions
                    # are NOT sealed (they correct already-routed events).
                    cur.execute(
                        """SELECT id, status, start_window_start
                           FROM key_migrations
                           WHERE from_key = %s AND status IN ('CUT', 'REOPENED')
                             AND %s >= start_window_start
                           ORDER BY id LIMIT 1""",
                        (ev["key"], ev["event_time"]),
                    )
                    seal = cur.fetchone()
                    if seal is not None:
                        rejected_reason = (
                            f"from_key sealed by migration {seal[0]} ({seal[1]})")
                        migration_id = seal[0]
                cur.execute(
                    """INSERT INTO stream_events
                           (stream, event_id, event_time, key, type, retracts, payload,
                            seq, rejected, rejected_reason, migration_id)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (stream, event_id) DO NOTHING""",
                    (stream, ev["event_id"], ev["event_time"], ev["key"], ev["type"],
                     ev.get("retracts"),
                     psycopg2.extras.Json(ev["payload"]) if ev.get("payload") is not None else None,
                     ev["seq"], rejected_reason is not None, rejected_reason,
                     migration_id),
                )
                if cur.rowcount == 0:
                    continue  # replayed row, already applied
                if ev["type"] == "upsert":
                    ws, _ = window_of(ev["event_time"], WINDOW_MS)
                    if rejected_reason is not None:
                        # Sealed: the event must appear nowhere — not on the
                        # from_key's own results and not routed to the to_key.
                        log.info("upsert %s on sealed from_key %s rejected: %s",
                                 ev["event_id"], ev["key"], rejected_reason)
                        continue
                    dirty.append((ws, ev["key"], "LATE_EVENT",
                                  {"stream": stream, "event_id": ev["event_id"],
                                   "event_time": ev["event_time"]}))
                else:  # retract: the *target's* window is the one affected
                    cur.execute(
                        """SELECT event_time, key, rejected
                           FROM stream_events WHERE stream = %s AND event_id = %s""",
                        (stream, ev["retracts"]),
                    )
                    target = cur.fetchone()
                    if target:
                        target_time, target_key, target_rejected = target
                        ws, _ = window_of(target_time, WINDOW_MS)
                        # A retraction of an event the migration routed (and
                        # which belongs to the start window) can invalidate the
                        # cut — the sweep re-derives the routing from the
                        # surviving events; just make sure the to_key windows it
                        # touched get recomputed this tick.
                        cur.execute(
                            """SELECT id, to_key, start_window_start
                               FROM key_migrations
                               WHERE from_key = %s AND status IN ('CUT', 'REOPENED')
                                 AND %s >= start_window_start
                               ORDER BY id LIMIT 1""",
                            (target_key, target_time),
                        )
                        cut = cur.fetchone()
                        if cut is not None:
                            mid, to_key, cut_start = cut
                            # Every to_key window the live routing touches may
                            # change; the migration sweep (derive_migrations)
                            # decides reopen / recut / void from the surviving
                            # events and re-emits each affected window.
                            cur.execute(
                                """SELECT DISTINCT (e.event_time / %s) * %s
                                   FROM stream_events e, key_migrations m
                                   WHERE m.id = %s AND e.key = m.from_key
                                     AND e.type = 'upsert' AND NOT e.rejected
                                     AND e.event_time >= m.start_window_start
                                     AND NOT EXISTS (
                                         SELECT 1 FROM stream_events r
                                         WHERE r.type = 'retract' AND r.stream = e.stream
                                           AND r.retracts = e.event_id)""",
                                (WINDOW_MS, WINDOW_MS, mid),
                            )
                            for (rws,) in cur.fetchall():
                                dirty.append((rws, to_key, "RETRACTION",
                                              {"stream": stream,
                                               "retract_event_id": ev["event_id"],
                                               "retracted_event_id": ev["retracts"],
                                               "migration_id": mid}))
                        # A window the cut already routed away is no longer the
                        # from_key's business: it must never be re-emitted as
                        # its own result by the ordinary dirty-window path.
                        from_key_routed = cut is not None and ws >= cut_start
                        if not target_rejected and not from_key_routed:
                            dirty.append((ws, target_key, "RETRACTION",
                                          {"stream": stream,
                                           "retract_event_id": ev["event_id"],
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


def due_graces(cur):
    """ACTIVE close graces whose wall-clock deadline has elapsed, oldest first."""
    cur.execute(
        """SELECT id, window_start, window_end, key, extra_ms
           FROM window_graces
           WHERE status = 'ACTIVE' AND due_at <= now()
           ORDER BY due_at, id""")
    return [{"id": r[0], "window_start": r[1], "window_end": r[2],
             "key": r[3], "extra_ms": r[4]} for r in cur.fetchall()]


def active_grace_pairs(cur):
    """The {(window_start, key)} set currently held by an ACTIVE close grace."""
    cur.execute("SELECT window_start, key FROM window_graces WHERE status = 'ACTIVE'")
    return {(r[0], r[1]) for r in cur.fetchall()}


def held_tail_pairs(cur):
    """The {(window_start, from_key)} pairs an OPEN outbound migration holds:
    every from_key window (that actually has events) at/after its start
    window. Such windows neither emit their own result nor count as the
    from_key order's missing/pending business while the declaration waits to
    cut — the tail is explicitly on hold, not a missing result. CUT/REOPENED
    migrations are excluded: the tail is already routed away (excluded from
    the order via routed_tail_windows) and only an emitted result's ordinary
    correction path may touch pre-cut windows."""
    cur.execute(
        """SELECT start_window_start, from_key FROM key_migrations
           WHERE status = 'OPEN'""")
    rows = cur.fetchall()
    if not rows:
        return set()
    cur.execute(
        """SELECT DISTINCT (event_time / %s) * %s AS ws, key
           FROM stream_events WHERE type = 'upsert' AND NOT rejected""",
        (WINDOW_MS, WINDOW_MS))
    event_windows = cur.fetchall()
    held = set()
    for start, from_key in rows:
        for ws, key in event_windows:
            if key == from_key and ws >= start:
                held.add((ws, from_key))
    return held


def routed_tail_windows(cur, key):
    """Window starts of ``key`` whose tail already left via a CUT/REOPENED
    outbound migration (windows at/after its start). They are no longer this
    key's business: never counted missing or pending in its order. Returns
    the minimum such start when any migration applies (all later windows are
    covered, since at most one non-VOID outbound migration exists per key)."""
    cur.execute(
        """SELECT min(start_window_start) FROM key_migrations
           WHERE from_key = %s AND status IN ('CUT', 'REOPENED')""",
        (key,))
    return cur.fetchone()[0]


def routed_tail_pairs(cur):
    """The {(window_start, from_key)} pairs a CUT/REOPENED outbound migration
    already routed away: from_key windows (that actually hold events) at/after
    its start. They must never produce an own result again — the tail belongs
    to the to_key now ("切过去之后迁出键不能再出自己的结果")."""
    cur.execute(
        """SELECT start_window_start, from_key FROM key_migrations
           WHERE status IN ('CUT', 'REOPENED')""")
    rows = cur.fetchall()
    if not rows:
        return set()
    cur.execute(
        """SELECT DISTINCT (event_time / %s) * %s AS ws, key
           FROM stream_events WHERE type = 'upsert' AND NOT rejected""",
        (WINDOW_MS, WINDOW_MS))
    event_windows = cur.fetchall()
    routed = set()
    for start, from_key in rows:
        for ws, key in event_windows:
            if key == from_key and ws >= start:
                routed.add((ws, from_key))
    return routed


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

    A per-(window, key) close grace (关窗宽限) changes exactly one pair's
    timing: while ACTIVE its INITIAL is held even after both sides crossed;
    due graces are swept FIRST here, and emit() publishes the then-current
    recomputation at the deadline regardless of crossing evidence — still
    one-sided, it goes out one-sided ("到点了还是一边，按单边出"), or the
    grace lapses (EXPIRED) if all its data was retracted away. Every other
    (window, key) keeps its ordinary window end and is merely skipped while
    its own grace is active — other keys are never blocked by it.
    """
    with conn.cursor() as cur:
        wms = current_watermarks(cur)
        key_max = key_max_event_times(cur)
        sides = effective_data_sides(cur)
        due = due_graces(cur)
        held = active_grace_pairs(cur)

        def tail_is_migrating(cur, key, ws):
            """Live check: this from_key window is covered by an unfinished
            outbound migration (OPEN holds it pending the cut; CUT/REOPENED
            routed it away). Queried per window rather than from a snapshot
            taken at tick start, because derive_migrations runs earlier in
            the same tick and can flip OPEN -> CUT in between."""
            cur.execute(
                """SELECT 1 FROM key_migrations
                   WHERE from_key = %s AND status <> 'VOID'
                     AND %s >= start_window_start LIMIT 1""",
                (key, ws))
            return cur.fetchone() is not None

    for g in due:
        key, ws = g["key"], g["window_start"]
        with conn.cursor() as cur:
            migrating = tail_is_migrating(cur, key, ws)
        if migrating:
            continue  # an outbound migration holds/routed this tail window
        end = ws + WINDOW_MS
        marks = emission_marks(wms, key_max, sides, key, ws)
        # The deadline, not the crossing evidence, decides now: emit takes
        # the ACTIVE row FOR UPDATE and flips it FIRED/EXPIRED atomically.
        emit(conn, ws, key, "INITIAL", {
            "grace": {"id": g["id"], "extra_ms": g["extra_ms"], "fired_at_deadline": True},
            "side_a": side_evidence(marks["a"], end),
            "side_b": side_evidence(marks["b"], end),
            "watermark_a": marks["a"]["watermark"],
            "watermark_b": marks["b"]["watermark"],
        }, grace=g)
        held.discard((ws, key))
    for key, ws in sides:
        if (ws, key) in held:
            continue  # an active grace holds exactly this one pair
        with conn.cursor() as cur:
            migrating = tail_is_migrating(cur, key, ws)
        if migrating:
            continue  # an unfinished outbound migration covers this tail
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
    the same per-key gate that drives emission. A window held by an ACTIVE
    close grace counts as pending: it deliberately waits past the window end
    for the other side and must not read as a missing result to the order;
    the deadline sweep emits it before orders are built on the same tick.

    A window of this key is not this order's business at all while an
    outbound migration covers it: an OPEN migration HOLDS its tail pending
    the cut (counted as pending — the business is explicitly waiting, not
    missing a result), and a CUT/REOPENED migration has routed the tail to
    another key (excluded entirely, like a window with no own events)."""
    wms = current_watermarks(cur)
    key_max = key_max_event_times(cur, key)
    sides = effective_data_sides(cur, key)
    graced = active_grace_pairs(cur)
    held = held_tail_pairs(cur)
    routed_from = routed_tail_windows(cur, key)
    missing, pending = [], []
    for ws in business_windows(cur, key) - bound:
        if routed_from is not None and ws >= routed_from:
            continue  # tail routed away to the to_key — not this order
        if (ws, key) in held:
            pending.append(ws)  # an OPEN migration holds this tail window
            continue
        if (ws, key) in graced:
            pending.append(ws)
            continue
        marks = emission_marks(wms, key_max, sides, key, ws)
        due = window_ready(marks["a"], marks["b"], ws + WINDOW_MS)
        (missing if due else pending).append(ws)
    return sorted(missing), sorted(pending)


def fold_order(cur, key, reason, trigger_ws=None, trigger_result_version=None,
               trigger_result_id=None):
    """Recompute the key's order from its current bindings and append one
    order version. Used by the result builder (trigger = the result version
    just committed) and by gap-carry resolution (reason CARRY_RESOLVED,
    trigger = the target result that closed a carry). Returns
    (order_id, version, status) or None when the key has no order row yet.

    The caller already holds the key's gate row lock; a non-advancing fold
    (identical status and snapshot) writes nothing.
    """
    cur.execute("SELECT id, head_version, ever_closed, status FROM biz_orders WHERE key = %s",
                (key,))
    row = cur.fetchone()
    if row is None:
        return None
    order_id, head_version, ever_closed, prev_status = row
    cur.execute(
        """SELECT window_start, window_end, result_version, result_status, has_gap,
                  match_count, unmatched_a, unmatched_b, carried_a, carried_b,
                  payload_hash
           FROM biz_order_windows WHERE order_id = %s ORDER BY window_start""",
        (order_id,),
    )
    cols = [d[0] for d in cur.description]
    bindings = [dict(zip(cols, r)) for r in cur.fetchall()]
    missing, pending = order_window_gaps(cur, key, {b["window_start"] for b in bindings})
    cur.execute(
        "SELECT count(*) FROM gap_carries WHERE key = %s AND status IN ('OPEN', 'REOPENED')",
        (key,),
    )
    open_carries = cur.fetchone()[0]
    cur.execute(
        "SELECT count(*) FROM key_migrations WHERE from_key = %s AND status = 'OPEN'",
        (key,),
    )
    open_migrations = cur.fetchone()[0]
    status = evaluate_order(bindings, missing, pending, ever_closed, reason,
                            open_carry_count=open_carries,
                            open_migration_count=open_migrations,
                            prev_status=prev_status)
    version = head_version + 1
    cur.execute(
        """INSERT INTO biz_order_versions
               (order_id, version, status, reason, trigger_window_start,
                trigger_result_version, trigger_result_id, snapshot,
                missing_windows, pending_windows)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (order_id, version, status, reason, trigger_ws, trigger_result_version,
         trigger_result_id,
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
    return order_id, version, status


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
    carried = (payload or {}).get("carried") or {}
    carried_a = len(carried.get("a", []))
    carried_b = len(carried.get("b", []))
    with conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO biz_orders (key, status, head_version)
               VALUES (%s, 'OPEN', 0) ON CONFLICT (key) DO NOTHING""",
            (key,),
        )
        cur.execute(
            "SELECT head_version FROM biz_orders WHERE key = %s",
            (key,),
        )
        head_version = cur.fetchone()[0]
        cur.execute(
            "SELECT result_status FROM biz_order_windows WHERE key = %s AND window_start = %s",
            (key, ws),
        )
        prev = cur.fetchone()
        cur.execute(
            """INSERT INTO biz_order_windows
                   (order_id, key, window_start, window_end, result_version, result_status,
                    has_gap, match_count, unmatched_a, unmatched_b, carried_a, carried_b,
                    payload_hash)
               VALUES ((SELECT id FROM biz_orders WHERE key = %s), %s, %s, %s, %s, %s,
                       %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (key, window_start) DO UPDATE SET
                   result_version = EXCLUDED.result_version,
                   result_status  = EXCLUDED.result_status,
                   has_gap        = EXCLUDED.has_gap,
                   match_count    = EXCLUDED.match_count,
                   unmatched_a    = EXCLUDED.unmatched_a,
                   unmatched_b    = EXCLUDED.unmatched_b,
                   carried_a      = EXCLUDED.carried_a,
                   carried_b      = EXCLUDED.carried_b,
                   payload_hash   = EXCLUDED.payload_hash,
                   updated_at     = now()""",
            (key, key, ws, we, result["version"], result_status, has_gap,
             match_count, unmatched_a, unmatched_b, carried_a, carried_b,
             result["payload_hash"]),
        )
        reason = "ORDER_OPENED" if head_version == 0 else order_reason(
            prev[0] if prev else None, result_status)
        folded = fold_order(cur, key, reason, ws, result["version"], result["id"])
        cur.execute("UPDATE biz_order_state SET last_result_id = %s WHERE id = 1",
                    (result["id"],))
    _, version, status = folded
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


# ---------------------------------------------------------------------------
# gap carry-forwards (缺口结转)
# ---------------------------------------------------------------------------
# A carry routes the leftover events of one side of a CLOSED source window to
# a LATER, still-unemitted window of the SAME business key. The machinery below
# enforces, all under the key's gate row lock (the same lock emit() takes, so
# the "target not emitted yet" rule cannot be raced by a close on another
# thread):
#
# - only live leftovers of the source's CURRENT head can be routed; one event
#   rides at most one carry in the whole history (gap_carry_items' global unique
#   index, so a VOID carry's events can never be carried a second time, and an
#   event can never sit in two OPEN carries);
# - the target is the same key, a later window boundary, and has produced no
#   result yet ("只能接到还没出过结果的后面那一窗，已经出过的再接要失败");
# - once a source or target correction/retraction touches a closed carry it is
#   REOPENED (matched again on the next derivation) or VOID (a carried event
#   was retracted / got paired at the source by a late event) — it never keeps
#   showing CLOSED; void carries can never match again;
# - target pairing is derived from the target's stored head payload: pairs
#   tagged with the carry id are visibly carry pairs (never the target's own
#   pair); all items matched closes the carry and freezes matched_snapshot
#   ("对上了要关上，并留下当时对上的样子").

def carry_event(cur, carry_id, key, event, target_version=None, detail=None):
    cur.execute(
        """INSERT INTO gap_carry_events (carry_id, key, event, target_version, detail)
           VALUES (%s, %s, %s, %s, %s)""",
        (carry_id, key, event, target_version,
         psycopg2.extras.Json(detail) if detail is not None else None),
    )


def carry_row_dict(cur, carry_id):
    cur.execute(
        """SELECT c.*,
                  (SELECT count(*) FROM gap_carry_items i WHERE i.carry_id = c.id)
                      AS n_items,
                  (SELECT count(*) FROM gap_carry_items i
                    WHERE i.carry_id = c.id AND i.item_status = 'MATCHED')
                      AS n_matched
           FROM gap_carries c WHERE c.id = %s""",
        (carry_id,),
    )
    return dict(zip([d[0] for d in cur.description], cur.fetchone()))


def create_carry(conn, source_window_start, target_window_start, key, side,
                 event_ids=None, operator=None, note=None):
    """Open one gap carry from a closed source window's leftovers to a later,
    not-yet-emitted window of the same key.

    ``event_ids`` defaults to every CURRENT leftover of the carried side.
    Raises HTTPException: 422 bad window boundary/order/side, 404 source has
    no live head, 409 target already emitted / not later / event not a leftover
    / event already on another carry (open, closed or void).
    """
    if side not in ("a", "b"):
        raise HTTPException(422, "side must be 'a' or 'b'")
    if not valid_window_start(source_window_start, WINDOW_MS):
        raise HTTPException(422, f"source_window_start {source_window_start} is not a window boundary")
    if not valid_window_start(target_window_start, WINDOW_MS):
        raise HTTPException(422, f"target_window_start {target_window_start} is not a window boundary")
    if target_window_start <= source_window_start:
        raise HTTPException(422, "target_window_start must be a LATER window than the source")
    with conn, conn.cursor() as cur:
        # Same lock order as every other writer: key gate row first.
        cur.execute(
            "INSERT INTO release_gates (key) VALUES (%s) ON CONFLICT (key) DO NOTHING",
            (key,),
        )
        cur.execute("SELECT 1 FROM release_gates WHERE key = %s FOR UPDATE", (key,))
        cur.fetchone()

        head = get_head(cur, source_window_start, key)
        if head is None or head["status"] == "RETRACTED":
            raise HTTPException(
                404,
                f"source window {source_window_start} of key {key!r} has no live result "
                "to carry from")
        # Plain own-events recomputation: carry creation only routes THIS
        # window's own leftovers, so routed-in carry/migration guests must not
        # appear as its gap.
        native = render_window_payload(cur, source_window_start, key,
                                       autoload=False)
        leftovers = set(native.get(f"unmatched_{side}", []))
        if not leftovers:
            raise HTTPException(
                409,
                f"source window {source_window_start} of key {key!r} has no leftover on "
                f"side {side!r} to carry")
        # The target must never have produced a result — not even a retracted
        # one ("已经出过的再接要失败"); what went out (or briefly existed) is a
        # fact, corrections are its only path, not an incoming carry.
        target_head = get_head(cur, target_window_start, key)
        if target_head is not None:
            raise HTTPException(
                409,
                f"target window {target_window_start} of key {key!r} already produced "
                f"result v{target_head['version']} — a carry can only target a window "
                "that has not emitted yet (只能接到还没出过结果的后面那一窗)")
        wanted = leftovers if event_ids is None else set(event_ids)
        if not wanted:
            raise HTTPException(422, "event_ids must be non-empty")
        unknown = sorted(wanted - leftovers)
        if unknown:
            raise HTTPException(
                409,
                f"event(s) {unknown} are not a CURRENT unmatched_{side} leftover of "
                f"source window {source_window_start} (already paired, retracted, or "
                "already carried out)")
        # Global history check — one event rides at most one carry, ever. The
        # partial-less unique index on gap_carry_items is the hard backstop.
        cur.execute(
            """SELECT i.event_id, i.carry_id, c.status
               FROM gap_carry_items i JOIN gap_carries c ON c.id = i.carry_id
               WHERE i.key = %s AND i.side = %s AND i.event_id = ANY(%s)""",
            (key, side, sorted(wanted)),
        )
        dupes = cur.fetchall()
        if dupes:
            detail = "; ".join(
                f"{eid} already on carry {cid} ({st})" for eid, cid, st in dupes)
            raise HTTPException(
                409,
                f"the same unmatched event cannot ride two carries — and a void "
                f"carry's events can never be carried again: {detail}")

        by_stream = load_effective_events(cur, source_window_start, key)
        events = sorted((e for e in by_stream[side] if e["event_id"] in wanted),
                        key=lambda e: (e["event_time"], e["event_id"]))
        missing_events = sorted(wanted - {e["event_id"] for e in events})
        if missing_events:
            raise HTTPException(
                409,
                f"event(s) {missing_events} are not effective {side}-side events of "
                f"source window {source_window_start} (retracted or from the other side)")
        cur.execute(
            """INSERT INTO gap_carries
                   (key, side, source_window_start, source_window_end,
                    target_window_start, target_window_end, source_version,
                    created_by, note)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *""",
            (key, side, source_window_start, source_window_start + WINDOW_MS,
             target_window_start, target_window_start + WINDOW_MS,
             head["version"], operator, note),
        )
        carry = dict(zip([d[0] for d in cur.description], cur.fetchone()))
        cid = carry["id"]
        for e in events:
            cur.execute(
                """INSERT INTO gap_carry_items
                       (carry_id, key, side, event_id, event_time, payload)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (cid, key, side, e["event_id"], e["event_time"],
                 psycopg2.extras.Json(e["payload"]) if e.get("payload") is not None else None),
            )
        carry_event(cur, cid, key, "CARRY_OPENED", detail={
            "source_window_start": source_window_start,
            "target_window_start": target_window_start,
            "side": side,
            "source_version": head["version"],
            "events": [e["event_id"] for e in events],
        }, )
        carry = carry_row_dict(cur, cid)

    # Same logical operation: the source window's result now shows those
    # leftovers as routed out (a new audited result version). Separate
    # transaction on purpose — if it fails, a later tick's emit reaches the
    # same content via outgoing_carries_for_window, and the reverse split (carry
    # row without the correction) self-heals the same way. emit() recomputes
    # from the committed carry, so no annotation is passed in.
    emitted = emit(conn, source_window_start, key, "CARRY_FORWARD", {
        "carry_id": cid, "op": "opened",
        "target_window_start": target_window_start, "side": side,
        "events": [e["event_id"] for e in events],
    })
    log.info("gap carry %s opened: key=%s side=%s window %d -> %d events=%d (source re-emitted=%s)",
             cid, key, side, source_window_start, target_window_start, len(events), emitted)
    return carry


def invalidate_dirty_carries(conn, dirty):
    """Re-validate carries whose SOURCE window changed (late event /
    retraction pulled in this tick), BEFORE the dirty windows are re-emitted.

    A carried event that was retracted (source_retracted) or got paired at the
    source by a late opposite-side event (source_paired) kills the carry
    outright (VOID: dead forever, never matches again). A CLOSED carry whose
    leftovers merely moved (hash drift without a dead item) is REOPENED here —
    the closed match is a fact of the past version, but it must not keep
    showing CLOSED against the new source; the next derivation closes it again
    with a fresh snapshot if the target still pairs everything.

    Returns the (key, target_window_start) set of carries voided this call, so
    the caller can recompute their already-emitted targets in the same tick.
    """
    voided_targets = set()
    by_key_ws = {}
    for ws, key, reason, detail in dirty:
        by_key_ws.setdefault((key, ws), reason)
    if not by_key_ws:
        return voided_targets
    with conn, conn.cursor() as cur:
        for (key, ws), reason in by_key_ws.items():
            # Lock order matches every other writer: key gate row first,
            # then that key's carry rows (create_carry holds the gate lock
            # while inserting, so the reverse order would deadlock).
            cur.execute(
                "INSERT INTO release_gates (key) VALUES (%s) ON CONFLICT (key) DO NOTHING",
                (key,),
            )
            cur.execute("SELECT 1 FROM release_gates WHERE key = %s FOR UPDATE", (key,))
            cur.fetchone()
            cur.execute(
                """SELECT id, target_window_start FROM gap_carries
                   WHERE key = %s AND source_window_start = %s
                     AND status <> 'VOID'
                   ORDER BY id FOR UPDATE""",
                (key, ws),
            )
            carry_rows = cur.fetchall()
            if not carry_rows:
                continue
            carry_ids = [r[0] for r in carry_rows]
            targets = {cid: r[1] for cid, r in zip(carry_ids, carry_rows)}
            # Plain own-events recompute: a carried event's fate is judged
            # against the source window's own pairing, never routed guests.
            native = render_window_payload(cur, ws, key, autoload=False)
            by_stream = load_effective_events(cur, ws, key)
            for cid in carry_ids:
                cur.execute(
                    "SELECT side, status FROM gap_carries WHERE id = %s FOR UPDATE",
                    (cid,),
                )
                side, status = cur.fetchone()
                live_ids = {e["event_id"] for e in by_stream[side]}
                cur.execute(
                    """SELECT event_id FROM gap_carry_items
                       WHERE carry_id = %s AND item_status <> 'DEAD'
                       ORDER BY event_id""",
                    (cid,),
                )
                items = [r[0] for r in cur.fetchall()]
                dead = {}
                for eid in items:
                    fate = carry_item_fate(eid, side, native, live_ids)
                    if fate:
                        dead[eid] = fate
                if dead:
                    cur.execute(
                        """UPDATE gap_carry_items SET item_status = 'DEAD', updated_at = now()
                           WHERE carry_id = %s AND event_id = ANY(%s)""",
                        (cid, sorted(dead)),
                    )
                    # A mixed batch (some retracted, some source-paired) dies
                    # with the retraction as the void reason — dead either way.
                    reason0 = ("source_retracted"
                               if "source_retracted" in dead.values()
                               else "source_paired")
                    cur.execute(
                        """UPDATE gap_carries
                           SET status = 'VOID', void_reason = %s, voided_at = now()
                           WHERE id = %s""",
                        (reason0, cid),
                    )
                    carry_event(cur, cid, key, "CARRY_VOIDED", detail={
                        "reason": reason0, "dead_events": sorted(dead),
                        "trigger": reason,
                    })
                    voided_targets.add((key, targets[cid]))
                    log.info("gap carry %s VOID (%s: %s) by source window %d correction",
                             cid, reason0, sorted(dead), ws)
                    continue
                if status == "CLOSED":
                    # No dead item, but the source content moved behind the
                    # closed carry — "原来那一窗后来又订正" invalidates the close
                    # until re-derived.
                    cur.execute(
                        """UPDATE gap_carries
                           SET status = 'REOPENED', target_version = NULL,
                               matched_snapshot = NULL, closed_at = NULL,
                               reopened_at = now()
                           WHERE id = %s""",
                        (cid,),
                    )
                    cur.execute(
                        """UPDATE gap_carry_items
                           SET item_status = 'CARRIED', matched_version = NULL,
                               matched_against = NULL, updated_at = now()
                           WHERE carry_id = %s AND item_status = 'MATCHED'""",
                        (cid,),
                    )
                    carry_event(cur, cid, key, "CARRY_REOPENED", detail={
                        "reason": "source_corrected", "trigger": reason})
                    log.info("gap carry %s REOPENED by source window %d correction", cid, ws)
    return voided_targets


def derive_carries(conn):
    """Derive every live carry's item state from its target's CURRENT head.

    Runs once per tick after close/orders: target payloads already include
    live carry events (carries_for_window at emit time), and each carry pair is
    tagged with the carry id. Here those tags drive the item rows:

    - items the target no longer pairs: MATCHED -> CARRIED; a CLOSED carry with
      any such item flips REOPENED (a target correction/withdrawal undid it —
      "已经关上的必须重开，不能还显示对上了"); a fully-retracted target has no
      head payload, which reopens every carry aimed at it;
    - items newly paired: CARRIED -> MATCHED with the pairing event recorded;
    - every item MATCHED closes the carry: status CLOSED, target_version and
      the frozen matched_snapshot of "当时对上的样子", plus one order fold with
      reason CARRY_RESOLVED (genuine progress that may close a WAITING order).

    Idempotent: a carry whose derived state equals its stored state is untouched.
    """
    # (key, target_window, target_version) folds already recorded in this
    # derivation: several carries closing in one tick at the same target share
    # one CARRY_RESOLVED order version, not one each.
    folded = set()
    with conn, conn.cursor() as cur:
        cur.execute(
            """SELECT id, key, side, target_window_start
               FROM gap_carries
               WHERE status IN ('OPEN', 'REOPENED', 'CLOSED')
               ORDER BY id""")
        live = cur.fetchall()
        # One key at a time, gate lock first and carry rows second — the same
        # order as create_carry/invalidate/emit, so concurrent writers cannot
        # deadlock against this sweep.
        by_key = {}
        for cid, key, side, tws in live:
            by_key.setdefault(key, []).append((cid, side, tws))
        for key in sorted(by_key):
            cur.execute(
                "INSERT INTO release_gates (key) VALUES (%s) ON CONFLICT (key) DO NOTHING",
                (key,),
            )
            cur.execute("SELECT 1 FROM release_gates WHERE key = %s FOR UPDATE", (key,))
            cur.fetchone()
        for cid, key, side, tws in live:
            cur.execute("SELECT status, target_version FROM gap_carries WHERE id = %s FOR UPDATE",
                        (cid,))
            status, closed_version = cur.fetchone()
            head = get_head_full(cur, tws, key)
            payload = head["payload"] if head and head["status"] != "RETRACTED" else None
            cur.execute(
                """SELECT event_id, item_status, matched_version, matched_against
                   FROM gap_carry_items WHERE carry_id = %s ORDER BY event_id""",
                (cid,),
            )
            full = cur.fetchall()
            cur_ids = {eid for eid, *_ in full}

            # The target payload's CURRENT view of this carry:
            # {carried_event_id: (opposite event id, target version)}.
            current_pairs = {}
            if payload is not None:
                other = opposite_side(side)
                for p in payload.get("pairs", []):
                    tag = p.get("carry")
                    if tag and tag.get("carry_id") == cid:
                        current_pairs[p[f"{side}_event_id"]] = (
                            p[f"{other}_event_id"], head["version"])

            def reopen(reason, detail):
                """Move the carry back to REOPENED (CLOSED -> open); the rest
                of this same derivation re-closes it when everything still
                pairs, recording a fresh close snapshot and event trail."""
                cur.execute(
                    """UPDATE gap_carries
                       SET status = 'REOPENED', target_version = NULL,
                           matched_snapshot = NULL, closed_at = NULL,
                           reopened_at = now()
                       WHERE id = %s""",
                    (cid,),
                )
                cur.execute(
                    """UPDATE gap_carry_items
                       SET item_status = 'CARRIED', matched_version = NULL,
                           matched_against = NULL, updated_at = now()
                       WHERE carry_id = %s""",
                    (cid,),
                )
                carry_event(cur, cid, key, "CARRY_REOPENED", detail=detail)
                log.info("gap carry %s REOPENED (%s): %s", cid, reason, detail)

            lost = [eid for eid, st, _, _ in full
                    if st == "MATCHED" and eid not in current_pairs]
            drift = []
            if status == "CLOSED" and not lost:
                # Every event still pairs, but did the pairing (or the target
                # version) move? "对上了" froze the then-current match; a
                # different match/version must visibly reopen+reclose.
                for eid, st, mv, magainst in full:
                    now_pair = current_pairs.get(eid)
                    if (st == "MATCHED" and now_pair is not None
                            and (now_pair[0] != magainst or now_pair[1] != mv)):
                        drift.append({"event_id": eid,
                                      "matched_against": magainst,
                                      "now_against": now_pair[0]})
            if status == "CLOSED" and (lost or drift):
                reopen("target correction", {
                    "reason": "target_corrected",
                    "lost_events": lost, "drifted_pairs": drift,
                    "target_window_start": tws,
                    "target_version": head["version"] if head else None})
                status = "REOPENED"
            # Record fresh pairings for OPEN/reopened carries. A CLOSED carry
            # with nothing lost and nothing drifted is fully stable: its
            # snapshot/events are never rewritten on repeat derivations.
            new_pairs = []
            if status in ("OPEN", "REOPENED") and payload is not None:
                new_pairs = sorted(current_pairs)
                for eid in new_pairs:
                    against_eid, ver = current_pairs[eid]
                    cur.execute(
                        """UPDATE gap_carry_items
                           SET item_status = 'MATCHED', matched_version = %s,
                               matched_against = %s, updated_at = now()
                           WHERE carry_id = %s AND event_id = %s
                             AND item_status = 'CARRIED'""",
                        (ver, against_eid, cid, eid),
                    )
                    # Rowcount guard: the same target version persists across
                    # ticks, so only the transition CARRIED -> MATCHED writes
                    # the event — a repeat derivation is a no-op, not another
                    # ITEM_MATCHED.
                    if cur.rowcount:
                        carry_event(cur, cid, key, "ITEM_MATCHED",
                                    target_version=ver, detail={
                                        "event_id": eid, "matched_against": against_eid,
                                        "target_window_start": tws})
            paired = set(current_pairs)
            if status in ("OPEN", "REOPENED") and cur_ids and cur_ids <= paired:
                # Everything matched: close the carry and freeze the picture.
                cur.execute(
                    """SELECT event_id, matched_against
                       FROM gap_carry_items WHERE carry_id = %s ORDER BY event_id""",
                    (cid,),
                )
                matches = [{"event_id": eid, "matched_against": other}
                           for eid, other in cur.fetchall()]
                snapshot = {
                    "target_window_start": tws,
                    "target_version": head["version"],
                    "target_payload_hash": head["payload_hash"],
                    "side": side,
                    "matches": matches,
                }
                cur.execute(
                    """UPDATE gap_carries
                       SET status = 'CLOSED', target_version = %s,
                           matched_snapshot = %s, closed_at = now(),
                           reopened_at = NULL, voided_at = NULL
                       WHERE id = %s""",
                    (head["version"], psycopg2.extras.Json(snapshot), cid),
                )
                carry_event(cur, cid, key, "CARRY_CLOSED",
                            target_version=head["version"], detail=snapshot)
                log.info("gap carry %s CLOSED at target window %d v%d (%d events)",
                         cid, tws, head["version"], len(matches))
                # The carry resolution is genuine progress: re-fold the order so
                # a WAITING order can reach CLOSED without waiting for another
                # result version. One CARRY_RESOLVED version per (target, target
                # version) — deduplicated across carries closing this sweep and
                # against prior sweeps via the order-version history check.
                sig = (key, tws, head["version"])
                if sig not in folded:
                    folded.add(sig)
                    cur.execute("SELECT id FROM biz_orders WHERE key = %s", (key,))
                    order_row = cur.fetchone()
                    already = False
                    if order_row is not None:
                        cur.execute(
                            """SELECT 1 FROM biz_order_versions
                               WHERE order_id = %s AND reason = 'CARRY_RESOLVED'
                                 AND trigger_window_start = %s
                                 AND trigger_result_version = %s LIMIT 1""",
                            (order_row[0], tws, head["version"]),
                        )
                        already = cur.fetchone() is not None
                    if order_row is not None and not already:
                        cur.execute(
                            "SELECT id FROM results WHERE window_start = %s AND key = %s "
                            "AND version = %s",
                            (tws, key, head["version"]),
                        )
                        rid = cur.fetchone()[0]
                        fold_order(cur, key, "CARRY_RESOLVED", tws, head["version"], rid)


def emit_voided_targets(conn, voided_targets):
    """Carries voided this tick took their events out of the target's pairing:
    an already-emitted target window must be corrected in the same tick.
    Targets with no result yet need nothing: their normal INITIAL render
    (carries_for_window skips VOID carries) is already correct."""
    for key, tws in sorted(voided_targets):
        with conn.cursor() as cur:
            h = get_head(cur, tws, key)
        if h is not None:
            emit(conn, tws, key, "CARRY_FORWARD", {"op": "carry_voided"})


# ---------------------------------------------------------------------------
# business-key migrations (业务键迁出)
# ---------------------------------------------------------------------------
# A migration routes the whole tail of one business key (from_key) to another
# key (to_key) from a start window S on. While OPEN the from_key's windows at
# and after S are held (no own result); once both sides of the from_key have
# crossed S's end the migration CUTS: every surviving from_key event with
# event_time >= S starts routing into the to_key, the cut snapshots the two
# sides' versions, and the from_key is sealed (later upserts are rejected at
# pull time). A later correction/retraction invalidates the cut (REOPENED);
# the derivation either settles a fresh CUT (MIGRATION_RECUT) or auto-voids
# when nothing routed survives. Manual void is only possible while OPEN.
#
# Every writer takes the involved keys' gate row locks in a fixed order
# (sorted by key, then the migration row), the same order the emit path uses,
# so a cut emitting to_key windows and an ordinary from_key emit cannot
# interleave.

def migration_event(cur, migration_id, from_key, to_key, event,
                    window_start=None, detail=None, operator=None):
    cur.execute(
        """INSERT INTO key_migration_events
               (migration_id, from_key, to_key, event, window_start, detail, operator)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (migration_id, from_key, to_key, event, window_start,
         psycopg2.extras.Json(detail) if detail is not None else None, operator),
    )


def migration_row_dict(cur, migration_id):
    cur.execute("SELECT * FROM key_migrations WHERE id = %s", (migration_id,))
    row = cur.fetchone()
    return dict(row) if isinstance(row, dict) else (
        dict(zip([d[0] for d in cur.description], row)) if row else None)


def outbound_migrations_for_key(cur, key):
    """All non-VOID migrations routing this key's tail AWAY: status, start
    window. A CUT/REOPENED migration removes the key's tail windows from its
    order; an OPEN migration blocks its order from CLOSED."""
    cur.execute(
        """SELECT id, to_key, start_window_start, status
           FROM key_migrations WHERE from_key = %s AND status <> 'VOID'
           ORDER BY id""",
        (key,),
    )
    return [dict(zip(["id", "to_key", "start_window_start", "status"], r))
            for r in cur.fetchall()]


def routed_events_signature(cur, migration_id):
    """The live routed set of a (cut or reopened) migration: every surviving
    (non-retracted, non-rejected) from_key upsert at/after the start window,
    bucketed by ORIGIN window, plus the per-origin-window lists. This is the
    frozen content a cut promises; a change on the next derivation invalidates
    the cut (REOPENED). Returns ``{"windows": {ws: {"a": [...], "b": [...]}},
    "events": {event_id: (side, origin_ws)}}``."""
    cur.execute(
        """SELECT e.stream, e.event_id, (e.event_time / %s) * %s AS origin_ws
           FROM key_migrations m
           JOIN stream_events e
             ON e.key = m.from_key AND e.type = 'upsert' AND NOT e.rejected
                AND e.event_time >= m.start_window_start
                AND NOT EXISTS (
                    SELECT 1 FROM stream_events r
                    WHERE r.type = 'retract' AND r.stream = e.stream
                      AND r.retracts = e.event_id)
           WHERE m.id = %s
           ORDER BY origin_ws, e.stream, e.event_time, e.event_id""",
        (WINDOW_MS, WINDOW_MS, migration_id),
    )
    windows, events = {}, {}
    for side, event_id, origin_ws in cur.fetchall():
        windows.setdefault(origin_ws, {"a": [], "b": []})[side].append(event_id)
        events[event_id] = (side, origin_ws)
    return {"windows": windows, "events": events}


def create_migration(conn, from_key, to_key, start_window_start,
                     operator=None, note=None):
    """Declare a migration (status OPEN). Preconditions (migration_create_error):
    from_key != to_key; the start window is a boundary (422); neither key has
    produced a result at S or later (409); neither key is already a side of an
    unfinished migration (409). The from_key's windows at/after S are held from
    the next tick on; the declaration itself emits nothing."""
    if not from_key or not to_key:
        raise HTTPException(422, "from_key and to_key are required")
    if not valid_window_start(start_window_start, WINDOW_MS):
        raise HTTPException(422,
                            f"start_window_start {start_window_start} is not a window boundary")
    keys = sorted({from_key, to_key})
    with conn, conn.cursor() as cur:
        # Fixed global lock order across both keys.
        for k in keys:
            cur.execute(
                "INSERT INTO release_gates (key) VALUES (%s) ON CONFLICT (key) DO NOTHING",
                (k,),
            )
        for k in keys:
            cur.execute("SELECT 1 FROM release_gates WHERE key = %s FOR UPDATE", (k,))
            cur.fetchone()

        from_emitted = get_head(cur, start_window_start, from_key) is not None
        # Any result at S or later on the from_key tail blocks declaration.
        if not from_emitted:
            cur.execute(
                "SELECT 1 FROM results WHERE key = %s AND window_start >= %s LIMIT 1",
                (from_key, start_window_start),
            )
            from_emitted = cur.fetchone() is not None
        cur.execute(
            "SELECT 1 FROM results WHERE key = %s AND window_start >= %s LIMIT 1",
            (to_key, start_window_start),
        )
        to_emitted = cur.fetchone() is not None
        cur.execute(
            "SELECT 1 FROM key_migrations WHERE from_key = %s AND status <> 'VOID' LIMIT 1",
            (from_key,),
        )
        from_busy = cur.fetchone() is not None
        cur.execute(
            """SELECT 1 FROM key_migrations
               WHERE (from_key = %s OR to_key = %s) AND status <> 'VOID' LIMIT 1""",
            (to_key, to_key),
        )
        to_busy = cur.fetchone() is not None
        err = migration_create_error(from_emitted, to_emitted,
                                     from_busy, to_busy, from_key == to_key)
        if err is not None:
            messages = {
                "same_key": "from_key and to_key must differ (同一业务键不能迁给自己)",
                "from_window_emitted":
                    f"from_key {from_key!r} already produced a result at window "
                    f"{start_window_start} or later — 已经出过结果的尾巴不能迁出",
                "to_window_emitted":
                    f"to_key {to_key!r} already produced a result at window "
                    f"{start_window_start} or later — 迁入键已经出过的窗口不能当起始窗",
                "from_key_busy":
                    f"from_key {from_key!r} already has an unfinished migration "
                    "(同一迁出键不能同时开着两笔迁出)",
                "to_key_busy":
                    f"to_key {to_key!r} is already a side of an unfinished migration "
                    "(一个键不能同时卷进两笔没结束的迁出)",
            }
            raise HTTPException(409, messages[err])

        cur.execute(
            """INSERT INTO key_migrations
                   (from_key, to_key, start_window_start, start_window_end,
                    created_by, note)
               VALUES (%s, %s, %s, %s, %s, %s) RETURNING *""",
            (from_key, to_key, start_window_start,
             start_window_start + WINDOW_MS, operator, note),
        )
        row = cur.fetchone()
        migration = dict(row) if isinstance(row, dict) else dict(
            zip([d[0] for d in cur.description], row))
        mid = migration["id"]
        migration_event(cur, mid, from_key, to_key, "MIGRATION_OPENED",
                        window_start=start_window_start,
                        detail={"start_window_start": start_window_start},
                        operator=operator)
        migration = migration_row_dict(cur, mid)
    log.info("migration %s opened: %s -> %s from window %d",
             mid, from_key, to_key, start_window_start)
    return migration


def move_migration_start(conn, migration_id, new_start_window_start,
                         operator=None):
    """Move the start window of an OPEN migration (the declaration is not cut
    yet). Same preconditions on the new S as a fresh declaration; the old tail
    windows go back to ordinary closing. Append-only trail."""
    if not valid_window_start(new_start_window_start, WINDOW_MS):
        raise HTTPException(422,
                            f"start_window_start {new_start_window_start} is not a window boundary")
    with conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM key_migrations WHERE id = %s FOR UPDATE",
                    (migration_id,))
        row = cur.fetchone()
        if row is None:
            raise HTTPException(404, "no such migration")
        m = dict(row) if isinstance(row, dict) else dict(
            zip([d[0] for d in cur.description], row))
        if m["status"] != "OPEN":
            raise HTTPException(
                409, f"migration {migration_id} is {m['status']} — only an OPEN "
                "migration's start window can be moved")
        from_key, to_key, old_start = m["from_key"], m["to_key"], m["start_window_start"]
        if new_start_window_start == old_start:
            return migration_row_dict(cur, migration_id)
        for k in sorted({from_key, to_key}):
            cur.execute("SELECT 1 FROM release_gates WHERE key = %s FOR UPDATE", (k,))
            cur.fetchone()
        cur.execute(
            "SELECT 1 FROM results WHERE key = %s AND window_start >= %s LIMIT 1",
            (from_key, new_start_window_start),
        )
        from_emitted = cur.fetchone() is not None
        cur.execute(
            "SELECT 1 FROM results WHERE key = %s AND window_start >= %s LIMIT 1",
            (to_key, new_start_window_start),
        )
        to_emitted = cur.fetchone() is not None
        if from_emitted:
            raise HTTPException(
                409, f"from_key {from_key!r} already produced a result at window "
                f"{new_start_window_start} or later")
        if to_emitted:
            raise HTTPException(
                409, f"to_key {to_key!r} already produced a result at window "
                f"{new_start_window_start} or later")
        cur.execute(
            """UPDATE key_migrations
               SET start_window_start = %s, start_window_end = %s
               WHERE id = %s""",
            (new_start_window_start, new_start_window_start + WINDOW_MS, migration_id),
        )
        migration_event(cur, migration_id, from_key, to_key,
                        "START_WINDOW_CHANGED", window_start=new_start_window_start,
                        detail={"old_start_window_start": old_start,
                                "new_start_window_start": new_start_window_start},
                        operator=operator)
        migration = migration_row_dict(cur, migration_id)
    log.info("migration %s start window moved: %d -> %d",
             migration_id, old_start, new_start_window_start)
    return migration


def void_migration(conn, migration_id, reason="operator_void", operator=None):
    """Void an OPEN migration (the only manual-void state) or an already
    auto-voided row (idempotent). After a void the from_key emits its own
    windows again; the same row can never cut. The from_key order is folded
    (MIGRATION_VOIDED) once, and its windows return to ordinary closing this
    tick."""
    if reason not in ("operator_void", "source_retracted"):
        raise HTTPException(422, "bad void reason")
    with conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM key_migrations WHERE id = %s FOR UPDATE",
                    (migration_id,))
        row = cur.fetchone()
        if row is None:
            raise HTTPException(404, "no such migration")
        m = dict(row) if isinstance(row, dict) else dict(
            zip([d[0] for d in cur.description], row))
        if m["status"] == "VOID":
            return migration_row_dict(cur, migration_id)
        if m["status"] != "OPEN" and reason == "operator_void":
            raise HTTPException(
                409, f"migration {migration_id} is {m['status']} — 作废只能在还没切过去时做")
        from_key, to_key = m["from_key"], m["to_key"]
        for k in sorted({from_key, to_key}):
            cur.execute("SELECT 1 FROM release_gates WHERE key = %s FOR UPDATE", (k,))
            cur.fetchone()
        cur.execute(
            """UPDATE key_migrations
               SET status = 'VOID', void_reason = %s, voided_at = now()
               WHERE id = %s""",
            (reason, migration_id),
        )
        migration_event(cur, migration_id, from_key, to_key, "MIGRATION_VOIDED",
                        window_start=m["start_window_start"],
                        detail={"reason": reason}, operator=operator)
        migration = migration_row_dict(cur, migration_id)
    # The from_key tail comes back: close it normally this tick, let the
    # ordinary builder fold its new results, then append the migration-driven
    # order version. Done outside the row-lock transaction via emit helpers.
    close_key_windows(conn, from_key)
    build_orders(conn)
    with conn, conn.cursor() as cur:
        cur.execute("SELECT id FROM biz_orders WHERE key = %s", (from_key,))
        if cur.fetchone() is not None:
            fold_order(cur, from_key, "MIGRATION_VOIDED",
                       m["start_window_start"], None, None)
    log.info("migration %s voided (%s): %s -> %s",
             migration_id, reason, from_key, to_key)
    return migration


def close_key_windows(conn, key):
    """Emit every now-due window of one key (ordinary per-key closing). Used
    after an OPEN migration is voided so its released tail starts producing
    its own results immediately."""
    with conn.cursor() as cur:
        wms = current_watermarks(cur)
        key_max = key_max_event_times(cur, key)
        sides = effective_data_sides(cur, key)
    for (k, ws) in list(sides.keys()):
        if k != key:
            continue
        with conn.cursor() as cur:
            if get_head(cur, ws, key) is not None:
                continue
        end = ws + WINDOW_MS
        marks = emission_marks(wms, key_max, sides, key, ws)
        if window_ready(marks["a"], marks["b"], end):
            emit(conn, ws, key, "INITIAL", {
                "side_a": side_evidence(marks["a"], end),
                "side_b": side_evidence(marks["b"], end),
                "watermark_a": marks["a"]["watermark"],
                "watermark_b": marks["b"]["watermark"],
            })


def cut_migration(cur, m, reopen):
    """Mark a migration CUT under the caller's transaction and held gate
    locks: freeze the routed signature, record the from_key head strictly
    before S ("两边当时各是哪一版"), and write the lifecycle event. Returns
    the sorted origin windows the live routing touches — the caller emits the
    corresponding to_key windows after the transaction commits."""
    mid = m["id"]
    from_key, to_key = m["from_key"], m["to_key"]
    start = m["start_window_start"]
    sig = routed_events_signature(cur, mid)
    cur.execute(
        """SELECT window_start, version FROM results
           WHERE key = %s AND window_start < %s
           ORDER BY window_start DESC, version DESC LIMIT 1""",
        (from_key, start),
    )
    pre = cur.fetchone()
    cut_from_window_start = pre[0] if pre else None
    cut_from_version = pre[1] if pre else None

    emitted_windows = sorted(sig["windows"].keys())
    cur.execute(
        """UPDATE key_migrations
           SET status = 'CUT', cut_at = now(), cut_signature = %s,
               cut_from_version = %s, cut_from_window_start = %s,
               recut_count = recut_count + %s, void_reason = NULL,
               voided_at = NULL, last_block_reason = NULL
           WHERE id = %s""",
        (psycopg2.extras.Json(sig["windows"]), cut_from_version,
         cut_from_window_start, 1 if reopen else 0, mid),
    )
    migration_event(cur, mid, from_key, to_key,
                    "MIGRATION_RECUT" if reopen else "MIGRATION_CUT",
                    window_start=start,
                    detail={"cut_from_version": cut_from_version,
                            "cut_from_window_start": cut_from_window_start,
                            "routed_windows": emitted_windows,
                            "routed_events": len(sig["events"])})
    return emitted_windows


def lock_keys(cur, keys):
    """Take the gate-row locks for every involved key in one fixed global
    order (sorted key), the same order every migration writer and emit()
    uses, so concurrent migrations can never deadlock."""
    for k in sorted(set(keys)):
        cur.execute(
            "INSERT INTO release_gates (key) VALUES (%s) ON CONFLICT (key) DO NOTHING",
            (k,))
    for k in sorted(set(keys)):
        cur.execute("SELECT 1 FROM release_gates WHERE key = %s FOR UPDATE", (k,))
        cur.fetchone()


def derive_migrations(conn):
    """One migration lifecycle sweep, every tick.

    Phase 1 (per migration, one locked transaction each) decides the state
    transition and commits it:
    - OPEN with both sides of the from_key across S's end: CUT (or auto-VOID
      source_retracted when nothing routable survives); an OPEN not yet
      crossed just records why it waits;
    - CUT/REOPENED re-derived from surviving events: no routed events left ->
      terminal VOID (source_retracted); the live per-origin routing differing
      from the frozen cut signature -> REOPENED then re-CUT in the same
      transaction with a fresh snapshot (MIGRATION_REOPENED + MIGRATION_RECUT,
      "已经切过去的必须重开再重切，不能还显示切成功了"); identical -> no-op.

    Phase 2 (after the state transaction commits) emits the affected to_key
    windows (render picks the now-committed routing up), lets the ordinary
    order builder fold them, and folds the from_key order once with
    MIGRATION_CUT / MIGRATION_VOIDED. Emitting under a separate transaction is
    the same split create_carry uses and self-heals on crash: the committed
    CUT row makes the next sweep/tick re-derive identical content.
    """
    with conn.cursor() as cur:
        cur.execute(
            """SELECT id FROM key_migrations
               WHERE status IN ('OPEN', 'CUT', 'REOPENED') ORDER BY id""")
        ids = [r[0] for r in cur.fetchall()]
        wms = current_watermarks(cur)
        key_max = key_max_event_times(cur)
        sides = effective_data_sides(cur)

    actions = []
    for mid in ids:
        action = None
        with conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM key_migrations WHERE id = %s FOR UPDATE",
                        (mid,))
            row = cur.fetchone()
            m = dict(row) if isinstance(row, dict) else dict(
                zip([d[0] for d in cur.description], row))
            from_key, to_key, start = m["from_key"], m["to_key"], m["start_window_start"]
            lock_keys(cur, (from_key, to_key))

            if m["status"] == "OPEN":
                marks = emission_marks(wms, key_max, sides, from_key, start)
                if not window_ready(marks["a"], marks["b"], start + WINDOW_MS):
                    why_a = side_evidence(marks["a"], start + WINDOW_MS)
                    why_b = side_evidence(marks["b"], start + WINDOW_MS)
                    reason = f"waiting: side_a={why_a} side_b={why_b}"
                    if m["last_block_reason"] != reason:
                        cur.execute(
                            "UPDATE key_migrations SET last_block_reason = %s WHERE id = %s",
                            (reason, mid))
                    continue
                sig = routed_events_signature(cur, mid)
                if not sig["events"]:
                    cur.execute(
                        """UPDATE key_migrations
                           SET status = 'VOID', void_reason = 'source_retracted',
                               voided_at = now() WHERE id = %s""",
                        (mid,))
                    migration_event(cur, mid, from_key, to_key, "MIGRATION_VOIDED",
                                    window_start=start,
                                    detail={"reason": "source_retracted",
                                            "at": "cut_with_no_events"})
                    action = {"void": True, "emit": []}
                    log.info("migration %s auto-voided at cut (no routed events)", mid)
                else:
                    windows = cut_migration(cur, m, reopen=False)
                    action = {"void": False, "emit": windows}
                    log.info("migration %s CUT %s -> %s at window %d (%d events, %d windows)",
                             mid, from_key, to_key, start,
                             len(sig["events"]), len(windows))

            elif m["status"] in ("CUT", "REOPENED"):
                sig = routed_events_signature(cur, mid)
                frozen = {int(k): v for k, v in (m.get("cut_signature") or {}).items()}
                live = sig["windows"]
                if not sig["events"]:
                    cur.execute(
                        """UPDATE key_migrations
                           SET status = 'VOID', void_reason = 'source_retracted',
                               voided_at = now() WHERE id = %s""",
                        (mid,))
                    migration_event(cur, mid, from_key, to_key, "MIGRATION_VOIDED",
                                    window_start=start,
                                    detail={"reason": "source_retracted"})
                    action = {"void": True, "emit": sorted(frozen.keys())}
                    log.info("migration %s auto-voided: every routed event retracted", mid)
                elif frozen != live:
                    cur.execute(
                        """UPDATE key_migrations SET status = 'REOPENED'
                           WHERE id = %s AND status = 'CUT'""",
                        (mid,))
                    migration_event(cur, mid, from_key, to_key, "MIGRATION_REOPENED",
                                    window_start=start,
                                    detail={"frozen": frozen, "live": live})
                    windows = cut_migration(cur, migration_row_dict(cur, mid), reopen=True)
                    action = {"void": False, "emit": windows}
                    log.info("migration %s REOPENED and re-cut with a fresh snapshot", mid)
                # content identical: stable CUT, nothing to do
        if action is not None:
            actions.append((mid, from_key, to_key, start, action))

    # Phase 2: publish. The migration row is already committed, so the to_key
    # render sees the routing; emit() takes its own gate-lock transaction.
    for mid, from_key, to_key, start, action in actions:
        for ws in action["emit"]:
            emit(conn, ws, to_key, "MIGRATION",
                 {"migration_id": mid,
                  "op": "voided" if action["void"] else "cut"})
        if action["void"]:
            # The tail comes back to the from_key; close its due windows.
            close_key_windows(conn, from_key)
        build_orders(conn)
        with conn, conn.cursor() as cur:
            cur.execute("SELECT id FROM biz_orders WHERE key = %s", (from_key,))
            if cur.fetchone() is not None:
                fold_order(cur, from_key,
                           "MIGRATION_VOIDED" if action["void"] else "MIGRATION_CUT",
                           start, None, None)



def tick(conn):
    dirty = []
    for stream, base_url in INGESTS.items():
        dirty.extend(pull_stream(conn, stream, base_url))
    refresh_watermarks(conn)
    # Carries whose SOURCE window changed are re-validated first: a retracted
    # or source-paired carried event voids the carry (dead forever) before any
    # window is re-rendered, so neither source nor target keeps pairing a dead
    # event; a closed carry whose source merely corrected is reopened.
    voided_targets = invalidate_dirty_carries(conn, dirty)
    # Late data: recompute only windows that already have a result —
    # windows not yet emitted will be covered by close_windows below. A
    # from_key window held by an OPEN migration or already routed away by a
    # CUT migration is never emitted as the from_key's own result, even if a
    # late event/retraction otherwise dirties it: its content belongs to the
    # to_key (derive_migrations handles the re-emission there).
    with conn.cursor() as cur:
        held_migrations = held_tail_pairs(cur)
        routed = routed_tail_pairs(cur)
    seen = set()
    for ws, key, reason, detail in dirty:
        if (ws, key, reason) in seen:
            continue
        seen.add((ws, key, reason))
        if (key, ws) in held_migrations or (key, ws) in routed:
            continue
        with conn.cursor() as cur:
            head = get_head(cur, ws, key)
        if head is not None:
            emit(conn, ws, key, reason, detail)
    # Migration lifecycle BEFORE ordinary window closing: an OPEN migration
    # whose from_key start window crossed on both sides THIS tick must cut and
    # route its tail into the to_key — running close_windows first would emit
    # the from_key's own INITIAL for that same window, breaking the invariant
    # that a held/routed tail never produces an own result. The cut's to_key
    # versions are emitted here; orders are folded after closing below.
    derive_migrations(conn)
    close_windows(conn)
    build_orders(conn)
    # Recompute already-emitted targets of carries voided this tick (their
    # carry events just vanished from the pairing), then derive every live
    # carry's item state from the now-current target heads — partial matches,
    # close (with the frozen match snapshot + CARRY_RESOLVED order fold) and
    # target-correction reopen all happen here.
    emit_voided_targets(conn, voided_targets)
    derive_carries(conn)
    # Carries closing this tick fold orders with CARRY_RESOLVED; a migration
    # cut this tick may settle those same orders.
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
               ON CONFLICT (subscriber_id, window_start, key, version, redelivery_seq) DO NOTHING""",
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
# per-(window, key) close grace (关窗宽限)
# ---------------------------------------------------------------------------
# One operational extension of a single not-yet-emitted (window, key): hold its
# INITIAL result extra_ms past the ordinary per-key window end and wait for the
# opposite side. The scope is exactly that pair — other keys and this key's
# other windows keep their original window ends, and nothing is blocked on
# them. Rules enforced here, all under the key's gate-row lock (taken before
# every grace-row lock, the same order emit()/the deadline sweep use):
#
# - the window must hold effective data and must not have emitted yet
#   ("已经出过结果的窗不能再宽，再点要失败");
# - at most one ACTIVE grace per (window, key)
#   ("同一窗不能叠两条还没到期的宽限" — also a partial unique index);
# - granting never publishes anything; the deadline sweeper in close_windows
#   emits the then-current version (one-sided included) or lapses the grace
#   when the data is gone.
# Grace rows are append-only: FIRED/EXPIRED history stays queryable.

def grant_window_grace(conn, window_start, key, extra_ms, created_by=None, note=None):
    """Grant one ACTIVE close grace to a single (window, key).

    Returns the new grace row (dict) or raises HTTPException: 404 when the
    window has no effective data yet (nothing to wait for), 409 when the
    window already emitted a result or already has an ACTIVE grace.
    """
    window_end = window_start + WINDOW_MS
    # Plain cursor: get_head()/effective_data_sides() unpack rows positionally
    # (a RealDictCursor would make the former KeyError on row[0] and turn the
    # latter's tuple unpacking into dict-key iteration — the 500/404 this
    # function used to return). The inserted row is re-dictified for the
    # response below.
    with conn, conn.cursor() as cur:
        # Same lock order as emit(): gate row first, grace rows second.
        cur.execute(
            "INSERT INTO release_gates (key) VALUES (%s) ON CONFLICT (key) DO NOTHING",
            (key,),
        )
        cur.execute("SELECT 1 FROM release_gates WHERE key = %s FOR UPDATE", (key,))
        cur.fetchone()
        # Lock the pair's grace history so two concurrent grants cannot both
        # pass the ACTIVE check (the partial unique index is the hard backstop).
        cur.execute(
            """SELECT id, status FROM window_graces
               WHERE window_start = %s AND key = %s
               ORDER BY id FOR UPDATE""",
            (window_start, key),
        )
        grace_rows = cur.fetchall()
        if any(r[1] == "ACTIVE" for r in grace_rows):
            active = next(r for r in grace_rows if r[1] == "ACTIVE")
            raise HTTPException(
                409,
                f"window {window_start} of key {key!r} already has ACTIVE grace "
                f"{active[0]} that has not come due (同一窗不能叠两条还没到期的宽限)")
        head = get_head(cur, window_start, key)
        has_data = (key, window_start) in effective_data_sides(cur, key)
        # The ACTIVE check was already done above under the row locks (plus a
        # partial unique index backstops concurrent grants); here only the
        # data/result preconditions remain.
        err = grace_grant_error(has_data, head is not None, False)
        if err == "already_emitted":
            raise HTTPException(
                409,
                f"window {window_start} of key {key!r} already produced result v"
                f"{head['version']} ({head['status']}) — emitted windows cannot be "
                "graced again (已经出过结果的窗不能再宽)")
        if err == "window_not_active":
            raise HTTPException(
                404,
                f"window {window_start} of key {key!r} has no effective data yet — "
                "nothing to hold; send its events first")
        cur.execute(
            """INSERT INTO window_graces
                   (window_start, window_end, key, extra_ms, due_at, created_by, note)
               VALUES (%s, %s, %s, %s,
                       now() + %s * INTERVAL '1 millisecond', %s, %s)
               RETURNING *""",
            (window_start, window_end, key, extra_ms, extra_ms, created_by, note),
        )
        row = dict(zip([d[0] for d in cur.description], cur.fetchone()))
    log.info("close grace %s granted to window=%d key=%s extra=%dms due=%s",
             row["id"], window_start, key, extra_ms, row["due_at"])
    return row


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
  -- A settlement pins one pair for this subscriber: versions above an
  -- ACTIVE FREEZE/SUPPRESS pin are never copied by a later replay job
  -- ("这一版别再补") — SUPPRESS pins at 0, so the whole result is skipped.
  AND NOT EXISTS (
      SELECT 1 FROM reconciliation_settlements rs
      WHERE rs.subscriber_id = %s
        AND rs.window_start = r.window_start
        AND rs.key = r.key
        AND rs.status = 'ACTIVE'
        AND rs.effect IN ('FREEZE', 'SUPPRESS')
        AND r.version > rs.pinned_version
  )
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
            (from_ws, to_ws, subscriber_id),
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
                   ON CONFLICT (subscriber_id, window_start, key, version, redelivery_seq) DO NOTHING""",
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
            """SELECT bool_or(status IN ('DELIVERED', 'RETRYING')) AS dispatched,
                      bool_or(status = 'PENDING') AS any_pending,
                      count(*) AS rows
               FROM deliveries
               WHERE subscriber_id = %s AND window_start = %s
                 AND key = %s AND version = %s""",
            (subscriber_id, window_start, key, version),
        )
        sent = cur.fetchone()
        dispatched = bool(sent["rows"]) and sent["dispatched"]
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
                      if sent["rows"] == 0 else
                      {"reason": "version_not_yet_delivered",
                       "delivery_status": "PENDING"})
            record_posting_event(
                cur, subscriber_id, window_start, key, "REPORT_REJECTED",
                version, prev, prev or (None, None, None), detail)
            return None
        # Settlement gates (认/驳是在结账那一刻才落到这里的，开着账的裁决
        # 什么都不挡): an ACTIVE FREEZE pins the pair at one version forever;
        # an ACTIVE SUPPRESS accepts no report at all; an unfulfilled REDRIVE
        # only accepts its pin (or above). FULFILLED settlements never block.
        cur.execute(
            """SELECT effect, status, pinned_version FROM reconciliation_settlements
               WHERE subscriber_id = %s AND window_start = %s AND key = %s""",
            (subscriber_id, window_start, key),
        )
        st = cur.fetchone()
        if st is not None:
            reason = settlement_blocks_report(
                st["effect"], st["status"], st["pinned_version"], version)
            if reason is not None:
                record_posting_event(
                    cur, subscriber_id, window_start, key, "REPORT_REJECTED",
                    version, prev, prev or (None, None, None),
                    {"reason": reason, "settlement_effect": st["effect"],
                     "pinned_version": st["pinned_version"]})
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
        # Discharge an ACTIVE settlement that this report settles: a REDRIVE
        # (驳它没入过) once the re-driven pinned version is reported; a
        # CONTINUE (驳它没跟上) once reports move past the pinned lagging
        # version — the disputed gap is then gone. FREEZE/SUPPRESS never
        # discharge; MARK_DELIVERED/NONE are born FULFILLED.
        if st is not None and settlement_fulfils(
                st["effect"], st["status"], st["pinned_version"], version):
            cur.execute(
                """UPDATE reconciliation_settlements
                   SET status = 'FULFILLED', fulfilled_at = now()
                   WHERE subscriber_id = %s AND window_start = %s AND key = %s
                     AND status = 'ACTIVE'""",
                (subscriber_id, window_start, key),
            )
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
                        'redelivery_seq', d.redelivery_seq,
                        'attempts', d.attempts, 'last_error', d.last_error,
                        'delivered_at', d.delivered_at)
                    ORDER BY d.version, d.redelivery_seq) AS delivery_ladder
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
        batch = get_reconciliation(cur, batch["id"])
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
                       - b.confirmed_items - b.rejected_items AS unresolved_items,
                   (SELECT count(*) FROM reconciliation_settlements st
                     WHERE st.batch_id = b.id) AS settled_items
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
    """Close a batch once every non-aligned item carries a verdict, and
    *settle* each adjudicated item (对账落账) in the very same transaction.

    Fails 409 while any LAGGING / AHEAD_UNCONFIRMED / NOT_REPORTED item is
    still undecided — one undecided row keeps the whole batch open. A verdict
    changes nothing while the batch is OPEN; only at close does it land as one
    immutable reconciliation_settlements row per (subscriber, window, key) and
    start driving delivery and posting reports for that pair ("结完才动"):

    - LAGGING + CONFIRMED     -> FREEZE at the reported version (hold the rest);
    - LAGGING + REJECTED      -> CONTINUE (the lagging gate never holds it again);
    - NOT_REPORTED + CONFIRMED -> SUPPRESS at 0 (never redeliver, never accept
                                 a report; in-flight versions are pulled back);
    - NOT_REPORTED + REJECTED -> REDRIVE the lowest sent version until reported;
    - AHEAD_UNCONFIRMED + CONFIRMED -> in-flight versions up to the reported
                                 version are marked DELIVERED and the ledger is
                                 aligned (MARK_DELIVERED);
    - AHEAD_UNCONFIRMED + REJECTED  -> NONE (ordinary retries continue).

    The same pair can only be settled once ("同一条只能落到一次"): closing a
    batch whose verdict would re-settle an already-settled pair fails 409 and
    names the blocking batch — FULFILLED rows included, they stay as history.
    Closing an already-closed batch is an idempotent no-op.
    """
    with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Serialize against concurrent deliveries/reports (which lock the
        # subscriber row) and against another close on the same batch.
        cur.execute("SELECT b.*, s.name AS subscriber_name FROM reconciliation_batches b "
                    "JOIN subscribers s ON s.id = b.subscriber_id "
                    "WHERE b.id = %s FOR UPDATE OF b",
                    (batch_id,))
        batch = cur.fetchone()
        if batch is None:
            raise HTTPException(404, "no such reconciliation batch")
        if batch["status"] == "CLOSED":
            return get_reconciliation(cur, batch_id)
        cur.execute(
            """SELECT i.*,
                      (SELECT min(d.version) FROM deliveries d
                        WHERE d.subscriber_id = %s
                          AND d.window_start = i.window_start AND d.key = i.key)
                          AS lowest_sent_version
               FROM reconciliation_items i
               WHERE i.batch_id = %s FOR UPDATE""",
            (batch["subscriber_id"], batch_id),
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

        # 同一条只能落到一次: every adjudicated pair must have no prior
        # settlement — by ANY batch, including FULFILLED ones.
        decided = [(r["window_start"], r["key"]) for r in items
                   if r["decision"] is not None]
        conflicts = []
        if decided:
            cur.execute(
                """SELECT st.window_start, st.key, st.batch_id, b.status AS batch_status
                   FROM reconciliation_settlements st
                   JOIN reconciliation_batches b ON b.id = st.batch_id
                   WHERE st.subscriber_id = %s
                     AND (st.window_start, st.key) IN %s
                   ORDER BY st.batch_id""",
                (batch["subscriber_id"], tuple(decided)),
            )
            conflicts = cur.fetchall()
        if conflicts:
            detail = "; ".join(
                f"({r['window_start']}, {r['key']!r}) already settled by batch "
                f"{r['batch_id']} ({r['batch_status']})" for r in conflicts)
            raise HTTPException(
                409,
                f"reconciliation batch {batch_id} cannot settle: the same pair can only be "
                f"settled once (同一条只能落到一次) — {detail}")

        settled = []
        for it in items:
            if it["decision"] is None:
                continue  # ALIGNED rows carry no verdict and are not settled
            effect = settlement_effect(it["item_status"], it["decision"])
            pin = settlement_pin_version(
                effect, it["reported_version"], it["lowest_sent_version"])
            born_fulfilled = effect in ("MARK_DELIVERED", "NONE")
            cur.execute(
                """INSERT INTO reconciliation_settlements
                       (batch_id, subscriber_id, window_start, key, effect, status,
                        pinned_version, snapshot_item_status, snapshot_decision,
                        settled_by, fulfilled_at)
                   VALUES (%s, %s, %s, %s, %s,
                           %s, %s, %s, %s, %s,
                           CASE WHEN %s THEN now() END)""",
                (batch_id, batch["subscriber_id"], it["window_start"], it["key"],
                 effect, "FULFILLED" if born_fulfilled else "ACTIVE", pin,
                 it["item_status"], it["decision"], operator, born_fulfilled),
            )
            apply_settlement_effect(cur, batch, it, effect, pin, operator)
            settled.append({"window_start": it["window_start"], "key": it["key"],
                            "effect": effect, "pinned_version": pin})

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
                                   "rejected_items": batch["rejected_items"],
                                   "settled_items": len(settled),
                                   "settlements": settled})),
        )
        closed = get_reconciliation(cur, batch_id)
    log.info("reconciliation batch %s closed by %s (%d items, %d settled: %s)",
             batch_id, operator, batch["total_items"], len(settled),
             {e: sum(1 for s in settled if s["effect"] == e)
              for e in ("FREEZE", "CONTINUE", "SUPPRESS", "REDRIVE",
                        "MARK_DELIVERED", "NONE")})
    return closed


def apply_settlement_effect(cur, batch, item, effect, pin, operator):
    """Execute one verdict's delivery/ledger effect inside the close txn.

    Runs after its immutable settlement row was inserted. All reads/writes
    target exactly one (subscriber, window, key) pair — no other result or
    subscriber is touched.
    """
    sub_id = batch["subscriber_id"]
    ws, key = item["window_start"], item["key"]

    if effect in ("FREEZE", "SUPPRESS"):
        # Stop sending the versions above the pin: pull back every still-
        # undelivered outbox row (REALTIME or BACKFILL). PENDING rows were
        # never on the wire; RETRYING rows may have reached the downstream,
        # but at-least-once + downstream version dedup makes the pullback safe.
        # Already DELIVERED rows are the sent fact and are never touched.
        cur.execute(
            """UPDATE deliveries
               SET status = 'PENDING', last_attempt_at = NULL,
                   delivered_at = NULL, next_attempt_at = now(), last_error = NULL,
                   attempts = 0
               WHERE subscriber_id = %s AND window_start = %s AND key = %s
                 AND version > %s AND status <> 'DELIVERED'""",
            (sub_id, ws, key, pin),
        )

    if effect == "REDRIVE":
        # Re-drive the lowest sent version until the downstream reports it.
        # If the pinned version was already DELIVERED once (the usual
        # NOT_REPORTED case: we confirm sending it, it never booked it),
        # merely resetting the old row would not re-POST and a reused
        # delivery_id would be dropped by a delivery_id-deduplicating
        # downstream — insert a NEW outbox row for the SAME version
        # (new id, redelivery_seq = previous max + 1). It carries the same
        # (window, key, version) identity so the downstream books/amends that
        # one result, not a new success. If the original row is still
        # PENDING/RETRYING, no copy is needed: just make it due immediately.
        # A re-delivery never enters the version-order barrier
        # (redelivery_seq <> 0), so later versions are not held back by it.
        cur.execute(
            """SELECT status, window_end, kind, payload, COALESCE(
                      (SELECT max(redelivery_seq) FROM deliveries d2
                        WHERE d2.subscriber_id = d.subscriber_id
                          AND d2.window_start = d.window_start
                          AND d2.key = d.key AND d2.version = d.version), 0)
                      AS max_seq
               FROM deliveries d
               WHERE subscriber_id = %s AND window_start = %s AND key = %s
                 AND version = %s AND redelivery_seq = 0""",
            (sub_id, ws, key, pin),
        )
        pin_row = cur.fetchone()
        if pin_row is not None and pin_row["status"] == "DELIVERED":
            cur.execute(
                """INSERT INTO deliveries
                       (subscriber_id, window_start, window_end, key, version,
                        kind, payload, channel, redelivery_seq, next_attempt_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, 'REALTIME', %s, now())""",
                (sub_id, ws, pin_row["window_end"], key, pin, pin_row["kind"],
                 psycopg2.extras.Json(pin_row["payload"])
                 if pin_row["payload"] is not None else None,
                 pin_row["max_seq"] + 1),
            )
        elif pin_row is not None:
            cur.execute(
                """UPDATE deliveries
                   SET next_attempt_at = now(), last_error = NULL
                   WHERE subscriber_id = %s AND window_start = %s AND key = %s
                     AND version = %s AND redelivery_seq = 0""",
                (sub_id, ws, key, pin),
            )

    if effect == "MARK_DELIVERED":
        # It reported the pin version while our delivery of it was still
        # unconfirmed (lost response / slow retry): accept that as delivered.
        # Versions 1..pin are marked DELIVERED and the posting ledger is
        # aligned to pin — delivery rows are the sent fact and stay in order.
        cur.execute(
            """UPDATE deliveries
               SET status = 'DELIVERED', last_attempt_at = now(),
                   delivered_at = now(), last_error = NULL
               WHERE subscriber_id = %s AND window_start = %s AND key = %s
                 AND version <= %s AND status <> 'DELIVERED'""",
            (sub_id, ws, key, pin),
        )
        cur.execute(
            """SELECT reported_version, delivered_up_to, status FROM posting_ledger
               WHERE subscriber_id = %s AND window_start = %s AND key = %s
               FOR UPDATE""",
            (sub_id, ws, key),
        )
        row = cur.fetchone()
        prev = ((row["reported_version"], row["delivered_up_to"], row["status"])
                if row else None)
        new_status = posting_status(pin, pin)  # ALIGNED
        if row is None:
            cur.execute(
                """INSERT INTO posting_ledger
                       (subscriber_id, window_start, key,
                        reported_version, delivered_up_to, status)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (sub_id, ws, key, pin, pin, new_status),
            )
        else:
            cur.execute(
                """UPDATE posting_ledger
                   SET reported_version = %s, delivered_up_to = %s,
                       status = %s, updated_at = now()
                   WHERE subscriber_id = %s AND window_start = %s AND key = %s""",
                (pin, pin, new_status, sub_id, ws, key),
            )
        cur.execute(
            """INSERT INTO posting_events
                   (subscriber_id, window_start, key, event, cause_version,
                    prev_reported, prev_delivered, prev_status,
                    reported_version, delivered_up_to, status, detail)
               VALUES (%s, %s, %s, 'SETTLEMENT_ADVANCED', %s, %s, %s, %s, %s, %s, %s, %s)""",
            (sub_id, ws, key, pin,
             prev[0] if prev else None, prev[1] if prev else None,
             prev[2] if prev else None, pin, pin, new_status,
             psycopg2.extras.Json(
                 {"reason": "reconciliation_mark_delivered", "batch_id": batch["id"],
                  "operator": operator})),
        )

    # FREEZE / CONTINUE / SUPPRESS / REDRIVE / NONE: no other delivery-row
    # rewrite — the effects are enforced by the dispatch due gates, the
    # backfill copy predicate and the posting-report checks below.


# Live (post-opening) state shown next to the frozen snapshot in item queries.
# Correlated subqueries read the outbox/ledger as they stand NOW; they never
# write back to reconciliation_items, so the photograph cannot be retaken.
RECON_ITEMS_SQL = """
SELECT i.*, b.subscriber_id, s.name AS subscriber,
       rs.effect AS settlement_effect,
       rs.status AS settlement_status,
       rs.pinned_version AS pinned_version,
       rs.batch_id AS settlement_batch_id,
       rs.settled_at AS settled_at,
       rs.fulfilled_at AS settlement_fulfilled_at,
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
LEFT JOIN reconciliation_settlements rs
  ON rs.subscriber_id = b.subscriber_id
 AND rs.window_start = i.window_start AND rs.key = i.key
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
       d.attempts, d.created_at, d.channel, d.backfill_job_id,
       d.redelivery_seq
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
        AND p.redelivery_seq = 0
  )
  AND NOT EXISTS (
      -- posting-ledger flow control: this downstream's posted position lags
      -- what we already delivered for THIS result — hold its later versions
      -- until it reports catching up. Scoped to one (window, key): other
      -- results of the same downstream are never held back by this one.
      -- A CLOSED batch's REJECT verdict on the lag (CONTINUE settlement)
      -- permanently lifts this gate for that one pair: we keep sending what
      -- we sent, it was not behind as far as delivery is concerned.
      SELECT 1 FROM posting_ledger pl
      WHERE pl.subscriber_id = d.subscriber_id
        AND pl.window_start = d.window_start
        AND pl.key = d.key
        AND pl.status = 'LAGGING'
        AND NOT EXISTS (
            SELECT 1 FROM reconciliation_settlements rs
            WHERE rs.subscriber_id = pl.subscriber_id
              AND rs.window_start = pl.window_start
              AND rs.key = pl.key
              AND rs.effect = 'CONTINUE'
              AND rs.status = 'ACTIVE'
        )
  )
  AND NOT EXISTS (
      -- reconciliation settlement flow control: a CLOSED batch CONFIRMED the
      -- pair at a pinned version (FREEZE = a lagging pair's then-reported
      -- version, SUPPRESS = 0 for a version it never booked). Versions above
      -- the pin are never delivered to THIS downstream afterward; the rows
      -- were pulled back to PENDING at close and stay parked here forever.
      -- REDRIVE needs no clause: its pinned row was reset to PENDING, so the
      -- ordinary version-order barrier above parks later versions behind it.
      SELECT 1 FROM reconciliation_settlements rs
      WHERE rs.subscriber_id = d.subscriber_id
        AND rs.window_start = d.window_start
        AND rs.key = d.key
        AND rs.status = 'ACTIVE'
        AND rs.effect IN ('FREEZE', 'SUPPRESS')
        AND d.version > rs.pinned_version
  )
ORDER BY d.id
LIMIT %s
"""

BACKFILL_DUE_SQL = """
SELECT d.id, d.subscriber_id, s.name AS subscriber, s.url,
       d.window_start, d.window_end, d.key, d.version, d.kind, d.payload,
       d.attempts, d.created_at, d.channel, d.backfill_job_id,
       d.redelivery_seq
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
        AND p.redelivery_seq = 0
  )
  AND NOT EXISTS (
      -- the posting-ledger gate applies to replays exactly as to live
      -- traffic: a downstream behind on posting this result gets no further
      -- versions of it, whichever channel they ride — unless a CLOSED batch
      -- REJECTED that lag (ACTIVE CONTINUE settlement): then we keep sending.
      SELECT 1 FROM posting_ledger pl
      WHERE pl.subscriber_id = d.subscriber_id
        AND pl.window_start = d.window_start
        AND pl.key = d.key
        AND pl.status = 'LAGGING'
        AND NOT EXISTS (
            SELECT 1 FROM reconciliation_settlements rs
            WHERE rs.subscriber_id = pl.subscriber_id
              AND rs.window_start = pl.window_start
              AND rs.key = pl.key
              AND rs.effect = 'CONTINUE'
              AND rs.status = 'ACTIVE'
        )
  )
  AND NOT EXISTS (
      -- settled pins hold on the backfill channel too: versions above an
      -- ACTIVE FREEZE/SUPPRESS pin are never sent, even when a replay job
      -- copied them (at close those backfill rows were parked in PENDING).
      SELECT 1 FROM reconciliation_settlements rs
      WHERE rs.subscriber_id = d.subscriber_id
        AND rs.window_start = d.window_start
        AND rs.key = d.key
        AND rs.status = 'ACTIVE'
        AND rs.effect IN ('FREEZE', 'SUPPRESS')
        AND d.version > rs.pinned_version
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
        "redelivery_seq": row.get("redelivery_seq", 0),
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
                gates = {r["key"]: r["open"] for r in cur.fetchall()}
                cur.execute(
                    """SELECT window_start, key, id, due_at, extra_ms, created_at
                       FROM window_graces WHERE status = 'ACTIVE'""")
                graces = {(r["window_start"], r["key"]):
                              {"id": r["id"], "due_at": r["due_at"],
                               "extra_ms": r["extra_ms"], "created_at": r["created_at"]}
                          for r in cur.fetchall()}
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
        grace = graces.get((ws, r["key"]))
        crossed = window_ready(marks["a"], marks["b"], ws + WINDOW_MS)
        out.append({
            "window_start": ws,
            "window_end": ws + WINDOW_MS,
            "key": r["key"],
            "upserts": r["upserts"],
            "retracts": r["retracts"],
            "closed": crossed,
            # An ACTIVE close grace deliberately keeps an otherwise crossed
            # window unemitted until due_at; that is the only state in which
            # `closed` is true but no head version exists.
            "closed_effective": crossed and grace is None,
            "head_version": head["version"] if head else None,
            "head_status": head["status"] if head else None,
            "released": bool(head and head["released"]),
            "gate_open": gates.get(r["key"], False),
            "grace_active": grace is not None,
            "grace_due_at": grace["due_at"] if grace else None,
            "grace_extra_ms": grace["extra_ms"] if grace else None,
            "grace_id": grace["id"] if grace else None,
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
                    count(w.order_id) FILTER (WHERE w.has_gap) AS gap_windows,
                    (SELECT count(*) FROM gap_carries c
                      WHERE c.key = o.key
                        AND c.status IN ('OPEN', 'REOPENED')) AS open_carries
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
                              has_gap, match_count, unmatched_a, unmatched_b,
                              carried_a, carried_b, payload_hash
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
# per-(window, key) close grace (关窗宽限)
# ---------------------------------------------------------------------------

class GraceIn(BaseModel):
    window_start: int = Field(
        ge=0,
        description="start of the window to hold; must be a tumbling-window boundary")
    key: str = Field(min_length=1, max_length=200)
    extra_ms: int = Field(
        gt=0,
        description="extra wall-clock time to wait for the opposite side past "
                    "the deadline tick; the window fires at now()+extra_ms")
    operator: Optional[str] = Field(default=None, max_length=200)
    note: Optional[str] = Field(default=None, max_length=2000)


@app.post("/window-graces", status_code=201)
def window_grace_grant(body: GraceIn):
    """Grant one close grace to a single not-yet-emitted (window, key).

    The window keeps computing internally but its INITIAL result is held for
    ``extra_ms`` of wall-clock time even after both of that key's sides cross
    the window end — other keys and this key's other windows keep their
    original window ends and are never held up by it. At the deadline the
    then-current version is emitted: still one-sided, it goes out one-sided
    (the wait never continues past the deadline); if every event was
    retracted away meanwhile, the grace lapses (EXPIRED) without a result and
    ordinary waiting resumes. A window that already produced a result, or one
    that already has an ACTIVE grace, cannot be granted another one (409); a
    window with no effective data yet is 404; ``window_start`` must be a
    window boundary (422).
    """
    if not valid_window_start(body.window_start, WINDOW_MS):
        raise HTTPException(
            422, f"window_start {body.window_start} is not a window boundary "
                 f"(window size {WINDOW_MS}ms)")
    if body.extra_ms > MAX_GRACE_EXTRA_MS:
        raise HTTPException(
            422, f"extra_ms must be <= {MAX_GRACE_EXTRA_MS} (30 days)")
    conn = connect()
    try:
        try:
            row = grant_window_grace(conn, body.window_start, body.key,
                                     body.extra_ms, body.operator, body.note)
        except psycopg2.errors.UniqueViolation:
            raise HTTPException(
                409, "an ACTIVE close grace for this (window, key) already exists")
    finally:
        conn.close()
    return {"grace": row}


@app.get("/window-graces")
def window_graces(window_start: Optional[int] = None, key: Optional[str] = None,
                  status: Optional[str] = None, active_only: bool = False,
                  limit: int = Query(default=500)):
    """Close-grace ledger — which windows are currently held, when their
    deadline is, and for which business key.

    Rows are append-only: ACTIVE (still holding; ``due_at`` is the wall-clock
    deadline), FIRED (the deadline elapsed with data left — ``fired_version``
    is the version that went out then), EXPIRED (the deadline elapsed after
    all data was retracted away — nothing emitted, the window is back to
    ordinary waiting and may be graced again once data returns). Filters:
    key / window_start / status / active_only; newest grants first.
    """
    if status is not None and status not in ("ACTIVE", "FIRED", "EXPIRED"):
        raise HTTPException(422, "status must be ACTIVE, FIRED or EXPIRED")
    conds, args = [], []
    if window_start is not None:
        conds.append("window_start = %s")
        args.append(window_start)
    if key is not None:
        conds.append("key = %s")
        args.append(key)
    if status is not None:
        conds.append("status = %s")
        args.append(status)
    if active_only:
        conds.append("status = 'ACTIVE'")
    sql = "SELECT * FROM window_graces"
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY id DESC LIMIT %s"
    args.append(min(max(limit, 1), 5000))
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, args)
            rows = cur.fetchall()
    finally:
        conn.close()
    return {"graces": rows, "count": len(rows)}


# ---------------------------------------------------------------------------
# gap carry-forwards (缺口结转)
# ---------------------------------------------------------------------------

class CarryIn(BaseModel):
    key: str = Field(min_length=1, max_length=200)
    source_window_start: int = Field(
        ge=0, description="closed window whose leftovers are routed out")
    target_window_start: int = Field(
        ge=0, description="LATER window (same key) that has not emitted yet")
    side: str = Field(description="which side's leftovers ride the carry: a|b")
    event_ids: Optional[list[str]] = Field(
        default=None,
        description="subset of the source side's CURRENT unmatched events; "
                    "default = every leftover on that side")
    operator: Optional[str] = Field(default=None, max_length=200)
    note: Optional[str] = Field(default=None, max_length=2000)


def carry_json(cur, row):
    """One carry header plus its item rows and live counters.

    The caller's cursor is a RealDictCursor, so fetched rows are already
    dicts keyed by column name — they must be used as-is. Zipping column
    names over a dict iterates its KEYS, which would write the literal field
    names as values (event_id -> "event_id", item_status -> "item_status", …)
    and hide the real carried event id the carry was opened with.
    """
    cur.execute(
        """SELECT event_id, event_time, side, item_status, matched_version,
                  matched_against, updated_at
           FROM gap_carry_items WHERE carry_id = %s
           ORDER BY event_time, event_id""",
        (row["id"],),
    )
    fetched = cur.fetchall()
    items = ([dict(r) for r in fetched] if fetched and isinstance(fetched[0], dict)
             else [dict(zip([d[0] for d in cur.description], r)) for r in fetched])
    carried = [i for i in items if i["item_status"] == "CARRIED"]
    out = dict(row)
    out["items"] = items
    out["item_count"] = len(items)
    out["matched_count"] = len(items) - len(carried)
    return out


@app.post("/gap-carries", status_code=201)
def gap_carry_create(body: CarryIn):
    """Open a gap carry: route one side's unmatched leftovers of a CLOSED
    source window to a later, not-yet-emitted window of the SAME key.

    The carried events leave the source's unmatched gap (the source gets a
    CARRY_FORWARD correction recording the route) and are injected into the
    target side when IT emits; pairs they form are tagged with this carry id
    (never written as the target's own native pairs). While unpaired the carry
    is OPEN and queryable (from which window to which, carrying which events);
    once every event pairs it closes and freezes the close-time match
    snapshot. Rules (all 409 unless noted): target must not have emitted yet;
    source must have a live one-sided leftover on that side; different keys
    can never share a carry; one unmatched event can never be in two open
    carries — nor carried again after its carry was voided; target must be a
    later window boundary (422).
    """
    if body.event_ids is not None:
        if not body.event_ids:
            raise HTTPException(422, "event_ids must be non-empty when given")
        if len(body.event_ids) != len(set(body.event_ids)):
            raise HTTPException(422, "event_ids must not repeat")
    conn = connect()
    try:
        try:
            created = create_carry(conn, body.source_window_start,
                                   body.target_window_start, body.key, body.side,
                                   body.event_ids, body.operator, body.note)
            # Re-read through carry_json so the 201 response carries the same
            # item rows (real event ids, status) GET /gap-carries/{id} returns.
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM gap_carries WHERE id = %s",
                            (created["id"],))
                carry = carry_json(cur, cur.fetchone())
        except psycopg2.errors.UniqueViolation:
            raise HTTPException(
                409, "one of these events already rides another carry "
                     "(open, closed or void) — 同一条不能结第二次")
    finally:
        conn.close()
    return {"carry": carry}


@app.get("/gap-carries")
def gap_carries(key: Optional[str] = None, status: Optional[str] = None,
                side: Optional[str] = None, source_window_start: Optional[int] = None,
                target_window_start: Optional[int] = None,
                open_only: bool = False, limit: int = Query(default=200)):
    """Carry ledger — for every carry: OPEN/CLOSED/REOPENED/VOID, which source
    window routes to which target window, which side, which events ride it and
    which of them have paired (with the opposing event and target version).
    CLOSED carries carry the frozen matched_snapshot ("当时对上的样子").
    Filters: key / status / side / source window / target window / open_only.
    """
    if status is not None and status not in ("OPEN", "CLOSED", "REOPENED", "VOID"):
        raise HTTPException(422, "status must be OPEN, CLOSED, REOPENED or VOID")
    if side is not None and side not in ("a", "b"):
        raise HTTPException(422, "side must be 'a' or 'b'")
    conds, args = [], []
    if key is not None:
        conds.append("c.key = %s")
        args.append(key)
    if status is not None:
        conds.append("c.status = %s")
        args.append(status)
    if side is not None:
        conds.append("c.side = %s")
        args.append(side)
    if source_window_start is not None:
        conds.append("c.source_window_start = %s")
        args.append(source_window_start)
    if target_window_start is not None:
        conds.append("c.target_window_start = %s")
        args.append(target_window_start)
    if open_only:
        conds.append("c.status IN ('OPEN', 'REOPENED')")
    where = (" WHERE " + " AND ".join(conds)) if conds else ""
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""SELECT c.* FROM gap_carries c{where}
                    ORDER BY c.id DESC LIMIT %s""",
                args + [min(max(limit, 1), 5000)],
            )
            rows = [carry_json(cur, r) for r in cur.fetchall()]
    finally:
        conn.close()
    return {"carries": rows, "count": len(rows)}


@app.get("/gap-carries/{carry_id}")
def gap_carry_get(carry_id: int):
    """One carry: header, frozen close-time snapshot (when CLOSED) and each
    carried event's current item state."""
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM gap_carries WHERE id = %s", (carry_id,))
            row = cur.fetchone()
            if row is None:
                raise HTTPException(404, "no such gap carry")
            payload = carry_json(cur, row)
    finally:
        conn.close()
    return {"carry": payload}


@app.get("/gap-carries/{carry_id}/events")
def gap_carry_events(carry_id: int, limit: int = Query(default=1000)):
    """Append-only carry trail, chronological: CARRY_OPENED, every
    ITEM_MATCHED, CARRY_CLOSED (with the frozen snapshot), CARRY_REOPENED
    (source/target later corrected or withdrew) and CARRY_VOIDED (a carried
    event was retracted or paired at the source — dead forever)."""
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT key FROM gap_carries WHERE id = %s", (carry_id,))
            if cur.fetchone() is None:
                raise HTTPException(404, "no such gap carry")
            cur.execute(
                """SELECT * FROM gap_carry_events
                   WHERE carry_id = %s ORDER BY id LIMIT %s""",
                (carry_id, min(max(limit, 1), 5000)),
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    return {"carry_id": carry_id, "events": rows}


# ---------------------------------------------------------------------------
# business-key migrations (业务键迁出)
# ---------------------------------------------------------------------------

class MigrationIn(BaseModel):
    from_key: str = Field(min_length=1, max_length=200,
                          description="business key whose tail migrates away (迁出键)")
    to_key: str = Field(min_length=1, max_length=200,
                        description="business key the tail routes into (迁入键)")
    start_window_start: int = Field(
        ge=0, description="first window routed over, a window boundary (起始窗)")
    operator: Optional[str] = Field(default=None, max_length=200)
    note: Optional[str] = Field(default=None, max_length=2000)


class MigrationMoveIn(BaseModel):
    start_window_start: int = Field(ge=0, description="new start window boundary")
    operator: Optional[str] = Field(default=None, max_length=200)


def migration_json(cur, row):
    """One migration header plus live routed counters derived from the current
    effective events, so OPEN rows show what is held and CUT rows show what
    actually routes after later retractions."""
    if isinstance(row, dict):
        out = dict(row)
        mid = row["id"]
    else:
        out = dict(zip([d[0] for d in cur.description], row))
        mid = out["id"]
    cur.execute(
        """SELECT count(DISTINCT e.event_id) AS n_events,
                  count(DISTINCT ((e.event_time / %s) * %s)) AS n_windows
           FROM key_migrations m
           JOIN stream_events e
             ON e.key = m.from_key AND e.type = 'upsert' AND NOT e.rejected
                AND e.event_time >= m.start_window_start
                AND NOT EXISTS (
                    SELECT 1 FROM stream_events r
                    WHERE r.type = 'retract' AND r.stream = e.stream
                      AND r.retracts = e.event_id)
           WHERE m.id = %s""",
        (WINDOW_MS, WINDOW_MS, mid),
    )
    got = cur.fetchone()
    if isinstance(got, dict):
        n_events, n_windows = got["n_events"], got["n_windows"]
    else:
        n_events, n_windows = got
    out["routed_event_count"] = n_events
    out["routed_window_count"] = n_windows
    return out


@app.post("/key-migrations", status_code=201)
def key_migration_create(body: MigrationIn):
    """Declare a business-key migration (OPEN): from this start window on the
    from_key's tail no longer produces its own results and, once both sides of
    the from_key cross the start window's end, routes into the to_key. The
    start window must be a boundary (422); from_key and to_key must differ and
    neither may already have produced a result at/after S, nor be a side of an
    unfinished migration (409). The new migration is OPEN and queryable at once
    with exactly the submitted start window."""
    conn = connect()
    try:
        try:
            migration = create_migration(
                conn, body.from_key, body.to_key, body.start_window_start,
                body.operator, body.note)
        except psycopg2.errors.UniqueViolation:
            raise HTTPException(
                409, "one of these keys already has an unfinished migration "
                     "(同一迁出/迁入键不能同时开着两笔迁出)")
    finally:
        conn.close()
    return {"migration": migration}


@app.get("/key-migrations")
def key_migrations(from_key: Optional[str] = None, to_key: Optional[str] = None,
                   key: Optional[str] = None, status: Optional[str] = None,
                   open_only: bool = False, limit: int = Query(default=200)):
    """Migration ledger (newest first): OPEN/CUT/REOPENED/VOID, the two keys,
    the start window, cut snapshots, void reason and live routed counters.
    Filters: from_key / to_key / key (either side) / status / open_only."""
    if status is not None and status not in MIGRATION_STATUSES:
        raise HTTPException(422, f"status must be one of {MIGRATION_STATUSES}")
    conds, args = [], []
    if from_key is not None:
        conds.append("m.from_key = %s")
        args.append(from_key)
    if to_key is not None:
        conds.append("m.to_key = %s")
        args.append(to_key)
    if key is not None:
        conds.append("(m.from_key = %s OR m.to_key = %s)")
        args.extend([key, key])
    if status is not None:
        conds.append("m.status = %s")
        args.append(status)
    if open_only:
        conds.append("m.status IN ('OPEN', 'REOPENED')")
    where = (" WHERE " + " AND ".join(conds)) if conds else ""
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT * FROM key_migrations m{where} ORDER BY m.id DESC LIMIT %s",
                args + [min(max(limit, 1), 5000)],
            )
            rows = [migration_json(cur, r) for r in cur.fetchall()]
    finally:
        conn.close()
    return {"migrations": rows}


def _get_migration_or_404(cur, migration_id):
    cur.execute("SELECT * FROM key_migrations WHERE id = %s", (migration_id,))
    row = cur.fetchone()
    if row is None:
        raise HTTPException(404, "no such migration")
    return row


@app.get("/key-migrations/{migration_id}")
def key_migration_get(migration_id: int):
    """One migration's full current state: header, frozen cut signature and,
    via the events trail, why it opened / cut / re-cut / reopened / voided."""
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            row = _get_migration_or_404(cur, migration_id)
            payload = migration_json(cur, row)
    finally:
        conn.close()
    return {"migration": payload}


@app.get("/key-migrations/{migration_id}/events")
def key_migration_events(migration_id: int, limit: int = Query(default=1000)):
    """Append-only migration trail, chronological: MIGRATION_OPENED,
    START_WINDOW_CHANGED, MIGRATION_CUT / MIGRATION_RECUT, MIGRATION_REOPENED
    and MIGRATION_VOIDED (with reason)."""
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            _get_migration_or_404(cur, migration_id)
            cur.execute(
                """SELECT * FROM key_migration_events
                   WHERE migration_id = %s ORDER BY id LIMIT %s""",
                (migration_id, min(max(limit, 1), 5000)),
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    return {"migration_id": migration_id, "events": rows}


@app.post("/key-migrations/{migration_id}/start-window", status_code=200)
def key_migration_move(migration_id: int, body: MigrationMoveIn):
    """Move the start window of an OPEN migration (still uncut). Same
    preconditions on the new S as a fresh declaration; only an OPEN row can be
    re-aimed (409 after the cut). The change is append-only trail."""
    conn = connect()
    try:
        migration = move_migration_start(
            conn, migration_id, body.start_window_start, body.operator)
    finally:
        conn.close()
    return {"migration": migration}


@app.post("/key-migrations/{migration_id}/void")
def key_migration_void(migration_id: int,
                       body: Optional[dict] = None):
    """Void an OPEN migration ("作废只能在还没切过去时做"): the from_key emits
    its own windows again and this row can never cut; a terminal row is
    idempotent. Manual void of a CUT/REOPENED migration is 409 — those only end
    by auto-void when every routed event is retracted at the source."""
    operator = (body or {}).get("operator")
    conn = connect()
    try:
        migration = void_migration(conn, migration_id,
                                   reason="operator_void", operator=operator)
    finally:
        conn.close()
    return {"migration": migration}


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
                   d.channel, d.backfill_job_id, d.redelivery_seq"""


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
            "redelivery_seq": r["redelivery_seq"],
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
           ((pl.status = 'LAGGING'
              AND NOT EXISTS (
                  SELECT 1 FROM reconciliation_settlements rs
                  WHERE rs.subscriber_id = s.id
                    AND rs.window_start = sent.window_start
                    AND rs.key = sent.key
                    AND rs.effect = 'CONTINUE'
                    AND rs.status = 'ACTIVE'))
             OR EXISTS (
                  SELECT 1 FROM reconciliation_settlements rs
                  WHERE rs.subscriber_id = s.id
                    AND rs.window_start = sent.window_start
                    AND rs.key = sent.key
                    AND rs.status = 'ACTIVE'
                    AND rs.effect IN ('FREEZE', 'SUPPRESS')
                    AND sent.sent_up_to > rs.pinned_version)) AS gated,
           rs.effect AS settlement_effect,
           rs.status AS settlement_status,
           rs.pinned_version AS pinned_version,
           pl.first_reported_at, pl.updated_at AS ledger_updated_at
    FROM sent
    JOIN subscribers s ON s.id = sent.subscriber_id
    LEFT JOIN posting_ledger pl
      ON pl.subscriber_id = sent.subscriber_id
     AND pl.window_start = sent.window_start
     AND pl.key = sent.key
    LEFT JOIN reconciliation_settlements rs
      ON rs.subscriber_id = sent.subscriber_id
     AND rs.window_start = sent.window_start
     AND rs.key = sent.key"""
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
               - b.confirmed_items - b.rejected_items AS unresolved_items,
           (SELECT count(*) FROM reconciliation_settlements st
             WHERE st.batch_id = b.id) AS settled_items
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


# ---------------------------------------------------------------------------
# reconciliation settlements (对账落账): which pair has landed, pinned where
# ---------------------------------------------------------------------------

@app.get("/settlements")
def settlements(batch_id: Optional[int] = None,
                subscriber_id: Optional[int] = None,
                subscriber: Optional[str] = None,
                key: Optional[str] = None,
                window_start: Optional[int] = None,
                effect: Optional[str] = None,
                status: Optional[str] = None,
                active_only: bool = False,
                limit: int = Query(default=500)):
    """The settled verdict of every adjudicated pair — answer to
    "这一批每条落到没有、钉在哪一版".

    One immutable row per (subscriber, window, key): the CLOSED batch that
    settled it, the effect applied at close (FREEZE / CONTINUE / SUPPRESS /
    REDRIVE / MARK_DELIVERED / NONE), the version it is pinned at and whether
    the settlement is still ACTIVE or already FULFILLED (a REDRIVE whose pin
    was reported, or a CONTINUE whose downstream caught up). Filters:
    batch_id / subscriber / key / window_start / effect / status /
    active_only.
    """
    if effect is not None and effect not in (
            "FREEZE", "CONTINUE", "SUPPRESS", "REDRIVE",
            "MARK_DELIVERED", "NONE"):
        raise HTTPException(
            422, "effect must be FREEZE, CONTINUE, SUPPRESS, REDRIVE, "
                 "MARK_DELIVERED or NONE")
    if status is not None and status not in ("ACTIVE", "FULFILLED"):
        raise HTTPException(422, "status must be ACTIVE or FULFILLED")
    sql = """
    SELECT st.*, s.name AS subscriber, s.url,
           rb.window_start_from AS batch_window_start_from,
           rb.window_start_to AS batch_window_start_to,
           rb.created_by AS batch_created_by
    FROM reconciliation_settlements st
    JOIN subscribers s ON s.id = st.subscriber_id
    JOIN reconciliation_batches rb ON rb.id = st.batch_id"""
    conds, args = [], []
    if batch_id is not None:
        conds.append("st.batch_id = %s")
        args.append(batch_id)
    if subscriber_id is not None:
        conds.append("st.subscriber_id = %s")
        args.append(subscriber_id)
    if subscriber is not None:
        conds.append("s.name = %s")
        args.append(subscriber)
    if key is not None:
        conds.append("st.key = %s")
        args.append(key)
    if window_start is not None:
        conds.append("st.window_start = %s")
        args.append(window_start)
    if effect is not None:
        conds.append("st.effect = %s")
        args.append(effect)
    if status is not None:
        conds.append("st.status = %s")
        args.append(status)
    if active_only:
        conds.append("st.status = 'ACTIVE'")
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY st.id DESC LIMIT %s"
    args.append(min(max(limit, 1), 5000))
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, args)
            rows = cur.fetchall()
    finally:
        conn.close()
    return {"settlements": rows, "count": len(rows)}
