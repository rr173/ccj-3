"""Ingestion service for one event stream.

The same image is deployed twice (stream A / stream B), differentiated only by
environment configuration. Responsibilities:

- durably store events with a monotonically increasing ``seq`` (the consumption
  cursor for the aligner);
- idempotent ingest: ``event_id`` is unique, duplicates are acknowledged as
  deduped, never double-stored, never an error;
- maintain the stream watermark and expose it to the aligner;
- expose an operator watermark override (used e.g. to dial the watermark back
  during backfills) so downstream behaviour under regression is explicit.
"""
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import List, Optional

import psycopg2
import psycopg2.extras
from fastapi import Body, FastAPI, HTTPException
from pydantic import BaseModel, Field, model_validator

STREAM = os.environ.get("STREAM_NAME", "stream")
DSN = os.environ["DATABASE_DSN"]
GRACE_MS = int(os.environ.get("WATERMARK_GRACE_MS", "60000"))
IDLE_MS = int(os.environ.get("IDLE_TIMEOUT_MS", "300000"))
BOOT_MS = int(time.time() * 1000)  # for idleness when the stream never had data

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger(f"ingest-{STREAM}")

DDL = """
CREATE TABLE IF NOT EXISTS events (
    seq         BIGSERIAL PRIMARY KEY,
    event_id    TEXT NOT NULL UNIQUE,
    event_time  BIGINT NOT NULL,
    key         TEXT NOT NULL,
    type        TEXT NOT NULL DEFAULT 'upsert' CHECK (type IN ('upsert', 'retract')),
    retracts    TEXT,
    payload     JSONB,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS meta (
    id                 SMALLINT PRIMARY KEY,
    max_event_time     BIGINT,
    last_ingest_ms     BIGINT,
    watermark_override BIGINT
);
INSERT INTO meta (id) VALUES (1) ON CONFLICT DO NOTHING;
"""


def now_ms():
    return int(time.time() * 1000)


def connect(retries=60, delay=1.0):
    for attempt in range(retries):
        try:
            return psycopg2.connect(DSN)
        except psycopg2.OperationalError:
            if attempt == retries - 1:
                raise
            time.sleep(delay)


@asynccontextmanager
async def lifespan(app):
    conn = connect()
    with conn, conn.cursor() as cur:
        cur.execute(DDL)
    conn.close()
    log.info("ingest-%s ready (grace=%dms idle_timeout=%dms)", STREAM, GRACE_MS, IDLE_MS)
    yield


app = FastAPI(title=f"ingest-{STREAM}", lifespan=lifespan)


class EventIn(BaseModel):
    event_id: str = Field(min_length=1)
    event_time: int = Field(ge=0, description="event time, epoch millis")
    key: str = Field(min_length=1, description="alignment/join key")
    type: str = Field(default="upsert", pattern="^(upsert|retract)$")
    retracts: Optional[str] = Field(default=None, description="event_id being retracted")
    payload: Optional[dict] = None

    @model_validator(mode="after")
    def check_retract(self):
        if self.type == "retract" and not self.retracts:
            raise ValueError("retract event must set 'retracts'")
        if self.type == "upsert" and self.retracts:
            raise ValueError("upsert event must not set 'retracts'")
        return self


@app.post("/events")
def post_events(body=Body(...)):
    """Accept a single event object, a list, or {"events": [...]}.

    The batch is validated up front and applied in one transaction, so a bad
    event rejects the whole batch instead of silently dropping data.
    """
    raw = body.get("events") if isinstance(body, dict) and "events" in body else body
    if isinstance(raw, dict):
        raw = [raw]
    if not isinstance(raw, list) or not raw:
        raise HTTPException(400, "body must be an event, a list of events, or {'events': [...]}")
    try:
        events = [EventIn.model_validate(e) for e in raw]
    except ValueError as exc:
        raise HTTPException(422, str(exc))

    accepted, deduped = [], []
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            for e in events:
                cur.execute(
                    """INSERT INTO events (event_id, event_time, key, type, retracts, payload)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       ON CONFLICT (event_id) DO NOTHING
                       RETURNING seq""",
                    (e.event_id, e.event_time, e.key, e.type, e.retracts,
                     psycopg2.extras.Json(e.payload) if e.payload is not None else None),
                )
                (accepted if cur.fetchone() else deduped).append(e.event_id)
            if accepted:
                accepted_max = max(e.event_time for e in events if e.event_id in accepted)
                cur.execute(
                    """UPDATE meta
                       SET max_event_time = GREATEST(COALESCE(max_event_time, 0), %s),
                           last_ingest_ms = %s
                       WHERE id = 1""",
                    (accepted_max, now_ms()),
                )
    finally:
        conn.close()
    if deduped:
        log.info("deduped %d event(s): %s", len(deduped), deduped)
    return {"stream": STREAM, "accepted": len(accepted), "deduped": deduped}


@app.get("/events")
def get_events(after_seq: int = 0, limit: int = 500):
    """Durable replay endpoint used by the aligner. ``seq`` is gap-tolerant."""
    limit = min(max(limit, 1), 5000)
    conn = connect()
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT seq, event_id, event_time, key, type, retracts, payload, ingested_at
                   FROM events WHERE seq > %s ORDER BY seq LIMIT %s""",
                (after_seq, limit),
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    return {"stream": STREAM, "events": rows, "count": len(rows)}


@app.get("/watermark")
def get_watermark():
    """Current watermark for this stream.

    watermark = max_event_time - grace, advanced by wall clock when the stream
    has been idle past IDLE_TIMEOUT_MS (so one quiet side cannot stall
    alignment forever). An operator override, when set, wins outright — this
    is also how a deliberate watermark regression is applied.
    """
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT max_event_time, last_ingest_ms, watermark_override FROM meta WHERE id = 1")
            max_et, last_ingest, override = cur.fetchone()
    finally:
        conn.close()

    now = now_ms()
    # last_ingest is NULL until the first accepted event; fall back to process
    # boot so a stream that *never* produces still goes idle instead of
    # stalling the other side's alignment forever.
    last_activity = last_ingest if last_ingest is not None else BOOT_MS
    idle = (now - last_activity) > IDLE_MS
    if override is not None:
        watermark, source = override, "override"
    elif max_et is None:
        if idle:
            watermark, source = now - GRACE_MS, "idle_timeout"
        else:
            watermark, source = None, "no_data"
    else:
        watermark, source = max_et - GRACE_MS, "event_time"
        if idle:
            watermark = max(watermark, now - GRACE_MS)
            source = "idle_timeout"
    return {
        "stream": STREAM,
        "watermark": watermark,
        "source": source,
        "max_event_time": max_et,
        "grace_ms": GRACE_MS,
        "idle": idle,
        "idle_timeout_ms": IDLE_MS,
        "override": override,
    }


class OverrideIn(BaseModel):
    watermark: Optional[int] = Field(default=None, ge=0)


@app.post("/watermark/override")
def set_watermark_override(body: OverrideIn):
    """Force the watermark to a fixed value (pass null to clear).

    Setting a *lower* value is the supported way to dial the watermark back;
    the aligner records the regression and keeps serving existing results.
    """
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("UPDATE meta SET watermark_override = %s WHERE id = 1", (body.watermark,))
    finally:
        conn.close()
    log.warning("watermark override set to %s", body.watermark)
    return get_watermark()


@app.get("/healthz")
def healthz():
    conn = connect(retries=1)
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
    finally:
        conn.close()
    return {"status": "ok", "stream": STREAM}
