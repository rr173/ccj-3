"""Pure windowing / join / versioning logic for the aligner.

This module is intentionally dependency-free so the alignment semantics can be
unit-tested without standing up the service stack. The service layer
(``main.py``) only does IO: pulling events, persisting versions, serving queries.
"""
import hashlib
import json


def window_of(event_time_ms, window_size_ms):
    """Tumbling window [start, end) containing ``event_time_ms``."""
    start = (event_time_ms // window_size_ms) * window_size_ms
    return start, start + window_size_ms


def effective(events):
    """Drop retract events and the upserts they retract.

    A retraction applies to its target regardless of the retract event's own
    timestamp, so callers must pass *all* retractions known for the stream,
    not only those falling inside the window being computed.
    """
    retracted = {e["retracts"] for e in events if e.get("type") == "retract" and e.get("retracts")}
    return [
        e
        for e in events
        if e.get("type", "upsert") == "upsert" and e["event_id"] not in retracted
    ]


def compute_payload(key, window_start, window_end, a_upserts, b_upserts):
    """Build the deterministic business result for one (window, key).

    Inputs must already be retraction-filtered upserts. Events on each side
    are ordered by (event_time, event_id) and paired positionally; leftovers
    are reported as unmatched. Returns None when both sides are empty — the
    caller treats that as a retraction of any previously emitted result.
    """
    a = sorted(a_upserts, key=lambda e: (e["event_time"], e["event_id"]))
    b = sorted(b_upserts, key=lambda e: (e["event_time"], e["event_id"]))
    if not a and not b:
        return None
    n = min(len(a), len(b))
    pairs = [
        {
            "a_event_id": a[i]["event_id"],
            "b_event_id": b[i]["event_id"],
            "a_payload": a[i].get("payload"),
            "b_payload": b[i].get("payload"),
        }
        for i in range(n)
    ]
    return {
        "key": key,
        "window_start": window_start,
        "window_end": window_end,
        "match_count": n,
        "pairs": pairs,
        "unmatched_a": [e["event_id"] for e in a[n:]],
        "unmatched_b": [e["event_id"] for e in b[n:]],
    }


def payload_hash(payload):
    if payload is None:
        return None
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def delivery_kind(reason, status):
    """Classify a result version for downstream delivery.

    NEW        — first version of a result (downstream books it);
    CORRECTION — a later live version (downstream *amends* the existing
                 booking under the same (window_start, key) identity — it
                 must never be booked as another new success);
    WITHDRAWAL — the result became empty (downstream reverses the booking).
    """
    if status == "RETRACTED":
        return "WITHDRAWAL"
    return "NEW" if reason == "INITIAL" else "CORRECTION"


def released_version_kind(version, first_released_version, reason, status):
    """Classify a version when replaying versions that crossed the release gate.

    The first version that was externally released is always NEW to that
    downstream, even if it was an internal correction (for example v3 when v1
    and v2 were held internally). Later released versions keep their original
    external classification. Versions below ``first_released_version`` are
    internal-only and must not be replayed by the caller.
    """
    if version == first_released_version:
        return "NEW"
    return delivery_kind(reason, status)


def normalize_backfill_range(from_ms, to_ms, window_size_ms):
    """Snap an event-time replay range onto tumbling-window boundaries.

    The caller speaks event time ("replay everything covering [from, to)"),
    while candidate selection compares against ``window_start``. Without this,
    a bound landing *inside* a window — ``from`` at ws+δ, say — silently drops
    the whole window that actually contains that instant: its window_start is
    below the bound even though the requested time falls in it. A bound already
    on a window boundary is left untouched, so callers that pass exact window
    starts keep getting the same half-open ``[window_start, window_start)``
    selection.

    Returns ``(from_window_start, to_window_start)``:
    - the lower bound floors to the start of the window containing ``from``;
    - the upper bound ceils to the end of the window containing ``to - 1`` — the
      window is included whenever the range reaches into it.
    """
    from_ws = (from_ms // window_size_ms) * window_size_ms
    # Boundary right after the window containing the last included event time
    # (to - 1): that window's window_start is below the bound, so it would be
    # missed by a plain `< to` comparison unless we move the bound up.
    to_ws = ((to_ms - 1) // window_size_ms + 1) * window_size_ms
    return from_ws, to_ws


def retry_delay_ms(attempts, base_ms, max_ms):
    """Exponential backoff after ``attempts`` failed attempts (attempts >= 1)."""
    return min(base_ms * (2 ** (attempts - 1)), max_ms)


# ---------------------------------------------------------------------------
# External release gate (对外放行闸门), per business key.
#
# Alignment keeps computing internal result versions exactly as before — they
# stay audited and queryable — but a version is only *given externally* (i.e.
# outbox deliveries are created) once its (window, key) is released. Releasing
# is one-shot per window: the version that is head AT RELEASE TIME goes out as
# NEW (intermediate versions held back internally are never sent). A released
# window is past the gate forever: every later correction / withdrawal /
# revival is delivered regardless of whether the key's gate has since been
# closed — closing only ever holds back windows that have never been out, and
# nothing that went out can be reclaimed.
# ---------------------------------------------------------------------------

def should_deliver(gate_open, ever_released, status):
    """Whether a freshly committed result version creates outbox deliveries.

    ``gate_open``      — the key's per-business release switch is open;
    ``ever_released``  — this (window, key) has been externally released once;
    ``status``         — the new version's status (CURRENT / RETRACTED).

    - never released, gate closed  → HELD: computed and queryable internally,
      but nothing reaches the outbox (no push downstream);
    - never released, gate open    → first live version is released at once as
      NEW;
    - never released, head RETRACTED → still nothing: the outside world never
      knew this window, there is nothing to withdraw — a later revival while
      the gate is open goes out as the first NEW;
    - released before              → every later version flows
      (CORRECTION / WITHDRAWAL), gate open or closed. Closing the gate never
      swallows a window that has already crossed it.
    """
    if ever_released:
        return True
    return bool(gate_open and status == "CURRENT")


def releasable(head_status, ever_released):
    """An explicit release can only publish a held window that currently has a
    live (non-empty) head and has never been released before. A window held
    while fully retracted releases nothing (nothing ever went out); releasing
    it again is a no-op, not a re-send."""
    return (not ever_released) and head_status == "CURRENT"


def decide(head, new_payload):
    """Decide the next version given the current head row and a recomputed payload.

    ``head`` is None or ``{"version": int, "status": str, "payload_hash": str|None}``.
    Returns None for a no-op (idempotent recompute — same content must never
    produce a new version), otherwise ``{"version", "status", "payload_hash"}``
    for the row to insert. Status is CURRENT when there is a live payload and
    RETRACTED when the result has become empty.
    """
    new_hash = payload_hash(new_payload)
    if head is None:
        if new_payload is None:
            return None
        return {"version": 1, "status": "CURRENT", "payload_hash": new_hash}
    if head["status"] == "RETRACTED" and new_payload is None:
        return None
    if head["payload_hash"] == new_hash:
        return None
    return {
        "version": head["version"] + 1,
        "status": "CURRENT" if new_payload is not None else "RETRACTED",
        "payload_hash": new_hash,
    }


# ---------------------------------------------------------------------------
# Downstream posting ledger (下游入账台账), per (subscriber, window, key).
#
# Delivery being at-least-once, "we sent it" never means "it is booked": the
# downstream itself reports which version of a result it has posted (入账),
# and the ledger compares that against the last version we confirmed
# DELIVERED. A pair enters the ledger at its first accepted report; pairs the
# downstream never reported on keep flowing exactly as before (no gate).
# ---------------------------------------------------------------------------

POSTING_STATUSES = ("ALIGNED", "LAGGING", "AHEAD_UNCONFIRMED")


def posting_status(reported_version, delivered_up_to):
    """Alignment between what the downstream says it posted and what we
    confirmed delivered, for one (downstream, result) pair.

    ``reported_version`` — the version the downstream last reported as posted
    (None/0 = it has not reported anything);
    ``delivered_up_to``  — the highest version we confirmed DELIVERED to it
    (None/0 = nothing delivered yet).

    - ``ALIGNED``           — reported == delivered: the books balance;
    - ``LAGGING``           — reported < delivered: it is behind. Later
                              versions of THIS result are held for THIS
                              downstream until it reports catching up;
    - ``AHEAD_UNCONFIRMED`` — reported > delivered: it claims a version whose
                              delivery we have not confirmed yet (the row is
                              still PENDING/RETRYING — a lost response leaves
                              us retrying what it already posted). Nothing is
                              held: our own retry is what closes the gap.
    """
    reported = reported_version or 0
    delivered = delivered_up_to or 0
    if reported == delivered:
        return "ALIGNED"
    if reported < delivered:
        return "LAGGING"
    return "AHEAD_UNCONFIRMED"


def posting_gate_allows(status):
    """Whether further versions of a result may be dispatched to a downstream
    whose ledger status is ``status``. Only LAGGING holds the gate: while it
    is behind, later versions of that one result wait for its posting to
    catch up; other results of the same downstream are never affected."""
    return status != "LAGGING"


def reportable_delivery_status(delivery_status):
    """Whether a downstream's posting report for one version may count, given
    that version's delivery status for that downstream.

    Only versions we actually put on the wire can be reported as posted:
    ``DELIVERED`` (confirmed) or ``RETRYING`` (dispatched at least once, the
    response was lost — the downstream may genuinely have booked it, which is
    exactly the AHEAD_UNCONFIRMED case). A version never dispatched —
    ``PENDING``, e.g. still held by the ledger gate — has not been sent to
    it, so its "I already posted it" report cannot count: the report is
    rejected and the ledger stays put.
    """
    return delivery_status in ("DELIVERED", "RETRYING")


# ---------------------------------------------------------------------------
# Per-key emission gate (按业务键分开关窗).
#
# A result for (window, key) is emitted only when *that key's own two sides*
# have both crossed the window end — never on a global min-watermark, so one
# quiet business cannot stall the others, and a slow business simply keeps
# waiting: its stored events plus the absence of a result *are* the waiting
# state, re-evaluated every tick, never dropped. Emission is one-shot per
# (window, key) and results are append-only, so a watermark regression can
# never un-emit what already went out. Late events / retractions recompute
# already-emitted windows through the usual correction path, unchanged.
# ---------------------------------------------------------------------------

# Watermark sources that count as a real progress promise. An idle-timeout
# watermark is wall-clock guesswork about a silent stream, not a promise —
# on its own it must never finalize a side the business is still waiting for.
CROSSING_SOURCES = ("event_time", "override")


def side_evidence(mark, window_end):
    """Why one stream counts as past ``window_end`` for one key, or None.

    ``mark`` — {"watermark", "source", "key_max_event_time",
    "key_has_data_in_window"} for one (stream, key, window). Three kinds of
    crossing evidence, any sufficient:

    - ``own_progress``: the key itself has an event on this stream at or
      beyond the window end — the business has moved on by itself, no need
      to wait for the rest of the stream;
    - ``watermark``: the stream watermark covers the window end AND comes
      from a real promise (event data or an operator override) — not from
      idle wall-clock advancement;
    - ``idle_finalized``: the stream has gone idle (wall-clock watermark
      covers the window end) AND the key already has effective data on
      this side *in this window*. A business whose both sides have arrived
      must not be stuck forever behind a silent stream. A side with no
      data in the window is NOT crossed by idleness — a business still
      waiting for it keeps waiting, never force-closed one-sided.
    """
    key_max = mark.get("key_max_event_time")
    if key_max is not None and key_max >= window_end:
        return "own_progress"
    watermark = mark.get("watermark")
    if watermark is None or watermark < window_end:
        return None
    source = mark.get("source")
    if source in CROSSING_SOURCES:
        return "watermark"
    if source == "idle_timeout" and mark.get("key_has_data_in_window"):
        return "idle_finalized"
    return None


def window_ready(mark_a, mark_b, window_end):
    """True iff both streams have crossed ``window_end`` for one key."""
    return (side_evidence(mark_a, window_end) is not None
            and side_evidence(mark_b, window_end) is not None)


# ---------------------------------------------------------------------------
# business orders (业务单): one order per business key, assembled from the
# aligned window results. Pure state machine — the service layer supplies the
# current bindings and the window-gaps view, this decides the order's status.
# ---------------------------------------------------------------------------

ORDER_STATUSES = ("OPEN", "WAITING", "CLOSED", "REOPENED", "VOID")

ORDER_REASONS = ("ORDER_OPENED", "WINDOW_JOINED", "WINDOW_CORRECTED",
                 "WINDOW_WITHDRAWN", "WINDOW_REVIVED")


def evaluate_order(bindings, missing, pending, ever_closed, reason):
    """Decide an order's status from its current window bindings.

    ``bindings``    — one dict per bound window: {"result_status", "has_gap"}.
    ``missing``     — due business windows (effective events exist, watermark
                      has passed) whose results are not in the order yet.
    ``pending``     — business windows known from events but not yet due.
    ``ever_closed`` — the order has reached CLOSED at least once before.
    ``reason``      — what the triggering result version did to its window.

    CLOSE requires all of: at least one live window; every known business
    window bound (nothing missing or pending); no withdrawn window left
    unresolved; no live window still waiting for the opposite side. All
    windows withdrawn -> VOID (the business is gone, not a success).

    A correction or withdrawal NEVER (re)closes an order that has been closed
    before: the close is invalidated and stays invalidated — the order shows
    REOPENED, never "still closed", no matter how well the data matches after
    the correction. Only genuine new business can close it again: a new
    window joining (WINDOW_JOINED) or a withdrawn window reviving
    (WINDOW_REVIVED), evaluated against the close conditions. Anything else
    short of CLOSE is OPEN while windows are still being collected and
    WAITING once only one-sided gaps remain — labelled REOPENED instead once
    the order has been closed before.
    """
    live = [b for b in bindings if b["result_status"] == "CURRENT"]
    if not live:
        return "VOID"
    if ever_closed and reason in ("WINDOW_CORRECTED", "WINDOW_WITHDRAWN"):
        return "REOPENED"
    if missing or pending or any(b["result_status"] == "RETRACTED" for b in bindings):
        return "REOPENED" if ever_closed else "OPEN"
    if any(b["has_gap"] for b in live):
        return "REOPENED" if ever_closed else "WAITING"
    return "CLOSED"


def order_reason(prev_status, new_status):
    """Classify what a result version did to its window's binding.

    ``prev_status`` is None when the window enters the order for the first
    time (the caller turns the very first version of an order into
    ORDER_OPENED instead).
    """
    if prev_status is None:
        return "WINDOW_JOINED"
    if new_status == "RETRACTED":
        return "WINDOW_WITHDRAWN"
    if prev_status == "RETRACTED":
        return "WINDOW_REVIVED"
    return "WINDOW_CORRECTED"


def build_order_snapshot(bindings):
    """Deterministic full-order snapshot: every window and the result version
    it is bound to, ordered by window. Stored on each order version so the
    close-time shape of the order is preserved forever."""
    return [
        {
            "window_start": b["window_start"],
            "window_end": b["window_end"],
            "result_version": b["result_version"],
            "result_status": b["result_status"],
            "has_gap": b["has_gap"],
            "match_count": b["match_count"],
            "unmatched_a": b["unmatched_a"],
            "unmatched_b": b["unmatched_b"],
            "payload_hash": b["payload_hash"],
        }
        for b in sorted(bindings, key=lambda b: b["window_start"])
    ]
