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
# Reconciliation batches (对账批次)
#
# A batch is a point-in-time reconciliation for ONE downstream over ONE
# event-time range of windows. At opening, every result that has any delivery
# row for that downstream in range is snapshotted once: which version we sent
# up to, which is confirmed delivered, which is still on the wire (PENDING /
# RETRYING), and which version the downstream reported posted. Later ledger
# movement never rewrites the snapshot — the batch answers "what did the books
# say at opening", and the query layer can additionally show the live state
# next to it.
#
# Each snapshot falls into exactly one of four buckets:
# - ALIGNED           — it reports exactly the version we confirmed delivered;
# - LAGGING           — it reports an older version than we delivered;
# - AHEAD_UNCONFIRMED — it reports a version we have not confirmed delivered
#                       (still retrying / held — possibly genuinely posted);
# - NOT_REPORTED      — we sent versions but it never reported this result.
#
# ALIGNED rows need no action; every other row must be adjudicated one by one
# (CONFIRMED = 认账 / REJECTED = 驳) before the batch can close. The decision
# is a reconciliation verdict, not a ledger write — it never moves the posting
# ledger or the outbox. Once the batch is CLOSED its decisions are frozen.
# ---------------------------------------------------------------------------

RECON_ITEM_STATUSES = ("ALIGNED", "LAGGING", "AHEAD_UNCONFIRMED", "NOT_REPORTED")
RECON_DECISIONS = ("CONFIRMED", "REJECTED")
RECON_BATCH_STATUSES = ("OPEN", "CLOSED")


def reconciliation_item_status(sent_version, delivered_version,
                               inflight_version, reported_version):
    """Classify one snapshotted (downstream, result) pair at batch opening.

    ``sent_version``      — highest version with a delivery row of any status
                            (PENDING / RETRYING / DELIVERED); never None, since
                            an item exists only because at least one row exists;
    ``delivered_version`` — highest version confirmed DELIVERED (None = none);
    ``inflight_version``  — lowest version not yet DELIVERED (None = all sent
                            versions are confirmed delivered);
    ``reported_version``  — version the downstream last reported posted
                            (None = it never reported this result).

    The ordering mirrors the live posting ledger (``posting_status``), with
    one extra bucket: no report at all is NOT_REPORTED rather than an empty
    ledger row.
    """
    if reported_version is None:
        return "NOT_REPORTED"
    if delivered_version is None or reported_version > delivered_version:
        return "AHEAD_UNCONFIRMED"
    if reported_version < delivered_version:
        return "LAGGING"
    return "ALIGNED"


def reconciliation_item_unresolved(item_status, decision):
    """Whether a batch item still needs adjudication before the batch can
    close. ALIGNED rows are settled by definition; every other row is open
    until someone records a CONFIRMED/REJECTED decision on it."""
    return item_status != "ALIGNED" and decision is None


def reconciliation_can_close(items):
    """True iff every item is settled: each is either ALIGNED at snapshot time
    or carries an adjudication. ``items`` is an iterable of
    ``(item_status, decision)`` pairs."""
    return all(not reconciliation_item_unresolved(status, decision)
               for status, decision in items)


# ---------------------------------------------------------------------------
# Settlement of closed reconciliation batches (对账落账)
#
# Verdicts recorded while a batch is OPEN change nothing — they are only the
# reconciliation outcome on the frozen photograph. When the batch closes, each
# adjudicated non-aligned item is *settled* exactly once into an immutable
# reconciliation_settlements row keyed by (subscriber, window, key), and from
# that moment the verdict drives delivery and posting reports for that one
# pair:
#
# item class \\ decision   CONFIRMED (认)                REJECTED (驳)
# LAGGING                  FREEZE: pin at the version    CONTINUE: keep
#                          it reported at snapshot time; sending every version
#                          later versions are never sent  we sent; the lagging
#                          to this downstream and reports gate never holds this
#                          above the pin are rejected     pair again
# NOT_REPORTED             SUPPRESS: pin at 0 — this      REDRIVE: pin at the
#                          version is never (re)delivered, lowest sent version,
#                          backfill never copies it, and  which is re-driven
#                          every report is rejected        until it reports it
# AHEAD_UNCONFIRMED        MARK_DELIVERED: the in-flight  NONE: ordinary
#                          versions up to its reported     retries continue
#                          version are confirmed at close
#
# "同一条只能落到一次" is the UNIQUE(subscriber, window, key) constraint: a
# later batch whose close would settle an already-settled pair fails 409.
# Everything is scoped to one pair — other subscribers, other results and
# other windows of the same key are never affected.
# ---------------------------------------------------------------------------

SETTLEMENT_EFFECTS = ("FREEZE", "CONTINUE", "SUPPRESS", "REDRIVE",
                      "MARK_DELIVERED", "NONE")
SETTLEMENT_STATUSES = ("ACTIVE", "FULFILLED")

# Effects that hold back not-yet-delivered versions above their pin.
SETTLEMENT_HOLD_EFFECTS = ("FREEZE", "SUPPRESS")


def settlement_effect(item_status, decision):
    """Map a frozen snapshot bucket + its closed verdict to the delivery/
    reporting effect settled at batch close.

    ALIGNED items are never adjudicated and therefore never settled; asking
    for their effect is a caller error."""
    if item_status == "LAGGING":
        return "FREEZE" if decision == "CONFIRMED" else "CONTINUE"
    if item_status == "NOT_REPORTED":
        return "SUPPRESS" if decision == "CONFIRMED" else "REDRIVE"
    if item_status == "AHEAD_UNCONFIRMED":
        return "MARK_DELIVERED" if decision == "CONFIRMED" else "NONE"
    raise ValueError(f"item_status {item_status!r} is never settled")


def settlement_pin_version(effect, reported_version, lowest_sent_version):
    """The version a settlement pins the pair at.

    - SUPPRESS (认它没入过) pins at 0: not even the first version may be
      (re)delivered or reported;
    - REDRIVE (不认它没入过) pins at the lowest version we ever sent — that is
      the version re-driven until the downstream reports it;
    - every other effect pins at the version the downstream reported at
      snapshot time (FREEZE/CONTINUE) / at close acceptance (MARK_DELIVERED);
      NONE carries the reported version for the record only.
    """
    if effect == "SUPPRESS":
        return 0
    if effect == "REDRIVE":
        return lowest_sent_version
    return reported_version


def settlement_blocks_report(effect, settlement_status, pinned_version, version):
    """Whether a posting report is rejected by a settled pair.

    Returns None when the report may pass through the ordinary ledger rules,
    or a machine-readable rejection reason string:

    - FREEZE: the pair stands at the pinned (then-reported) version forever —
      only an idempotent re-report of that exact version is accepted;
    - SUPPRESS: the downstream never booked it and that is now the settled
      truth — no report of any version can succeed;
    - REDRIVE: while the re-driven pin version is still unreported, a report
      below it cannot count (ordinary delivery rules normally reject those
      first — this is the belt-and-braces check).
    FULFILLED settlements and non-gating effects never block.
    """
    if settlement_status != "ACTIVE":
        return None
    if effect == "FREEZE":
        return None if version == pinned_version else "frozen_by_settlement"
    if effect == "SUPPRESS":
        return "suppressed_by_settlement"
    if effect == "REDRIVE" and version < pinned_version:
        return "below_redrive_pin"
    return None


def settlement_fulfils(effect, settlement_status, pinned_version, version):
    """Whether an accepted posting report discharges an ACTIVE settlement.

    REDRIVE is fulfilled once the downstream reports the re-driven pin version
    (or above — it cannot report above without the pin having been on the
    wire); CONTINUE is fulfilled once its reports move past the version it was
    lagging at, i.e. the dispute is over. FREEZE / SUPPRESS stay ACTIVE
    forever; MARK_DELIVERED / NONE are born FULFILLED.
    """
    if settlement_status != "ACTIVE":
        return False
    if effect == "REDRIVE":
        return version >= pinned_version
    if effect == "CONTINUE":
        return version > pinned_version
    return False


def ranges_overlap(from_a, to_a, from_b, to_b):
    """Half-open interval overlap: [from_a, to_a) vs [from_b, to_b)."""
    return from_a < to_b and from_b < to_a


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
# Per-(window, key) close grace (关窗宽限).
#
# A not-yet-emitted (window, key) can be granted ONE active grace extension:
# hold its INITIAL result past the ordinary per-key window end and wait a
# little longer for the opposite side. The scope is exactly that one pair —
# every other key and window keeps closing at its own window end, never held
# up by it. While the grace is active the window simply does not emit, even if
# both sides have already crossed; at the wall-clock deadline the version as
# it stands THEN is emitted, one side included ("到点了还是一边，按单边出，
# 不能再空等"). Late events / retractions landing during the grace keep
# changing the internal computation exactly as usual, so the deadline emits
# the then-current recomputation. A window whose data is all retracted before
# the deadline has nothing to emit: the grace expires and the window returns
# to ordinary per-key waiting. A window that already produced a result can
# never be graced again, and two not-yet-due graces cannot stack on one
# window (the ACTIVE row is unique per (window, key)).
# ---------------------------------------------------------------------------

GRACE_STATUSES = ("ACTIVE", "FIRED", "EXPIRED")

# Grace grants above this are almost certainly a caller mistake, not a real
# "wait a little longer for the other side" request (30 days).
MAX_GRACE_EXTRA_MS = 30 * 24 * 3600 * 1000


def valid_window_start(window_start, window_size_ms):
    """A grant must name a tumbling-window boundary, not a time inside one."""
    return window_start >= 0 and window_size_ms > 0 \
        and window_start % window_size_ms == 0


def grace_grant_error(has_effective_data, head_exists, has_active_grace):
    """Whether a (window, key) may receive a grace now.

    Returns None when allowed, otherwise a machine-readable reason:

    - ``already_emitted``    — the window already produced a result (a later
                               RETRACTED head included — what went out is a
                               fact, corrections are its only path); this
                               takes precedence over the data check, since a
                               fully retracted head is still a past result;
    - ``window_not_active``  — no effective (non-retracted) data in the window
                               yet: there is nothing to hold/wait for;
    - ``active_grace_exists``— one not-yet-due grace already holds it.
    """
    if head_exists:
        return "already_emitted"
    if not has_effective_data:
        return "window_not_active"
    if has_active_grace:
        return "active_grace_exists"
    return None


def grace_fate(status, deadline_ms, now_ms, has_effective_data):
    """Fate of a grace row on a tick.

    Returns None for a non-ACTIVE row; otherwise:

    - ``HOLD``   — deadline still ahead: the window must not emit yet, even if
                   both sides have already crossed the window end;
    - ``FIRE``   — deadline reached and effective data remains: emit the
                   then-current version, one-sided included;
    - ``EXPIRE`` — deadline reached with no effective data left (everything
                   was retracted): nothing to emit, the grace lapses and the
                   window goes back to ordinary per-key waiting.
    """
    if status != "ACTIVE":
        return None
    if now_ms < deadline_ms:
        return "HOLD"
    return "FIRE" if has_effective_data else "EXPIRE"


# ---------------------------------------------------------------------------
# business orders (业务单): one order per business key, assembled from the
# aligned window results. Pure state machine — the service layer supplies the
# current bindings and the window-gaps view, this decides the order's status.
# ---------------------------------------------------------------------------

ORDER_STATUSES = ("OPEN", "WAITING", "CLOSED", "REOPENED", "VOID")

ORDER_REASONS = ("ORDER_OPENED", "WINDOW_JOINED", "WINDOW_CORRECTED",
                 "WINDOW_WITHDRAWN", "WINDOW_REVIVED", "CARRY_RESOLVED")


def evaluate_order(bindings, missing, pending, ever_closed, reason,
                   open_carry_count=0):
    """Decide an order's status from its current window bindings.

    ``bindings``    — one dict per bound window: {"result_status", "has_gap"}.
    ``missing``     — due business windows (effective events exist, watermark
                      has passed) whose results are not in the order yet.
    ``pending``     — business windows known from events but not yet due.
    ``ever_closed`` — the order has reached CLOSED at least once before.
    ``reason``      — what the triggering result version did to its window.
    ``open_carry_count`` — non-terminal gap carries (OPEN / REOPENED) still
                      moving events between this key's windows: the order is
                      not fully resolved while any exists ("结转还没对上" is
                      a waiting state, not a success), so CLOSE is blocked.

    CLOSE requires all of: at least one live window; every known business
    window bound (nothing missing or pending); no withdrawn window left
    unresolved; no live window still waiting for the opposite side; no
    still-open carry. All windows withdrawn -> VOID (the business is gone, not
    a success).

    A correction or withdrawal NEVER (re)closes an order that has been closed
    before: the close is invalidated and stays invalidated — the order shows
    REOPENED, never "still closed", no matter how well the data matches after
    the correction. Only genuine new business can close it again: a new
    window joining (WINDOW_JOINED), a withdrawn window reviving
    (WINDOW_REVIVED) or a carry finally closing (CARRY_RESOLVED), evaluated
    against the close conditions. Anything else short of CLOSE is OPEN while
    windows are still being collected and WAITING once only one-sided gaps
    (or in-flight carries) remain — labelled REOPENED instead once the order
    has been closed before.
    """
    live = [b for b in bindings if b["result_status"] == "CURRENT"]
    if not live:
        return "VOID"
    if ever_closed and reason in ("WINDOW_CORRECTED", "WINDOW_WITHDRAWN"):
        return "REOPENED"
    if missing or pending or any(b["result_status"] == "RETRACTED" for b in bindings):
        return "REOPENED" if ever_closed else "OPEN"
    if any(b["has_gap"] for b in live) or carry_open_block_order(open_carry_count):
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


# ---------------------------------------------------------------------------
# Gap carry-forwards (缺口结转).
#
# A closed window whose one side came out longer can carry its leftover events
# to a LATER window of the SAME business key, before that later window has
# produced any result. Carried events are injected into the target's longer
# side and paired against the target's own leftovers; any pair involving a
# carried event is visibly a carry pair (it carries the carry id and the source
# window — never written as a native pair of the target window).
#
# A carry has a lifecycle:
#   OPEN      created, none (or only part) of its events have matched yet;
#   CLOSED    every carried event matched in the target — a frozen
#             matched_snapshot preserves "当时对上的样子";
#   REOPENED  a CLOSED carry whose match was later undone (the target window
#             corrected/withdrew, or got its own longer-side events so the
#             carried event is leftover again) — it must never keep showing
#             CLOSED;
#   VOID      dead forever: one of its events was retracted at the source or
#             got paired at the source by a late opposite-side event. A void
#             carry can never match again, and (together with the global
#             per-event history in gap_carry_items) its events can never be
#             carried a second time — "结转出去的那几条，原来那一窗不能再拿去
#             结第二次", "作废的不能再拿去对".
#
# The same unmatched event therefore cannot sit in two OPEN carries either:
# event identity is unique across the whole carry-items history, a database
# constraint backstopped by create_carry's checks.
# ---------------------------------------------------------------------------

CARRY_STATUSES = ("OPEN", "CLOSED", "REOPENED", "VOID")
CARRY_SIDES = ("a", "b")
# Item-level match state. CARRIED = still waiting at the target; MATCHED = the
# target paired it; DEAD = the carry was voided for this item (retracted at the
# source, or paired at the source by a late event).
CARRY_ITEM_STATUSES = ("CARRIED", "MATCHED", "DEAD")
# Why a carry died. source_retracted = a carried event was retracted;
# source_paired = a late opposite-side event paired it at the source window.
CARRY_VOID_REASONS = ("source_retracted", "source_paired")


def opposite_side(side):
    return "b" if side == "a" else "a"


def annotate_source_payload(payload, side, carry_id, item_event_ids):
    """Move a source window's just-carried leftover events out of its
    ``unmatched_<side>`` list into the payload's ``carried`` bookkeeping.

    The source window's own result is corrected in the same logical operation
    that opens the carry: the events are no longer "waiting for the opposite
    side in THIS window" — they are en route to another window. Carried events
    never disappear from the source history, they are just visibly routed. The
    annotation is idempotent: a replay of the same carry never moves an event
    twice, and an event already annotated for a different carry is left alone
    (the create path rejects double-carries before this is ever called).
    """
    if payload is None:
        return None
    ids = list(item_event_ids)
    key = f"unmatched_{side}"
    carried = dict(payload.get("carried") or {})
    existing = set(carried.get(side, []))
    to_move = [e for e in payload.get(key, []) if e in ids and e not in existing]
    if not to_move:
        return payload
    new_payload = dict(payload)
    new_payload[key] = [e for e in payload.get(key, []) if e not in to_move]
    new_payload["carried"] = {
        **carried,
        side: sorted(existing | set(to_move)),
    }
    return new_payload


def compute_payload_with_carries(key, window_start, window_end,
                                 a_upserts, b_upserts, carries):
    """Compute a target window's result with live carry events routed in.

    ``carries`` is a list of {"id", "side", "source_window_start", "events":
    [{"event_id", "event_time", "payload"}]} for every non-VOID carry aimed at
    this window (VOID carries are excluded by the caller).

    Two pairing pools, in strict order:

    1. NATIVE pairing - the target window's own events pair exactly as in
       compute_payload (sorted by (event_time, event_id), positionally). A
       routed-in event can NEVER displace one of these pairs, no matter where
       its event_time sorts: the carry was routed to fill the target's gap, not
       to rearrange the target's own business.
    2. CARRY pairing - each carried event then pairs against the target's own
       leftover on the opposite side (the gap this carry was opened for). Two
       carried events never pair each other (routed leftovers from two windows
       closing against each other would prove nothing about the target). When
       several carries compete for one leftover, the OLDEST carry wins
       (carry id, then event_time, event_id) - deterministic FIFO.

    Every carry pair is tagged {"carry_id", "source_window_start"}, so the
    result can never present a carry match as the target window's own native
    pair. Unpaired carried events stay OUT of unmatched_* (they are not the
    target's events - they remain CARRIED on their carry, which is how a
    partially matched carry stays OPEN); the target's own leftovers it did not
    pair stay in unmatched_* and may themselves be carried onward later.
    """
    a_native = sorted(a_upserts, key=lambda e: (e["event_time"], e["event_id"]))
    b_native = sorted(b_upserts, key=lambda e: (e["event_time"], e["event_id"]))
    if not a_native and not b_native and not carries:
        return None

    # 1) native pairing pool (unchanged from compute_payload)
    n = min(len(a_native), len(b_native))
    pairs = [
        {
            "a_event_id": a_native[i]["event_id"],
            "b_event_id": b_native[i]["event_id"],
            "a_payload": a_native[i].get("payload"),
            "b_payload": b_native[i].get("payload"),
            "carry": None,
        }
        for i in range(n)
    ]
    a_left = list(a_native[n:])   # target's own A leftovers
    b_left = list(b_native[n:])   # target's own B leftovers

    # 2) carry pairing pool - only against the target's own opposite leftovers.
    queued = {"a": [], "b": []}
    for c in sorted(carries, key=lambda c: c["id"]):
        tag = {"carry_id": c["id"], "source_window_start": c["source_window_start"]}
        for e in sorted(c["events"], key=lambda e: (e["event_time"], e["event_id"])):
            queued[c["side"]].append((e, tag))
    queued["a"].sort(key=lambda t: (t[1]["carry_id"], t[0]["event_time"], t[0]["event_id"]))
    queued["b"].sort(key=lambda t: (t[1]["carry_id"], t[0]["event_time"], t[0]["event_id"]))

    carry_pairs = []
    for ea, tag in queued["a"]:
        if b_left:
            eb = b_left.pop(0)  # native leftovers are already (time,id)-sorted
            carry_pairs.append((ea, eb, tag))
    for eb, tag in queued["b"]:
        if a_left:
            ea = a_left.pop(0)
            carry_pairs.append((ea, eb, tag))

    for ea, eb, tag in carry_pairs:
        pairs.append({
            "a_event_id": ea["event_id"], "b_event_id": eb["event_id"],
            "a_payload": ea.get("payload"), "b_payload": eb.get("payload"),
            "carry": tag,
        })
    return {
        "key": key,
        "window_start": window_start,
        "window_end": window_end,
        "match_count": len(pairs) - len(carry_pairs),
        "carry_match_count": len(carry_pairs),
        "pairs": pairs,
        "unmatched_a": [e["event_id"] for e in a_left],
        "unmatched_b": [e["event_id"] for e in b_left],
        "carried": {},
    }


def carry_matched_event_ids(payload, carry_id):
    """The set of THIS carry's events the target payload currently pairs.
    Recomputed from the payload on every derivation, so a CLOSED carry is
    reopened automatically when a later target version stops pairing one of
    its events ("已经关上的必须重开，不能还显示对上了")."""
    if payload is None:
        return set()
    ids = set()
    for p in payload.get("pairs", []):
        tag = p.get("carry")
        if tag and tag.get("carry_id") == carry_id:
            ids.add(p["a_event_id"])
            ids.add(p["b_event_id"])
    return ids


def carry_item_fate(item_event_id, item_side, native_payload, live_event_ids):
    """Fate of one carried event against the SOURCE window's recomputation.

    Returns None while the event is still a valid leftover of the source
    (eligible to keep riding the carry), otherwise a machine-readable death:

    - ``source_retracted`` — the event is no longer an effective (non-retracted)
      event at all;
    - ``source_paired``    — a late opposite-side event paired it at the source
      window, so routing it elsewhere would double-count it.

    ``live_event_ids`` is the set of currently effective event ids of its
    stream in the source window; ``native_payload`` is the source's ordinary
    (carry-free) recomputation.
    """
    if item_event_id not in live_event_ids:
        return "source_retracted"
    unmatched = set(native_payload.get(f"unmatched_{item_side}", [])) if native_payload else set()
    if item_event_id not in unmatched:
        # live but no longer a leftover on its side: it got paired at source
        # (or, defensibly, consumed some other way — either way it cannot be
        # routed to another window anymore).
        return "source_paired"
    return None


def carry_open_block_order(open_count):
    """An order cannot reach CLOSED while one of its keys' carries is still
    open/reopened (events are in flight between two windows)."""
    return bool(open_count)


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
            "carried_a": b.get("carried_a", []),
            "carried_b": b.get("carried_b", []),
            "payload_hash": b["payload_hash"],
        }
        for b in sorted(bindings, key=lambda b: b["window_start"])
    ]
