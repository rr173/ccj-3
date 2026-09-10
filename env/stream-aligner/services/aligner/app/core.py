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


def retry_delay_ms(attempts, base_ms, max_ms):
    """Exponential backoff after ``attempts`` failed attempts (attempts >= 1)."""
    return min(base_ms * (2 ** (attempts - 1)), max_ms)


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
# business orders (业务单): one order per business key, assembled from the
# aligned window results. Pure state machine — the service layer supplies the
# current bindings and the window-gaps view, this decides the order's status.
# ---------------------------------------------------------------------------

ORDER_STATUSES = ("OPEN", "WAITING", "CLOSED", "REOPENED", "VOID")

ORDER_REASONS = ("ORDER_OPENED", "WINDOW_JOINED", "WINDOW_CORRECTED",
                 "WINDOW_WITHDRAWN", "WINDOW_REVIVED")


def evaluate_order(bindings, missing, pending, ever_closed, head_status, reason):
    """Decide an order's status from its current window bindings.

    ``bindings``    — one dict per bound window: {"result_status", "has_gap"}.
    ``missing``     — due business windows (effective events exist, watermark
                      has passed) whose results are not in the order yet.
    ``pending``     — business windows known from events but not yet due.
    ``ever_closed`` — the order has reached CLOSED at least once before.
    ``head_status`` — the order's current head status.
    ``reason``      — what the triggering result version did to its window.

    CLOSE requires all of: at least one live window; every known business
    window bound (nothing missing or pending); no withdrawn window left
    unresolved; no live window still waiting for the opposite side. All
    windows withdrawn -> VOID (the business is gone, not a success).

    A correction or withdrawal landing on a CLOSED order always reopens it —
    the order must never sail through a post-close change still showing
    CLOSED, even if everything still matches. It re-closes when a *later*
    trigger finds the close conditions met again. Anything else short of
    CLOSE is OPEN while windows are still being collected and WAITING once
    only one-sided gaps remain — labelled REOPENED instead once the order
    has been closed before.
    """
    live = [b for b in bindings if b["result_status"] == "CURRENT"]
    if not live:
        return "VOID"
    if head_status == "CLOSED" and reason in ("WINDOW_CORRECTED", "WINDOW_WITHDRAWN"):
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
