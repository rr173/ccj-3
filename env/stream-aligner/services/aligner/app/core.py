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
