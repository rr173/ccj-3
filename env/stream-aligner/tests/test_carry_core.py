"""Stdlib-only unit tests for the gap carry-forward (缺口结转) core semantics.

Run: python3 tests/test_carry_core.py
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "aligner"))

from app.core import (  # noqa: E402
    annotate_source_payload, carry_item_fate, carry_matched_event_ids,
    carry_open_block_order, compute_payload, compute_payload_with_carries,
    evaluate_order, opposite_side)

W = 30_000


def ev(eid, t, payload=None):
    return {"event_id": eid, "event_time": t, "payload": payload}


def carry(cid, side, events, source=0):
    return {"id": cid, "side": side, "source_window_start": source,
            "events": events}


class TestAnnotateSourcePayload(unittest.TestCase):
    def test_leftovers_move_out_of_unmatched_into_carried(self):
        p = compute_payload("k", 0, W,
                            [ev("a1", 1000), ev("a2", 2000)], [ev("b1", 1500)])
        out = annotate_source_payload(p, "a", carry_id=7, item_event_ids=["a2"])
        self.assertEqual(out["unmatched_a"], [])  # a2 left the window's gap
        self.assertEqual(out["carried"], {"a": ["a2"]})
        # idempotent: a second call does not duplicate / mis-move anything
        again = annotate_source_payload(out, "a", carry_id=7, item_event_ids=["a2"])
        self.assertEqual(again["carried"], {"a": ["a2"]})

    def test_none_payload_passes_through(self):
        self.assertIsNone(annotate_source_payload(None, "a", 1, ["x"]))

    def test_events_not_in_unmatched_are_not_carried(self):
        p = compute_payload("k", 0, W, [ev("a1", 1000)], [ev("b1", 1000)])
        out = annotate_source_payload(p, "a", 1, ["a1"])  # a1 is paired, not leftover
        self.assertEqual(out, p)
        self.assertEqual(out.get("carried", {}), {})


class TestComputePayloadWithCarries(unittest.TestCase):
    def test_carry_pairs_against_target_leftover_and_is_tagged(self):
        # target window: B is longer (b2 unmatched); a carried A event from
        # an earlier window pairs b2, visibly a carry pair, never a native one.
        c = carry(1, "a", [ev("ca1", 42_000)])
        out = compute_payload_with_carries(
            "k", W, 2 * W,
            [ev("a1", W + 1000)], [ev("b1", W + 1000), ev("b2", W + 2000)],
            [c])
        self.assertEqual(out["match_count"], 1)
        self.assertEqual(out["carry_match_count"], 1)
        native, carry_pair = out["pairs"]
        self.assertIsNone(native["carry"])
        self.assertEqual(native["a_event_id"], "a1")
        self.assertEqual(carry_pair["a_event_id"], "ca1")
        self.assertEqual(carry_pair["b_event_id"], "b2")
        self.assertEqual(carry_pair["carry"],
                         {"carry_id": 1, "source_window_start": 0})
        self.assertEqual(out["unmatched_a"], [])
        self.assertEqual(out["unmatched_b"], [])

    def test_unpaired_carried_event_does_not_become_target_unmatched(self):
        # target has no opposite leftover: the carried event stays on its
        # carry (CARRIED), it is not the target window's own unmatched event.
        c = carry(1, "a", [ev("ca1", 42_000)])
        out = compute_payload_with_carries("k", W, 2 * W, [], [], [c])
        self.assertEqual(out["pairs"], [])
        self.assertEqual(out["unmatched_a"], [])
        self.assertEqual(out["unmatched_b"], [])

    def test_carried_event_never_displaces_native_pair_even_with_earlier_time(self):
        # The carried event sorts BEFORE the native A by event_time, but native
        # pairs are formed from the native pool first: a1 still pairs b1, and
        # the carry fills the real gap (b2). A global time merge would wrongly
        # produce ca1-b1, a1-b2 and present a carry pair as the target's own.
        c = carry(1, "a", [ev("ca1", W - 5000)])
        out = compute_payload_with_carries(
            "k", W, 2 * W,
            [ev("a1", W + 1000)], [ev("b1", W + 1000), ev("b2", W + 2000)],
            [c])
        native = out["pairs"][0]
        cp = out["pairs"][1]
        self.assertIsNone(native["carry"])
        self.assertEqual((native["a_event_id"], native["b_event_id"]), ("a1", "b1"))
        self.assertEqual(cp["carry"]["carry_id"], 1)
        self.assertEqual((cp["a_event_id"], cp["b_event_id"]), ("ca1", "b2"))
        self.assertEqual(out["unmatched_b"], [])

    def test_two_carries_same_side_oldest_gets_first_leftover(self):
        c1 = carry(1, "a", [ev("old", 40_000)], source=0)
        c2 = carry(2, "a", [ev("new", 40_000)], source=W)
        out = compute_payload_with_carries(
            "k", 2 * W, 3 * W, [],
            [ev("b1", 2 * W + 1000)], [c1, c2])
        self.assertEqual(out["carry_match_count"], 1)
        self.assertEqual(out["pairs"][0]["a_event_id"], "old")
        self.assertEqual(out["pairs"][0]["carry"]["carry_id"], 1)

    def test_no_events_means_none(self):
        self.assertIsNone(compute_payload_with_carries("k", W, 2 * W, [], [], []))

    def test_matched_event_ids_per_carry(self):
        c1 = carry(1, "a", [ev("x", 40_000)], source=0)
        c2 = carry(2, "a", [ev("y", 40_000)], source=W)
        out = compute_payload_with_carries(
            "k", 2 * W, 3 * W, [],
            [ev("b1", 2 * W + 1000), ev("b2", 2 * W + 2000)], [c1, c2])
        self.assertEqual(carry_matched_event_ids(out, 1), {"x", "b1"})
        self.assertEqual(carry_matched_event_ids(out, 2), {"y", "b2"})
        self.assertEqual(carry_matched_event_ids(None, 1), set())

    def test_recompute_with_already_matched_carry_keeps_the_pair(self):
        # The target re-renders for an unrelated reason (a late native event):
        # a CLOSED carry's already-matched item is re-injected, so the carried
        # event keeps pairing SOME leftover and stays visibly a carry pair.
        # Native pool pairs first (a1-b1, a9-b2); the carry then fills the
        # remaining leftover b9 — the carry never drops out of the result.
        c = carry(1, "a", [ev("ca1", -1000)], source=0)
        out = compute_payload_with_carries(
            "k", W, 2 * W,
            [ev("a1", W + 1000), ev("a9", W + 3000)],
            [ev("b1", W + 1000), ev("b2", W + 2000), ev("b9", W + 3200)],
            [c])
        tagged = [(p["a_event_id"], p["b_event_id"]) for p in out["pairs"]
                  if p["carry"] is not None]
        self.assertEqual(tagged, [("ca1", "b9")])
        self.assertEqual(out["unmatched_b"], [])
        self.assertEqual(carry_matched_event_ids(out, 1), {"ca1", "b9"})


class TestCarryItemFate(unittest.TestCase):
    def test_alive_while_still_a_leftover(self):
        p = compute_payload("k", 0, W, [ev("a1", 1000)], [])
        self.assertIsNone(carry_item_fate("a1", "a", p, {"a1"}))

    def test_retracted_at_source_kills(self):
        p = compute_payload("k", 0, W, [], [])
        self.assertEqual(carry_item_fate("a1", "a", p, set()),
                         "source_retracted")

    def test_paired_at_source_by_late_event_kills(self):
        # a1 is now natively paired at the source (late b arrived) — routing it
        # elsewhere would double-count.
        p = compute_payload("k", 0, W,
                            [ev("a1", 1000)], [ev("b1", 2000)])
        self.assertEqual(carry_item_fate("a1", "a", p, {"a1"}),
                         "source_paired")


class TestOrderCarryBlocking(unittest.TestCase):
    def test_open_carry_blocks_close(self):
        bindings = [{
            "window_start": 0, "window_end": W, "result_version": 1,
            "result_status": "CURRENT", "has_gap": False,
            "match_count": 1, "unmatched_a": 0, "unmatched_b": 0,
            "payload_hash": "h",
        }]
        closed = evaluate_order(bindings, [], [], False, "WINDOW_JOINED",
                                open_carry_count=0)
        waiting = evaluate_order(bindings, [], [], False, "WINDOW_JOINED",
                                 open_carry_count=1)
        self.assertEqual(closed, "CLOSED")
        self.assertEqual(waiting, "WAITING")
        # and it still blocks after the order was closed before
        reop = evaluate_order(bindings, [], [], True, "WINDOW_JOINED",
                              open_carry_count=1)
        self.assertEqual(reop, "REOPENED")

    def test_carry_resolved_is_genuine_progress(self):
        bindings = [{
            "window_start": 0, "window_end": W, "result_version": 2,
            "result_status": "CURRENT", "has_gap": False,
            "match_count": 1, "unmatched_a": 0, "unmatched_b": 0,
            "payload_hash": "h2",
        }]
        # CARRY_RESOLVED is not in the forced-reopen reasons: a resolved carry
        # may take an ever-closed order back to CLOSED.
        self.assertEqual(
            evaluate_order(bindings, [], [], True, "CARRY_RESOLVED",
                           open_carry_count=0),
            "CLOSED")

    def test_helper(self):
        self.assertTrue(carry_open_block_order(1))
        self.assertFalse(carry_open_block_order(0))
        self.assertEqual(opposite_side("a"), "b")
        self.assertEqual(opposite_side("b"), "a")


if __name__ == "__main__":
    unittest.main(verbosity=2)
