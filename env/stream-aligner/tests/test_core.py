"""Stdlib-only unit tests for the aligner core semantics. Run: python3 tests/test_core.py"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "aligner"))

from app.core import compute_payload, decide, effective, payload_hash, window_of  # noqa: E402

W = 30_000


def ev(eid, t, key="k", typ="upsert", retracts=None, payload=None):
    return {"event_id": eid, "event_time": t, "key": key, "type": typ,
            "retracts": retracts, "payload": payload}


class TestWindowing(unittest.TestCase):
    def test_window_boundaries(self):
        self.assertEqual(window_of(0, W), (0, W))
        self.assertEqual(window_of(W - 1, W), (0, W))
        self.assertEqual(window_of(W, W), (W, 2 * W))
        self.assertEqual(window_of(123_456, W), (120_000, 150_000))


class TestEffective(unittest.TestCase):
    def test_retraction_removes_target(self):
        events = [ev("a1", 1), ev("a2", 2), ev("r1", 3, typ="retract", retracts="a1")]
        self.assertEqual([e["event_id"] for e in effective(events)], ["a2"])

    def test_retraction_applies_regardless_of_retract_timestamp(self):
        # retract event sits in a *later* window; target must still be filtered
        events = [ev("a1", 1), ev("r1", 10 * W, typ="retract", retracts="a1")]
        self.assertEqual(effective(events), [])

    def test_retract_of_unknown_event_is_harmless(self):
        events = [ev("a1", 1), ev("r1", 2, typ="retract", retracts="nope")]
        self.assertEqual([e["event_id"] for e in effective(events)], ["a1"])


class TestComputePayload(unittest.TestCase):
    def test_pairs_in_event_time_order(self):
        a = [ev("a2", 2000), ev("a1", 1000)]
        b = [ev("b1", 1500), ev("b2", 2500)]
        p = compute_payload("k", 0, W, a, b)
        self.assertEqual(p["match_count"], 2)
        self.assertEqual([(x["a_event_id"], x["b_event_id"]) for x in p["pairs"]],
                         [("a1", "b1"), ("a2", "b2")])
        self.assertEqual(p["unmatched_a"], [])
        self.assertEqual(p["unmatched_b"], [])

    def test_unmatched_side_reported(self):
        p = compute_payload("k", 0, W, [ev("a1", 1)], [ev("b1", 1), ev("b2", 2)])
        self.assertEqual(p["match_count"], 1)
        self.assertEqual(p["unmatched_b"], ["b2"])

    def test_one_sided_window_still_produces_result(self):
        p = compute_payload("k", 0, W, [], [ev("b1", 1)])
        self.assertEqual(p["match_count"], 0)
        self.assertEqual(p["unmatched_b"], ["b1"])

    def test_empty_both_sides_is_none(self):
        self.assertIsNone(compute_payload("k", 0, W, [], []))

    def test_deterministic_regardless_of_input_order(self):
        a = [ev("a1", 1), ev("a2", 2)]
        b = [ev("b1", 1), ev("b2", 2)]
        p1 = compute_payload("k", 0, W, a, b)
        p2 = compute_payload("k", 0, W, list(reversed(a)), list(reversed(b)))
        self.assertEqual(payload_hash(p1), payload_hash(p2))


class TestDecide(unittest.TestCase):
    def test_initial_emit(self):
        d = decide(None, compute_payload("k", 0, W, [ev("a1", 1)], [ev("b1", 1)]))
        self.assertEqual(d["version"], 1)
        self.assertEqual(d["status"], "CURRENT")

    def test_initial_empty_is_noop(self):
        self.assertIsNone(decide(None, None))

    def test_same_payload_is_noop(self):
        p = compute_payload("k", 0, W, [ev("a1", 1)], [ev("b1", 1)])
        head = {"version": 1, "status": "CURRENT", "payload_hash": payload_hash(p)}
        self.assertIsNone(decide(head, p))

    def test_change_bumps_version(self):
        p1 = compute_payload("k", 0, W, [ev("a1", 1)], [ev("b1", 1)])
        p2 = compute_payload("k", 0, W, [ev("a1", 1)], [ev("b1", 1), ev("b2", 2)])
        head = {"version": 1, "status": "CURRENT", "payload_hash": payload_hash(p1)}
        d = decide(head, p2)
        self.assertEqual((d["version"], d["status"]), (2, "CURRENT"))

    def test_emptying_retracts_result(self):
        p1 = compute_payload("k", 0, W, [ev("a1", 1)], [ev("b1", 1)])
        head = {"version": 1, "status": "CURRENT", "payload_hash": payload_hash(p1)}
        d = decide(head, None)
        self.assertEqual((d["version"], d["status"]), (2, "RETRACTED"))

    def test_retracted_stays_retracted_on_repeat(self):
        head = {"version": 2, "status": "RETRACTED", "payload_hash": None}
        self.assertIsNone(decide(head, None))

    def test_revival_after_retraction(self):
        head = {"version": 2, "status": "RETRACTED", "payload_hash": None}
        p = compute_payload("k", 0, W, [ev("a9", 1)], [])
        d = decide(head, p)
        self.assertEqual((d["version"], d["status"]), (3, "CURRENT"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
