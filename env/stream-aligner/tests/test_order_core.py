"""Stdlib-only unit tests for the business-order state machine.

Run: python3 tests/test_order_core.py
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "aligner"))

from app.core import (build_order_snapshot, evaluate_order,  # noqa: E402
                      order_reason)


def binding(status="CURRENT", has_gap=False, ws=0):
    return {"window_start": ws, "result_status": status, "has_gap": has_gap}


class TestEvaluateOrder(unittest.TestCase):
    def test_no_live_windows_is_void(self):
        self.assertEqual(evaluate_order([binding("RETRACTED")], [], [], False), "VOID")
        self.assertEqual(evaluate_order([], [], [], False), "VOID")

    def test_void_wins_even_after_close(self):
        # whole business withdrawn after a close: VOID, never "still closed"
        self.assertEqual(evaluate_order([binding("RETRACTED")], [], [], True), "VOID")

    def test_missing_window_keeps_order_open(self):
        self.assertEqual(evaluate_order([binding()], [60_000], [], False), "OPEN")

    def test_pending_window_keeps_order_open(self):
        self.assertEqual(evaluate_order([binding()], [], [60_000], False), "OPEN")

    def test_missing_beats_gap(self):
        # structural incompleteness dominates: OPEN, not WAITING
        self.assertEqual(evaluate_order([binding(has_gap=True)], [60_000], [], False), "OPEN")

    def test_one_sided_gap_is_waiting(self):
        self.assertEqual(evaluate_order([binding(has_gap=True)], [], [], False), "WAITING")

    def test_all_matched_closes(self):
        self.assertEqual(evaluate_order([binding(), binding(ws=30_000)], [], [], False), "CLOSED")

    def test_retracted_window_does_not_block_close(self):
        # a withdrawn window is resolved business, not a hole
        bs = [binding(), binding("RETRACTED", ws=30_000)]
        self.assertEqual(evaluate_order(bs, [], [], False), "CLOSED")

    def test_gap_after_close_is_reopened_not_waiting(self):
        self.assertEqual(evaluate_order([binding(has_gap=True)], [], [], True), "REOPENED")

    def test_missing_after_close_is_reopened_not_open(self):
        self.assertEqual(evaluate_order([binding()], [60_000], [], True), "REOPENED")

    def test_reclose_after_reopen(self):
        self.assertEqual(evaluate_order([binding()], [], [], True), "CLOSED")


class TestOrderReason(unittest.TestCase):
    def test_first_binding_joins(self):
        self.assertEqual(order_reason(None, "CURRENT"), "WINDOW_JOINED")

    def test_live_to_live_is_correction(self):
        self.assertEqual(order_reason("CURRENT", "CURRENT"), "WINDOW_CORRECTED")

    def test_live_to_retracted_is_withdrawal(self):
        self.assertEqual(order_reason("CURRENT", "RETRACTED"), "WINDOW_WITHDRAWN")

    def test_retracted_to_live_is_revival(self):
        self.assertEqual(order_reason("RETRACTED", "CURRENT"), "WINDOW_REVIVED")


class TestBuildOrderSnapshot(unittest.TestCase):
    def snap_binding(self, ws, version, status="CURRENT"):
        return {"window_start": ws, "window_end": ws + 30_000,
                "result_version": version, "result_status": status,
                "has_gap": False, "match_count": 1, "unmatched_a": 0,
                "unmatched_b": 0, "payload_hash": f"h{ws}"}

    def test_sorted_by_window_regardless_of_input_order(self):
        snap = build_order_snapshot([self.snap_binding(60_000, 3), self.snap_binding(0, 1)])
        self.assertEqual([s["window_start"] for s in snap], [0, 60_000])

    def test_records_each_windows_result_version(self):
        snap = build_order_snapshot([self.snap_binding(0, 2), self.snap_binding(30_000, 5)])
        self.assertEqual({s["window_start"]: s["result_version"] for s in snap},
                         {0: 2, 30_000: 5})

    def test_deterministic(self):
        bs = [self.snap_binding(30_000, 1), self.snap_binding(0, 4, "RETRACTED")]
        self.assertEqual(build_order_snapshot(bs), build_order_snapshot(list(reversed(bs))))


if __name__ == "__main__":
    unittest.main(verbosity=2)
