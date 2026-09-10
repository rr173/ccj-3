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


def ev_status(bindings, missing=None, pending=None, ever_closed=False,
              head="OPEN", reason="WINDOW_JOINED"):
    return evaluate_order(bindings, missing or [], pending or [],
                          ever_closed, head, reason)


class TestEvaluateOrder(unittest.TestCase):
    def test_no_live_windows_is_void(self):
        self.assertEqual(ev_status([binding("RETRACTED")], reason="WINDOW_WITHDRAWN"), "VOID")
        self.assertEqual(ev_status([], reason="WINDOW_WITHDRAWN"), "VOID")

    def test_void_wins_even_after_close(self):
        # whole business withdrawn after a close: VOID, never "still closed"
        self.assertEqual(ev_status([binding("RETRACTED")], ever_closed=True,
                                   head="CLOSED", reason="WINDOW_WITHDRAWN"), "VOID")

    def test_correction_on_closed_order_reopens_even_when_still_matched(self):
        # a benign post-close correction must visibly reopen, not stay CLOSED
        self.assertEqual(ev_status([binding()], ever_closed=True,
                                   head="CLOSED", reason="WINDOW_CORRECTED"), "REOPENED")

    def test_partial_withdrawal_on_closed_order_reopens(self):
        # one window withdrawn, another still live: must not still show CLOSED
        bs = [binding("RETRACTED"), binding(ws=30_000)]
        self.assertEqual(ev_status(bs, ever_closed=True,
                                   head="CLOSED", reason="WINDOW_WITHDRAWN"), "REOPENED")

    def test_reclose_when_later_trigger_finds_conditions_met(self):
        self.assertEqual(ev_status([binding()], ever_closed=True,
                                   head="REOPENED", reason="WINDOW_CORRECTED"), "CLOSED")

    def test_withdrawn_window_blocks_close(self):
        bs = [binding(), binding("RETRACTED", ws=30_000)]
        self.assertEqual(ev_status(bs, reason="WINDOW_WITHDRAWN"), "OPEN")

    def test_withdrawn_window_blocks_reclose(self):
        bs = [binding(), binding("RETRACTED", ws=30_000)]
        self.assertEqual(ev_status(bs, ever_closed=True, head="REOPENED",
                                   reason="WINDOW_CORRECTED"), "REOPENED")

    def test_join_on_closed_order_is_growth_not_reopen(self):
        # a new window joining a closed order re-evaluates and may re-close
        bs = [binding(), binding(ws=30_000)]
        self.assertEqual(ev_status(bs, ever_closed=True,
                                   head="CLOSED", reason="WINDOW_JOINED"), "CLOSED")

    def test_missing_window_keeps_order_open(self):
        self.assertEqual(ev_status([binding()], missing=[60_000]), "OPEN")

    def test_pending_window_keeps_order_open(self):
        self.assertEqual(ev_status([binding()], pending=[60_000]), "OPEN")

    def test_missing_beats_gap(self):
        # structural incompleteness dominates: OPEN, not WAITING
        self.assertEqual(ev_status([binding(has_gap=True)], missing=[60_000],
                                   reason="WINDOW_CORRECTED"), "OPEN")

    def test_one_sided_gap_is_waiting(self):
        self.assertEqual(ev_status([binding(has_gap=True)], reason="WINDOW_CORRECTED"),
                         "WAITING")

    def test_all_matched_closes(self):
        self.assertEqual(ev_status([binding(), binding(ws=30_000)]), "CLOSED")

    def test_gap_after_close_is_reopened_not_waiting(self):
        self.assertEqual(ev_status([binding(has_gap=True)], ever_closed=True,
                                   head="REOPENED", reason="WINDOW_CORRECTED"), "REOPENED")

    def test_missing_after_close_is_reopened_not_open(self):
        self.assertEqual(ev_status([binding()], missing=[60_000], ever_closed=True,
                                   head="REOPENED", reason="WINDOW_CORRECTED"), "REOPENED")


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
