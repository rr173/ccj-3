"""Stdlib-only unit tests for reconciliation-batch core semantics.

Run: python3 tests/test_recon_core.py
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "aligner"))

from app.core import (ranges_overlap, reconciliation_can_close,  # noqa: E402
                      reconciliation_item_status,
                      reconciliation_item_unresolved)


class TestReconciliationItemStatus(unittest.TestCase):
    def test_aligned(self):
        # reported exactly the confirmed delivered version
        self.assertEqual(reconciliation_item_status(
            sent_version=3, delivered_version=3,
            inflight_version=None, reported_version=3), "ALIGNED")

    def test_lagging(self):
        # we delivered up to v3, it still reports v1
        self.assertEqual(reconciliation_item_status(
            sent_version=3, delivered_version=3,
            inflight_version=None, reported_version=1), "LAGGING")

    def test_ahead_unconfirmed_against_retrying_version(self):
        # v2 is on the wire being retried; it posted v2 anyway
        self.assertEqual(reconciliation_item_status(
            sent_version=2, delivered_version=1,
            inflight_version=2, reported_version=2), "AHEAD_UNCONFIRMED")

    def test_ahead_unconfirmed_when_nothing_delivered(self):
        # it reports a version that exists but nothing is confirmed delivered
        self.assertEqual(reconciliation_item_status(
            sent_version=1, delivered_version=None,
            inflight_version=1, reported_version=1), "AHEAD_UNCONFIRMED")

    def test_not_reported_when_delivered(self):
        # we delivered v2, it never reported this result
        self.assertEqual(reconciliation_item_status(
            sent_version=2, delivered_version=2,
            inflight_version=None, reported_version=None), "NOT_REPORTED")

    def test_not_reported_when_only_pending(self):
        # delivery rows exist but none was even dispatched yet (all PENDING)
        self.assertEqual(reconciliation_item_status(
            sent_version=1, delivered_version=None,
            inflight_version=1, reported_version=None), "NOT_REPORTED")

    def test_withdrawal_version_counts_like_any_version(self):
        # a WITHDRAWAL delivered at v4 and reported as posted is just ALIGNED
        self.assertEqual(reconciliation_item_status(
            sent_version=4, delivered_version=4,
            inflight_version=None, reported_version=4), "ALIGNED")


class TestReconciliationItemUnresolved(unittest.TestCase):
    def test_aligned_never_needs_verdict(self):
        self.assertFalse(reconciliation_item_unresolved("ALIGNED", None))
        self.assertFalse(reconciliation_item_unresolved("ALIGNED", "CONFIRMED"))

    def test_discrepancies_need_a_verdict(self):
        for status in ("LAGGING", "AHEAD_UNCONFIRMED", "NOT_REPORTED"):
            self.assertTrue(reconciliation_item_unresolved(status, None))
            self.assertFalse(reconciliation_item_unresolved(status, "CONFIRMED"))
            self.assertFalse(reconciliation_item_unresolved(status, "REJECTED"))


class TestReconciliationCanClose(unittest.TestCase):
    def test_all_aligned_closes_without_verdicts(self):
        self.assertTrue(reconciliation_can_close(
            [("ALIGNED", None), ("ALIGNED", None)]))

    def test_one_undecided_keeps_batch_open(self):
        items = [("ALIGNED", None),
                 ("LAGGING", None),
                 ("NOT_REPORTED", "REJECTED"),
                 ("AHEAD_UNCONFIRMED", "CONFIRMED")]
        self.assertFalse(reconciliation_can_close(items))

    def test_every_discrepancy_verdicted_closes(self):
        items = [("ALIGNED", None),
                 ("LAGGING", "CONFIRMED"),
                 ("NOT_REPORTED", "REJECTED"),
                 ("AHEAD_UNCONFIRMED", "REJECTED")]
        self.assertTrue(reconciliation_can_close(items))

    def test_empty_batch_closes(self):
        self.assertTrue(reconciliation_can_close([]))


class TestRangesOverlap(unittest.TestCase):
    def test_overlapping(self):
        self.assertTrue(ranges_overlap(0, 10, 5, 15))
        self.assertTrue(ranges_overlap(5, 15, 0, 10))
        self.assertTrue(ranges_overlap(0, 10, 2, 8))

    def test_touching_is_not_overlap(self):
        # half-open: [0,10) and [10,20) never cover the same window
        self.assertFalse(ranges_overlap(0, 10, 10, 20))
        self.assertFalse(ranges_overlap(10, 20, 0, 10))

    def test_disjoint(self):
        self.assertFalse(ranges_overlap(0, 10, 20, 30))

    def test_same_range_overlaps(self):
        self.assertTrue(ranges_overlap(0, 10, 0, 10))


if __name__ == "__main__":
    unittest.main(verbosity=2)
