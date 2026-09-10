"""Stdlib-only unit tests for the aligner core semantics. Run: python3 tests/test_core.py"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "aligner"))

from app.core import (compute_payload, decide, delivery_kind, effective,  # noqa: E402
                      normalize_backfill_range, payload_hash,
                      released_version_kind, releasable, retry_delay_ms,
                      should_deliver, side_evidence, window_of, window_ready)

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


class TestDeliveryKind(unittest.TestCase):
    def test_first_version_is_new(self):
        self.assertEqual(delivery_kind("INITIAL", "CURRENT"), "NEW")

    def test_late_event_and_partial_retraction_are_corrections(self):
        self.assertEqual(delivery_kind("LATE_EVENT", "CURRENT"), "CORRECTION")
        self.assertEqual(delivery_kind("RETRACTION", "CURRENT"), "CORRECTION")

    def test_emptied_result_is_withdrawal(self):
        self.assertEqual(delivery_kind("RETRACTION", "RETRACTED"), "WITHDRAWAL")
        self.assertEqual(delivery_kind("LATE_EVENT", "RETRACTED"), "WITHDRAWAL")

    def test_revival_after_withdrawal_is_correction_not_new(self):
        # a result coming back after being withdrawn must amend, not double-book
        self.assertEqual(delivery_kind("LATE_EVENT", "CURRENT"), "CORRECTION")


class TestReleasedVersionKind(unittest.TestCase):
    def test_first_released_version_is_always_new(self):
        self.assertEqual(released_version_kind(3, 3, "LATE_EVENT", "CURRENT"), "NEW")
        self.assertEqual(released_version_kind(5, 5, "RETRACTION", "CURRENT"), "NEW")

    def test_replay_caller_excludes_versions_before_first_release(self):
        # released_version_kind only receives versions >= first_release; v1/v2
        # are filtered by the replay candidate query because they never crossed.
        first_release = 3
        replayed_versions = [v for v in (1, 2, 3, 4) if v >= first_release]
        self.assertEqual(replayed_versions, [3, 4])
        self.assertEqual([released_version_kind(v, first_release,
                                                 "LATE_EVENT", "CURRENT")
                          for v in replayed_versions], ["NEW", "CORRECTION"])

    def test_later_released_versions_keep_original_kind(self):
        self.assertEqual(released_version_kind(4, 3, "LATE_EVENT", "CURRENT"), "CORRECTION")
        self.assertEqual(released_version_kind(5, 3, "RETRACTION", "RETRACTED"), "WITHDRAWAL")


class TestSideEvidence(unittest.TestCase):
    """Per-(key, stream, window) crossing evidence for the emission gate."""
    END = 60_000

    def mark(self, watermark=None, source=None, key_max=None, has_data=False):
        return {"watermark": watermark, "source": source,
                "key_max_event_time": key_max,
                "key_has_data_in_window": has_data}

    def test_own_progress_crosses_without_any_watermark(self):
        # the key itself has an event beyond the window end: it has moved on
        self.assertEqual(side_evidence(self.mark(key_max=self.END), self.END),
                         "own_progress")
        self.assertEqual(side_evidence(self.mark(key_max=self.END + 1), self.END),
                         "own_progress")

    def test_own_progress_before_window_end_does_not_cross(self):
        self.assertIsNone(side_evidence(self.mark(key_max=self.END - 1), self.END))

    def test_event_time_watermark_crosses(self):
        self.assertEqual(side_evidence(self.mark(self.END, "event_time"), self.END),
                         "watermark")

    def test_override_watermark_crosses(self):
        # the operator override is an explicit promise — also how windows are
        # forced closed when a stream is gone for good
        self.assertEqual(side_evidence(self.mark(self.END, "override"), self.END),
                         "watermark")

    def test_idle_watermark_crosses_a_side_that_has_data(self):
        # both-sides-arrived businesses must not be stuck behind a silent
        # stream: idleness finalizes a side that already has data
        self.assertEqual(
            side_evidence(self.mark(self.END, "idle_timeout", has_data=True),
                          self.END),
            "idle_finalized")

    def test_idle_watermark_never_crosses_an_empty_side(self):
        # a side the business is still waiting for is NOT crossed by idle
        # wall-clock advance, no matter how far it goes
        self.assertIsNone(side_evidence(self.mark(100 * self.END, "idle_timeout"),
                                        self.END))

    def test_idle_with_data_still_needs_the_watermark_to_reach_the_end(self):
        self.assertIsNone(side_evidence(
            self.mark(self.END - 1, "idle_timeout", has_data=True), self.END))

    def test_no_data_does_not_cross(self):
        self.assertIsNone(side_evidence(self.mark(None, "no_data"), self.END))
        self.assertIsNone(side_evidence(self.mark(), self.END))

    def test_short_watermark_does_not_cross(self):
        self.assertIsNone(side_evidence(self.mark(self.END - 1, "event_time"),
                                        self.END))

    def test_own_progress_wins_over_watermark_as_evidence(self):
        m = self.mark(self.END, "event_time", key_max=self.END)
        self.assertEqual(side_evidence(m, self.END), "own_progress")


class TestWindowReady(unittest.TestCase):
    END = 60_000

    def mark(self, watermark=None, source=None, key_max=None, has_data=False):
        return {"watermark": watermark, "source": source,
                "key_max_event_time": key_max,
                "key_has_data_in_window": has_data}

    def test_ready_only_when_both_sides_cross(self):
        a = self.mark(key_max=self.END)
        b = self.mark(key_max=self.END)
        self.assertTrue(window_ready(a, b, self.END))
        self.assertFalse(window_ready(a, self.mark(), self.END))
        self.assertFalse(window_ready(self.mark(), b, self.END))
        self.assertFalse(window_ready(self.mark(), self.mark(), self.END))

    def test_quiet_business_waits_without_blocking_evidence(self):
        # one side has never seen this key and its watermark is idle: wait
        a = self.mark(self.END, "event_time")
        b = self.mark(100 * self.END, "idle_timeout")
        self.assertFalse(window_ready(a, b, self.END))

    def test_each_side_may_cross_by_different_evidence(self):
        a = self.mark(key_max=self.END)                    # own progress
        b = self.mark(self.END, "event_time")              # stream promise
        self.assertTrue(window_ready(a, b, self.END))

    def test_both_sides_with_data_close_once_streams_go_idle(self):
        a = self.mark(self.END, "idle_timeout", has_data=True)
        b = self.mark(self.END, "idle_timeout", has_data=True)
        self.assertTrue(window_ready(a, b, self.END))

    def test_one_sided_business_keeps_waiting_when_streams_go_idle(self):
        a = self.mark(self.END, "idle_timeout", has_data=True)
        b = self.mark(self.END, "idle_timeout")            # no data on this side
        self.assertFalse(window_ready(a, b, self.END))


class TestShouldDeliver(unittest.TestCase):
    """The per-key external release gate sitting between a committed internal
    version and the delivery outbox."""

    def test_held_while_gate_closed_and_never_released(self):
        # computed and queryable, but not given externally
        self.assertFalse(should_deliver(False, False, "CURRENT"))

    def test_flows_when_gate_open_and_never_released(self):
        self.assertTrue(should_deliver(True, False, "CURRENT"))

    def test_retracted_while_held_sends_nothing(self):
        # the outside world never knew it: nothing to withdraw
        self.assertFalse(should_deliver(True, False, "RETRACTED"))
        self.assertFalse(should_deliver(False, False, "RETRACTED"))

    def test_released_window_keeps_flowing_after_gate_closes(self):
        # corrections and withdrawals are never swallowed by a closed gate
        self.assertTrue(should_deliver(False, True, "CURRENT"))
        self.assertTrue(should_deliver(False, True, "RETRACTED"))
        self.assertTrue(should_deliver(True, True, "CURRENT"))
        self.assertTrue(should_deliver(True, True, "RETRACTED"))


class TestReleasable(unittest.TestCase):
    def test_live_held_head_is_releasable(self):
        self.assertTrue(releasable("CURRENT", False))

    def test_already_released_is_not_releasable_again(self):
        # what went out can never be taken back or re-sent as new
        self.assertFalse(releasable("CURRENT", True))
        self.assertFalse(releasable("RETRACTED", True))

    def test_retracted_held_head_is_not_releasable(self):
        # fully withdrawn before ever going out: a later revival releases as NEW
        self.assertFalse(releasable("RETRACTED", False))


class TestRetryDelay(unittest.TestCase):
    def test_exponential_backoff(self):
        self.assertEqual(retry_delay_ms(1, 1000, 60000), 1000)
        self.assertEqual(retry_delay_ms(2, 1000, 60000), 2000)
        self.assertEqual(retry_delay_ms(3, 1000, 60000), 4000)

    def test_capped_at_max(self):
        self.assertEqual(retry_delay_ms(100, 1000, 60000), 60000)
        self.assertEqual(retry_delay_ms(1, 5000, 3000), 3000)


class TestNormalizeBackfillRange(unittest.TestCase):
    """Bounds speak event time; the window containing a bound must be picked.

    A time landing inside a window — not at its start — must still replay the
    whole window (补一段历史: 时间落在那一窗里, 那一窗就要能补上).
    """

    def test_exact_window_starts_are_unchanged(self):
        # callers already passing aligned boundaries keep the same range
        self.assertEqual(normalize_backfill_range(0, W, W), (0, W))
        self.assertEqual(normalize_backfill_range(W, 3 * W, W), (W, 3 * W))

    def test_lower_bound_inside_window_floors_to_window_start(self):
        # from = ws + δ: the window containing it is included
        self.assertEqual(normalize_backfill_range(1000, W, W), (0, W))
        self.assertEqual(normalize_backfill_range(W + 1, 2 * W, W), (W, 2 * W))

    def test_upper_bound_inside_window_includes_that_window(self):
        # exclusive to = ws + δ still reaches into the window starting at ws
        self.assertEqual(normalize_backfill_range(0, 1, W), (0, W))
        self.assertEqual(normalize_backfill_range(0, W + 1, W), (0, 2 * W))

    def test_range_inside_one_single_window_covers_whole_window(self):
        # both bounds mid-window: the whole window is backfilled, never dropped
        self.assertEqual(normalize_backfill_range(1000, 2000, W), (0, W))
        self.assertEqual(normalize_backfill_range(W + 100, W + 200, W),
                         (W, 2 * W))

    def test_range_across_several_windows(self):
        self.assertEqual(normalize_backfill_range(W + 100, 4 * W + 100, W),
                         (W, 5 * W))

    def test_window_start_of_normalized_range_aligns(self):
        from_ws, to_ws = normalize_backfill_range(123_456, 234_567, W)
        self.assertEqual(from_ws % W, 0)
        self.assertEqual(to_ws % W, 0)
        self.assertLessEqual(from_ws, 123_456)
        self.assertGreaterEqual(to_ws, 234_567)
        self.assertLess(from_ws, to_ws)


if __name__ == "__main__":
    unittest.main(verbosity=2)
