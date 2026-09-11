"""Stdlib-only unit tests for reconciliation settlement (对账落账) semantics.

Run: python3 tests/test_settlement_core.py
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "aligner"))

from app.core import (settlement_blocks_report, settlement_effect,  # noqa: E402
                      settlement_fulfils, settlement_pin_version)


class TestSettlementEffect(unittest.TestCase):
    def test_lagging_confirmed_is_freeze(self):
        # 认它没跟上 -> 停在它当时报到的那一版
        self.assertEqual(settlement_effect("LAGGING", "CONFIRMED"), "FREEZE")

    def test_lagging_rejected_is_continue(self):
        # 不认它没跟上 -> 按我送到的接着送
        self.assertEqual(settlement_effect("LAGGING", "REJECTED"), "CONTINUE")

    def test_not_reported_confirmed_is_suppress(self):
        # 认它没入过 -> 这一版别再补
        self.assertEqual(settlement_effect("NOT_REPORTED", "CONFIRMED"), "SUPPRESS")

    def test_not_reported_rejected_is_redrive(self):
        # 不认它没入过 -> 这一版还得再给它
        self.assertEqual(settlement_effect("NOT_REPORTED", "REJECTED"), "REDRIVE")

    def test_ahead_confirmed_is_mark_delivered(self):
        # 认了"对上了我还在重试的版本" -> 结账时把在途版本确认为送达
        self.assertEqual(
            settlement_effect("AHEAD_UNCONFIRMED", "CONFIRMED"), "MARK_DELIVERED")

    def test_ahead_rejected_is_none(self):
        # 驳了 -> 照旧重试，没有额外效果
        self.assertEqual(settlement_effect("AHEAD_UNCONFIRMED", "REJECTED"), "NONE")

    def test_aligned_is_never_settled(self):
        # 对上的不用管：没有裁决也就没有落账
        with self.assertRaises(ValueError):
            settlement_effect("ALIGNED", "CONFIRMED")


class TestSettlementPin(unittest.TestCase):
    def test_freeze_pins_at_reported_version(self):
        self.assertEqual(
            settlement_pin_version("FREEZE", reported_version=2,
                                   lowest_sent_version=1), 2)

    def test_continue_pins_at_reported_version(self):
        self.assertEqual(
            settlement_pin_version("CONTINUE", reported_version=1,
                                   lowest_sent_version=1), 1)

    def test_suppress_pins_at_zero(self):
        # 没入过且认了：连第一版也不再给
        self.assertEqual(
            settlement_pin_version("SUPPRESS", reported_version=None,
                                   lowest_sent_version=1), 0)

    def test_redrive_pins_at_lowest_sent(self):
        # 没入过但不认：从最早送出去的那一版开始重给
        self.assertEqual(
            settlement_pin_version("REDRIVE", reported_version=None,
                                   lowest_sent_version=2), 2)

    def test_mark_delivered_pins_at_reported_version(self):
        self.assertEqual(
            settlement_pin_version("MARK_DELIVERED", reported_version=3,
                                   lowest_sent_version=1), 3)


class TestSettlementBlocksReport(unittest.TestCase):
    def test_freeze_accepts_only_the_pinned_version(self):
        # 钉在 v2：报 v2 是幂等重报，报 v3（以后报到也不算成功）被拒
        self.assertIsNone(
            settlement_blocks_report("FREEZE", "ACTIVE", 2, version=2))
        self.assertEqual(
            settlement_blocks_report("FREEZE", "ACTIVE", 2, version=3),
            "frozen_by_settlement")
        self.assertEqual(
            settlement_blocks_report("FREEZE", "ACTIVE", 2, version=1),
            "frozen_by_settlement")

    def test_suppress_rejects_every_report(self):
        self.assertEqual(
            settlement_blocks_report("SUPPRESS", "ACTIVE", 0, version=1),
            "suppressed_by_settlement")

    def test_redrive_blocks_reports_below_pin_only_while_active(self):
        self.assertEqual(
            settlement_blocks_report("REDRIVE", "ACTIVE", 2, version=1),
            "below_redrive_pin")
        self.assertIsNone(
            settlement_blocks_report("REDRIVE", "ACTIVE", 2, version=2))

    def test_fulfilled_settlement_never_blocks(self):
        self.assertIsNone(
            settlement_blocks_report("FREEZE", "FULFILLED", 2, version=5))
        self.assertIsNone(
            settlement_blocks_report("SUPPRESS", "FULFILLED", 0, version=1))

    def test_non_gating_effects_pass_through(self):
        for effect in ("CONTINUE", "MARK_DELIVERED", "NONE"):
            self.assertIsNone(
                settlement_blocks_report(effect, "ACTIVE", 1, version=4))


class TestSettlementFulfils(unittest.TestCase):
    def test_redrive_fulfilled_when_pin_reported(self):
        # 它报到重发的那一版（或更高）-> 落账完成，恢复正常
        self.assertFalse(
            settlement_fulfils("REDRIVE", "ACTIVE", pinned_version=2, version=1))
        self.assertTrue(
            settlement_fulfils("REDRIVE", "ACTIVE", pinned_version=2, version=2))
        self.assertTrue(
            settlement_fulfils("REDRIVE", "ACTIVE", pinned_version=2, version=3))

    def test_continue_fulfilled_once_reports_move_past_pin(self):
        # 重复报钉住的那一版不算争议消除；报到后面的版才算
        self.assertFalse(
            settlement_fulfils("CONTINUE", "ACTIVE", pinned_version=1, version=1))
        self.assertTrue(
            settlement_fulfils("CONTINUE", "ACTIVE", pinned_version=1, version=2))

    def test_freeze_and_suppress_never_fulfil(self):
        self.assertFalse(
            settlement_fulfils("FREEZE", "ACTIVE", pinned_version=2, version=2))
        self.assertFalse(
            settlement_fulfils("SUPPRESS", "ACTIVE", pinned_version=0, version=1))

    def test_already_fulfilled_does_not_fulfil_again(self):
        self.assertFalse(
            settlement_fulfils("REDRIVE", "FULFILLED", pinned_version=2, version=3))


if __name__ == "__main__":
    unittest.main(verbosity=2)
