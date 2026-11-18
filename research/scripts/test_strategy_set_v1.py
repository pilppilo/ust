"""Fixture tests for frozen paper signals. No broker or orders."""
import unittest
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from decision_record import DecisionLedger
from strategy_set_v1 import candidates, choose

START = datetime(2026, 11, 17, 14, 30, tzinfo=timezone.utc)


def bar(index, *, opening=100.0, high=100.1, low=99.9, close=100.0, vwap=100.0):
    return {"time": (START + timedelta(minutes=index)).isoformat(), "open": opening,
            "high": high, "low": low, "close": close, "volume": 100,
            "vwap": vwap, "source": "iex"}


def run(bars, *, ask=100.05, prior=None, median=None):
    now = datetime.fromisoformat(bars[-1]["time"]) + timedelta(minutes=1, seconds=5)
    item = {"available": True, "quote": {"ask": str(ask)}, "bars": bars,
            "prior_close": prior, "orb_width_median": median}
    return candidates({"observed_at": now.isoformat(), "symbols": {"SPY": item}})


class StrategySetTest(unittest.TestCase):
    def test_orb_breakout_uses_complete_range_and_median(self):
        bars = [bar(i) for i in range(31)]
        bars.append(bar(31, high=100.5, close=100.4))
        result = run(bars, ask=100.45, median=.001)
        self.assertIn("orb", [candidate["strategy_id"] for candidate in result])
        self.assertNotIn("orb", [candidate["strategy_id"] for candidate in run(bars, median=.01)])

    def test_vwap_reversion_requires_iex_volume_and_fixed_target(self):
        bars = [bar(i) for i in range(32)]
        bars[-2] = bar(30, low=99.6, close=99.7)
        bars[-1] = bar(31, low=99.7, close=99.85)
        result = run(bars, ask=99.90)
        candidate = next(item for item in result if item["strategy_id"] == "vwap_reversion")
        self.assertEqual(candidate["proposal"]["take_profit_limit"], "100.00")
        bars[2]["source"] = "quote_mid"
        self.assertNotIn("vwap_reversion", [item["strategy_id"] for item in run(bars, ask=99.9)])

    def test_trend_pullback_uses_contiguous_twenty_minutes(self):
        bars = [bar(i, close=100 if i < 15 else 100.9, high=101.0) for i in range(20)]
        bars[-1] = bar(19, close=101.5, high=101.6, low=100.5)
        result = run(bars, ask=101.55)
        self.assertIn("trend_pullback", [item["strategy_id"] for item in result])
        del bars[3]
        self.assertNotIn("trend_pullback", [item["strategy_id"] for item in run(bars, ask=101.55)])

    def test_gap_continuation_requires_prior_close_and_breakout(self):
        bars = [bar(i, opening=100.4, high=100.6, low=100.3, close=100.5) for i in range(7)]
        bars[-1] = bar(6, opening=100.5, high=101.1, low=100.4, close=101.0)
        self.assertIn("gap_continuation", [item["strategy_id"] for item in run(bars, ask=101.05, prior=100)])
        self.assertNotIn("gap_continuation", [item["strategy_id"] for item in run(bars, ask=101.05)])

    def test_selector_balances_pairs_and_suppresses_an_attempted_trigger(self):
        available = [{"strategy_id": "orb", "symbol": "SPY", "trigger_time": "2026-11-17T15:01:00Z"},
                     {"strategy_id": "gap_continuation", "symbol": "QQQ", "trigger_time": "2026-11-17T15:01:00Z"}]
        with tempfile.TemporaryDirectory() as directory, patch("strategy_set_v1.candidates", return_value=available):
            path = Path(directory) / "decisions.jsonl"
            self.assertEqual(choose({}, {}, str(path))["selected"], available[0])
            ledger = DecisionLedger(path)
            ledger.append({"tick_utc": "2026-11-17T15:02:00Z", "judged": False,
                           "action": {"status": "submitted", "kind": "entry_order"},
                           "selected_candidate": available[0]})
            self.assertEqual(choose({}, {}, str(path))["selected"], available[1])
            self.assertIsNone(choose({}, {"lifecycles": [{"symbol": "QQQ", "state": "open"}]}, str(path))["selected"])


if __name__ == "__main__":
    unittest.main()
