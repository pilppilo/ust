import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from trade_metrics import trade_metrics


def quote(seq, stamp, bid, ask, kind="abstain", decision_id=None):
    return {"seq": seq, "tick_utc": f"2026-11-17T{stamp}+00:00",
            "context": {"bid": bid, "ask": ask},
            "action": {"kind": kind, "lifecycle_id": None},
            "candidates": [{"decision_id": decision_id}] if decision_id else []}


def bracket(symbol="SPY"):
    return {
        "id": "P", "client_order_id": "C", "order_class": "bracket", "side": "buy", "symbol": symbol,
        "status": "filled", "filled_qty": "10", "filled_avg_price": "100.01",
        "filled_at": "2026-11-17T14:00:05Z",
        "legs": [
            {"id": "T", "group_role": "take_profit", "status": "canceled", "filled_qty": "0",
             "filled_avg_price": None, "filled_at": None, "limit_price": "100.50",
             "side": "sell", "symbol": symbol},
            {"id": "S", "group_role": "stop_loss", "status": "filled", "filled_qty": "10",
             "filled_avg_price": "98.45", "filled_at": "2026-11-17T14:03:00Z",
             "stop_price": "98.50", "side": "sell", "symbol": symbol},
        ],
    }


def make_bundle(root, orders):
    ledger = [
        quote(0, "13:50:00", 99.90, 99.92),
        quote(1, "14:00:00", 100.00, 100.02, "entry_order", "ctx1"),
        quote(2, "14:01:00", 99.60, 99.62),
        quote(3, "14:02:00", 99.00, 99.02),
    ]
    journal = [
        {"kind": "writer_intent_created", "intent_id": "C",
         "ticket": {"normalized_inputs": {"context_id": "ctx1"}}},
        {"kind": "ulifecycle_entry_bound", "client_order_id": "C", "lifecycle_id": "L1"},
    ]
    receipt = {"account_pin_matched": True, "flat_and_no_open_orders": True,
               "position_count": 0, "open_order_count": 0, "session_orders_complete": True}
    files = {
        "summary.json": json.dumps({"session_date": "2026-11-17", "verdict": "CLOSED_FLAT",
                                    "read_before_stop": receipt, "read_after_stop": receipt}),
        "after_stop_orders_session.json": json.dumps(orders),
        "engine_runtime/engine_journal.jsonl": "\n".join(map(json.dumps, journal)),
        "strategy_state/decision-ledger.jsonl": "\n".join(map(json.dumps, ledger)),
    }
    entries = {}
    for name, raw in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw.encode())
        entries[name] = {"sha256": hashlib.sha256(raw.encode()).hexdigest()}
    (root / "MANIFEST.json").write_text(json.dumps({"files": entries}))


class TradeMetricsTest(unittest.TestCase):
    def test_stop_trade_metrics_match_hand_computation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            make_bundle(root, [bracket()])
            trade = trade_metrics(root)["trades"][0]
        self.assertIsNone(trade["unavailable"])
        m = trade["metrics"]
        self.assertEqual(m["exit_kind"], "stop")
        self.assertEqual(m["gross_pnl_usd"], "-15.60")
        self.assertEqual(m["stop_distance_per_share"], "1.51")
        self.assertEqual(m["target_distance_per_share"], "0.49")
        self.assertEqual(m["stop_fill_slippage_per_share"], "0.05")
        self.assertEqual(m["hold_seconds"], 175.0)
        self.assertAlmostEqual(float(m["realized_r"]), -1.56 / 1.51, places=6)
        # Entry filled at 100.01 against a 100.02 ask: price improvement of 0.01.
        self.assertEqual(m["entry_slippage_per_share_vs_decision_ask"], "-0.01")
        # Lowest sampled bid in the hold window is 99.00.
        self.assertEqual(m["sampled_max_adverse_per_share"], "1.01")
        self.assertAlmostEqual(m["sampled_adverse_fraction_of_stop"], 1.01 / 1.51, places=3)
        self.assertEqual(m["sampled_max_favorable_per_share"], "0")
        self.assertEqual(m["pre_entry_samples"], 2)

    def test_multi_symbol_bundle_is_not_computed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            other = bracket("QQQ") | {"id": "Q", "client_order_id": "Q"}
            make_bundle(root, [bracket(), other])
            trades = trade_metrics(root)["trades"]
        self.assertTrue(all(t["metrics"] is None and "multi_symbol" in t["unavailable"] for t in trades))

    def test_tampered_ledger_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            make_bundle(root, [bracket()])
            with (root / "strategy_state/decision-ledger.jsonl").open("ab") as handle:
                handle.write(b"\n{}")
            with self.assertRaisesRegex(ValueError, "hash mismatch"):
                trade_metrics(root)

    def test_missing_exit_fill_stays_unavailable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            order = bracket()
            order["legs"][1].update(status="canceled", filled_qty="0")
            make_bundle(root, [order])
            trade = trade_metrics(root)["trades"][0]
        self.assertIsNone(trade["metrics"])
        self.assertEqual(trade["unavailable"], "exit_or_stop_unresolved")


if __name__ == "__main__":
    unittest.main()
