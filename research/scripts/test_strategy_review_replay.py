import hashlib
import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

from news_veto import parse_utc
from paper_research_service import nightly, opening_context, stage
from strategy_review_replay import news_context


class ReviewNewsTest(unittest.TestCase):
    def test_replay_uses_receipt_and_heartbeat_before_decision(self):
        rows = [
            {"type": "heartbeat", "at": "2026-11-16T14:39:30Z"},
            {"type": "heartbeat", "at": "2026-11-16T14:40:01Z"},
            {"type": "headline", "id": 1, "symbols": ["SPY"],
             "received_at": "2026-11-16T14:40:00Z"},
            {"type": "headline", "id": 2, "symbols": ["SPY"],
             "created_at": "2026-11-16T14:30:00Z", "received_at": "2026-11-16T14:40:01Z"},
            {"type": "headline", "id": 3, "symbols": ["SPY"],
             "created_at": "2026-11-16T14:30:00Z"},
            {"type": "headline", "id": 4, "symbols": ["QQQ"],
             "received_at": "2026-11-16T14:39:00Z"},
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "news.jsonl"
            path.write_text("\n".join(map(json.dumps, rows)), encoding="utf-8")
            result = news_context(path, parse_utc("2026-11-16T14:40:00Z"), "SPY")
        self.assertEqual(result["heartbeat_age_seconds"], 30)
        self.assertEqual([row["id"] for row in result["symbol_tagged_metadata"]], [1])
        self.assertEqual(result["other_metadata_count"], 1)


class NightlyReplayTest(unittest.TestCase):
    def test_saves_blind_assessment_before_submitting_outcome(self):
        packet = {
            "day": "2026-11-17", "columns": [], "limitations": "fixture",
            "closure_manifest_sha256": "fixture", "observations": [], "shadow": [],
            "action_counts": {}, "closing_news": [], "market_close_context": [],
            "broker_order_count": 1, "unresolved": [],
            "cases": [{"seq": 3, "observations": [[3, "14:45Z", 1.0, 1.01]], "outcome": {"fill": 123.0, "gross_paper_pnl_usd": None}}],
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "packet.json"
            source.write_text(json.dumps(packet), encoding="utf-8")
            output = root / "result.json"
            calls = []

            def infer(_, task, evidence):
                calls.append(evidence)
                if len(calls) == 1:
                    self.assertNotIn("outcome", evidence)
                    self.assertNotIn("fill", json.dumps(evidence))
                elif len(calls) == 2:
                    prior = json.loads((root / "result.seq-3.blind.json").read_bytes())
                    self.assertEqual(prior["analysis"], evidence["assessment"])
                    self.assertEqual(evidence["outcome"]["fill"], 123.0)
                return "Assessment citing seq 3" if len(calls) == 1 else f"Assessment {len(calls)}"

            with patch("paper_research_service.infer", side_effect=infer):
                nightly(Namespace(packet=source, output=output))
            result = json.loads(output.read_bytes())
            self.assertEqual(result["trades"][0]["outcome"], "Assessment 2")
            self.assertEqual(result["session"], "Assessment 3")


class StageTest(unittest.TestCase):
    def test_preserves_unclassified_orders_and_refuses_corrupt_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bundle = root / "e1_closure_2026-11-17_fixture"
            bundle.mkdir()
            receipt = {"account_pin_matched": True, "flat_and_no_open_orders": True,
                       "position_count": 0, "open_order_count": 0,
                       "session_orders_complete": True}
            files = {
                "summary.json": json.dumps({
                    "session_date": "2026-11-17", "verdict": "CLOSED_FLAT",
                    "read_before_stop": receipt, "read_after_stop": receipt,
                }),
                "after_stop_orders_session.json": json.dumps([
                    {"id": "engine-close", "order_class": "simple", "status": "filled"},
                    {"id": "unlinked", "order_class": "bracket", "status": "filled"},
                ]),
                "strategy_state/decision-ledger.jsonl": "",
            }
            entries = {}
            for name, raw in files.items():
                path = bundle / name
                path.parent.mkdir(exist_ok=True)
                path.write_bytes(raw.encode())
                entries[name] = {"sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            manifest = json.dumps({"files": entries}).encode()
            (bundle / "MANIFEST.json").write_bytes(manifest)
            (bundle / "MANIFEST_SHA256.txt").write_text(hashlib.sha256(manifest).hexdigest())
            news = root / "news.jsonl"
            news.touch()
            packet = root / "packet.json"
            args = Namespace(day="2026-11-17", closures=root, market=None, news=news,
                             packet=packet, shadow_state=None)
            stage(args)
            result = json.loads(packet.read_bytes())
            self.assertEqual(result["broker_order_count"], 2)
            self.assertEqual([row["reason"] for row in result["unresolved"]],
                             ["unclassified_top_level_order", "missing_or_ambiguous_decision"])
            (bundle / "after_stop_orders_session.json").write_text("[]")
            with self.assertRaisesRegex(ValueError, "evidence hash mismatch"):
                stage(args)


class OpeningContextTest(unittest.TestCase):
    def test_uses_fresh_observed_quotes_and_as_of_prior_close(self):
        def sample(observed, received, bid, ask):
            return {
                "observed_utc": observed, "fresh": True,
                "quotes": [{"symbol": "SPY", "last_quote": [[received, bid, ask]]}],
            }

        samples = [
            sample("2026-11-17T14:30:15Z", "2026-11-17T14:30:14Z", 100.9, 101.1),
            sample("2026-11-17T14:31:15Z", "2026-11-17T14:31:14Z", 101.9, 102.1),
            sample("2026-11-17T14:32:15Z", "2026-11-17T14:32:16Z", 900, 901),
        ]
        prior = {"feed": "iex", "session_date": "2026-11-17", "previous_session": "2026-11-16",
                 "generated_at": "2026-11-17T13:00:00Z", "symbols": {"SPY": {
                     "prior_close": 100.0, "prior_close_time": "2026-11-16T20:59:00Z",
                 }}}
        cutoff = parse_utc("2026-11-17T14:33:15Z")
        row = opening_context(samples, prior, cutoff)["SPY"]
        self.assertEqual(row["opening_proxy_gap_bps"], 100)
        self.assertEqual(row["sample_count"], 2)
        self.assertEqual((row["sampled_mid_low"], row["sampled_mid_high"]), (101, 102))
        self.assertAlmostEqual(row["spread_median_bps"], 19.705, places=3)
        prior["generated_at"] = "2026-11-17T14:34:00Z"
        self.assertIsNone(opening_context(samples, prior, cutoff)["SPY"]["opening_proxy_gap_bps"])


if __name__ == "__main__":
    unittest.main()
