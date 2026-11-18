"""Deterministic per-trade metrics from a preserved closure bundle. Every number is computed
from broker fills and decision-ledger quote rows; no model is involved. Prints JSON.

Limits that every consumer must keep: ledger quotes are sparse runner samples (one per cycle),
so path extremes are sampled and understate the true extremes; the ledger context has no symbol
field, so metrics are produced only when the bundle traded a single symbol; fees are not included.
"""
import argparse
import hashlib
import json
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

from news_veto import parse_utc
from paper_session_review import review

LEDGER = "strategy_state/decision-ledger.jsonl"
ORDERS = "after_stop_orders_session.json"
PRE_ENTRY_MINUTES = 30


def _verified(root, name):
    manifest = json.loads((root / "MANIFEST.json").read_bytes())["files"]
    raw = (root / name).read_bytes()
    if hashlib.sha256(raw).hexdigest() != manifest[name]["sha256"]:
        raise ValueError(f"Closure evidence hash mismatch: {name}")
    return raw


def _quotes(ledger):
    rows = []
    for row in ledger:
        context = row.get("context") or {}
        bid, ask = context.get("bid"), context.get("ask")
        if (isinstance(bid, (int, float)) and isinstance(ask, (int, float))
                and bid > 0 and ask >= bid and (stamp := parse_utc(row.get("tick_utc")))):
            rows.append((stamp, row["seq"], float(bid), float(ask)))
    return rows


def _bps(value, base):
    return round(value / base * 10000, 3)


def _one(trade, parents, quotes):
    out = {"lifecycle_id": trade["lifecycle_id"], "ledger_seq": trade["ledger_seq"],
           "symbol": trade["symbol"], "metrics": None, "unavailable": None}
    try:
        legs = [leg for leg in trade["legs"] if Decimal(leg["filled_qty"]) > 0]
        entry = Decimal(trade["entry_fill_price"])
        entry_at = parse_utc(trade["entry_fill_utc"])
        stop = Decimal(trade["stop_price"]) if trade["stop_price"] else None
        pnl = Decimal(trade["gross_paper_pnl_usd"]) if trade["gross_paper_pnl_usd"] else None
    except (InvalidOperation, TypeError, KeyError):
        out["unavailable"] = "missing_fill_evidence"
        return out
    if len(legs) != 1 or stop is None or pnl is None or entry_at is None:
        out["unavailable"] = "exit_or_stop_unresolved"
        return out
    exit_leg = legs[0]
    exit_at = parse_utc(exit_leg["filled_at"])
    exit_price = Decimal(exit_leg["filled_avg_price"])
    decision = next(((t, s, b, a) for t, s, b, a in quotes if s == trade["ledger_seq"]), None)
    if exit_at is None or decision is None:
        out["unavailable"] = "exit_time_or_decision_quote_missing"
        return out
    # Long-only: the stop sits below entry, exits sell at the bid.
    stop_distance = entry - stop
    if stop_distance <= 0:
        out["unavailable"] = "stop_not_below_entry (short or malformed; not computed)"
        return out
    target = next((Decimal(leg["limit_price"]) for leg in parents.get(trade["broker_parent_id"], [])
                   if leg.get("limit_price") is not None), None)
    decision_at, _, bid0, ask0 = decision
    hold = [q for q in quotes if entry_at <= q[0] <= exit_at]
    before = [q for q in quotes if decision_at - timedelta(minutes=PRE_ENTRY_MINUTES) <= q[0] <= decision_at]
    mids = [(b + a) / 2 for _, _, b, a in before]
    entry_f = float(entry)
    metrics = {
        "decision_utc": decision_at.isoformat(),
        "decision_bid": bid0, "decision_ask": ask0,
        "decision_spread_bps": _bps(ask0 - bid0, (ask0 + bid0) / 2),
        "entry_slippage_per_share_vs_decision_ask": str(entry - Decimal(str(ask0))),
        "entry_slippage_bps_vs_decision_ask": _bps(entry_f - ask0, ask0),
        "hold_seconds": round((exit_at - entry_at).total_seconds(), 1),
        "exit_price": str(exit_price),
        "exit_kind": "stop" if exit_leg.get("id") and exit_leg["id"] in {
            leg["id"] for leg in parents.get(trade["broker_parent_id"], []) if leg.get("stop_price")
        } else "take_profit",
        "gross_pnl_usd": str(pnl),
        "gross_pnl_bps_of_entry": _bps(float(pnl / Decimal(exit_leg["filled_qty"])), entry_f),
        "stop_distance_per_share": str(stop_distance),
        "stop_distance_bps": _bps(float(stop_distance), entry_f),
        "stop_distance_in_spreads": round(float(stop_distance) / max(ask0 - bid0, 0.005), 1),
        "target_distance_per_share": str(target - entry) if target is not None else None,
        "realized_r": str((exit_price - entry) / stop_distance),
        "pre_entry_window_minutes": PRE_ENTRY_MINUTES,
        "pre_entry_samples": len(before),
        "pre_entry_range_bps": _bps(max(mids) - min(mids), min(mids)) if len(mids) >= 2 else None,
        "pre_entry_drift_bps": _bps(mids[-1] - mids[0], mids[0]) if len(mids) >= 2 else None,
        "stop_distance_vs_pre_entry_range": round(
            float(stop_distance) / (max(mids) - min(mids)), 2) if len(mids) >= 2 and max(mids) > min(mids) else None,
        "hold_samples": len(hold),
        "sampled_path": "ledger quote rows only; extremes are lower bounds on the true extremes",
    }
    if hold:
        lows = min(b for _, _, b, _ in hold)
        highs = max(b for _, _, b, _ in hold)
        metrics["sampled_max_adverse_per_share"] = str(max(entry - Decimal(str(lows)), Decimal(0)))
        metrics["sampled_max_favorable_per_share"] = str(max(Decimal(str(highs)) - entry, Decimal(0)))
        metrics["sampled_adverse_fraction_of_stop"] = round(
            float(max(entry - Decimal(str(lows)), Decimal(0)) / stop_distance), 3)
        if target is not None and target > entry:
            metrics["sampled_favorable_fraction_of_target"] = round(
                float(max(Decimal(str(highs)) - entry, Decimal(0)) / (target - entry)), 3)
    if exit_leg.get("id") and metrics["exit_kind"] == "stop":
        metrics["stop_fill_slippage_per_share"] = str(stop - exit_price)
    out["metrics"] = metrics
    return out


def trade_metrics(root):
    report = review(root)
    ledger = [json.loads(line) for line in _verified(root, LEDGER).splitlines() if line.strip()]
    orders = json.loads(_verified(root, ORDERS))
    parents = {order["id"]: order.get("legs") or [] for order in orders}
    symbols = {trade["symbol"] for trade in report["trades"]}
    quotes = _quotes(ledger)
    if len(symbols) > 1:
        return {"schema": "trade-metrics-v1", "closure_manifest_sha256": report["closure_manifest_sha256"],
                "trades": [{"lifecycle_id": t["lifecycle_id"], "symbol": t["symbol"], "metrics": None,
                            "unavailable": "multi_symbol_bundle: ledger context has no symbol"}
                           for t in report["trades"]]}
    return {"schema": "trade-metrics-v1", "closure_manifest_sha256": report["closure_manifest_sha256"],
            "trades": [_one(trade, parents, quotes) for trade in report["trades"]]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("closure_bundle", type=Path)
    print(json.dumps(trade_metrics(parser.parse_args().closure_bundle), indent=2))
