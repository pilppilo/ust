"""Review a preserved paper closure bundle. Reads local evidence and prints JSON."""
import argparse
import hashlib
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path


def review(root):
    manifest_bytes = (root / "MANIFEST.json").read_bytes()
    manifest = json.loads(manifest_bytes)["files"]

    def read(name, *, lines=False, optional=False):
        if optional and name not in manifest:
            return []
        raw = (root / name).read_bytes()
        if hashlib.sha256(raw).hexdigest() != manifest[name]["sha256"]:
            raise ValueError(f"Closure evidence hash mismatch: {name}")
        return [json.loads(line) for line in raw.splitlines() if line.strip()] if lines else json.loads(raw)

    summary = read("summary.json")
    orders = read("after_stop_orders_session.json")
    if not isinstance(orders, list) or len(orders) >= 500:
        raise ValueError("Incomplete final broker order snapshot")

    journal = read("engine_runtime/engine_journal.jsonl", lines=True, optional=True)
    ledger = read("strategy_state/decision-ledger.jsonl", lines=True, optional=True)
    bindings = {}
    contexts = {}
    for row in journal:
        if row.get("kind") == "writer_intent_created":
            context = row.get("ticket", {}).get("normalized_inputs", {}).get("context_id")
            if context:
                contexts.setdefault(row["intent_id"], set()).add(context)
        if row.get("kind") == "ulifecycle_entry_bound":
            bindings.setdefault(row["client_order_id"], set()).add(row["lifecycle_id"])
    decisions = {}
    by_context = {}
    for row in ledger:
        lifecycle = row["action"].get("lifecycle_id")
        if row["action"].get("kind") == "entry_order":
            for candidate in row.get("candidates", []):
                by_context.setdefault(candidate.get("decision_id"), []).append(row)
        if lifecycle:
            decisions.setdefault(lifecycle, []).append(row)

    trades = []
    for parent in orders:
        if parent.get("order_class") != "bracket":
            continue
        ids = bindings.get(parent.get("client_order_id"), set())
        lifecycle = next(iter(ids)) if len(ids) == 1 else None
        matches = decisions.get(lifecycle, [])
        if not matches:
            context_ids = contexts.get(parent.get("client_order_id"), set())
            if len(context_ids) == 1:
                matches = by_context.get(next(iter(context_ids)), [])
        decision = matches[0] if len(matches) == 1 else {}
        legs = parent.get("legs") or []
        stops = [leg for leg in legs if leg.get("stop_price") is not None]
        trade = {
            "symbol": parent.get("symbol"),
            "broker_parent_id": parent.get("id"),
            "parent_status": parent.get("status"),
            "entry_fill_price": parent.get("filled_avg_price"),
            "entry_fill_utc": parent.get("filled_at"),
            "lifecycle_id": lifecycle,
            "ledger_seq": decision.get("seq"),
            "candidates": decision.get("candidates"),
            "thesis": decision.get("thesis"),
            "entry_filters": decision.get("filters"),
            "stop_price": stops[0]["stop_price"] if len(stops) == 1 else None,
            "legs": [{key: leg.get(key) for key in (
                "id", "group_role", "status", "filled_qty", "filled_avg_price", "filled_at"
            )} for leg in legs],
            "stop_assessment": "Requires decision-time thesis and market context",
            "gross_paper_pnl_usd": None,
        }
        try:
            qty = Decimal(parent["filled_qty"])
            exits = [leg for leg in legs if Decimal(leg["filled_qty"]) > 0]
            side = parent.get("side")
            if (parent.get("status") == "filled" and qty > 0 and len(exits) == 1
                    and side in ("buy", "sell")):
                leg = exits[0]
                opposite = "sell" if side == "buy" else "buy"
                if (leg.get("status") == "filled" and Decimal(leg["filled_qty"]) == qty
                        and leg.get("side") == opposite and leg.get("symbol") == parent.get("symbol")):
                    entry = Decimal(parent["filled_avg_price"])
                    exit_price = Decimal(leg["filled_avg_price"])
                    if all(value.is_finite() and value > 0 for value in (qty, entry, exit_price)):
                        direction = 1 if side == "buy" else -1
                        trade["gross_paper_pnl_usd"] = str(direction * qty * (exit_price - entry))
                        if len(stops) == 1:
                            stop = Decimal(stops[0]["stop_price"])
                            if stop.is_finite() and stop > 0:
                                trade["stop_distance_per_share"] = str(direction * (entry - stop))
                                if leg is stops[0]:
                                    trade["adverse_stop_fill_per_share"] = str(direction * (stop - exit_price))
        except (InvalidOperation, TypeError, KeyError):
            pass  # Missing fill evidence stays unresolved.
        trades.append(trade)

    return {
        "schema": "paper-session-review-v1",
        "session_date": summary["session_date"],
        "closure_verdict": summary["verdict"],
        "closure_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "scope": "Broker bracket orders and nested exits. P&L uses observed fills before fees.",
        "other_top_level_orders": sum(order.get("order_class") != "bracket" for order in orders),
        "news_evidence": "Entry filter records when present. Raw news is outside this bundle review.",
        "parameter_proposals": [],
        "trades": trades,
    }


def thesis_packet(root, through_seq):
    """Return only decision-time observations, with no order or outcome fields."""
    ledger_name = "strategy_state/decision-ledger.jsonl"
    manifest = json.loads((root / "MANIFEST.json").read_bytes())
    ledger = (root / ledger_name).read_bytes()
    digest = hashlib.sha256(ledger).hexdigest()
    if digest != manifest["files"][ledger_name]["sha256"]:
        raise ValueError("Closure evidence hash mismatch: decision ledger")

    rows = []
    found = False
    for line in ledger.splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if row["seq"] > through_seq:
            break
        context = row["context"]
        rows.append({
            "seq": row["seq"],
            "tick_utc": row["tick_utc"],
            "quote": {key: context.get(key) for key in (
                "bid", "ask", "mid", "spread_bps", "source", "age_seconds"
            )},
            "input_verdicts": row.get("input_verdicts"),
        })
        if row["seq"] == through_seq:
            found = True
            break
    if not found:
        raise ValueError(f"Decision ledger has no sequence {through_seq}")

    return {
        "schema": "paper-thesis-packet-v1",
        "through_seq": through_seq,
        "ledger_sha256": digest,
        "observations": rows,
        "evidence_limits": [
            "Sparse runner quote samples",
            "No raw news, event calendar, prior close or completed bars in this packet",
        ],
        "review_task": [
            "Cite observations by sequence and compare plausible explanations.",
            "Propose an entry thesis or stand down and state what evidence would change it.",
            "Propose a cost-aware test on an independent window for any alternative.",
        ],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("closure_bundle", type=Path)
    parser.add_argument("--through-seq", type=int, help="Emit a blind thesis packet through this ledger row")
    args = parser.parse_args()
    result = (review(args.closure_bundle) if args.through_seq is None
              else thesis_packet(args.closure_bundle, args.through_seq))
    print(json.dumps(result, indent=2))
