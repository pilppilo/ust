"""Prepare and review paper evidence, or record a shadow thesis. No order interfaces."""
import argparse
import hashlib
import json
import math
import re
import subprocess
import time
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from statistics import median

from news_veto import parse_utc
from paper_session_review import review, thesis_packet
from strategy_review_replay import news_context
from trade_metrics import trade_metrics


def save(path, value):
    with path.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2)
        handle.write("\n")


def verified_ledger(bundle):
    manifest = (bundle / "MANIFEST.json").read_bytes()
    digest = hashlib.sha256(manifest).hexdigest()
    if digest != (bundle / "MANIFEST_SHA256.txt").read_text().strip():
        raise ValueError("Closure manifest digest mismatch")
    name = "strategy_state/decision-ledger.jsonl"
    raw = (bundle / name).read_bytes()
    if hashlib.sha256(raw).hexdigest() != json.loads(manifest)["files"][name]["sha256"]:
        raise ValueError("Decision ledger digest mismatch")
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def compact(packet):
    return [[row["seq"], round(parse_utc(row["tick_utc"]).timestamp() % 86400, 1),
             row["quote"]["bid"], row["quote"]["ask"]]
            for row in packet["observations"]]


def quote_transitions(observations):
    facts = []
    previous = None
    for seq, _, bid, ask in observations:
        if not isinstance(bid, (int, float)) or not isinstance(ask, (int, float)) or bid <= 0 or ask < bid:
            previous = None
            continue
        if previous:
            prior_seq, prior_bid, prior_ask = previous
            facts.append({
                "from_seq": prior_seq, "to_seq": seq,
                "bid_delta": round(bid - prior_bid, 4),
                "ask_delta": round(ask - prior_ask, 4),
                "spread_bps": round((ask - bid) / ((ask + bid) / 2) * 10000, 3),
                "nonoverlap": "up" if bid > prior_ask else "down" if ask < prior_bid else None,
            })
        previous = seq, bid, ask
    return facts


def session_windows(observations):
    windows = {}
    for seq, utc_seconds, bid, ask in observations:
        if not isinstance(bid, (int, float)) or not isinstance(ask, (int, float)) or bid <= 0 or ask < bid:
            continue
        start = int(utc_seconds // 1800) * 1800
        mid = (bid + ask) / 2
        spread = (ask - bid) / mid * 10000
        bucket = windows.setdefault(start, [start, seq, seq, 0, mid, mid, mid, mid, 0])
        bucket[2] = seq
        bucket[3] += 1
        bucket[5] = min(bucket[5], mid)
        bucket[6] = max(bucket[6], mid)
        bucket[7] = mid
        bucket[8] = max(bucket[8], spread)
    return [[round(value, 4) for value in bucket] for bucket in windows.values()]


def stage(args):
    day = args.day or datetime.now(timezone.utc).date().isoformat()
    bundles = sorted(args.closures.glob(f"e1_closure_{day}_*"))
    if len(bundles) != 1:
        raise ValueError(f"Expected one closure for {day}, found {len(bundles)}")
    bundle = bundles[0]
    ledger = verified_ledger(bundle)
    report = review(bundle)
    summary = json.loads((bundle / "summary.json").read_bytes())
    for phase in ("read_before_stop", "read_after_stop"):
        receipt = summary[phase]
        if not (receipt["account_pin_matched"] and receipt["flat_and_no_open_orders"]
                and receipt["position_count"] == 0 and receipt["open_order_count"] == 0):
            raise ValueError(f"Broker receipt did not verify flat: {phase}")
    if not summary["read_after_stop"]["session_orders_complete"]:
        raise ValueError("Final broker session orders are incomplete")
    if report["closure_verdict"] != "CLOSED_FLAT" or report["session_date"] != day:
        raise ValueError("Closure did not establish a flat session")
    orders = json.loads((bundle / "after_stop_orders_session.json").read_bytes())
    seq_counts = Counter(trade["ledger_seq"] for trade in report["trades"])
    unresolved = [
        {"reason": "unclassified_top_level_order",
         **{key: order.get(key) for key in ("id", "symbol", "side", "status", "filled_qty", "filled_avg_price")}}
        for order in orders if order.get("order_class") != "bracket"
    ]
    if not orders and any(row["action"]["kind"] == "entry_order" for row in ledger):
        unresolved.append({"reason": "ledger_entry_without_broker_order"})
    cases = []
    for trade in report["trades"]:
        seq = trade["ledger_seq"]
        if seq is None or seq_counts[seq] != 1:
            unresolved.append({"reason": "missing_or_ambiguous_decision", "trade": trade})
            continue
        if trade["gross_paper_pnl_usd"] is None:
            unresolved.append({"reason": "bracket_fill_unresolved", "trade": trade})
        packet = thesis_packet(bundle, seq)
        cutoff = parse_utc(packet["observations"][-1]["tick_utc"])
        cases.append({
            "seq": seq, "symbol": trade["symbol"],
            "columns": ["sequence", "UTC timestamp", "bid", "ask"],
            "observations": [[row["seq"], row["tick_utc"], row["quote"]["bid"], row["quote"]["ask"]]
                             for row in packet["observations"]],
            "news": news_context(args.news, cutoff, trade["symbol"]),
            "outcome": {k: trade[k] for k in (
                "parent_status", "entry_fill_price", "entry_fill_utc", "stop_price",
                "legs", "gross_paper_pnl_usd"
            )} | {key: trade.get(key) for key in (
                "stop_distance_per_share", "adverse_stop_fill_per_share"
            )},
        })
    session = thesis_packet(bundle, ledger[-1]["seq"]) if ledger else {"observations": []}
    close_time = parse_utc(f"{day}T21:00:00Z")
    shadows = []
    if args.shadow_state:
        for path in sorted(args.shadow_state.glob(f"{day}-*.json")):
            record = json.loads(path.read_bytes())
            stamp = parse_utc(record["evidence"]["observed_utc"])
            if stamp and parse_utc(f"{day}T14:30:00Z") <= stamp <= close_time:
                shadows.append(record)
    try:
        metrics = trade_metrics(bundle)
    except Exception as exc:  # Supplementary facts must not block staging; the failure is recorded.
        metrics = {"error": f"{type(exc).__name__}: {exc}"}
    payload = {
        "trade_metrics": metrics,
        "day": day, "closure_manifest_sha256": report["closure_manifest_sha256"],
        "columns": ["seq", "UTC seconds since midnight", "bid", "ask"],
        "limitations": "Sparse runner observations. No headline text or consolidated bars.",
        "observations": compact(session), "cases": cases, "shadow": shadows[-1:],
        "broker_order_count": len(orders), "unresolved": unresolved,
        "action_counts": dict(Counter(row["action"]["status"] for row in ledger)),
        "market_close_context": market_context(args.market, parse_utc(f"{day}T21:00:05Z"))
        if args.market else [],
        "closing_news": [news_context(args.news, close_time, symbol) for symbol in ("SPY", "QQQ")],
    }
    save(args.packet, payload)


def infer(args, task, evidence, max_tokens=220):
    prompt = ("<|im_start|>system\nYou are a paper strategy researcher. Use only the supplied data. "
              "Cite sequence numbers or times, address competing explanations and data gaps, "
              "and give an invalidation condition. Proposals are research only. "
              "Use executable spreads, fees and slippage in any test design. "
              "Use at most 120 words. Do not call tools or issue orders.<|im_end|>\n<|im_start|>user\n"
              + task + "\n" + json.dumps(evidence, separators=(",", ":"))
              + "<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n")
    result = subprocess.run(
        [str(args.llama), "-m", str(args.model), "-t", "2", "-tb", "2", "-c", "12288", "-b", "256", "-ub", "64",
         "-n", str(max_tokens), "--temp", "0", "--no-display-prompt", "--no-context-shift",
         "-no-cnv", "-f", "/dev/stdin"],
        input=prompt, stdout=subprocess.PIPE, text=True, check=True, timeout=600,
    )
    answer = result.stdout.split("[end of text]")[0].strip()
    if len(answer) < 80 or "<|im_start|>" in answer:
        raise ValueError("Model returned no substantive analysis")
    return answer


def nightly(args):
    packet = json.loads(args.packet.read_bytes())
    save(args.output.with_name(f"{args.output.stem}.evidence.json"), packet)
    facts = {
        "closure_manifest_sha256": packet["closure_manifest_sha256"],
        "broker_order_count": packet["broker_order_count"],
        "unresolved": packet["unresolved"],
        "session_windows": session_windows(packet["observations"]),
        "action_counts": packet["action_counts"],
        "linked_gross_paper_pnl_usd": str(sum(
            (Decimal(case["outcome"]["gross_paper_pnl_usd"]) for case in packet["cases"]
             if case["outcome"]["gross_paper_pnl_usd"] is not None), Decimal(0))),
        "trades": [{"seq": case["seq"], "outcome": case["outcome"],
                    "quote_transitions": quote_transitions(case["observations"])}
                   for case in packet["cases"]],
        "trade_metrics": packet.get("trade_metrics"),
        "strategy_assessment": "Pending qualified research review. Fills do not validate a thesis.",
    }
    save(args.output.with_name(f"{args.output.stem}.facts.json"), facts)
    if getattr(args, "facts_only", False):
        return
    common = {key: packet[key] for key in ("day", "columns", "limitations")}
    output = {"closure_manifest_sha256": packet["closure_manifest_sha256"], "trades": []}
    for case in packet["cases"]:
        blind = infer(
            args, "No actual trade thesis or outcome is supplied. Compare two explanations of "
            "the quotes, grounding your claims in explicit citations such as 'seq 1'. "
            "Then choose a research thesis or stand down. News receipt metadata provides "
            "feed coverage only. It does not identify a catalyst.",
            common | {k: v for k, v in case.items() if k != "outcome"}
            | {"quote_transitions": quote_transitions(case["observations"])},
        )
        cited = {int(value) for value in re.findall(r"\bseq(?:uence)?\s*#?\s*(\d+)", blind, re.I)}
        available = {row[0] for row in case["observations"]}
        evidence_quality = "cited_observed_rows" if cited and cited <= available else "review_required"
        save(args.output.with_name(f"{args.output.stem}.seq-{case['seq']}.blind.json"), {
            "closure_manifest_sha256": packet["closure_manifest_sha256"], "analysis": blind,
            "evidence_quality": evidence_quality,
        })
        outcome = infer(args, "Review this execution against the recorded blind assessment. "
                        "Interpret the implications without restating numbers. This observation cannot "
                        "validate a thesis or estimate strategy performance.",
                        {"assessment": blind, "outcome": case["outcome"]})
        record = {"seq": case["seq"], "blind": blind, "outcome": outcome,
                  "evidence_quality": evidence_quality}
        save(args.output.with_name(f"{args.output.stem}.seq-{case['seq']}.outcome.json"), record)
        output["trades"].append(record)
        if evidence_quality == "review_required":
            print(f"REVIEW_REQUIRED: seq {case['seq']} lacks valid ledger citations", flush=True)
    output["session"] = infer(
        args, "Review the entire session via half-hour quote summaries. Propose a falsifiable "
        "alternative versus standing down for a future independent window.",
        {"day": packet["day"], "limitations": packet["limitations"],
         "window_columns": ["UTC seconds", "first seq", "last seq", "usable samples",
                            "first mid", "low mid", "high mid", "last mid", "max spread bps"],
         "half_hour_windows": session_windows(packet["observations"]),
         "decision_status_counts": packet["action_counts"],
         "closing_news": packet["closing_news"],
         "market_close_context": [{**row, "tail": row["tail"][-1:]}
                                  for row in packet["market_close_context"]],
         "broker_bracket_count": len(packet["cases"])},
    )
    save(args.output.with_name(f"{args.output.stem}.session.json"), {"analysis": output["session"]})
    output["shadow_comparison"] = [
        infer(args, "Compare the shadow thesis to the session analysis using the shadow snapshot "
              "for its original judgment. Separate new evidence and say what has changed.",
              {"shadow": record, "session_review": output["session"]})
        for record in packet["shadow"]
    ]
    save(args.output, output)
    if any(trade["evidence_quality"] == "review_required" for trade in output["trades"]):
        raise ValueError("Report preserved, but blind assessment evidence checks failed")


def market_context(path, now):
    live = []
    for name in ("latest.json", "QQQ-latest.json"):
        row = json.loads((path / name).read_bytes())
        item = {key: row.get(key) for key in (
            "symbol", "observed_at_utc", "fresh", "last_source_timestamp_utc",
            "last_received_at_utc",
        )}
        quotes = [quote for quote in row.get("tail", []) if
                  (received := parse_utc(quote.get("received_at_utc"))) and received <= now]
        # A short top-of-book tail is venue context, not complete session history.
        item["tail"] = [[quote.get(key) for key in ("received_at_utc", "bid", "ask")]
                        for quote in quotes[::32][-16:] + quotes[-1:]]
        live.append(item)
    return live


def opening_context(samples, prior, cutoff):
    """Summarize valid opening snapshots and an as-of prior-session IEX record."""
    results = {}
    market_open = cutoff.replace(hour=14, minute=30, second=0, microsecond=0)
    generated = parse_utc(prior.get("generated_at"))
    prior_usable = (generated and generated <= cutoff and prior.get("feed") == "iex"
                    and prior.get("session_date") == cutoff.date().isoformat())
    for symbol in ("SPY", "QQQ"):
        quotes = {}
        for sample in samples:
            sampled_at = parse_utc(sample["observed_utc"])
            if not sample["fresh"] or not sampled_at or not market_open <= sampled_at <= cutoff:
                continue
            for row in sample["quotes"]:
                if row["symbol"] != symbol or not row["last_quote"]:
                    continue
                stamp, bid, ask = row["last_quote"][0]
                received = parse_utc(stamp)
                if (received and market_open <= received <= sampled_at
                        and (sampled_at - received).total_seconds() <= 5
                        and all(isinstance(x, (int, float)) and math.isfinite(x) for x in (bid, ask))
                        and 0 < bid <= ask):
                    mid = (bid + ask) / 2
                    quotes[received] = (mid, (ask - bid) / mid * 10000)
        ordered = sorted(quotes.items())
        anchor = prior.get("symbols", {}).get(symbol, {}) if prior_usable else {}
        close = anchor.get("prior_close")
        close_at = parse_utc(anchor.get("prior_close_time"))
        if not (isinstance(close, (int, float)) and math.isfinite(close) and close > 0
                and close_at and close_at.date().isoformat() == prior.get("previous_session")
                and close_at < market_open):
            close, close_at = None, None
        mids = [value[0] for _, value in ordered]
        spreads = [value[1] for _, value in ordered]
        results[symbol] = {
            "prior_iex_final_minute_close": close,
            "prior_close_bar_utc": close_at.isoformat() if close_at else None,
            "opening_proxy_gap_bps": round((mids[0] / close - 1) * 10000, 3)
            if mids and close is not None else None,
            "first_sample_utc": ordered[0][0].isoformat() if ordered else None,
            "sample_count": len(ordered),
            "sampled_mid_low": min(mids) if mids else None,
            "sampled_mid_high": max(mids) if mids else None,
            "spread_median_bps": round(median(spreads), 3) if spreads else None,
            "spread_max_bps": round(max(spreads), 3) if spreads else None,
        }
    return results


def shadow_thesis(args, evidence):
    return infer(args, "Compare two plausible setups. Give a decision with a thesis or "
                 "stand-down, then state the specific new evidence that would change it. "
                 "Use the supplied elapsed seconds for the opening window; avoid inferring durations "
                 "from clock notation. This is a shadow assessment without authority to trade.", evidence)


def shadow(args):
    opening = []
    for index in range(args.opening_samples):
        now = datetime.now(timezone.utc)
        live = market_context(args.market, now)
        fresh = all(row["tail"] and row["fresh"] and parse_utc(row["observed_at_utc"])
                    and 0 <= (now - parse_utc(row["observed_at_utc"])).total_seconds() <= 15
                    for row in live)
        opening.append({
            "observed_utc": now.isoformat(), "fresh": fresh,
            "quotes": [{"symbol": row["symbol"], "last_quote": row["tail"][-1:]} for row in live],
        })
        if index + 1 < args.opening_samples:
            time.sleep(60)
    prior = {}
    prior_path = args.prior_context or args.market / f"strategy-context-{now.date().isoformat()}.json"
    prior_status = "missing"
    if prior_path.is_file():
        try:
            loaded = json.loads(prior_path.read_bytes())
            if isinstance(loaded, dict):
                prior, prior_status = loaded, "loaded; values subject to as-of checks"
            else:
                prior_status = "malformed"
        except (OSError, ValueError):
            prior_status = "unreadable or malformed"
    opening_span = (now - parse_utc(opening[0]["observed_utc"])).total_seconds()
    evidence = {"observed_utc": now.isoformat(), "opening_samples": opening, "quotes": live,
                "opening_elapsed_seconds": round(opening_span, 1),
                "opening_context": opening_context(opening, prior, now),
                "prior_context_generated_utc": prior.get("generated_at"),
                "prior_context_status": prior_status,
                "news": [news_context(args.news, now, row["symbol"]) for row in live],
                "limitations": "Opening range and gap use sampled IEX midpoints, not bars or auction prints. No macro calendar."}
    if not fresh or (args.opening_samples > 1 and sum(row["fresh"] for row in opening) < 3):
        analysis = "Stand down. Too few fresh opening quote snapshots."
    elif args.facts_only:
        analysis = "Stand down. Opening evidence captured. Strategy assessment pending qualification."
    else:
        analysis = shadow_thesis(args, evidence)
    save(args.output, {"evidence": evidence, "analysis": analysis})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("stage", "nightly", "shadow"))
    parser.add_argument("--day")
    parser.add_argument("--facts-only", action="store_true")
    parser.add_argument("--opening-samples", type=int, choices=range(1, 6), default=1)
    for field in ("closures", "market", "news", "packet", "output", "llama", "model", "shadow-state", "prior-context"):
        parser.add_argument(f"--{field}", type=Path)
    args = parser.parse_args()
    {"stage": stage, "nightly": nightly, "shadow": shadow}[args.mode](args)


if __name__ == "__main__":
    main()
