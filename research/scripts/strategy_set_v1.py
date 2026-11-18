"""Frozen exploratory long-only signals for SPY and QQQ.

See plans/strategy/PLAN.md, first paper strategy rules version 1. This module
proposes brackets only. It never sizes or sends orders.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from desk_broker import MinuteBar
from desk_orb import opening_ranges
from session_bars import _timestamp
from decision_record import verify
import json
from pathlib import Path

ET = ZoneInfo("America/New_York")
SYMBOLS = ("SPY", "QQQ")
REVISIONS = {name: f"{name}-v1" for name in ("orb", "vwap_reversion", "trend_pullback", "gap_continuation")}
CENT = Decimal("0.01")


def _bracket(ask: str, stop_fraction: str, target: Decimal) -> dict[str, str] | None:
    entry = Decimal(ask).quantize(CENT, rounding=ROUND_HALF_UP)
    stop = (entry * (Decimal(1) - Decimal(stop_fraction))).quantize(CENT, rounding=ROUND_HALF_UP)
    target = target.quantize(CENT, rounding=ROUND_HALF_UP)
    if not Decimal(0) < stop < entry < target:
        return None
    return {"side": "long", "entry_limit": f"{entry:.2f}", "stop_price": f"{stop:.2f}",
            "take_profit_limit": f"{target:.2f}"}


def _contiguous(bars: list[Mapping[str, Any]]) -> bool:
    return all(_timestamp(right["time"]) - _timestamp(left["time"]) == timedelta(minutes=1)
               for left, right in zip(bars, bars[1:]))


def _vwap(bars: list[Mapping[str, Any]]) -> float | None:
    if not bars or any(bar.get("source") != "iex" or bar.get("vwap") is None for bar in bars):
        return None
    total = sum(bar["volume"] for bar in bars)
    if total <= 0:
        return None
    return sum(bar["vwap"] * bar["volume"] for bar in bars) / total


def candidates(inputs: Mapping[str, Any]) -> list[dict[str, Any]]:
    """All firing strategy/symbol pairs using the same immutable input snapshot."""
    now = _timestamp(inputs["observed_at"])
    minute = now.replace(second=0, microsecond=0)
    result = []
    for symbol in SYMBOLS:
        item = inputs.get("symbols", {}).get(symbol, {})
        if item.get("available") is not True:
            continue
        bars = item.get("bars") or []
        if len(bars) < 2 or _timestamp(bars[-1]["time"]) != minute - timedelta(minutes=1):
            continue
        current, previous = bars[-1], bars[-2]
        if not _contiguous(bars[-2:]):
            continue
        ask = Decimal(item["quote"]["ask"])
        trigger = _timestamp(current["time"]).astimezone(ET)
        clock_minute = trigger.hour * 60 + trigger.minute
        fired: list[tuple[str, dict[str, str] | None]] = []
        median = item.get("orb_width_median")
        if median is not None and 10 * 60 + 1 <= clock_minute <= 14 * 60:
            typed = [MinuteBar(symbol, _timestamp(bar["time"]), bar["open"], bar["high"],
                               bar["low"], bar["close"], bar["volume"]) for bar in bars]
            opening = opening_ranges({symbol: typed}, symbols=(symbol,)).get(symbol)
            if opening and opening.width_ratio >= median and previous["close"] <= opening.high < current["close"]:
                fired.append(("orb", _bracket(str(ask), "0.005", ask * Decimal("1.01"))))
        if clock_minute >= 10 * 60 and _contiguous(bars):
            first = _timestamp(bars[0]["time"]).astimezone(ET)
            if (first.hour, first.minute) == (9, 30):
                previous_vwap, current_vwap = _vwap(bars[:-1]), _vwap(bars)
                if (previous_vwap is not None and current_vwap is not None
                        and previous["close"] <= previous_vwap * .998
                        and previous["close"] / previous_vwap < current["close"] / current_vwap < 1):
                    fired.append(("vwap_reversion", _bracket(str(ask), "0.0035", Decimal(str(current_vwap)))))
        if len(bars) >= 20 and _contiguous(bars[-20:]):
            mean5 = sum(bar["close"] for bar in bars[-5:]) / 5
            mean20 = sum(bar["close"] for bar in bars[-20:]) / 20
            if mean5 >= mean20 * 1.001 and current["low"] <= mean5 < current["close"]:
                fired.append(("trend_pullback", _bracket(str(ask), "0.0035", ask * Decimal("1.007"))))
        prior_close = item.get("prior_close")
        if (prior_close is not None and len(bars) >= 6 and _contiguous(bars[-6:])
                and clock_minute >= 9 * 60 + 36):
            first = _timestamp(bars[0]["time"]).astimezone(ET)
            if ((first.hour, first.minute) == (9, 30) and bars[0]["open"] >= prior_close * 1.003
                    and current["close"] > max(bar["high"] for bar in bars[-6:-1])):
                fired.append(("gap_continuation", _bracket(str(ask), "0.004", ask * Decimal("1.008"))))
        for name, proposal in fired:
            if proposal is not None:
                result.append({"strategy_id": name, "revision": REVISIONS[name], "symbol": symbol,
                               "trigger_time": current["time"], "proposal": proposal})
    return result


def proposal(inputs: Mapping[str, Any]) -> Mapping[str, str] | None:
    selected = inputs.get("selected_candidate")
    return selected.get("proposal") if isinstance(selected, Mapping) else None


def choose(inputs: Mapping[str, Any], state: Mapping[str, Any], ledger_path: str) -> dict[str, Any]:
    """Choose the least-submitted eligible pair, with fixed ties and no repeated trigger."""
    possible = candidates(inputs)
    path = Path(ledger_path)
    verify(path)
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()] if path.exists() else []
    counts: dict[tuple[str, str], int] = {}
    attempted: set[tuple[str, str, str]] = set()
    for record in records:
        action = record.get("action") or {}
        selected = record.get("selected_candidate") or {}
        status = action.get("status")
        if status not in ("entry_attempt_started", "submitted", "submit_refused"):
            continue
        key = (selected.get("strategy_id"), selected.get("symbol"))
        if not all(isinstance(part, str) for part in key):
            continue
        if status in ("submitted", "submit_refused"):
            counts[key] = counts.get(key, 0) + 1
        if isinstance(selected.get("trigger_time"), str):
            attempted.add((*key, selected["trigger_time"]))
    occupied = {str(item.get("symbol")) for item in state.get("lifecycles", [])
                if isinstance(item, Mapping) and item.get("state") not in ("closed", "released")}
    active = state.get("active_lifecycle")
    if isinstance(active, Mapping) and active.get("state") not in ("closed", "released"):
        occupied.add(str(active.get("symbol") or "SPY"))
    eligible = [item for item in possible if item["symbol"] not in occupied
                and (item["strategy_id"], item["symbol"], item["trigger_time"]) not in attempted]
    order = {name: i for i, name in enumerate(REVISIONS)}
    eligible.sort(key=lambda item: (counts.get((item["strategy_id"], item["symbol"]), 0),
                                    order[item["strategy_id"]], SYMBOLS.index(item["symbol"])))
    return {"candidates": possible, "selected": eligible[0] if eligible else None}
